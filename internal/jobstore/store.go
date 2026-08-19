// Package jobstore persists version-3 jobs and their immutable replay bindings.
package jobstore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/internal/protocol"
	bolt "go.etcd.io/bbolt"
)

const (
	storeFormatVersion = "1"
	// defaultOpenTimeout bounds how long Open waits for the database file lock.
	// bbolt with nil options blocks indefinitely on the flock, which would let a
	// replacement daemon hang silently while a draining predecessor still holds
	// the database.
	defaultOpenTimeout = 500 * time.Millisecond
	artifactPromptTTL  = 14 * 24 * time.Hour
	artifactResultTTL  = 14 * 24 * time.Hour
	artifactLogTTL     = 30 * 24 * time.Hour
)

var (
	// ErrInvalid reports invalid caller input.
	ErrInvalid = errors.New("jobstore: invalid input")
	// ErrNotFound reports an unknown job ID.
	ErrNotFound = errors.New("jobstore: job not found")
	// ErrConflict reports an attempted replay with a different task hash.
	ErrConflict = errors.New("jobstore: request conflict")
	// ErrTerminal reports an attempt to alter a recorded terminal job.
	ErrTerminal = errors.New("jobstore: terminal job is immutable")
	// ErrBusy reports a database held by another daemon beyond Open's timeout.
	ErrBusy = errors.New("jobstore: root busy")
	// ErrCorrupt reports an unsafe or structurally invalid bbolt database.
	ErrCorrupt = errors.New("jobstore: corrupt database")
	// ErrIncompatible reports a bbolt file that is not this store's clean format.
	ErrIncompatible = errors.New("jobstore: incompatible database")

	bucketMeta     = []byte("meta")
	bucketRequests = []byte("requests")
	bucketJobs     = []byte("jobs")
	keyFormat      = []byte("format")
)

// RequestKey is the public compound idempotency identity. Both fields are
// opaque tokens; callers, not this store, establish any workspace identity.
type RequestKey struct {
	WorkspaceKey string
	RequestID    string
}

// Validate checks identity syntax only. It intentionally does not inspect a
// cwd, backend, schema, or any other part of the task.
func (key RequestKey) Validate() error {
	if err := validateOpaqueIdentityPart("workspaceKey", key.WorkspaceKey); err != nil {
		return err
	}
	return validateOpaqueIdentityPart("requestId", key.RequestID)
}

func validateOpaqueIdentityPart(field, value string) error {
	if value == "" || len(value) > 256 || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s must be non-empty valid UTF-8 of at most 256 bytes", ErrInvalid, field)
	}
	for _, r := range value {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("%w: %s contains whitespace or a control character", ErrInvalid, field)
		}
	}
	return nil
}

func (key RequestKey) storageKey() []byte {
	// Identity validation rejects control characters, including NUL, so the
	// separator cannot make two compound identities collide.
	return []byte(key.WorkspaceKey + "\x00" + key.RequestID)
}

// RequestBinding is the immutable value stored in the requests bucket.
type RequestBinding struct {
	JobID       string   `json:"jobId"`
	RequestHash [32]byte `json:"requestHash"`
}

// ArtifactPaths names the only sidecar data that artifact retention may remove.
// Job records and request bindings are never retention candidates.
type ArtifactPaths struct {
	Prompt string `json:"prompt,omitempty"`
	Log    string `json:"log,omitempty"`
	Result string `json:"result,omitempty"`
}

// Record is the durable job record. State, cleanup, failure class, and contract
// deliberately use the protocol's v3 types rather than parallel store types.
// Starting is the private persisted no-relaunch marker; its public projection
// is always State=running.
type Record struct {
	JobID         string                  `json:"jobId"`
	WorkspaceKey  string                  `json:"workspaceKey"`
	RequestID     string                  `json:"requestId"`
	Backend       string                  `json:"backend"`
	Model         string                  `json:"model,omitempty"`
	CWD           string                  `json:"cwd,omitempty"`
	Write         bool                    `json:"write"`
	Effort        string                  `json:"effort,omitempty"`
	TaskSpec      json.RawMessage         `json:"taskSpec,omitempty"`
	State         protocol.PublicState    `json:"state"`
	Starting      bool                    `json:"starting,omitempty"`
	Cleanup       protocol.Cleanup        `json:"cleanup"`
	CreatedAt     time.Time               `json:"createdAt"`
	UpdatedAt     time.Time               `json:"updatedAt"`
	StartedAt     *time.Time              `json:"startedAt,omitempty"`
	FinishedAt    *time.Time              `json:"finishedAt,omitempty"`
	FailureClass  protocol.FailureClass   `json:"failureClass,omitempty"`
	FailureReason string                  `json:"failureReason,omitempty"`
	Diagnostics   []string                `json:"diagnostics"`
	Contract      protocol.ContractResult `json:"contract"`
	ResultText    string                  `json:"resultText,omitempty"`
	Artifacts     ArtifactPaths           `json:"artifacts"`
}

// TerminalUpdate supplies the durable data for a first terminal transition.
// State must be completed, failed, canceled, or unknown.
type TerminalUpdate struct {
	State         protocol.PublicState
	Cleanup       protocol.Cleanup
	FailureClass  protocol.FailureClass
	FailureReason string
	Diagnostics   []string
	Contract      protocol.ContractResult
	ResultText    string
	FinishedAt    time.Time
}

// ConflictError identifies the existing binding that disagrees with a replay.
type ConflictError struct {
	Key           RequestKey
	ExistingJobID string
}

func (err *ConflictError) Error() string {
	return fmt.Sprintf("%s: (%s, %s) is already bound to %s", ErrConflict, err.Key.WorkspaceKey, err.Key.RequestID, err.ExistingJobID)
}

func (*ConflictError) Is(target error) bool { return target == ErrConflict }

// BusyError reports that the root remains exclusively locked after the bounded
// open timeout.
type BusyError struct {
	Path    string
	Timeout time.Duration
	Cause   error
}

func (err *BusyError) Error() string {
	return fmt.Sprintf("%s after %s: %s", ErrBusy, err.Timeout, err.Path)
}

func (err *BusyError) Unwrap() error { return err.Cause }

func (err *BusyError) Is(target error) bool { return target == ErrBusy }

// CorruptError identifies a database rejected before it can serve jobs.
type CorruptError struct {
	Path  string
	Cause error
}

func (err *CorruptError) Error() string {
	if err.Cause == nil {
		return fmt.Sprintf("%s: %s", ErrCorrupt, err.Path)
	}
	return fmt.Sprintf("%s: %s: %v", ErrCorrupt, err.Path, err.Cause)
}

func (err *CorruptError) Unwrap() error { return err.Cause }

func (*CorruptError) Is(target error) bool { return target == ErrCorrupt }

type artifactLayout struct {
	prompts string
	logs    string
	results string
}

// Store owns a single bbolt database and its sidecar artifact directories.
type Store struct {
	db        *bolt.DB
	path      string
	artifacts artifactLayout
}

// Open opens or creates one version-3 jobstore database at path. Existing
// databases receive a fault-isolated preflight before bbolt can mmap them.
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: database path is required", ErrInvalid)
	}
	path = filepath.Clean(path)

	fresh := false
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%w: database is not a regular file: %s", ErrInvalid, path)
		}
		if info.Size() == 0 {
			return nil, newCorruptError(path, fmt.Errorf("%w: bbolt database is zero-length", ErrCorrupt))
		}
		if err := preflightBoltPageHeaders(path); err != nil {
			return nil, typedOpenError(path, err)
		}
		if err := preflightBoltFreelist(path); err != nil {
			return nil, typedOpenError(path, err)
		}
	case errors.Is(err, os.ErrNotExist):
		fresh = true
	default:
		return nil, err
	}

	db, err := openBoltSafely(path, 0o600, &bolt.Options{Timeout: defaultOpenTimeout})
	if err != nil {
		return nil, typedOpenError(path, err)
	}

	store := &Store{
		db:   db,
		path: path,
		artifacts: artifactLayout{
			prompts: filepath.Join(filepath.Dir(path), "artifacts", "prompts"),
			logs:    filepath.Join(filepath.Dir(path), "artifacts", "logs"),
			results: filepath.Join(filepath.Dir(path), "artifacts", "results"),
		},
	}
	if fresh {
		err = store.initialize()
	} else {
		err = store.verifyInitialized()
	}
	if err == nil {
		err = store.verifyIntegrity()
	}
	if err != nil {
		_ = db.Close()
		return nil, typedOpenError(path, err)
	}
	return store, nil
}

// Close releases the bbolt file lock.
func (store *Store) Close() error {
	if store == nil || store.db == nil {
		return nil
	}
	return store.db.Close()
}

func (store *Store) initialize() error {
	return store.update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucket(bucketMeta)
		if err != nil {
			return err
		}
		if _, err := tx.CreateBucket(bucketRequests); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(bucketJobs); err != nil {
			return err
		}
		return meta.Put(keyFormat, []byte(storeFormatVersion))
	})
}

func (store *Store) verifyInitialized() error {
	return store.view(func(tx *bolt.Tx) error {
		found := make(map[string]bool, 3)
		if err := tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			if bucket == nil {
				return fmt.Errorf("%w: top-level key %q is not a bucket", ErrIncompatible, name)
			}
			switch string(name) {
			case string(bucketMeta), string(bucketRequests), string(bucketJobs):
				found[string(name)] = true
				return nil
			default:
				return fmt.Errorf("%w: unexpected top-level bucket %q", ErrIncompatible, name)
			}
		}); err != nil {
			return err
		}
		if len(found) != 3 {
			return fmt.Errorf("%w: expected exactly meta, requests, and jobs buckets", ErrIncompatible)
		}
		meta := tx.Bucket(bucketMeta)
		if meta == nil || string(meta.Get(keyFormat)) != storeFormatVersion {
			return fmt.Errorf("%w: unsupported or missing store format", ErrIncompatible)
		}
		return nil
	})
}

func (store *Store) verifyIntegrity() error {
	return store.view(func(tx *bolt.Tx) error {
		for err := range tx.Check() {
			if err != nil {
				return fmt.Errorf("%w: bbolt integrity check: %v", ErrCorrupt, err)
			}
		}
		return nil
	})
}

// SubmitTx binds a new record or returns a matching replay in one bbolt.Update.
// taskSpec must be the canonical bytes of the exact task specification. The
// method copies and hashes them before it opens a transaction or invokes mk, so
// no caller-side cwd or backend validation can precede the identity check.
// For a new identity, mk may return a validation error; SubmitTx returns it
// before either the job or its request binding is persisted.
func (store *Store) SubmitTx(key RequestKey, taskSpec []byte, mk func(id string) (Record, error)) (Record, bool, error) {
	if mk == nil {
		return Record{}, false, fmt.Errorf("%w: record factory is required", ErrInvalid)
	}
	if err := key.Validate(); err != nil {
		return Record{}, false, err
	}
	canonicalTaskSpec := append([]byte(nil), taskSpec...)
	hash := sha256.Sum256(canonicalTaskSpec)

	var result Record
	deduplicated := false
	err := store.update(func(tx *bolt.Tx) error {
		// The canonical task was copied and hashed before this transaction; no
		// caller-owned record code has run yet.
		requests, jobs, err := requiredBuckets(tx)
		if err != nil {
			return err
		}
		requestKey := key.storageKey()
		if encoded := requests.Get(requestKey); encoded != nil {
			binding, err := decodeBinding(encoded)
			if err != nil {
				return err
			}
			// A matching hash returns the original record without touching mk.
			if binding.RequestHash == hash {
				record, err := getRecord(jobs, binding.JobID)
				if err != nil {
					return err
				}
				if record.WorkspaceKey != key.WorkspaceKey || record.RequestID != key.RequestID {
					return fmt.Errorf("%w: binding %q disagrees with job %q identity", ErrCorrupt, requestKey, binding.JobID)
				}
				result = record
				deduplicated = true
				return nil
			}
			// A different hash is a typed conflict and writes nothing.
			return &ConflictError{Key: key, ExistingJobID: binding.JobID}
		}

		// A new key is the only path that calls the new-record factory. Caller
		// backend and cwd validation belongs outside this storage unit.
		id, err := newJobID(jobs)
		if err != nil {
			return err
		}
		record, err := mk(id)
		if err != nil {
			return err
		}
		if record.JobID == "" {
			record.JobID = id
		}
		if record.JobID != id {
			return fmt.Errorf("%w: record factory changed generated job ID", ErrInvalid)
		}
		if record.WorkspaceKey == "" {
			record.WorkspaceKey = key.WorkspaceKey
		}
		if record.RequestID == "" {
			record.RequestID = key.RequestID
		}
		if record.WorkspaceKey != key.WorkspaceKey || record.RequestID != key.RequestID {
			return fmt.Errorf("%w: record factory changed request identity", ErrInvalid)
		}
		// The stored bytes are the exact copied bytes that were hashed above, so
		// a factory cannot make the durable task disagree with its binding.
		record.TaskSpec = canonicalTaskSpec
		record.Artifacts = store.artifactPathsForID(id)
		normalizeNewRecord(&record, time.Now().UTC())
		if err := validateRecord(record); err != nil {
			return err
		}
		// Put the job first, then the binding, in this one transaction. An
		// error after the first Put rolls both writes back.
		if err := putRecord(jobs, record); err != nil {
			return err
		}
		binding, err := json.Marshal(RequestBinding{JobID: id, RequestHash: hash})
		if err != nil {
			return err
		}
		if err := requests.Put(requestKey, binding); err != nil {
			return err
		}
		result = record
		return nil
	})
	if err != nil {
		return Record{}, false, err
	}
	return result, deduplicated, nil
}

// Get returns one durable record by opaque job ID.
func (store *Store) Get(id string) (Record, error) {
	if err := validateJobID(id); err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	var result Record
	err := store.view(func(tx *bolt.Tx) error {
		jobs := tx.Bucket(bucketJobs)
		if jobs == nil {
			return fmt.Errorf("%w: missing jobs bucket", ErrCorrupt)
		}
		record, err := getRecord(jobs, id)
		if err != nil {
			return err
		}
		result = record
		return nil
	})
	return result, err
}

// List returns all records ordered by job ID.
func (store *Store) List() ([]Record, error) {
	return store.listWhere(func(Record) bool { return true })
}

// ListQueued returns recovered jobs that were never started.
func (store *Store) ListQueued() ([]Record, error) {
	return store.listWhere(func(record Record) bool {
		return record.State == protocol.PublicStateQueued
	})
}

// ListStartingOrRunning returns jobs that must not be relaunched after restart.
func (store *Store) ListStartingOrRunning() ([]Record, error) {
	return store.listWhere(func(record Record) bool {
		return record.Starting || record.State == protocol.PublicStateRunning
	})
}

func (store *Store) listWhere(include func(Record) bool) ([]Record, error) {
	var records []Record
	err := store.view(func(tx *bolt.Tx) error {
		jobs := tx.Bucket(bucketJobs)
		if jobs == nil {
			return fmt.Errorf("%w: missing jobs bucket", ErrCorrupt)
		}
		return jobs.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("%w: nested jobs bucket %q", ErrCorrupt, key)
			}
			record, err := decodeRecord(value, string(key))
			if err != nil {
				return err
			}
			if include(record) {
				records = append(records, record)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// MarkTerminal records the first terminal state. Later updates are rejected so
// a known result can never be overwritten by a late finalization or recovery.
func (store *Store) MarkTerminal(id string, terminal TerminalUpdate) (Record, error) {
	if !terminal.State.IsTerminal() {
		return Record{}, fmt.Errorf("%w: terminal state is required", ErrInvalid)
	}
	if err := validateJobID(id); err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}

	var result Record
	err := store.update(func(tx *bolt.Tx) error {
		jobs := tx.Bucket(bucketJobs)
		if jobs == nil {
			return fmt.Errorf("%w: missing jobs bucket", ErrCorrupt)
		}
		current, err := getRecord(jobs, id)
		if err != nil {
			return err
		}
		if current.State.IsTerminal() {
			return ErrTerminal
		}

		next := current
		next.State = terminal.State
		next.Starting = false
		next.Cleanup = terminal.Cleanup
		next.FailureClass = terminal.FailureClass
		next.FailureReason = terminal.FailureReason
		next.Diagnostics = append([]string(nil), terminal.Diagnostics...)
		next.Contract = terminal.Contract
		next.ResultText = terminal.ResultText
		if terminal.FinishedAt.IsZero() {
			now := time.Now().UTC()
			next.FinishedAt = &now
		} else {
			finished := terminal.FinishedAt.UTC()
			next.FinishedAt = &finished
		}

		next.UpdatedAt = time.Now().UTC()
		normalizeRecord(&next)
		if err := validateRecord(next); err != nil {
			return err
		}
		if err := putRecord(jobs, next); err != nil {
			return err
		}
		result = next
		return nil
	})
	return result, err
}

// SweepArtifacts removes only expired prompt, log, and result sidecar files.
// It intentionally does not open a transaction and cannot delete records,
// bindings, identity hashes, or ContractResult values.
func (store *Store) SweepArtifacts(now time.Time) error {
	if now.IsZero() {
		return fmt.Errorf("%w: sweep time is required", ErrInvalid)
	}
	now = now.UTC()
	for _, artifact := range []struct {
		dir string
		ttl time.Duration
	}{
		{dir: store.artifacts.prompts, ttl: artifactPromptTTL},
		{dir: store.artifacts.logs, ttl: artifactLogTTL},
		{dir: store.artifacts.results, ttl: artifactResultTTL},
	} {
		entries, err := os.ReadDir(artifact.dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if now.Sub(info.ModTime()) < artifact.ttl {
				continue
			}
			path := filepath.Join(artifact.dir, entry.Name())
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

func (store *Store) artifactPathsForID(id string) ArtifactPaths {
	return ArtifactPaths{
		Prompt: filepath.Join(store.artifacts.prompts, id+".json"),
		Log:    filepath.Join(store.artifacts.logs, id+".log"),
		Result: filepath.Join(store.artifacts.results, id+".txt"),
	}
}

func requiredBuckets(tx *bolt.Tx) (requests, jobs *bolt.Bucket, err error) {
	requests = tx.Bucket(bucketRequests)
	jobs = tx.Bucket(bucketJobs)
	if requests == nil || jobs == nil {
		return nil, nil, fmt.Errorf("%w: missing requests or jobs bucket", ErrCorrupt)
	}
	return requests, jobs, nil
}

func getRecord(jobs *bolt.Bucket, id string) (Record, error) {
	encoded := jobs.Get([]byte(id))
	if encoded == nil {
		return Record{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return decodeRecord(encoded, id)
}

func putRecord(jobs *bolt.Bucket, record Record) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return jobs.Put([]byte(record.JobID), encoded)
}

func decodeBinding(encoded []byte) (RequestBinding, error) {
	var binding RequestBinding
	if err := json.Unmarshal(encoded, &binding); err != nil {
		return RequestBinding{}, fmt.Errorf("%w: invalid request binding: %v", ErrCorrupt, err)
	}
	if err := validateJobID(binding.JobID); err != nil {
		return RequestBinding{}, fmt.Errorf("%w: invalid binding job ID: %v", ErrCorrupt, err)
	}
	return binding, nil
}

func decodeRecord(encoded []byte, expectedID string) (Record, error) {
	var record Record
	if err := json.Unmarshal(encoded, &record); err != nil {
		return Record{}, fmt.Errorf("%w: invalid job record %q: %v", ErrCorrupt, expectedID, err)
	}
	if record.JobID != expectedID {
		return Record{}, fmt.Errorf("%w: job key %q does not match job ID %q", ErrCorrupt, expectedID, record.JobID)
	}
	normalizeRecord(&record)
	if err := validateRecord(record); err != nil {
		return Record{}, fmt.Errorf("%w: invalid job record %q: %v", ErrCorrupt, expectedID, err)
	}
	return record, nil
}

func normalizeNewRecord(record *Record, now time.Time) {
	record.State = protocol.PublicStateQueued
	if record.Cleanup == "" {
		record.Cleanup = protocol.CleanupClean
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}
	normalizeRecord(record)
}

func normalizeRecord(record *Record) {
	record.CreatedAt = record.CreatedAt.UTC()
	record.UpdatedAt = record.UpdatedAt.UTC()
	if record.StartedAt != nil {
		value := record.StartedAt.UTC()
		record.StartedAt = &value
	}
	if record.FinishedAt != nil {
		value := record.FinishedAt.UTC()
		record.FinishedAt = &value
	}
	if record.Diagnostics == nil {
		record.Diagnostics = []string{}
	}
	if record.Contract.Violations == nil {
		record.Contract.Violations = []string{}
	}
}

func validateRecord(record Record) error {
	if err := validateJobID(record.JobID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := (RequestKey{WorkspaceKey: record.WorkspaceKey, RequestID: record.RequestID}).Validate(); err != nil {
		return err
	}
	if !validPublicState(record.State) {
		return fmt.Errorf("%w: invalid public state %q", ErrInvalid, record.State)
	}
	if record.Starting && record.State != protocol.PublicStateRunning {
		return fmt.Errorf("%w: starting marker requires running public state", ErrInvalid)
	}
	if !record.Cleanup.Valid() {
		return fmt.Errorf("%w: invalid cleanup %q", ErrInvalid, record.Cleanup)
	}
	if record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: createdAt and updatedAt are required", ErrInvalid)
	}
	if record.State.IsTerminal() && record.FinishedAt == nil {
		return fmt.Errorf("%w: terminal record requires finishedAt", ErrInvalid)
	}
	if record.State == protocol.PublicStateFailed {
		if !record.FailureClass.Valid() {
			return fmt.Errorf("%w: failed record requires a valid failure class", ErrInvalid)
		}
	} else if record.FailureClass != "" || record.FailureReason != "" {
		return fmt.Errorf("%w: failure fields require failed state", ErrInvalid)
	}
	if record.ResultText != "" && record.State != protocol.PublicStateCompleted {
		return fmt.Errorf("%w: result text requires completed state", ErrInvalid)
	}
	return nil
}

func validPublicState(state protocol.PublicState) bool {
	switch state {
	case protocol.PublicStateQueued,
		protocol.PublicStateRunning,
		protocol.PublicStateCompleted,
		protocol.PublicStateFailed,
		protocol.PublicStateCanceled,
		protocol.PublicStateUnknown:
		return true
	default:
		return false
	}
}

func newJobID(jobs *bolt.Bucket) (string, error) {
	for range 16 {
		bytes := make([]byte, 16)
		if _, err := rand.Read(bytes); err != nil {
			return "", err
		}
		id := "job_" + hex.EncodeToString(bytes)
		if jobs.Get([]byte(id)) == nil {
			return id, nil
		}
	}
	return "", fmt.Errorf("jobstore: could not allocate an opaque job ID")
}

func (store *Store) view(fn func(*bolt.Tx) error) (err error) {
	if store == nil || store.db == nil {
		return fmt.Errorf("%w: store is not open", ErrInvalid)
	}
	previous := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(previous)
	defer func() {
		if recovered := recover(); recovered != nil {
			if boltPanicIsCorruption(recovered) {
				err = newCorruptError(store.path, fmt.Errorf("bbolt read fault: %v", recovered))
				return
			}
			panic(recovered)
		}
		err = typedOperationError(store.path, err)
	}()
	return store.db.View(fn)
}

func (store *Store) update(fn func(*bolt.Tx) error) (err error) {
	if store == nil || store.db == nil {
		return fmt.Errorf("%w: store is not open", ErrInvalid)
	}
	previous := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(previous)
	defer func() {
		if recovered := recover(); recovered != nil {
			if boltPanicIsCorruption(recovered) {
				err = newCorruptError(store.path, fmt.Errorf("bbolt write fault: %v", recovered))
				return
			}
			panic(recovered)
		}
		err = typedOperationError(store.path, err)
	}()
	return store.db.Update(fn)
}

func typedOpenError(path string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, bolt.ErrTimeout) {
		return &BusyError{Path: path, Timeout: defaultOpenTimeout, Cause: err}
	}
	return typedOperationError(path, err)
}

func typedOperationError(path string, err error) error {
	if err == nil ||
		errors.Is(err, ErrInvalid) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrConflict) ||
		errors.Is(err, ErrTerminal) ||
		errors.Is(err, ErrBusy) ||
		errors.Is(err, ErrIncompatible) {
		return err
	}
	if errors.Is(err, ErrCorrupt) || boltErrorIsCorruption(err) {
		return newCorruptError(path, err)
	}
	if errors.Is(err, bolt.ErrTimeout) {
		return &BusyError{Path: path, Timeout: defaultOpenTimeout, Cause: err}
	}
	return err
}

func newCorruptError(path string, cause error) error {
	var existing *CorruptError
	if errors.As(cause, &existing) {
		return cause
	}
	return &CorruptError{Path: path, Cause: cause}
}

func validateJobID(jobID string) error {
	if (!strings.HasPrefix(jobID, "job_") && !strings.HasPrefix(jobID, "job-")) || len(jobID) <= len("job_") || len(jobID) > 128 {
		return fmt.Errorf("invalid job id %q", jobID)
	}
	for _, r := range jobID {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return fmt.Errorf("invalid job id %q", jobID)
	}
	return nil
}
