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
	// ErrStarting reports an attempt to start a job that already crossed the
	// durable no-relaunch boundary.
	ErrStarting = errors.New("jobstore: job is already starting")
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

// ArtifactPaths names the durable log and result sidecars for a job.
type ArtifactPaths struct {
	Log    string `json:"log,omitempty"`
	Result string `json:"result,omitempty"`
}

// ProcessClaim identifies the process group created for a job. StartToken is
// used with PID and PGID to distinguish a recycled PID from the launched
// process group during recovery.
type ProcessClaim struct {
	PID        int    `json:"pid"`
	PGID       int    `json:"pgid"`
	StartToken string `json:"startToken"`
}

// Record is the durable job record. State, cleanup, failure class, and contract
// deliberately use the protocol's v3 types rather than parallel store types.
// Starting is the private persisted no-relaunch marker; its public projection
// is always State=running.
type Record struct {
	JobID        string `json:"jobId"`
	WorkspaceKey string `json:"workspaceKey"`
	RequestID    string `json:"requestId"`
	Backend      string `json:"backend"`
	// BackendSessionID is assigned only from a backend's turn-final
	// observation. An omitted value means the job never produced a resumable
	// backend session; it is not an unknown-session marker.
	BackendSessionID string                  `json:"backendSessionId,omitempty"`
	Model            string                  `json:"model,omitempty"`
	CWD              string                  `json:"cwd,omitempty"`
	Write            bool                    `json:"write"`
	Effort           string                  `json:"effort,omitempty"`
	TaskSpec         json.RawMessage         `json:"taskSpec,omitempty"`
	State            protocol.PublicState    `json:"state"`
	Starting         bool                    `json:"starting,omitempty"`
	ProcessClaim     *ProcessClaim           `json:"processClaim,omitempty"`
	Cleanup          protocol.Cleanup        `json:"cleanup"`
	CreatedAt        time.Time               `json:"createdAt"`
	UpdatedAt        time.Time               `json:"updatedAt"`
	StartedAt        *time.Time              `json:"startedAt,omitempty"`
	FinishedAt       *time.Time              `json:"finishedAt,omitempty"`
	FailureClass     protocol.FailureClass   `json:"failureClass,omitempty"`
	FailureReason    string                  `json:"failureReason,omitempty"`
	Diagnostics      []string                `json:"diagnostics"`
	Contract         protocol.ContractResult `json:"contract"`
	ResultText       string                  `json:"resultText,omitempty"`
	ResultPath       string                  `json:"resultPath,omitempty"`
	ResultSHA256     string                  `json:"resultSHA256,omitempty"`
	ResultBytes      int64                   `json:"resultBytes,omitempty"`
	Artifacts        ArtifactPaths           `json:"artifacts"`
}

// TerminalUpdate supplies the durable data for a first terminal transition.
// State must be completed, failed, canceled, or unknown. Completed records
// always clear BackendSessionID; for other terminal states, an empty ID retains
// one already recorded by a retired turn.
type TerminalUpdate struct {
	State            protocol.PublicState
	Cleanup          protocol.Cleanup
	BackendSessionID string
	FailureClass     protocol.FailureClass
	FailureReason    string
	Diagnostics      []string
	Contract         protocol.ContractResult
	ResultText       string
	ResultPath       string
	ResultSHA256     string
	ResultBytes      int64
	FinishedAt       time.Time
}

// RecordLookup reads a record from the same submit transaction that is
// deciding a request binding. It lets a new-record factory validate an
// existing dependency without breaking the same-hash replay ordering.
type RecordLookup func(id string) (Record, error)

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
	return store.submitTx(key, taskSpec, func(id string, _ RecordLookup) (Record, error) {
		return mk(id)
	})
}

// SubmitTxWithLookup is SubmitTx for a new record that must validate another
// durable record. lookup reads through the active jobs bucket only after a
// matching replay has returned or a conflicting replay has failed, preserving
// SubmitTx's identity-before-validation rule.
func (store *Store) SubmitTxWithLookup(key RequestKey, taskSpec []byte, mk func(id string, lookup RecordLookup) (Record, error)) (Record, bool, error) {
	if mk == nil {
		return Record{}, false, fmt.Errorf("%w: record factory is required", ErrInvalid)
	}
	return store.submitTx(key, taskSpec, mk)
}

func (store *Store) submitTx(key RequestKey, taskSpec []byte, mk func(id string, lookup RecordLookup) (Record, error)) (Record, bool, error) {
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
		record, err := mk(id, func(id string) (Record, error) {
			return getRecord(jobs, id)
		})
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
			records = append(records, record)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

// MarkStarting commits the private no-relaunch marker before a caller spawns
// a backend process. Only queued jobs may make this transition.
func (store *Store) MarkStarting(id string) (Record, error) {
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
		if current.Starting {
			return ErrStarting
		}
		if current.State != protocol.PublicStateQueued {
			return fmt.Errorf("%w: queued state is required to start job", ErrInvalid)
		}

		next := current
		now := time.Now().UTC()
		next.State = protocol.PublicStateRunning
		next.Starting = true
		next.StartedAt = &now
		next.UpdatedAt = now
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

// RecordProcessClaim records the process identity in its own transaction after
// a backend has been spawned. It may transition a private starting record to
// ordinary running, but never changes immutable request fields or bindings.
func (store *Store) RecordProcessClaim(id string, claim ProcessClaim) (Record, error) {
	if err := validateJobID(id); err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := claim.validate(); err != nil {
		return Record{}, err
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
		if !current.Starting && current.State != protocol.PublicStateRunning {
			return fmt.Errorf("%w: process claim requires starting or running job", ErrInvalid)
		}

		next := current
		next.State = protocol.PublicStateRunning
		next.Starting = false
		next.ProcessClaim = &ProcessClaim{
			PID:        claim.PID,
			PGID:       claim.PGID,
			StartToken: claim.StartToken,
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

// RecordBackendSessionID records a non-empty backend session ID after a turn
// retires. It is deliberately a separate transaction from the process claim:
// a daemon can stop after a retired turn but before the job reaches terminal
// state, and the session must still be available for restart recovery.
func (store *Store) RecordBackendSessionID(id, sessionID string) (Record, error) {
	if err := validateJobID(id); err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if strings.TrimSpace(sessionID) == "" {
		return Record{}, fmt.Errorf("%w: backend session id is required", ErrInvalid)
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
		next.BackendSessionID = sessionID
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
		if terminal.State == protocol.PublicStateCompleted {
			next.BackendSessionID = ""
		} else if terminal.BackendSessionID != "" {
			next.BackendSessionID = terminal.BackendSessionID
		}
		next.FailureClass = terminal.FailureClass
		next.FailureReason = terminal.FailureReason
		next.Diagnostics = append([]string(nil), terminal.Diagnostics...)
		next.Contract = terminal.Contract
		next.ResultText = terminal.ResultText
		next.ResultPath = terminal.ResultPath
		next.ResultSHA256 = terminal.ResultSHA256
		next.ResultBytes = terminal.ResultBytes
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
func (store *Store) artifactPathsForID(id string) ArtifactPaths {
	return ArtifactPaths{
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
	record.BackendSessionID = ""
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
	if record.ProcessClaim != nil {
		claim := *record.ProcessClaim
		record.ProcessClaim = &claim
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
	if record.ProcessClaim != nil {
		if err := record.ProcessClaim.validate(); err != nil {
			return err
		}
	}
	if record.BackendSessionID != "" && strings.TrimSpace(record.BackendSessionID) == "" {
		return fmt.Errorf("%w: backend session id cannot be whitespace", ErrInvalid)
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

func (claim ProcessClaim) validate() error {
	if claim.PID <= 0 {
		return fmt.Errorf("%w: process PID must be positive", ErrInvalid)
	}
	if claim.PGID <= 0 {
		return fmt.Errorf("%w: process PGID must be positive", ErrInvalid)
	}
	return validateOpaqueIdentityPart("process start token", claim.StartToken)
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
		errors.Is(err, ErrStarting) ||
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
