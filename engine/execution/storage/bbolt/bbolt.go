// Package bbolt implements the root bbolt-backed admission repository.
package bbolt

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bolt "go.etcd.io/bbolt"
)

const envelopeSchemaVersion uint16 = 1

var (
	bucketMeta        = []byte("meta")
	bucketBindings    = []byte("bindings")
	bucketSafety      = []byte("safety")
	bucketProjections = []byte("projections")
	bucketTombstones  = []byte("tombstones")
	bucketQuarantine  = []byte("quarantine")

	keyDBUUID = []byte("db_uuid")
	keyMeta   = []byte("authority")

	bucketNames = [][]byte{
		bucketMeta,
		bucketBindings,
		bucketSafety,
		bucketProjections,
		bucketTombstones,
		bucketQuarantine,
	}
)

type recordKind string

const (
	kindDBUUID     recordKind = "db_uuid"
	kindMeta       recordKind = "meta"
	kindBinding    recordKind = "binding"
	kindSafety     recordKind = "safety"
	kindProjection recordKind = "projection"
	kindTombstone  recordKind = "tombstone"
	kindQuarantine recordKind = "quarantine"
)

// Repository is a single-file bbolt implementation of repository.Repository.
type Repository struct {
	db                           *bolt.DB
	testMu                       sync.Mutex
	failCommitAfterCommitForTest error
}

// Open opens or initializes a root bbolt repository database at path. The file
// is created with owner-only permissions.
func Open(path string, options *bolt.Options) (*Repository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: bbolt path is required", repository.ErrInvalidRecord)
	}
	db, err := bolt.Open(path, 0o600, options)
	if err != nil {
		return nil, err
	}
	repo := &Repository{db: db}
	if err := repo.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return repo, nil
}

// defaultOpenTimeout bounds how long NewRepository waits for the database
// file lock. bbolt with nil options blocks INDEFINITELY on the flock, which
// would let a replacement daemon hang silently in admission bootstrap while a
// draining predecessor still holds the database. Fail closed with a typed
// timeout error instead; the normal uncontended open is unaffected.
const defaultOpenTimeout = 10 * time.Second

// NewRepository opens or initializes a root bbolt repository database at path.
func NewRepository(path string) (*Repository, error) {
	return Open(path, &bolt.Options{Timeout: defaultOpenTimeout})
}

// OpenReadOnly opens an existing root bbolt repository database for inspection.
// It never initializes or mutates the file.
func OpenReadOnly(path string) (*Repository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: bbolt path is required", repository.ErrInvalidRecord)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: defaultOpenTimeout})
	if err != nil {
		return nil, err
	}
	return &Repository{db: db}, nil
}

func (r *Repository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *Repository) FailCommitAfterCallbackForTest(err error) {
	r.FailCommitAfterCommitForTest(err)
}

func (r *Repository) FailCommitAfterCommitForTest(err error) {
	if err == nil {
		err = fmt.Errorf("injected bbolt commit-phase failure")
	}
	r.testMu.Lock()
	defer r.testMu.Unlock()
	r.failCommitAfterCommitForTest = err
}

func (r *Repository) View(ctx context.Context, fn func(repository.ReadTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := loadState(tx)
		if err != nil {
			return err
		}
		return fn(readTx{state: &state})
	})
}

func (r *Repository) Update(ctx context.Context, fn func(repository.WriteTx) error) (commit repository.Commit, err error) {
	if err := ctx.Err(); err != nil {
		return repository.Commit{}, fmt.Errorf("%w: %w", repository.ErrDefinitelyNotCommitted, err)
	}
	tx, err := r.db.Begin(true)
	if err != nil {
		return repository.Commit{}, fmt.Errorf("%w: %w", repository.ErrDefinitelyNotCommitted, err)
	}
	txClosed := false
	defer func() {
		if !txClosed {
			_ = tx.Rollback()
		}
	}()

	txErr := func() (txErr error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				txErr = fmt.Errorf("%w: %v", repository.ErrTransactionPanic, recovered)
			}
		}()

		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := loadState(tx)
		if err != nil {
			return err
		}
		commit = repository.Commit{Generation: state.generation}
		next := state.clone()
		write := &writeTx{readTx: readTx{state: &next}}

		if err := fn(write); err != nil {
			return err
		}
		if write.changed {
			if err := next.validateForCommit(); err != nil {
				return err
			}
			next.advanceGeneration()
			if err := persistState(tx, next); err != nil {
				return err
			}
			commit = repository.Commit{Generation: next.generation}
		}
		return nil
	}()
	if txErr != nil {
		return commit, fmt.Errorf("%w: %w", repository.ErrDefinitelyNotCommitted, txErr)
	}
	if err := tx.Commit(); err != nil {
		return commit, fmt.Errorf("%w: %w", repository.ErrAmbiguousCommit, err)
	}
	txClosed = true
	if err := r.consumeCommitAfterCommitFaultForTest(); err != nil {
		return commit, fmt.Errorf("%w: %w", repository.ErrAmbiguousCommit, err)
	}
	return commit, nil
}

func (r *Repository) consumeCommitAfterCommitFaultForTest() error {
	r.testMu.Lock()
	defer r.testMu.Unlock()
	err := r.failCommitAfterCommitForTest
	r.failCommitAfterCommitForTest = nil
	return err
}

func (r *Repository) SnapshotBytes() []byte {
	var out []byte
	if err := r.db.View(func(tx *bolt.Tx) error {
		state, err := loadState(tx)
		if err != nil {
			return err
		}
		data, err := json.Marshal(snapshotState(state))
		if err != nil {
			return err
		}
		out = data
		return nil
	}); err != nil {
		panic(err)
	}
	return out
}

func (r *Repository) AnchorIdentity() (string, uint16, error) {
	var dbUUID string
	var schemaMajor uint16
	if err := r.db.View(func(tx *bolt.Tx) error {
		state, err := loadState(tx)
		if err != nil {
			return err
		}
		if state.dbUUIDDiagnostic != "" {
			return corruptRecordError("db_uuid", "authority", state.dbUUIDDiagnostic)
		}
		if err := validateDBUUID(state.dbUUID); err != nil {
			return err
		}
		meta := state.metaRecord()
		if meta.State != repository.RecordValid {
			return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, meta.State)
		}
		if err := meta.Value.Validate(); err != nil {
			return err
		}
		dbUUID = state.dbUUID
		schemaMajor = meta.Value.SchemaVersion
		return nil
	}); err != nil {
		return "", 0, err
	}
	return dbUUID, schemaMajor, nil
}

func (r *Repository) InjectCorruptSafetyForTest(jobID model.JobID, diagnostic string) {
	if err := jobID.Validate(); err != nil {
		panic(err)
	}
	if diagnostic == "" {
		diagnostic = "corrupt"
	}
	err := r.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketSafety)
		if err != nil {
			return err
		}
		key := jobIDKey(jobID)
		raw := bucket.Get(key)
		if raw == nil {
			return bucket.Put(key, []byte("corrupt safety: "+diagnostic))
		}
		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return bucket.Put(key, []byte("corrupt safety: "+diagnostic))
		}
		env.Checksum = strings.Repeat("0", sha256.Size*2)
		data, err := json.Marshal(env)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	})
	if err != nil {
		panic(err)
	}
}

func (r *Repository) InjectMissingMetaForTest() {
	err := r.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		return bucket.Delete(keyMeta)
	})
	if err != nil {
		panic(err)
	}
}

func (r *Repository) initialize() error {
	return r.db.Update(func(tx *bolt.Tx) error {
		for _, name := range bucketNames {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		if !databaseEmpty(tx) {
			return nil
		}
		dbUUID, err := newDBUUID()
		if err != nil {
			return err
		}
		state := newStoreState(dbUUID)
		return persistState(tx, state)
	})
}

func databaseEmpty(tx *bolt.Tx) bool {
	for _, name := range bucketNames {
		bucket := tx.Bucket(name)
		if bucket == nil {
			continue
		}
		key, _ := bucket.Cursor().First()
		if key != nil {
			return false
		}
	}
	return true
}

func newDBUUID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "bbolt-db-" + hex.EncodeToString(random[:]), nil
}

type envelope struct {
	Kind          recordKind      `json:"kind"`
	SchemaVersion uint16          `json:"schema_version"`
	Revision      uint64          `json:"revision"`
	Payload       json.RawMessage `json:"payload"`
	Checksum      string          `json:"checksum"`
}

func loadState(tx *bolt.Tx) (storeState, error) {
	state := emptyStoreState()
	meta := tx.Bucket(bucketMeta)
	if meta != nil {
		if raw := meta.Get(keyDBUUID); raw != nil {
			value, diagnostic := decodeEnvelope[string](kindDBUUID, keyDBUUID, raw, validateDBUUID, func(string) uint64 { return 0 })
			if diagnostic != "" {
				state.dbUUIDDiagnostic = diagnostic
			} else {
				state.dbUUID = value
			}
		}
		state.meta = loadSlot(meta, kindMeta, keyMeta, validateMeta, revisionMeta)
		if state.meta.state == repository.RecordValid {
			state.generation = state.meta.value.Generation
			state.nextJobSequence = state.meta.value.NextJobSequence
		}
	}

	if err := loadRequestBucket(tx, bucketBindings, kindBinding, state.bindings, validateBinding, revisionBinding); err != nil {
		return storeState{}, err
	}
	if err := loadRequestBucket(tx, bucketTombstones, kindTombstone, state.tombstones, validateTombstone, revisionTombstone); err != nil {
		return storeState{}, err
	}
	if err := loadJobBucket(tx, bucketSafety, kindSafety, state.safety, validateSafety, revisionSafety); err != nil {
		return storeState{}, err
	}
	if err := loadJobBucket(tx, bucketProjections, kindProjection, state.projections, validateProjectionForLoad, revisionProjection); err != nil {
		return storeState{}, err
	}
	if err := loadJobBucket(tx, bucketQuarantine, kindQuarantine, state.quarantines, validateQuarantine, revisionQuarantine); err != nil {
		return storeState{}, err
	}
	return state, nil
}

func loadSlot[T any](bucket *bolt.Bucket, kind recordKind, key []byte, validate func(T) error, revision func(T) uint64) recordSlot[T] {
	if bucket == nil {
		return recordSlot[T]{}
	}
	raw := bucket.Get(key)
	if raw == nil {
		return recordSlot[T]{}
	}
	value, diagnostic := decodeEnvelope(kind, key, raw, validate, revision)
	if diagnostic != "" {
		return corruptSlot[T](diagnostic)
	}
	return validSlot(value)
}

func loadRequestBucket[T any](tx *bolt.Tx, bucketName []byte, kind recordKind, out map[model.RequestKey]recordSlot[T], validate func(T) error, revision func(T) uint64) error {
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return nil
	}
	return bucket.ForEach(func(key, raw []byte) error {
		requestKey, err := parseRequestKey(key)
		if err != nil {
			return fmt.Errorf("%w: %s key %q: %v", repository.ErrCorruptRecord, kind, string(key), err)
		}
		value, diagnostic := decodeEnvelope(kind, key, raw, validate, revision)
		if diagnostic != "" {
			out[requestKey] = corruptSlot[T](diagnostic)
			return nil
		}
		out[requestKey] = validSlot(value)
		return nil
	})
}

func loadJobBucket[T any](tx *bolt.Tx, bucketName []byte, kind recordKind, out map[model.JobID]recordSlot[T], validate func(T) error, revision func(T) uint64) error {
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return nil
	}
	return bucket.ForEach(func(key, raw []byte) error {
		jobID, err := model.NewJobID(string(key))
		if err != nil {
			return fmt.Errorf("%w: %s key %q: %v", repository.ErrCorruptRecord, kind, string(key), err)
		}
		value, diagnostic := decodeEnvelope(kind, key, raw, validate, revision)
		if diagnostic != "" {
			out[jobID] = corruptSlot[T](diagnostic)
			return nil
		}
		out[jobID] = validSlot(value)
		return nil
	})
}

func decodeEnvelope[T any](kind recordKind, key, raw []byte, validate func(T) error, revision func(T) uint64) (T, string) {
	var zero T
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return zero, "envelope json: " + err.Error()
	}
	if env.Kind != kind {
		return zero, fmt.Sprintf("envelope kind %q, want %q", env.Kind, kind)
	}
	if env.SchemaVersion != envelopeSchemaVersion {
		return zero, fmt.Sprintf("envelope schema %d, want %d", env.SchemaVersion, envelopeSchemaVersion)
	}
	if env.Checksum != checksumEnvelope(env.Kind, env.SchemaVersion, env.Revision, key, env.Payload) {
		return zero, "checksum mismatch"
	}
	var value T
	if err := json.Unmarshal(env.Payload, &value); err != nil {
		return zero, "payload json: " + err.Error()
	}
	if err := validate(value); err != nil {
		return zero, "payload invalid: " + err.Error()
	}
	if got := revision(value); got != env.Revision {
		return zero, fmt.Sprintf("envelope revision %d, payload revision %d", env.Revision, got)
	}
	return value, ""
}

func persistState(tx *bolt.Tx, state storeState) error {
	for _, name := range bucketNames {
		if err := tx.DeleteBucket(name); err != nil && err != bolt.ErrBucketNotFound {
			return err
		}
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	meta := tx.Bucket(bucketMeta)
	if err := putEnvelope(meta, kindDBUUID, keyDBUUID, state.dbUUID, 0); err != nil {
		return err
	}
	if err := putEnvelope(meta, kindMeta, keyMeta, state.meta.value, revisionMeta(state.meta.value)); err != nil {
		return err
	}
	if err := persistRequestMap(tx.Bucket(bucketBindings), kindBinding, state.bindings, revisionBinding); err != nil {
		return err
	}
	if err := persistRequestMap(tx.Bucket(bucketTombstones), kindTombstone, state.tombstones, revisionTombstone); err != nil {
		return err
	}
	if err := persistJobMap(tx.Bucket(bucketSafety), kindSafety, state.safety, revisionSafety); err != nil {
		return err
	}
	if err := persistJobMap(tx.Bucket(bucketProjections), kindProjection, state.projections, revisionProjection); err != nil {
		return err
	}
	return persistJobMap(tx.Bucket(bucketQuarantine), kindQuarantine, state.quarantines, revisionQuarantine)
}

func persistRequestMap[T any](bucket *bolt.Bucket, kind recordKind, records map[model.RequestKey]recordSlot[T], revision func(T) uint64) error {
	for key, slot := range records {
		if slot.state != repository.RecordValid {
			continue
		}
		storageKey := requestKeyBytes(key)
		if err := putEnvelope(bucket, kind, storageKey, slot.value, revision(slot.value)); err != nil {
			return err
		}
	}
	return nil
}

func persistJobMap[T any](bucket *bolt.Bucket, kind recordKind, records map[model.JobID]recordSlot[T], revision func(T) uint64) error {
	for key, slot := range records {
		if slot.state != repository.RecordValid {
			continue
		}
		storageKey := jobIDKey(key)
		if err := putEnvelope(bucket, kind, storageKey, slot.value, revision(slot.value)); err != nil {
			return err
		}
	}
	return nil
}

func putEnvelope[T any](bucket *bolt.Bucket, kind recordKind, key []byte, value T, revision uint64) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	env := envelope{
		Kind:          kind,
		SchemaVersion: envelopeSchemaVersion,
		Revision:      revision,
		Payload:       append(json.RawMessage(nil), payload...),
	}
	env.Checksum = checksumEnvelope(env.Kind, env.SchemaVersion, env.Revision, key, env.Payload)
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return bucket.Put(key, data)
}

func checksumEnvelope(kind recordKind, schemaVersion uint16, revision uint64, key, payload []byte) string {
	hash := sha256.New()
	checksumField(hash, string(kind))
	checksumField(hash, strconv.FormatUint(uint64(schemaVersion), 10))
	checksumField(hash, strconv.FormatUint(revision, 10))
	checksumField(hash, string(key))
	checksumField(hash, string(payload))
	return hex.EncodeToString(hash.Sum(nil))
}

func checksumField(hash interface{ Write([]byte) (int, error) }, value string) {
	_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(value))
	_, _ = hash.Write([]byte{0})
}

type readTx struct {
	state *storeState
}

func (tx readTx) Meta() repository.Record[repository.AuthorityMeta] {
	return tx.state.metaRecord()
}

func (tx readTx) RootStats() (repository.AuthorityRootStats, error) {
	return tx.state.rootStats(), nil
}

func (tx readTx) LookupRequest(key model.RequestKey) repository.RequestImage {
	return repository.RequestImage{
		Binding:   recordFromMap(tx.state.bindings, key, cloneBinding),
		Tombstone: recordFromMap(tx.state.tombstones, key, cloneTombstone),
	}
}

func (tx readTx) LoadJob(jobID model.JobID) repository.JobImage {
	return tx.state.jobImage(jobID)
}

func (tx readTx) ListJobs(filter repository.JobFilter) ([]repository.JobImage, error) {
	if filter.BootID != "" {
		if err := filter.BootID.Validate(); err != nil {
			return nil, fmt.Errorf("%w: job_filter.boot_id: %v", repository.ErrInvalidRecord, err)
		}
	}
	ids := tx.state.jobIDs()
	images := make([]repository.JobImage, 0, len(ids))
	for _, id := range ids {
		image := tx.state.jobImage(id)
		if filter.BootID != "" || filter.NonterminalOnly {
			if image.Safety.State == repository.RecordCorrupt {
				return nil, fmt.Errorf("%w: safety %s: %s", repository.ErrCorruptRecord, id, image.Safety.Diagnostic)
			}
			if image.Safety.State != repository.RecordValid {
				continue
			}
			if filter.BootID != "" && image.Safety.Value.AdmittedBy.BootID != filter.BootID {
				continue
			}
			if filter.NonterminalOnly && image.Safety.Value.Terminal != nil {
				continue
			}
		}
		images = append(images, image)
	}
	return images, nil
}

func (tx readTx) ListNonterminalByBoot(bootID model.BootID) ([]repository.JobImage, error) {
	return tx.ListJobs(repository.JobFilter{BootID: bootID, NonterminalOnly: true})
}

type writeTx struct {
	readTx
	changed bool
}

func (tx *writeTx) AllocateJobID() (model.JobID, error) {
	if err := tx.state.rejectCorrupt(); err != nil {
		return "", err
	}
	if tx.state.meta.state != repository.RecordValid {
		return "", fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, tx.state.meta.state)
	}
	seq := tx.state.nextJobSequence
	if seq == 0 {
		return "", fmt.Errorf("%w: meta.next_job_sequence is required", repository.ErrInvalidRecord)
	}
	id := model.JobID(fmt.Sprintf("job-%020d", seq))
	if err := id.Validate(); err != nil {
		return "", fmt.Errorf("%w: allocated job_id: %v", repository.ErrInvalidRecord, err)
	}
	tx.state.nextJobSequence++
	tx.state.syncMeta()
	tx.changed = true
	return id, nil
}

func (tx *writeTx) PutMeta(meta repository.AuthorityMeta) error {
	if err := tx.state.rejectCorrupt(); err != nil {
		return err
	}
	current := tx.state.metaRecord()
	if err := repository.ValidateAuthorityMetaPut(current, meta, tx.state.generation, tx.state.nextJobSequence, tx.state.rootStats()); err != nil {
		return err
	}
	if current.State == repository.RecordValid && reflect.DeepEqual(current.Value, meta) {
		return nil
	}
	tx.state.nextJobSequence = meta.NextJobSequence
	tx.state.meta = validSlot(meta)
	tx.state.syncMeta()
	tx.changed = true
	return nil
}

func (tx *writeTx) PutBinding(binding model.Binding) error {
	if err := binding.Validate(); err != nil {
		return fmt.Errorf("%w: binding: %v", repository.ErrInvalidRecord, err)
	}
	if slot, ok := tx.state.tombstones[binding.RequestKey]; ok && slot.state == repository.RecordCorrupt {
		return corruptRecordError("tombstone", binding.RequestKey.String(), slot.diagnostic)
	}
	if slot, ok := tx.state.tombstones[binding.RequestKey]; ok && slot.state == repository.RecordValid {
		return fmt.Errorf("%w: request %s is tombstoned", repository.ErrConflict, binding.RequestKey.String())
	}
	slot, ok := tx.state.bindings[binding.RequestKey]
	if ok && slot.state == repository.RecordCorrupt {
		return corruptRecordError("binding", binding.RequestKey.String(), slot.diagnostic)
	}
	if ok && slot.state == repository.RecordValid {
		if reflect.DeepEqual(slot.value, binding) {
			return nil
		}
		return fmt.Errorf("%w: request %s already has a binding", repository.ErrConflict, binding.RequestKey.String())
	}
	tx.state.bindings[binding.RequestKey] = validSlot(cloneBinding(binding))
	tx.changed = true
	return nil
}

func (tx *writeTx) PutSafety(record model.SafetyRecord, expectedRevision uint64) error {
	if err := model.ValidateSafetyRecord(record); err != nil {
		return fmt.Errorf("%w: safety: %v", repository.ErrInvalidRecord, err)
	}
	slot, ok := tx.state.safety[record.JobID]
	if ok && slot.state == repository.RecordCorrupt {
		return corruptRecordError("safety", record.JobID.String(), slot.diagnostic)
	}
	if !ok || slot.state == repository.RecordMissing {
		if expectedRevision != 0 {
			return fmt.Errorf("%w: create safety %s expected revision %d", repository.ErrCASMismatch, record.JobID, expectedRevision)
		}
		if record.Revision != 1 {
			return fmt.Errorf("%w: initial safety revision must be 1", repository.ErrInvalidRecord)
		}
		tx.state.safety[record.JobID] = validSlot(cloneSafetyRecord(record))
		tx.changed = true
		return nil
	}
	if expectedRevision != slot.value.Revision {
		return fmt.Errorf("%w: safety %s expected revision %d, got %d", repository.ErrCASMismatch, record.JobID, expectedRevision, slot.value.Revision)
	}
	if reflect.DeepEqual(slot.value, record) {
		return nil
	}
	if record.Revision != slot.value.Revision+1 {
		return fmt.Errorf("%w: changed safety revision must advance by one", repository.ErrInvalidRecord)
	}
	tx.state.safety[record.JobID] = validSlot(cloneSafetyRecord(record))
	tx.changed = true
	return nil
}

func (tx *writeTx) PutProjection(projection model.JobProjection) error {
	if err := validateProjectionShape(projection); err != nil {
		return err
	}
	slot, ok := tx.state.projections[projection.JobID]
	if ok && slot.state == repository.RecordCorrupt {
		quarantine, quarantined := tx.state.quarantines[projection.JobID]
		if !quarantined || quarantine.state != repository.RecordValid || strings.TrimSpace(quarantine.value.Diagnostic) == "" {
			return corruptRecordError("projection", projection.JobID.String(), slot.diagnostic)
		}
	}
	if ok && slot.state == repository.RecordValid && reflect.DeepEqual(slot.value, projection) {
		return nil
	}
	tx.state.projections[projection.JobID] = validSlot(cloneProjection(projection))
	tx.changed = true
	return nil
}

func (tx *writeTx) PutQuarantine(record repository.QuarantineRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	slot, ok := tx.state.quarantines[record.JobID]
	if ok && slot.state == repository.RecordCorrupt {
		return corruptRecordError("quarantine", record.JobID.String(), slot.diagnostic)
	}
	if ok && slot.state == repository.RecordValid && reflect.DeepEqual(slot.value, record) {
		return nil
	}
	tx.state.quarantines[record.JobID] = validSlot(cloneQuarantine(record))
	tx.changed = true
	return nil
}

func (tx *writeTx) PutTombstone(tombstone repository.Tombstone) error {
	if err := tombstone.Validate(); err != nil {
		return err
	}
	slot, ok := tx.state.tombstones[tombstone.RequestKey]
	if ok && slot.state == repository.RecordCorrupt {
		return corruptRecordError("tombstone", tombstone.RequestKey.String(), slot.diagnostic)
	}
	if ok && slot.state == repository.RecordValid {
		if reflect.DeepEqual(slot.value, tombstone) {
			return nil
		}
		return fmt.Errorf("%w: request %s already has a tombstone", repository.ErrConflict, tombstone.RequestKey.String())
	}
	tx.state.tombstones[tombstone.RequestKey] = validSlot(cloneTombstone(tombstone))
	tx.changed = true
	return nil
}

func (tx *writeTx) DeleteLiveJob(jobID model.JobID) error {
	if err := jobID.Validate(); err != nil {
		return fmt.Errorf("%w: job_id: %v", repository.ErrInvalidRecord, err)
	}
	if err := tx.state.rejectCorrupt(); err != nil {
		return err
	}
	deleted := false
	if _, ok := tx.state.safety[jobID]; ok {
		delete(tx.state.safety, jobID)
		deleted = true
	}
	if _, ok := tx.state.projections[jobID]; ok {
		delete(tx.state.projections, jobID)
		deleted = true
	}
	if _, ok := tx.state.quarantines[jobID]; ok {
		delete(tx.state.quarantines, jobID)
		deleted = true
	}
	for key, slot := range tx.state.bindings {
		if slot.state == repository.RecordValid && slot.value.JobID == jobID {
			delete(tx.state.bindings, key)
			deleted = true
		}
	}
	if deleted {
		tx.changed = true
	}
	return nil
}

type storeState struct {
	dbUUID           string
	dbUUIDDiagnostic string
	generation       uint64
	nextJobSequence  uint64
	meta             recordSlot[repository.AuthorityMeta]
	bindings         map[model.RequestKey]recordSlot[model.Binding]
	tombstones       map[model.RequestKey]recordSlot[repository.Tombstone]
	safety           map[model.JobID]recordSlot[model.SafetyRecord]
	projections      map[model.JobID]recordSlot[model.JobProjection]
	quarantines      map[model.JobID]recordSlot[repository.QuarantineRecord]
}

func emptyStoreState() storeState {
	return storeState{
		bindings:    map[model.RequestKey]recordSlot[model.Binding]{},
		tombstones:  map[model.RequestKey]recordSlot[repository.Tombstone]{},
		safety:      map[model.JobID]recordSlot[model.SafetyRecord]{},
		projections: map[model.JobID]recordSlot[model.JobProjection]{},
		quarantines: map[model.JobID]recordSlot[repository.QuarantineRecord]{},
	}
}

func newStoreState(dbUUID string) storeState {
	state := emptyStoreState()
	state.dbUUID = dbUUID
	state.nextJobSequence = 1
	state.syncMeta()
	return state
}

func (s storeState) clone() storeState {
	return storeState{
		dbUUID:           s.dbUUID,
		dbUUIDDiagnostic: s.dbUUIDDiagnostic,
		generation:       s.generation,
		nextJobSequence:  s.nextJobSequence,
		meta:             cloneSlot(s.meta, cloneMeta),
		bindings:         cloneMap(s.bindings, cloneBinding),
		tombstones:       cloneMap(s.tombstones, cloneTombstone),
		safety:           cloneMap(s.safety, cloneSafetyRecord),
		projections:      cloneMap(s.projections, cloneProjection),
		quarantines:      cloneMap(s.quarantines, cloneQuarantine),
	}
}

func (s *storeState) syncMeta() {
	var admissionRoot repository.AdmissionRootMetadata
	var sealed bool
	if s.meta.state == repository.RecordValid {
		admissionRoot = s.meta.value.AdmissionRoot
		sealed = s.meta.value.Sealed
	}
	s.meta = validSlot(repository.AuthorityMeta{
		SchemaVersion:   repository.CurrentAuthorityMetaSchemaVersion,
		Generation:      s.generation,
		NextJobSequence: s.nextJobSequence,
		AdmissionRoot:   admissionRoot,
		Sealed:          sealed,
	})
}

func (s *storeState) advanceGeneration() {
	s.generation++
	s.syncMeta()
}

func (s *storeState) metaRecord() repository.Record[repository.AuthorityMeta] {
	slot := s.meta
	if slot.state == repository.RecordValid {
		slot.value.Generation = s.generation
		slot.value.NextJobSequence = s.nextJobSequence
	}
	return slot.record(cloneMeta)
}

func (s *storeState) jobImage(jobID model.JobID) repository.JobImage {
	image := repository.JobImage{
		Safety:     recordFromMap(s.safety, jobID, cloneSafetyRecord),
		Projection: recordFromMap(s.projections, jobID, cloneProjection),
		Quarantine: recordFromMap(s.quarantines, jobID, cloneQuarantine),
		Binding:    repository.MissingRecord[model.Binding](),
	}
	for _, slot := range s.bindings {
		if slot.state == repository.RecordValid && slot.value.JobID == jobID {
			image.Binding = slot.record(cloneBinding)
			break
		}
	}
	return image
}

func (s *storeState) jobIDs() []model.JobID {
	seen := map[model.JobID]struct{}{}
	for id := range s.safety {
		seen[id] = struct{}{}
	}
	for id := range s.projections {
		seen[id] = struct{}{}
	}
	for id := range s.quarantines {
		seen[id] = struct{}{}
	}
	for _, slot := range s.bindings {
		if slot.state == repository.RecordValid {
			seen[slot.value.JobID] = struct{}{}
		}
	}
	ids := make([]model.JobID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (s *storeState) rootStats() repository.AuthorityRootStats {
	stats := repository.AuthorityRootStats{
		Jobs:       len(s.jobIDs()),
		Bindings:   len(s.bindings),
		Tombstones: len(s.tombstones),
	}
	for _, slot := range s.safety {
		if slot.state != repository.RecordValid {
			continue
		}
		stats.LaunchRecords += slot.value.Attempt.Launches.Count()
		if slot.value.Terminal == nil {
			stats.RecoveryObligations++
		}
	}
	return stats
}

func (s *storeState) validateForCommit() error {
	if err := s.rejectCorrupt(); err != nil {
		return err
	}
	if s.meta.state != repository.RecordValid {
		return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, s.meta.state)
	}
	meta := s.meta.value
	meta.Generation = s.generation
	meta.NextJobSequence = s.nextJobSequence
	if err := meta.Validate(); err != nil {
		return err
	}

	for key, slot := range s.bindings {
		if slot.state != repository.RecordValid {
			return fmt.Errorf("%w: binding %s is %s", repository.ErrInvalidRecord, key.String(), slot.state)
		}
		binding := slot.value
		if err := binding.Validate(); err != nil {
			return fmt.Errorf("%w: binding %s: %v", repository.ErrInvalidRecord, key.String(), err)
		}
		if binding.RequestKey != key {
			return fmt.Errorf("%w: binding map key mismatch for %s", repository.ErrInvalidRecord, key.String())
		}
		if tombstone, ok := s.tombstones[key]; ok && tombstone.state == repository.RecordValid {
			return fmt.Errorf("%w: request %s has live binding and tombstone", repository.ErrConflict, key.String())
		}
		safety, ok := s.safety[binding.JobID]
		if !ok || safety.state != repository.RecordValid {
			return fmt.Errorf("%w: binding %s references missing safety record", repository.ErrInvalidRecord, key.String())
		}
		if err := binding.Matches(safety.value); err != nil {
			return fmt.Errorf("%w: binding %s: %v", repository.ErrInvalidRecord, key.String(), err)
		}
	}

	for jobID, slot := range s.safety {
		if slot.state != repository.RecordValid {
			return fmt.Errorf("%w: safety %s is %s", repository.ErrInvalidRecord, jobID, slot.state)
		}
		record := slot.value
		if record.JobID != jobID {
			return fmt.Errorf("%w: safety map key mismatch for %s", repository.ErrInvalidRecord, jobID)
		}
		if err := model.ValidateSafetyRecord(record); err != nil {
			return fmt.Errorf("%w: safety %s: %v", repository.ErrInvalidRecord, jobID, err)
		}
		binding, ok := s.bindings[record.RequestKey]
		if !ok || binding.state != repository.RecordValid {
			return fmt.Errorf("%w: safety %s has no request binding", repository.ErrInvalidRecord, jobID)
		}
		if binding.value.JobID != jobID {
			return fmt.Errorf("%w: safety %s request binding points to %s", repository.ErrInvalidRecord, jobID, binding.value.JobID)
		}
		projection, ok := s.projections[jobID]
		if !ok || projection.state != repository.RecordValid {
			return fmt.Errorf("%w: safety %s has no projection", repository.ErrInvalidRecord, jobID)
		}
		if err := validateProjectionMatches(projection.value, record); err != nil {
			return err
		}
	}

	for jobID, slot := range s.projections {
		if slot.state != repository.RecordValid {
			return fmt.Errorf("%w: projection %s is %s", repository.ErrInvalidRecord, jobID, slot.state)
		}
		if slot.value.JobID != jobID {
			return fmt.Errorf("%w: projection map key mismatch for %s", repository.ErrInvalidRecord, jobID)
		}
		safety, ok := s.safety[jobID]
		if !ok || safety.state != repository.RecordValid {
			return fmt.Errorf("%w: projection %s has no safety record", repository.ErrInvalidRecord, jobID)
		}
		if err := validateProjectionMatches(slot.value, safety.value); err != nil {
			return err
		}
	}

	for key, slot := range s.tombstones {
		if slot.state != repository.RecordValid {
			return fmt.Errorf("%w: tombstone %s is %s", repository.ErrInvalidRecord, key.String(), slot.state)
		}
		tombstone := slot.value
		if tombstone.RequestKey != key {
			return fmt.Errorf("%w: tombstone map key mismatch for %s", repository.ErrInvalidRecord, key.String())
		}
		if err := tombstone.Validate(); err != nil {
			return err
		}
		if _, ok := s.bindings[key]; ok {
			return fmt.Errorf("%w: request %s has tombstone and binding", repository.ErrConflict, key.String())
		}
		if _, ok := s.safety[tombstone.JobID]; ok {
			return fmt.Errorf("%w: tombstoned job %s still has live safety", repository.ErrConflict, tombstone.JobID)
		}
		if _, ok := s.projections[tombstone.JobID]; ok {
			return fmt.Errorf("%w: tombstoned job %s still has live projection", repository.ErrConflict, tombstone.JobID)
		}
	}

	for jobID, slot := range s.quarantines {
		if slot.state != repository.RecordValid {
			return fmt.Errorf("%w: quarantine %s is %s", repository.ErrInvalidRecord, jobID, slot.state)
		}
		if slot.value.JobID != jobID {
			return fmt.Errorf("%w: quarantine map key mismatch for %s", repository.ErrInvalidRecord, jobID)
		}
		if err := slot.value.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s *storeState) rejectCorrupt() error {
	if s.dbUUIDDiagnostic != "" {
		return corruptRecordError("db_uuid", "authority", s.dbUUIDDiagnostic)
	}
	if s.meta.state == repository.RecordCorrupt {
		return corruptRecordError("meta", "authority", s.meta.diagnostic)
	}
	for key, slot := range s.bindings {
		if slot.state == repository.RecordCorrupt {
			return corruptRecordError("binding", key.String(), slot.diagnostic)
		}
	}
	for key, slot := range s.tombstones {
		if slot.state == repository.RecordCorrupt {
			return corruptRecordError("tombstone", key.String(), slot.diagnostic)
		}
	}
	for id, slot := range s.safety {
		if slot.state == repository.RecordCorrupt {
			return corruptRecordError("safety", id.String(), slot.diagnostic)
		}
	}
	for id, slot := range s.projections {
		if slot.state == repository.RecordCorrupt {
			return corruptRecordError("projection", id.String(), slot.diagnostic)
		}
	}
	for id, slot := range s.quarantines {
		if slot.state == repository.RecordCorrupt {
			return corruptRecordError("quarantine", id.String(), slot.diagnostic)
		}
	}
	return nil
}

type recordSlot[T any] struct {
	state      repository.RecordState
	value      T
	diagnostic string
}

func validSlot[T any](value T) recordSlot[T] {
	return recordSlot[T]{state: repository.RecordValid, value: value}
}

func corruptSlot[T any](diagnostic string) recordSlot[T] {
	return recordSlot[T]{state: repository.RecordCorrupt, diagnostic: diagnostic}
}

func (slot recordSlot[T]) record(clone func(T) T) repository.Record[T] {
	switch slot.state {
	case repository.RecordValid:
		return repository.ValidRecord(clone(slot.value))
	case repository.RecordCorrupt:
		return repository.CorruptRecord[T](slot.diagnostic)
	default:
		return repository.MissingRecord[T]()
	}
}

func recordFromMap[K comparable, T any](m map[K]recordSlot[T], key K, clone func(T) T) repository.Record[T] {
	slot, ok := m[key]
	if !ok {
		return repository.MissingRecord[T]()
	}
	return slot.record(clone)
}

func cloneSlot[T any](slot recordSlot[T], clone func(T) T) recordSlot[T] {
	if slot.state != repository.RecordValid {
		return slot
	}
	return validSlot(clone(slot.value))
}

func cloneMap[K comparable, T any](in map[K]recordSlot[T], clone func(T) T) map[K]recordSlot[T] {
	out := make(map[K]recordSlot[T], len(in))
	for key, slot := range in {
		out[key] = cloneSlot(slot, clone)
	}
	return out
}

func cloneMeta(meta repository.AuthorityMeta) repository.AuthorityMeta {
	return meta
}

func cloneBinding(binding model.Binding) model.Binding {
	return binding
}

func cloneProjection(projection model.JobProjection) model.JobProjection {
	return projection
}

func cloneTombstone(tombstone repository.Tombstone) repository.Tombstone {
	return tombstone
}

func cloneQuarantine(record repository.QuarantineRecord) repository.QuarantineRecord {
	return record
}

func cloneSafetyRecord(record model.SafetyRecord) model.SafetyRecord {
	next := record
	next.Attempt.Launches = cloneLaunchSlots(record.Attempt.Launches)
	next.Acknowledgement = clonePtr(record.Acknowledgement)
	next.Cancel = clonePtr(record.Cancel)
	next.Outcome = clonePtr(record.Outcome)
	next.Result = clonePtr(record.Result)
	next.Terminal = clonePtr(record.Terminal)
	return next
}

func cloneLaunchSlots(slots model.LaunchSlots[model.LaunchProof]) model.LaunchSlots[model.LaunchProof] {
	return model.LaunchSlots[model.LaunchProof]{
		First:  cloneLaunchProof(slots.First),
		Second: cloneLaunchProof(slots.Second),
	}
}

func cloneLaunchProof(launch *model.LaunchProof) *model.LaunchProof {
	if launch == nil {
		return nil
	}
	copied := *launch
	copied.Group = clonePtr(launch.Group)
	copied.Grant = clonePtr(launch.Grant)
	copied.Released = clonePtr(launch.Released)
	copied.Quiescence = clonePtr(launch.Quiescence)
	return &copied
}

func clonePtr[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func validateDBUUID(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: db_uuid is required", repository.ErrInvalidRecord)
	}
	for _, r := range value {
		if unicode.IsSpace(r) || r == 0 || r == 0x7f {
			return fmt.Errorf("%w: db_uuid must not contain whitespace or control characters", repository.ErrInvalidRecord)
		}
	}
	return nil
}

func validateMeta(meta repository.AuthorityMeta) error {
	return meta.Validate()
}

func validateBinding(binding model.Binding) error {
	return binding.Validate()
}

func validateSafety(record model.SafetyRecord) error {
	return model.ValidateSafetyRecord(record)
}

func validateProjectionForLoad(projection model.JobProjection) error {
	return validateProjectionShape(projection)
}

func validateTombstone(tombstone repository.Tombstone) error {
	return tombstone.Validate()
}

func validateQuarantine(record repository.QuarantineRecord) error {
	return record.Validate()
}

func revisionMeta(meta repository.AuthorityMeta) uint64 {
	return meta.Generation
}

func revisionBinding(model.Binding) uint64 {
	return 0
}

func revisionSafety(record model.SafetyRecord) uint64 {
	return record.Revision
}

func revisionProjection(projection model.JobProjection) uint64 {
	return projection.Revision
}

func revisionTombstone(tombstone repository.Tombstone) uint64 {
	return tombstone.ExpiredGeneration
}

func revisionQuarantine(record repository.QuarantineRecord) uint64 {
	return record.Generation
}

func validateProjectionShape(projection model.JobProjection) error {
	if projection.SchemaVersion == 0 {
		return fmt.Errorf("%w: projection.schema_version is required", repository.ErrInvalidRecord)
	}
	if projection.Revision == 0 {
		return fmt.Errorf("%w: projection.revision is required", repository.ErrInvalidRecord)
	}
	if err := projection.JobID.Validate(); err != nil {
		return fmt.Errorf("%w: projection.job_id: %v", repository.ErrInvalidRecord, err)
	}
	if err := projection.RequestKey.Validate(); err != nil {
		return fmt.Errorf("%w: projection.request_key: %v", repository.ErrInvalidRecord, err)
	}
	if err := projection.TaskIdentity.Validate(); err != nil {
		return fmt.Errorf("%w: projection.task_identity: %v", repository.ErrInvalidRecord, err)
	}
	if err := projection.Mode.Validate(); err != nil {
		return fmt.Errorf("%w: projection.mode: %v", repository.ErrInvalidRecord, err)
	}
	if !projection.Decision.Valid() {
		return fmt.Errorf("%w: projection.decision is unknown", repository.ErrInvalidRecord)
	}
	if !projection.Dispatch.Valid() {
		return fmt.Errorf("%w: projection.dispatch is unknown", repository.ErrInvalidRecord)
	}
	if !projection.Outcome.Valid() {
		return fmt.Errorf("%w: projection.outcome is unknown", repository.ErrInvalidRecord)
	}
	if !projection.Public.Valid() {
		return fmt.Errorf("%w: projection.public is unknown", repository.ErrInvalidRecord)
	}
	if projection.TerminalCause != 0 && !projection.TerminalCause.Valid() {
		return fmt.Errorf("%w: projection.terminal_cause is unknown", repository.ErrInvalidRecord)
	}
	return nil
}

func validateProjectionMatches(projection model.JobProjection, record model.SafetyRecord) error {
	if err := validateProjectionShape(projection); err != nil {
		return err
	}
	expected, err := model.Project(record, model.ProjectionMetadata{SessionID: projection.SessionID})
	if err != nil {
		return fmt.Errorf("%w: project safety %s: %v", repository.ErrInvalidRecord, record.JobID, err)
	}
	if !reflect.DeepEqual(projection, expected) {
		return fmt.Errorf("%w: projection %s does not match safety revision %d", repository.ErrProjectionMismatch, projection.JobID, record.Revision)
	}
	return nil
}

func requestKeyBytes(key model.RequestKey) []byte {
	out := make([]byte, 0, len(key.WorkspaceKey.String())+1+len(key.RequestID.String()))
	out = append(out, key.WorkspaceKey.String()...)
	out = append(out, 0)
	out = append(out, key.RequestID.String()...)
	return out
}

func parseRequestKey(key []byte) (model.RequestKey, error) {
	parts := bytes.Split(key, []byte{0})
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		return model.RequestKey{}, fmt.Errorf("%w: request key is not workspace/request", repository.ErrInvalidRecord)
	}
	return model.NewRequestKey(string(parts[0]), string(parts[1]))
}

func jobIDKey(jobID model.JobID) []byte {
	return []byte(jobID.String())
}

func corruptRecordError(kind, key, diagnostic string) error {
	if diagnostic == "" {
		diagnostic = "corrupt"
	}
	return fmt.Errorf("%w: %s %s: %s", repository.ErrCorruptRecord, kind, key, diagnostic)
}

type snapshot struct {
	DBUUID           string                                       `json:"dbUUID"`
	DBUUIDDiagnostic string                                       `json:"dbUUIDDiagnostic,omitempty"`
	Generation       uint64                                       `json:"generation"`
	NextJobSequence  uint64                                       `json:"nextJobSequence"`
	Meta             repository.Record[repository.AuthorityMeta]  `json:"meta"`
	Bindings         []snapshotEntry[model.Binding]               `json:"bindings"`
	Tombstones       []snapshotEntry[repository.Tombstone]        `json:"tombstones"`
	Safety           []snapshotEntry[model.SafetyRecord]          `json:"safety"`
	Projections      []snapshotEntry[model.JobProjection]         `json:"projections"`
	Quarantines      []snapshotEntry[repository.QuarantineRecord] `json:"quarantines"`
}

type snapshotEntry[T any] struct {
	Key    string               `json:"key"`
	Record repository.Record[T] `json:"record"`
}

func snapshotState(state storeState) snapshot {
	return snapshot{
		DBUUID:           state.dbUUID,
		DBUUIDDiagnostic: state.dbUUIDDiagnostic,
		Generation:       state.generation,
		NextJobSequence:  state.nextJobSequence,
		Meta:             state.metaRecord(),
		Bindings:         snapshotRequestMap(state.bindings, cloneBinding),
		Tombstones:       snapshotRequestMap(state.tombstones, cloneTombstone),
		Safety:           snapshotJobMap(state.safety, cloneSafetyRecord),
		Projections:      snapshotJobMap(state.projections, cloneProjection),
		Quarantines:      snapshotJobMap(state.quarantines, cloneQuarantine),
	}
}

func snapshotRequestMap[T any](in map[model.RequestKey]recordSlot[T], clone func(T) T) []snapshotEntry[T] {
	keys := make([]model.RequestKey, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	out := make([]snapshotEntry[T], 0, len(keys))
	for _, key := range keys {
		out = append(out, snapshotEntry[T]{Key: key.String(), Record: in[key].record(clone)})
	}
	return out
}

func snapshotJobMap[T any](in map[model.JobID]recordSlot[T], clone func(T) T) []snapshotEntry[T] {
	keys := make([]model.JobID, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]snapshotEntry[T], 0, len(keys))
	for _, key := range keys {
		out = append(out, snapshotEntry[T]{Key: key.String(), Record: in[key].record(clone)})
	}
	return out
}
