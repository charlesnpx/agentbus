// Package bbolt implements the root bbolt-backed admission repository.
package bbolt

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bolt "go.etcd.io/bbolt"
)

const envelopeSchemaVersion uint16 = 1

var (
	// admissionRepositoryRequiredBuckets is the canonical top-level bucket set
	// created by initialize and required by existing-root preflight checks.
	admissionRepositoryRequiredBuckets = [...]string{
		"meta",
		"bindings",
		"binding_index",
		"safety",
		"projections",
		"tombstones",
		"quarantine",
	}

	// admissionRepositoryRequiredMetaKeys is the canonical meta bucket key set
	// required before an existing admission root may be recovered.
	admissionRepositoryRequiredMetaKeys = [...]string{
		"db_uuid",
		"authority",
	}

	bucketMeta         = []byte(admissionRepositoryRequiredBuckets[0])
	bucketBindings     = []byte(admissionRepositoryRequiredBuckets[1])
	bucketBindingIndex = []byte(admissionRepositoryRequiredBuckets[2])
	bucketSafety       = []byte(admissionRepositoryRequiredBuckets[3])
	bucketProjections  = []byte(admissionRepositoryRequiredBuckets[4])
	bucketTombstones   = []byte(admissionRepositoryRequiredBuckets[5])
	bucketQuarantine   = []byte(admissionRepositoryRequiredBuckets[6])

	keyDBUUID = []byte(admissionRepositoryRequiredMetaKeys[0])
	keyMeta   = []byte(admissionRepositoryRequiredMetaKeys[1])

	bucketNames = [][]byte{
		bucketMeta,
		bucketBindings,
		bucketBindingIndex,
		bucketSafety,
		bucketProjections,
		bucketTombstones,
		bucketQuarantine,
	}
)

// AdmissionRepositoryRequiredBuckets returns the canonical top-level bucket set
// created by initialize and required by existing-root preflight checks.
func AdmissionRepositoryRequiredBuckets() []string {
	return append([]string(nil), admissionRepositoryRequiredBuckets[:]...)
}

// AdmissionRepositoryRequiredMetaKeys returns the canonical meta bucket key set
// required before an existing admission root may be recovered.
func AdmissionRepositoryRequiredMetaKeys() []string {
	return append([]string(nil), admissionRepositoryRequiredMetaKeys[:]...)
}

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
	fileIdentity                 FileIdentity
	testMu                       sync.Mutex
	failCommitAfterCommitForTest error
	operationMu                  sync.Mutex
	operationCounter             *operationCounterForTest
}

type operationCounterForTest struct {
	mu     sync.Mutex
	counts map[string]int
}

// FileIdentity is the device/inode identity of the database file handle bbolt
// opened.
type FileIdentity struct {
	Dev uint64
	Ino uint64
}

// FileIdentityMismatchError reports that the database file bbolt opened is not
// the expected device/inode.
type FileIdentityMismatchError struct {
	Path     string
	Expected FileIdentity
	Opened   FileIdentity
}

func (e FileIdentityMismatchError) Error() string {
	return fmt.Sprintf("%s: bbolt repository identity changed while opening %s (expected dev=%d ino=%d, opened dev=%d ino=%d)", repository.ErrInvalidRecord, e.Path, e.Expected.Dev, e.Expected.Ino, e.Opened.Dev, e.Opened.Ino)
}

func (e FileIdentityMismatchError) Is(target error) bool {
	return target == repository.ErrInvalidRecord
}

// UnsupportedAuthorityMetaSchemaVersionError reports that the authority meta
// payload is structurally readable but declares a schema this binary does not
// support.
type UnsupportedAuthorityMetaSchemaVersionError struct {
	Path          string
	SchemaVersion uint16
}

func (e UnsupportedAuthorityMetaSchemaVersionError) Error() string {
	return fmt.Sprintf("%s: incompatible meta.schema_version %d, want %d: %s", repository.ErrInvalidRecord, e.SchemaVersion, repository.StrictAuthorityMetaSchemaVersion, e.Path)
}

func (e UnsupportedAuthorityMetaSchemaVersionError) Is(target error) bool {
	return target == repository.ErrInvalidRecord
}

// Open opens or initializes a root bbolt repository database at path. The file
// is created with owner-only permissions.
func Open(path string, options *bolt.Options) (*Repository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: bbolt path is required", repository.ErrInvalidRecord)
	}
	var identity FileIdentity
	db, err := bolt.Open(path, 0o600, optionsWithFileIdentity(options, &identity))
	if err != nil {
		return nil, err
	}
	repo := &Repository{db: db, fileIdentity: identity}
	if err := repo.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return repo, nil
}

// OpenExistingNoInit opens an existing root bbolt repository without creating,
// initializing, or repairing admission buckets or meta keys. It verifies the
// canonical initialized structure in a read transaction before returning.
func OpenExistingNoInit(path string, expectedIdentity *FileIdentity, options *bolt.Options) (*Repository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: bbolt path is required", repository.ErrInvalidRecord)
	}
	restoreNoFreelistSync := false
	if options != nil {
		restoreNoFreelistSync = options.NoFreelistSync
	}
	var identity FileIdentity
	db, err := bolt.Open(path, 0o600, optionsForExistingNoInit(options, &identity, expectedIdentity))
	if err != nil {
		return nil, err
	}
	repo := &Repository{db: db, fileIdentity: identity}
	if err := repo.verifyExistingNoInitInitializedStructure(); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.NoFreelistSync = restoreNoFreelistSync
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
func OpenReadOnly(path string, options ...*bolt.Options) (*Repository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: bbolt path is required", repository.ErrInvalidRecord)
	}
	readOnlyOptions := bolt.Options{ReadOnly: true, Timeout: defaultOpenTimeout}
	if len(options) > 0 && options[0] != nil {
		readOnlyOptions = *options[0]
		readOnlyOptions.ReadOnly = true
	}
	var identity FileIdentity
	db, err := bolt.Open(path, 0o600, optionsWithFileIdentity(&readOnlyOptions, &identity))
	if err != nil {
		return nil, err
	}
	repo := &Repository{db: db, fileIdentity: identity}
	if err := repo.verifyReadOnlyCompatibleSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return repo, nil
}

func (r *Repository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// OpenedFileIdentity returns the identity captured from bbolt's opened
// database file handle.
func (r *Repository) OpenedFileIdentity() (FileIdentity, error) {
	if r == nil || r.db == nil {
		return FileIdentity{}, fmt.Errorf("%w: bbolt repository is not open", repository.ErrInvalidRecord)
	}
	if r.fileIdentity == (FileIdentity{}) {
		return FileIdentity{}, fmt.Errorf("%w: bbolt repository file identity is unavailable", repository.ErrInvalidRecord)
	}
	return r.fileIdentity, nil
}

// VerifyInitializedStructure checks the required bbolt buckets and meta keys
// through this repository's open database handle.
func (r *Repository) VerifyInitializedStructure() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("%w: bbolt repository is not open", repository.ErrInvalidRecord)
	}
	return r.db.View(func(tx *bolt.Tx) error {
		return verifyInitializedStructureTx(tx, r.db.Path())
	})
}

func (r *Repository) verifyExistingNoInitInitializedStructure() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("%w: bbolt repository is not open", repository.ErrInvalidRecord)
	}
	return r.db.View(func(tx *bolt.Tx) error {
		schemaVersion, err := authorityMetaSchemaVersionTx(tx, r.db.Path())
		if err != nil {
			return err
		}
		if schemaVersion != repository.StrictAuthorityMetaSchemaVersion {
			return UnsupportedAuthorityMetaSchemaVersionError{Path: r.db.Path(), SchemaVersion: schemaVersion}
		}
		return verifyInitializedStructureTx(tx, r.db.Path())
	})
}

func (r *Repository) verifyReadOnlyCompatibleSchema() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("%w: bbolt repository is not open", repository.ErrInvalidRecord)
	}
	return r.db.View(func(tx *bolt.Tx) error {
		schemaVersion, err := authorityMetaSchemaVersionTx(tx, r.db.Path())
		if err != nil {
			return err
		}
		if schemaVersion != repository.StrictAuthorityMetaSchemaVersion {
			return UnsupportedAuthorityMetaSchemaVersionError{Path: r.db.Path(), SchemaVersion: schemaVersion}
		}
		return nil
	})
}

// AuthorityMetaSchemaVersion reads the authority meta payload schema after
// validating the enclosing envelope, but before full meta validation rejects
// unsupported schema versions as corrupt slots.
func (r *Repository) AuthorityMetaSchemaVersion() (uint16, error) {
	if r == nil || r.db == nil {
		return 0, fmt.Errorf("%w: bbolt repository is not open", repository.ErrInvalidRecord)
	}
	var schemaVersion uint16
	if err := r.db.View(func(tx *bolt.Tx) error {
		var err error
		schemaVersion, err = authorityMetaSchemaVersionTx(tx, r.db.Path())
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return schemaVersion, nil
}

func authorityMetaSchemaVersionTx(tx *bolt.Tx, path string) (uint16, error) {
	meta := tx.Bucket(bucketMeta)
	if meta == nil {
		return 0, fmt.Errorf("%w: admission repository missing bucket %q: %s", repository.ErrCorruptRecord, string(bucketMeta), path)
	}
	raw := meta.Get(keyMeta)
	if raw == nil {
		return 0, fmt.Errorf("%w: admission repository missing meta key %q: %s", repository.ErrInvalidRecord, string(keyMeta), path)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return 0, fmt.Errorf("%w: meta envelope json: %v", repository.ErrCorruptRecord, err)
	}
	if env.Kind != kindMeta {
		return 0, fmt.Errorf("%w: meta envelope kind %q, want %q", repository.ErrCorruptRecord, env.Kind, kindMeta)
	}
	if env.SchemaVersion != envelopeSchemaVersion {
		return 0, fmt.Errorf("%w: meta envelope schema %d, want %d", repository.ErrCorruptRecord, env.SchemaVersion, envelopeSchemaVersion)
	}
	if env.Checksum != checksumEnvelope(env.Kind, env.SchemaVersion, env.Revision, keyMeta, env.Payload) {
		return 0, fmt.Errorf("%w: meta envelope checksum mismatch", repository.ErrCorruptRecord)
	}
	var payload struct {
		SchemaVersion uint16
	}
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		return 0, fmt.Errorf("%w: meta payload json: %v", repository.ErrCorruptRecord, err)
	}
	return payload.SchemaVersion, nil
}

func optionsWithFileIdentity(options *bolt.Options, identity *FileIdentity) *bolt.Options {
	return optionsWithFileIdentityChecks(options, identity, nil, false, false)
}

func optionsForExistingNoInit(options *bolt.Options, identity, expectedIdentity *FileIdentity) *bolt.Options {
	cloned := optionsWithFileIdentityChecks(options, identity, expectedIdentity, true, true)
	cloned.NoFreelistSync = true
	return cloned
}

func optionsWithFileIdentityChecks(options *bolt.Options, identity, expectedIdentity *FileIdentity, existingOnly, rejectEmpty bool) *bolt.Options {
	var cloned bolt.Options
	if options == nil {
		cloned = *bolt.DefaultOptions
	} else {
		cloned = *options
	}
	openFile := cloned.OpenFile
	if openFile == nil {
		openFile = os.OpenFile
	}
	cloned.OpenFile = func(path string, flag int, perm os.FileMode) (*os.File, error) {
		openFlag := flag
		if existingOnly {
			openFlag &^= os.O_CREATE
		}
		file, err := openFile(path, openFlag, perm)
		if err != nil {
			return nil, err
		}
		if rejectEmpty {
			info, err := file.Stat()
			if err != nil {
				_ = file.Close()
				return nil, err
			}
			if info.Size() == 0 {
				_ = file.Close()
				return nil, fmt.Errorf("%w: admission repository is zero-length: %s", repository.ErrInvalidRecord, path)
			}
		}
		fileIdentity, err := fileIdentityFromFile(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if expectedIdentity != nil && fileIdentity != *expectedIdentity {
			_ = file.Close()
			return nil, FileIdentityMismatchError{Path: path, Expected: *expectedIdentity, Opened: fileIdentity}
		}
		if identity != nil {
			*identity = fileIdentity
		}
		return file, nil
	}
	return &cloned
}

func fileIdentityFromFile(file *os.File) (FileIdentity, error) {
	info, err := file.Stat()
	if err != nil {
		return FileIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return FileIdentity{}, fmt.Errorf("unexpected bbolt stat type %T", info.Sys())
	}
	return FileIdentity{Dev: uint64(stat.Dev), Ino: uint64(stat.Ino)}, nil
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

func (r *Repository) ResetOperationCountsForTest() {
	r.operationMu.Lock()
	defer r.operationMu.Unlock()
	r.operationCounter = &operationCounterForTest{counts: map[string]int{}}
}

func (r *Repository) OperationCountsForTest() map[string]int {
	r.operationMu.Lock()
	counter := r.operationCounter
	r.operationMu.Unlock()
	if counter == nil {
		return map[string]int{}
	}
	counter.mu.Lock()
	defer counter.mu.Unlock()
	out := make(map[string]int, len(counter.counts))
	for name, count := range counter.counts {
		out[name] = count
	}
	return out
}

func (r *Repository) operationCounterForTest() *operationCounterForTest {
	r.operationMu.Lock()
	defer r.operationMu.Unlock()
	return r.operationCounter
}

func (r *Repository) countOperationForTest(name string) {
	counter := r.operationCounterForTest()
	counter.count(name)
}

func (counter *operationCounterForTest) count(name string) {
	if counter == nil {
		return
	}
	counter.mu.Lock()
	defer counter.mu.Unlock()
	if counter.counts == nil {
		counter.counts = map[string]int{}
	}
	counter.counts[name]++
}

func (r *Repository) View(ctx context.Context, fn func(repository.ReadTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(readTx{tx: tx, counter: r.operationCounterForTest()})
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
		if meta := (readTx{tx: tx}).Meta(); meta.State == repository.RecordValid {
			commit = repository.Commit{Generation: meta.Value.Generation}
		}
		write := &writeTx{
			readTx: readTx{tx: tx, counter: r.operationCounterForTest()},
			dirty:  newDirtySet(),
		}

		if err := fn(write); err != nil {
			return err
		}
		if !write.changed {
			return nil
		}
		if err := write.validateDirtyForCommit(); err != nil {
			return err
		}
		nextMeta := write.Meta()
		if nextMeta.State != repository.RecordValid {
			if nextMeta.State == repository.RecordCorrupt {
				return repository.CorruptRecordError("meta", "authority", nextMeta.Diagnostic)
			}
			return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, nextMeta.State)
		}
		next := nextMeta.Value
		next.Generation++
		if err := write.putMetaRecord(next); err != nil {
			return err
		}
		commit = repository.Commit{Generation: next.Generation}
		return nil
	}()
	if txErr != nil {
		return commit, fmt.Errorf("%w: %w", repository.ErrDefinitelyNotCommitted, txErr)
	}
	if err := tx.Commit(); err != nil {
		txClosed = true
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
		snap, err := snapshotTx(tx)
		if err != nil {
			return err
		}
		data, err := json.Marshal(snap)
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
		uuidRecord := loadDBUUID(tx)
		if uuidRecord.State == repository.RecordCorrupt {
			return repository.CorruptRecordError("db_uuid", "authority", uuidRecord.Diagnostic)
		}
		if uuidRecord.State != repository.RecordValid {
			return fmt.Errorf("%w: db_uuid is %s", repository.ErrInvalidRecord, uuidRecord.State)
		}
		meta := readTx{tx: tx}.Meta()
		if meta.State == repository.RecordCorrupt {
			return repository.CorruptRecordError("meta", "authority", meta.Diagnostic)
		}
		if meta.State != repository.RecordValid {
			return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, meta.State)
		}
		if err := meta.Value.Validate(); err != nil {
			return err
		}
		dbUUID = uuidRecord.Value
		schemaMajor = meta.Value.SchemaVersion
		return nil
	}); err != nil {
		return "", 0, err
	}
	return dbUUID, schemaMajor, nil
}

func (r *Repository) AuditIntegrity(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return auditIntegrityTx(tx)
	})
}

func auditIntegrityTx(tx *bolt.Tx) error {
	read := readTx{tx: tx}
	var findings []error
	if err := verifyInitializedStructureTx(tx, "audit"); err != nil {
		findings = append(findings, repository.NewIntegrityFinding("structure", "", err))
	}
	uuid := loadDBUUID(tx)
	switch uuid.State {
	case repository.RecordValid:
	case repository.RecordCorrupt:
		findings = append(findings, repository.NewIntegrityFinding("db_uuid", "authority", repository.CorruptRecordError("db_uuid", "authority", uuid.Diagnostic)))
	default:
		findings = append(findings, repository.NewIntegrityFinding("db_uuid", "authority", fmt.Errorf("%w: db_uuid is %s", repository.ErrInvalidRecord, uuid.State)))
	}
	meta := read.Meta()
	switch meta.State {
	case repository.RecordValid:
		if err := meta.Value.Validate(); err != nil {
			findings = append(findings, repository.NewIntegrityFinding("meta", "authority", err))
		}
	case repository.RecordCorrupt:
		findings = append(findings, repository.NewIntegrityFinding("meta", "authority", repository.CorruptRecordError("meta", "authority", meta.Diagnostic)))
	default:
		findings = append(findings, repository.NewIntegrityFinding("meta", "authority", fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, meta.State)))
	}

	requests, jobs, err := auditKeysTx(read)
	if err != nil {
		findings = append(findings, repository.NewIntegrityFinding("keys", "", err))
	}
	for key := range requests {
		if err := read.validateBindingIndexForRequest(key); err != nil {
			findings = append(findings, repository.NewIntegrityFinding("binding_index", key.String(), err))
		}
		if err := repository.ValidateRequestClosure(key, read.LookupRequest(key), read.LoadJob); err != nil {
			findings = append(findings, repository.NewIntegrityFinding("request", key.String(), err))
		}
	}
	for jobID := range jobs {
		if err := repository.ValidateJobClosure(jobID, read.LoadJob(jobID), read.LookupRequest); err != nil {
			findings = append(findings, repository.NewIntegrityFinding("job", jobID.String(), err))
		}
	}
	if err := read.auditBindingIndexEntries(); err != nil {
		findings = append(findings, repository.NewIntegrityFinding("binding_index", "", err))
	}
	return repository.NewIntegrityError(findings)
}

func (r *Repository) InjectCorruptSafetyForTest(jobID model.JobID, diagnostic string) {
	if err := jobID.Validate(); err != nil {
		panic(err)
	}
	r.injectCorruptRecordForTest(bucketSafety, jobIDKey(jobID), "safety", diagnostic)
}

func (r *Repository) InjectCorruptBindingForTest(key model.RequestKey, diagnostic string) {
	if err := key.Validate(); err != nil {
		panic(err)
	}
	r.injectCorruptRecordForTest(bucketBindings, requestKeyBytes(key), "binding", diagnostic)
}

func (r *Repository) InjectCorruptTombstoneForTest(key model.RequestKey, diagnostic string) {
	if err := key.Validate(); err != nil {
		panic(err)
	}
	r.injectCorruptRecordForTest(bucketTombstones, requestKeyBytes(key), "tombstone", diagnostic)
}

func (r *Repository) InjectCorruptBindingIndexValueForTest(jobID model.JobID, key model.RequestKey) {
	if err := jobID.Validate(); err != nil {
		panic(err)
	}
	if err := key.Validate(); err != nil {
		panic(err)
	}
	err := r.db.Update(func(tx *bolt.Tx) error {
		index := tx.Bucket(bucketBindingIndex)
		if index == nil {
			return fmt.Errorf("binding_index bucket missing")
		}
		return index.Put(jobIDKey(jobID), requestKeyBytes(key))
	})
	if err != nil {
		panic(err)
	}
}

func (r *Repository) injectCorruptRecordForTest(bucketName, key []byte, kind, diagnostic string) {
	if diagnostic == "" {
		diagnostic = "corrupt"
	}
	err := r.db.Update(func(tx *bolt.Tx) error {
		r.countOperationForTest("create_bucket")
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		raw := bucket.Get(key)
		if raw == nil {
			return bucket.Put(key, []byte("corrupt "+kind+": "+diagnostic))
		}
		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return bucket.Put(key, []byte("corrupt "+kind+": "+diagnostic))
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
		r.countOperationForTest("create_bucket")
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
		if !databaseEmpty(tx) {
			schemaVersion, err := authorityMetaSchemaVersionTx(tx, r.db.Path())
			if err != nil {
				return err
			}
			if schemaVersion != repository.StrictAuthorityMetaSchemaVersion {
				return UnsupportedAuthorityMetaSchemaVersionError{Path: r.db.Path(), SchemaVersion: schemaVersion}
			}
			return verifyInitializedStructureTx(tx, r.db.Path())
		}
		for _, name := range bucketNames {
			r.countOperationForTest("create_bucket")
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		dbUUID, err := newDBUUID()
		if err != nil {
			return err
		}
		if err := putEnvelope(tx.Bucket(bucketMeta), kindDBUUID, keyDBUUID, dbUUID, 0); err != nil {
			return err
		}
		meta := repository.AuthorityMeta{
			SchemaVersion:   repository.StrictAuthorityMetaSchemaVersion,
			Generation:      0,
			NextJobSequence: 1,
		}
		return putEnvelope(tx.Bucket(bucketMeta), kindMeta, keyMeta, meta, revisionMeta(meta))
	})
}

func verifyInitializedStructureTx(tx *bolt.Tx, path string) error {
	for _, name := range admissionRepositoryRequiredBuckets {
		if tx.Bucket([]byte(name)) == nil {
			return fmt.Errorf("%w: admission repository missing bucket %q: %s", repository.ErrCorruptRecord, name, path)
		}
	}
	meta := tx.Bucket(bucketMeta)
	for _, key := range admissionRepositoryRequiredMetaKeys {
		if meta.Get([]byte(key)) == nil {
			return fmt.Errorf("%w: admission repository missing meta key %q: %s", repository.ErrInvalidRecord, key, path)
		}
	}
	return nil
}

func databaseEmpty(tx *bolt.Tx) bool {
	empty := true
	_ = tx.ForEach(func(_ []byte, _ *bolt.Bucket) error {
		empty = false
		return fmt.Errorf("stop")
	})
	return empty
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

func loadDBUUID(tx *bolt.Tx) repository.Record[string] {
	return loadSlot(tx.Bucket(bucketMeta), kindDBUUID, keyDBUUID, validateDBUUID, func(string) uint64 { return 0 }).record(func(value string) string { return value })
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
	tx      *bolt.Tx
	counter *operationCounterForTest
}

func (tx readTx) count(operation string) {
	if tx.counter != nil {
		tx.counter.count(operation)
	}
}

func (tx readTx) Meta() repository.Record[repository.AuthorityMeta] {
	tx.count("get:meta")
	return loadSlot(tx.tx.Bucket(bucketMeta), kindMeta, keyMeta, validateMeta, revisionMeta).record(cloneMeta)
}

func (tx readTx) RootStats() (repository.AuthorityRootStats, error) {
	liveJobs := map[model.JobID]struct{}{}
	var findings []error
	stats := repository.AuthorityRootStats{}
	for _, bucketName := range [][]byte{bucketSafety, bucketProjections, bucketQuarantine, bucketBindingIndex} {
		bucket := tx.tx.Bucket(bucketName)
		if bucket == nil {
			continue
		}
		tx.count("foreach:" + string(bucketName))
		if err := bucket.ForEach(func(key, raw []byte) error {
			jobID, err := model.NewJobID(string(key))
			if err != nil {
				findings = append(findings, fmt.Errorf("%w: %s key %q: %v", repository.ErrCorruptRecord, string(bucketName), string(key), err))
				return nil
			}
			if bytes.Equal(bucketName, bucketBindingIndex) {
				if err := tx.validateBindingIndexEntry(jobID, raw); err != nil {
					if !corruptRecordKind(err, "binding") {
						findings = append(findings, err)
					}
					return nil
				}
			}
			if bytes.Equal(bucketName, bucketSafety) {
				record, diagnostic := decodeEnvelope(kindSafety, key, raw, validateSafety, revisionSafety)
				if diagnostic != "" {
					findings = append(findings, repository.CorruptRecordError("safety", jobID.String(), diagnostic))
					return nil
				}
				stats.LaunchRecords += record.Attempt.Launches.Count()
				if record.Terminal == nil {
					stats.RecoveryObligations++
				}
			}
			liveJobs[jobID] = struct{}{}
			return nil
		}); err != nil {
			return repository.AuthorityRootStats{}, err
		}
	}
	stats.Bindings = countValidRequestRecords(tx, bucketBindings, kindBinding, validateBinding, revisionBinding, func(binding model.Binding) {
		if err := tx.validateBindingIndexForRequest(binding.RequestKey); err != nil {
			findings = append(findings, err)
			return
		}
		liveJobs[binding.JobID] = struct{}{}
	}, &findings)
	stats.Tombstones = countValidRequestRecords(tx, bucketTombstones, kindTombstone, validateTombstone, revisionTombstone, nil, &findings)
	if err := repository.NewIntegrityError(findings); err != nil {
		return repository.AuthorityRootStats{}, err
	}
	stats.Jobs = len(liveJobs)
	return stats, nil
}

func countValidRequestRecords[T any](tx readTx, bucketName []byte, kind recordKind, validate func(T) error, revision func(T) uint64, onValid func(T), findings *[]error) int {
	bucket := tx.tx.Bucket(bucketName)
	if bucket == nil {
		return 0
	}
	count := 0
	tx.count("foreach:" + string(bucketName))
	_ = bucket.ForEach(func(key, raw []byte) error {
		requestKey, err := parseRequestKey(key)
		if err != nil {
			*findings = append(*findings, fmt.Errorf("%w: %s key %q: %v", repository.ErrCorruptRecord, kind, string(key), err))
			return nil
		}
		value, diagnostic := decodeEnvelope(kind, key, raw, validate, revision)
		if diagnostic != "" {
			*findings = append(*findings, repository.CorruptRecordError(string(kind), requestKey.String(), diagnostic))
			return nil
		}
		count++
		if onValid != nil {
			onValid(value)
		}
		return nil
	})
	return count
}

func (tx readTx) LookupRequest(key model.RequestKey) repository.RequestImage {
	storageKey := requestKeyBytes(key)
	tx.count("get:bindings")
	tx.count("get:tombstones")
	return repository.RequestImage{
		Binding:   loadSlot(tx.tx.Bucket(bucketBindings), kindBinding, storageKey, validateBinding, revisionBinding).record(cloneBinding),
		Tombstone: loadSlot(tx.tx.Bucket(bucketTombstones), kindTombstone, storageKey, validateTombstone, revisionTombstone).record(cloneTombstone),
	}
}

func (tx readTx) LoadJob(jobID model.JobID) repository.JobImage {
	key := jobIDKey(jobID)
	tx.count("get:safety")
	tx.count("get:projections")
	tx.count("get:quarantine")
	return repository.JobImage{
		Binding:    tx.bindingByJobID(jobID),
		Safety:     loadSlot(tx.tx.Bucket(bucketSafety), kindSafety, key, validateSafety, revisionSafety).record(cloneSafetyRecord),
		Projection: loadSlot(tx.tx.Bucket(bucketProjections), kindProjection, key, validateProjectionForLoad, revisionProjection).record(cloneProjection),
		Quarantine: loadSlot(tx.tx.Bucket(bucketQuarantine), kindQuarantine, key, validateQuarantine, revisionQuarantine).record(cloneQuarantine),
	}
}

func (tx readTx) bindingByJobID(jobID model.JobID) repository.Record[model.Binding] {
	index := tx.tx.Bucket(bucketBindingIndex)
	if index == nil {
		return repository.MissingRecord[model.Binding]()
	}
	tx.count("get:binding_index")
	rawKey := index.Get(jobIDKey(jobID))
	if rawKey == nil {
		return repository.MissingRecord[model.Binding]()
	}
	requestKey, err := parseRequestKey(rawKey)
	if err != nil {
		return repository.CorruptRecord[model.Binding](fmt.Sprintf("binding_index: %v", err))
	}
	binding := tx.LookupRequest(requestKey).Binding
	if binding.State != repository.RecordValid {
		if binding.State == repository.RecordCorrupt {
			return binding
		}
		return repository.CorruptRecord[model.Binding]("binding_index references missing binding")
	}
	if binding.Value.JobID != jobID {
		return repository.CorruptRecord[model.Binding](fmt.Sprintf("binding_index points to job %s", binding.Value.JobID))
	}
	return binding
}

func (tx readTx) ListJobs(filter repository.JobFilter) ([]repository.JobImage, error) {
	if filter.BootID != "" {
		if err := filter.BootID.Validate(); err != nil {
			return nil, fmt.Errorf("%w: job_filter.boot_id: %v", repository.ErrInvalidRecord, err)
		}
	}
	ids, err := tx.liveJobIDSet()
	if err != nil {
		return nil, err
	}
	ordered := make([]model.JobID, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	images := make([]repository.JobImage, 0, len(ordered))
	for _, id := range ordered {
		image := tx.LoadJob(id)
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

func (tx readTx) liveJobIDSet() (map[model.JobID]struct{}, error) {
	seen := map[model.JobID]struct{}{}
	for _, bucketName := range [][]byte{bucketSafety, bucketProjections, bucketQuarantine, bucketBindingIndex} {
		bucket := tx.tx.Bucket(bucketName)
		if bucket == nil {
			continue
		}
		tx.count("foreach:" + string(bucketName))
		if err := bucket.ForEach(func(key, raw []byte) error {
			jobID, err := model.NewJobID(string(key))
			if err != nil {
				return fmt.Errorf("%w: %s key %q: %v", repository.ErrCorruptRecord, string(bucketName), string(key), err)
			}
			if bytes.Equal(bucketName, bucketBindingIndex) {
				if err := tx.validateBindingIndexEntry(jobID, raw); err != nil {
					return err
				}
			}
			seen[jobID] = struct{}{}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	bindings := tx.tx.Bucket(bucketBindings)
	if bindings != nil {
		tx.count("foreach:bindings")
		if err := bindings.ForEach(func(key, raw []byte) error {
			binding, diagnostic := decodeEnvelope(kindBinding, key, raw, validateBinding, revisionBinding)
			if diagnostic == "" {
				if err := tx.validateBindingIndexForRequest(binding.RequestKey); err != nil {
					return err
				}
				seen[binding.JobID] = struct{}{}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return seen, nil
}

func (tx readTx) auditJobIDSet() (map[model.JobID]struct{}, error) {
	seen, err := tx.liveJobIDSet()
	if err != nil {
		return nil, err
	}
	tombstones := tx.tx.Bucket(bucketTombstones)
	if tombstones != nil {
		tx.count("foreach:tombstones")
		if err := tombstones.ForEach(func(key, raw []byte) error {
			tombstone, diagnostic := decodeEnvelope(kindTombstone, key, raw, validateTombstone, revisionTombstone)
			if diagnostic == "" {
				seen[tombstone.JobID] = struct{}{}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return seen, nil
}

func (tx readTx) validateBindingIndexForRequest(key model.RequestKey) error {
	request := tx.LookupRequest(key)
	if request.Binding.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("binding", key.String(), request.Binding.Diagnostic)
	}
	if request.Binding.State != repository.RecordValid {
		return nil
	}
	binding := request.Binding.Value
	index := tx.tx.Bucket(bucketBindingIndex)
	if index == nil {
		return repository.CorruptRecordError("binding_index", binding.JobID.String(), "bucket is missing")
	}
	raw := index.Get(jobIDKey(binding.JobID))
	tx.count("get:binding_index")
	if raw == nil {
		return repository.CorruptRecordError("binding_index", binding.JobID.String(), "missing index entry")
	}
	indexedKey, err := parseRequestKey(raw)
	if err != nil {
		return repository.CorruptRecordError("binding_index", binding.JobID.String(), err.Error())
	}
	if indexedKey != key {
		return repository.CorruptRecordError("binding_index", binding.JobID.String(), fmt.Sprintf("points to %s, want %s", indexedKey, key))
	}
	return nil
}

func corruptRecordKind(err error, kind string) bool {
	var corrupt repository.CorruptRecordKindError
	return errors.As(err, &corrupt) && corrupt.Kind == kind
}

func (tx readTx) auditBindingIndexEntries() error {
	index := tx.tx.Bucket(bucketBindingIndex)
	if index == nil {
		return repository.CorruptRecordError("binding_index", "", "bucket is missing")
	}
	var findings []error
	tx.count("foreach:binding_index")
	if err := index.ForEach(func(jobKey, requestKeyBytes []byte) error {
		jobID, err := model.NewJobID(string(jobKey))
		if err != nil {
			findings = append(findings, fmt.Errorf("%w: binding_index key %q: %v", repository.ErrCorruptRecord, string(jobKey), err))
			return nil
		}
		if err := tx.validateBindingIndexEntry(jobID, requestKeyBytes); err != nil {
			findings = append(findings, err)
		}
		return nil
	}); err != nil {
		return err
	}
	return repository.NewIntegrityError(findings)
}

func (tx readTx) validateBindingIndexEntry(jobID model.JobID, rawRequestKey []byte) error {
	requestKey, err := parseRequestKey(rawRequestKey)
	if err != nil {
		return repository.CorruptRecordError("binding_index", jobID.String(), err.Error())
	}
	binding := tx.LookupRequest(requestKey).Binding
	if binding.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("binding", requestKey.String(), binding.Diagnostic)
	}
	if binding.State != repository.RecordValid {
		return repository.CorruptRecordError("binding_index", jobID.String(), "references missing binding")
	}
	if binding.Value.JobID != jobID {
		return repository.CorruptRecordError("binding_index", jobID.String(), fmt.Sprintf("points to binding for job %s", binding.Value.JobID))
	}
	return nil
}

func auditKeysTx(tx readTx) (map[model.RequestKey]struct{}, map[model.JobID]struct{}, error) {
	requests := map[model.RequestKey]struct{}{}
	jobs, err := tx.auditJobIDSet()
	if err != nil {
		return requests, jobs, err
	}
	for _, bucketName := range [][]byte{bucketBindings, bucketTombstones} {
		bucket := tx.tx.Bucket(bucketName)
		if bucket == nil {
			continue
		}
		tx.count("foreach:" + string(bucketName))
		if err := bucket.ForEach(func(key, _ []byte) error {
			requestKey, err := parseRequestKey(key)
			if err != nil {
				return fmt.Errorf("%w: %s key %q: %v", repository.ErrCorruptRecord, string(bucketName), string(key), err)
			}
			requests[requestKey] = struct{}{}
			return nil
		}); err != nil {
			return requests, jobs, err
		}
	}
	for jobID := range jobs {
		image := tx.LoadJob(jobID)
		if image.Safety.State == repository.RecordValid {
			requests[image.Safety.Value.RequestKey] = struct{}{}
		}
		if image.Projection.State == repository.RecordValid {
			requests[image.Projection.Value.RequestKey] = struct{}{}
		}
		if image.Binding.State == repository.RecordValid {
			requests[image.Binding.Value.RequestKey] = struct{}{}
		}
	}
	for key := range requests {
		request := tx.LookupRequest(key)
		if request.Binding.State == repository.RecordValid {
			jobs[request.Binding.Value.JobID] = struct{}{}
		}
		if request.Tombstone.State == repository.RecordValid {
			jobs[request.Tombstone.Value.JobID] = struct{}{}
		}
	}
	return requests, jobs, nil
}

type writeTx struct {
	readTx
	changed bool
	dirty   dirtySet
}

func (tx *writeTx) AllocateJobID() (model.JobID, error) {
	meta := tx.Meta()
	if meta.State == repository.RecordCorrupt {
		return "", repository.CorruptRecordError("meta", "authority", meta.Diagnostic)
	}
	if meta.State != repository.RecordValid {
		return "", fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, meta.State)
	}
	seq := meta.Value.NextJobSequence
	if seq == 0 {
		return "", fmt.Errorf("%w: meta.next_job_sequence is required", repository.ErrInvalidRecord)
	}
	id := model.JobID(fmt.Sprintf("job-%020d", seq))
	if err := id.Validate(); err != nil {
		return "", fmt.Errorf("%w: allocated job_id: %v", repository.ErrInvalidRecord, err)
	}
	meta.Value.NextJobSequence++
	if err := tx.putMetaRecord(meta.Value); err != nil {
		return "", err
	}
	tx.changed = true
	tx.dirty.markMeta()
	return id, nil
}

func (tx *writeTx) PutMeta(meta repository.AuthorityMeta) error {
	current := tx.Meta()
	stats := repository.AuthorityRootStats{}
	if current.State != repository.RecordValid {
		var err error
		stats, err = tx.RootStats()
		if err != nil {
			return err
		}
	}
	currentGeneration := uint64(0)
	currentNextJobSequence := uint64(0)
	if current.State == repository.RecordValid {
		currentGeneration = current.Value.Generation
		currentNextJobSequence = current.Value.NextJobSequence
	}
	if err := repository.ValidateAuthorityMetaPut(current, meta, currentGeneration, currentNextJobSequence, stats); err != nil {
		return err
	}
	if current.State == repository.RecordValid && reflect.DeepEqual(current.Value, meta) {
		return nil
	}
	if err := tx.putMetaRecord(meta); err != nil {
		return err
	}
	tx.changed = true
	tx.dirty.markMeta()
	return nil
}

func (tx *writeTx) PutBinding(binding model.Binding) error {
	if err := binding.Validate(); err != nil {
		return fmt.Errorf("%w: binding: %v", repository.ErrInvalidRecord, err)
	}
	request := tx.LookupRequest(binding.RequestKey)
	if request.Tombstone.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("tombstone", binding.RequestKey.String(), request.Tombstone.Diagnostic)
	}
	if request.Tombstone.State == repository.RecordValid {
		return fmt.Errorf("%w: request %s is tombstoned", repository.ErrConflict, binding.RequestKey.String())
	}
	if request.Binding.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("binding", binding.RequestKey.String(), request.Binding.Diagnostic)
	}
	if request.Binding.State == repository.RecordValid {
		if reflect.DeepEqual(request.Binding.Value, binding) {
			return nil
		}
		return fmt.Errorf("%w: request %s already has a binding", repository.ErrConflict, binding.RequestKey.String())
	}
	if err := tx.validateNoConflictingBindingIndex(binding); err != nil {
		return err
	}
	key := requestKeyBytes(binding.RequestKey)
	tx.count("put:bindings")
	if err := putEnvelope(tx.tx.Bucket(bucketBindings), kindBinding, key, cloneBinding(binding), revisionBinding(binding)); err != nil {
		return err
	}
	tx.count("put:binding_index")
	if err := tx.tx.Bucket(bucketBindingIndex).Put(jobIDKey(binding.JobID), key); err != nil {
		return err
	}
	tx.changed = true
	tx.dirty.markRequest(binding.RequestKey)
	tx.dirty.markJob(binding.JobID)
	return nil
}

func (tx *writeTx) PutSafety(record model.SafetyRecord, expectedRevision uint64) error {
	if err := model.ValidateSafetyRecord(record); err != nil {
		return fmt.Errorf("%w: safety: %v", repository.ErrInvalidRecord, err)
	}
	current := tx.LoadJob(record.JobID).Safety
	if current.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("safety", record.JobID.String(), current.Diagnostic)
	}
	if current.State == repository.RecordMissing {
		if expectedRevision != 0 {
			return fmt.Errorf("%w: create safety %s expected revision %d", repository.ErrCASMismatch, record.JobID, expectedRevision)
		}
		if record.Revision != 1 {
			return fmt.Errorf("%w: initial safety revision must be 1", repository.ErrInvalidRecord)
		}
	} else {
		if expectedRevision != current.Value.Revision {
			return fmt.Errorf("%w: safety %s expected revision %d, got %d", repository.ErrCASMismatch, record.JobID, expectedRevision, current.Value.Revision)
		}
		if reflect.DeepEqual(current.Value, record) {
			return nil
		}
		if record.Revision != current.Value.Revision+1 {
			return fmt.Errorf("%w: changed safety revision must advance by one", repository.ErrInvalidRecord)
		}
	}
	tx.count("put:safety")
	if err := putEnvelope(tx.tx.Bucket(bucketSafety), kindSafety, jobIDKey(record.JobID), cloneSafetyRecord(record), revisionSafety(record)); err != nil {
		return err
	}
	tx.changed = true
	tx.dirty.markJob(record.JobID)
	tx.dirty.markRequest(record.RequestKey)
	return nil
}

func (tx *writeTx) PutProjection(projection model.JobProjection) error {
	if err := repository.ValidateProjectionShape(projection); err != nil {
		return err
	}
	current := tx.LoadJob(projection.JobID).Projection
	if current.State == repository.RecordCorrupt {
		quarantine := tx.LoadJob(projection.JobID).Quarantine
		if quarantine.State != repository.RecordValid || strings.TrimSpace(quarantine.Value.Diagnostic) == "" {
			return repository.CorruptRecordError("projection", projection.JobID.String(), current.Diagnostic)
		}
	}
	if current.State == repository.RecordValid && reflect.DeepEqual(current.Value, projection) {
		return nil
	}
	tx.count("put:projections")
	if err := putEnvelope(tx.tx.Bucket(bucketProjections), kindProjection, jobIDKey(projection.JobID), cloneProjection(projection), revisionProjection(projection)); err != nil {
		return err
	}
	tx.changed = true
	tx.dirty.markJob(projection.JobID)
	tx.dirty.markRequest(projection.RequestKey)
	return nil
}

func (tx *writeTx) PutQuarantine(record repository.QuarantineRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	current := tx.LoadJob(record.JobID).Quarantine
	if current.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("quarantine", record.JobID.String(), current.Diagnostic)
	}
	if current.State == repository.RecordValid && reflect.DeepEqual(current.Value, record) {
		return nil
	}
	tx.count("put:quarantine")
	if err := putEnvelope(tx.tx.Bucket(bucketQuarantine), kindQuarantine, jobIDKey(record.JobID), cloneQuarantine(record), revisionQuarantine(record)); err != nil {
		return err
	}
	tx.changed = true
	tx.dirty.markJob(record.JobID)
	return nil
}

func (tx *writeTx) PutTombstone(tombstone repository.Tombstone) error {
	if err := tombstone.Validate(); err != nil {
		return err
	}
	request := tx.LookupRequest(tombstone.RequestKey)
	if request.Tombstone.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("tombstone", tombstone.RequestKey.String(), request.Tombstone.Diagnostic)
	}
	if request.Tombstone.State == repository.RecordValid {
		if reflect.DeepEqual(request.Tombstone.Value, tombstone) {
			return nil
		}
		return fmt.Errorf("%w: request %s already has a tombstone", repository.ErrConflict, tombstone.RequestKey.String())
	}
	tx.count("put:tombstones")
	if err := putEnvelope(tx.tx.Bucket(bucketTombstones), kindTombstone, requestKeyBytes(tombstone.RequestKey), cloneTombstone(tombstone), revisionTombstone(tombstone)); err != nil {
		return err
	}
	tx.changed = true
	tx.dirty.markRequest(tombstone.RequestKey)
	tx.dirty.markJob(tombstone.JobID)
	return nil
}

func (tx *writeTx) DeleteLiveJob(jobID model.JobID) error {
	if err := jobID.Validate(); err != nil {
		return fmt.Errorf("%w: job_id: %v", repository.ErrInvalidRecord, err)
	}
	image := tx.LoadJob(jobID)
	if image.Safety.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("safety", jobID.String(), image.Safety.Diagnostic)
	}
	if image.Projection.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("projection", jobID.String(), image.Projection.Diagnostic)
	}
	if image.Quarantine.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("quarantine", jobID.String(), image.Quarantine.Diagnostic)
	}
	deleted := false
	if image.Safety.State == repository.RecordValid {
		request := tx.LookupRequest(image.Safety.Value.RequestKey)
		if request.Binding.State == repository.RecordCorrupt {
			return repository.CorruptRecordError("binding", image.Safety.Value.RequestKey.String(), request.Binding.Diagnostic)
		}
		if request.Binding.State == repository.RecordValid {
			if request.Binding.Value.JobID != jobID {
				return fmt.Errorf("%w: safety %s request binding points to %s", repository.ErrInvalidRecord, jobID, request.Binding.Value.JobID)
			}
			if err := tx.deleteBinding(image.Safety.Value.RequestKey, jobID); err != nil {
				return err
			}
			tx.dirty.markRequest(image.Safety.Value.RequestKey)
			deleted = true
		}
	}
	if image.Binding.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("binding", jobID.String(), image.Binding.Diagnostic)
	}
	if image.Binding.State == repository.RecordValid {
		if err := tx.deleteBinding(image.Binding.Value.RequestKey, jobID); err != nil {
			return err
		}
		tx.dirty.markRequest(image.Binding.Value.RequestKey)
		deleted = true
	}
	for _, bucketName := range [][]byte{bucketSafety, bucketProjections, bucketQuarantine} {
		tx.count("delete:" + string(bucketName))
		if err := tx.tx.Bucket(bucketName).Delete(jobIDKey(jobID)); err != nil {
			return err
		}
	}
	if image.Safety.State != repository.RecordMissing || image.Projection.State != repository.RecordMissing || image.Quarantine.State != repository.RecordMissing {
		deleted = true
	}
	if deleted {
		tx.changed = true
		tx.dirty.markJob(jobID)
	}
	return nil
}

func (tx *writeTx) putMetaRecord(meta repository.AuthorityMeta) error {
	tx.count("put:meta")
	return putEnvelope(tx.tx.Bucket(bucketMeta), kindMeta, keyMeta, meta, revisionMeta(meta))
}

func (tx *writeTx) validateNoConflictingBindingIndex(binding model.Binding) error {
	index := tx.tx.Bucket(bucketBindingIndex)
	raw := index.Get(jobIDKey(binding.JobID))
	if raw == nil {
		return nil
	}
	indexedKey, err := parseRequestKey(raw)
	if err != nil {
		return repository.CorruptRecordError("binding_index", binding.JobID.String(), err.Error())
	}
	if indexedKey != binding.RequestKey {
		return repository.CorruptRecordError("binding_index", binding.JobID.String(), fmt.Sprintf("points to %s, want %s", indexedKey, binding.RequestKey))
	}
	return nil
}

func (tx *writeTx) deleteBinding(key model.RequestKey, jobID model.JobID) error {
	tx.count("delete:bindings")
	if err := tx.tx.Bucket(bucketBindings).Delete(requestKeyBytes(key)); err != nil {
		return err
	}
	tx.count("delete:binding_index")
	return tx.tx.Bucket(bucketBindingIndex).Delete(jobIDKey(jobID))
}

func (tx *writeTx) validateDirtyForCommit() error {
	if tx.dirty.meta {
		meta := tx.Meta()
		if meta.State == repository.RecordCorrupt {
			return repository.CorruptRecordError("meta", "authority", meta.Diagnostic)
		}
		if meta.State != repository.RecordValid {
			return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, meta.State)
		}
		if err := meta.Value.Validate(); err != nil {
			return err
		}
	}
	closure := tx.dirty.expand(tx.readTx)
	for key := range closure.requests {
		if err := tx.validateBindingIndexForRequest(key); err != nil {
			return err
		}
		if err := repository.ValidateRequestClosure(key, tx.LookupRequest(key), tx.LoadJob); err != nil {
			return err
		}
	}
	for jobID := range closure.jobs {
		if err := repository.ValidateJobClosure(jobID, tx.LoadJob(jobID), tx.LookupRequest); err != nil {
			return err
		}
	}
	return nil
}

func (tx *writeTx) validateBindingIndexForRequest(key model.RequestKey) error {
	return tx.readTx.validateBindingIndexForRequest(key)
}

type dirtySet struct {
	meta     bool
	requests map[model.RequestKey]struct{}
	jobs     map[model.JobID]struct{}
}

func newDirtySet() dirtySet {
	return dirtySet{
		requests: map[model.RequestKey]struct{}{},
		jobs:     map[model.JobID]struct{}{},
	}
}

func (d *dirtySet) markMeta() {
	d.meta = true
}

func (d *dirtySet) markRequest(key model.RequestKey) {
	if d.requests == nil {
		d.requests = map[model.RequestKey]struct{}{}
	}
	d.requests[key] = struct{}{}
}

func (d *dirtySet) markJob(jobID model.JobID) {
	if d.jobs == nil {
		d.jobs = map[model.JobID]struct{}{}
	}
	d.jobs[jobID] = struct{}{}
}

func (d dirtySet) expand(tx readTx) dirtySet {
	closure := newDirtySet()
	closure.meta = d.meta
	for key := range d.requests {
		closure.markRequest(key)
	}
	for jobID := range d.jobs {
		closure.markJob(jobID)
	}
	changed := true
	for changed {
		changed = false
		for key := range closure.requests {
			image := tx.LookupRequest(key)
			if image.Binding.State == repository.RecordValid {
				if _, ok := closure.jobs[image.Binding.Value.JobID]; !ok {
					closure.markJob(image.Binding.Value.JobID)
					changed = true
				}
			}
			if image.Tombstone.State == repository.RecordValid {
				if _, ok := closure.jobs[image.Tombstone.Value.JobID]; !ok {
					closure.markJob(image.Tombstone.Value.JobID)
					changed = true
				}
			}
		}
		for jobID := range closure.jobs {
			image := tx.LoadJob(jobID)
			if image.Binding.State == repository.RecordValid {
				if _, ok := closure.requests[image.Binding.Value.RequestKey]; !ok {
					closure.markRequest(image.Binding.Value.RequestKey)
					changed = true
				}
			}
			if image.Safety.State == repository.RecordValid {
				if _, ok := closure.requests[image.Safety.Value.RequestKey]; !ok {
					closure.markRequest(image.Safety.Value.RequestKey)
					changed = true
				}
			}
			if image.Projection.State == repository.RecordValid {
				if _, ok := closure.requests[image.Projection.Value.RequestKey]; !ok {
					closure.markRequest(image.Projection.Value.RequestKey)
					changed = true
				}
			}
		}
	}
	return closure
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
	copied.ReleaseOutcome = clonePtr(launch.ReleaseOutcome)
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

func snapshotTx(tx *bolt.Tx) (snapshot, error) {
	var snap snapshot
	uuid := loadDBUUID(tx)
	if uuid.State == repository.RecordValid {
		snap.DBUUID = uuid.Value
	} else if uuid.State == repository.RecordCorrupt {
		snap.DBUUIDDiagnostic = uuid.Diagnostic
	}
	meta := readTx{tx: tx}.Meta()
	snap.Meta = meta
	if meta.State == repository.RecordValid {
		snap.Generation = meta.Value.Generation
		snap.NextJobSequence = meta.Value.NextJobSequence
	}
	var err error
	snap.Bindings, err = scanRequestSnapshot(tx.Bucket(bucketBindings), kindBinding, validateBinding, revisionBinding)
	if err != nil {
		return snapshot{}, err
	}
	snap.Tombstones, err = scanRequestSnapshot(tx.Bucket(bucketTombstones), kindTombstone, validateTombstone, revisionTombstone)
	if err != nil {
		return snapshot{}, err
	}
	snap.Safety, err = scanJobSnapshot(tx.Bucket(bucketSafety), kindSafety, validateSafety, revisionSafety)
	if err != nil {
		return snapshot{}, err
	}
	snap.Projections, err = scanJobSnapshot(tx.Bucket(bucketProjections), kindProjection, validateProjectionForLoad, revisionProjection)
	if err != nil {
		return snapshot{}, err
	}
	snap.Quarantines, err = scanJobSnapshot(tx.Bucket(bucketQuarantine), kindQuarantine, validateQuarantine, revisionQuarantine)
	if err != nil {
		return snapshot{}, err
	}
	return snap, nil
}

func scanRequestSnapshot[T any](bucket *bolt.Bucket, kind recordKind, validate func(T) error, revision func(T) uint64) ([]snapshotEntry[T], error) {
	if bucket == nil {
		return nil, nil
	}
	out := make([]snapshotEntry[T], 0)
	if err := bucket.ForEach(func(key, raw []byte) error {
		requestKey, err := parseRequestKey(key)
		if err != nil {
			return fmt.Errorf("%w: %s key %q: %v", repository.ErrCorruptRecord, kind, string(key), err)
		}
		value, diagnostic := decodeEnvelope(kind, key, raw, validate, revision)
		record := repository.ValidRecord(value)
		if diagnostic != "" {
			record = repository.CorruptRecord[T](diagnostic)
		}
		out = append(out, snapshotEntry[T]{Key: requestKey.String(), Record: record})
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func scanJobSnapshot[T any](bucket *bolt.Bucket, kind recordKind, validate func(T) error, revision func(T) uint64) ([]snapshotEntry[T], error) {
	if bucket == nil {
		return nil, nil
	}
	out := make([]snapshotEntry[T], 0)
	if err := bucket.ForEach(func(key, raw []byte) error {
		jobID, err := model.NewJobID(string(key))
		if err != nil {
			return fmt.Errorf("%w: %s key %q: %v", repository.ErrCorruptRecord, kind, string(key), err)
		}
		value, diagnostic := decodeEnvelope(kind, key, raw, validate, revision)
		record := repository.ValidRecord(value)
		if diagnostic != "" {
			record = repository.CorruptRecord[T](diagnostic)
		}
		out = append(out, snapshotEntry[T]{Key: jobID.String(), Record: record})
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
