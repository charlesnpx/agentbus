// Package bbolt implements the root bbolt-backed admission repository.
package bbolt

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bolt "go.etcd.io/bbolt"
)

var initializeFreshForTest func() error

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

// RepositoryAlreadyExistsError reports that an explicit repository creation
// found any existing path at the target location.
type RepositoryAlreadyExistsError struct {
	Path string
}

func (e RepositoryAlreadyExistsError) Error() string {
	return fmt.Sprintf("%s: bbolt repository already exists: %s", repository.ErrInvalidRecord, e.Path)
}

func (e RepositoryAlreadyExistsError) Is(target error) bool {
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

// defaultOpenTimeout bounds how long repository opens wait for the database
// file lock. bbolt with nil options blocks indefinitely on the flock, which
// would let a replacement daemon hang silently in admission bootstrap while a
// draining predecessor still holds the database.
const defaultOpenTimeout = 10 * time.Second

// Create exclusively creates a fresh root bbolt repository database at path.
// It never opens, repairs, or reinitializes an existing file.
func Create(path string, options ...*bolt.Options) (*Repository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: bbolt path is required", repository.ErrInvalidRecord)
	}
	if _, err := os.Lstat(path); err == nil {
		return nil, RepositoryAlreadyExistsError{Path: path}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var identity FileIdentity
	db, err := openBoltSafely(path, 0o600, optionsForCreate(firstOpenOption(options), &identity))
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, RepositoryAlreadyExistsError{Path: path}
		}
		return nil, err
	}
	repo := &Repository{db: db, fileIdentity: identity}
	if err := repo.initializeFresh(); err != nil {
		closeErr := db.Close()
		removeErr := removeFailedCreate(path, identity)
		if removeErr != nil {
			log.Printf("agentbus bbolt: remove failed create %s: %v", path, removeErr)
		}
		return nil, errors.Join(err, closeErr, removeErr)
	}
	return repo, nil
}

func removeFailedCreate(path string, identity FileIdentity) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: failed create path is not a regular file: %s", repository.ErrInvalidRecord, path)
	}
	current, err := fileIdentityFromPath(path, info)
	if err != nil {
		return err
	}
	if current != identity {
		return FileIdentityMismatchError{Path: path, Expected: identity, Opened: current}
	}
	return os.Remove(path)
}

// OpenExisting opens an existing root bbolt repository without creating,
// initializing, or repairing admission buckets or meta keys.
func OpenExisting(path string, options ...*bolt.Options) (*Repository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: bbolt path is required", repository.ErrInvalidRecord)
	}
	if err := requireExistingRegularNonEmptyFile(path); err != nil {
		return nil, err
	}
	if err := preflightBoltPageHeaders(path); err != nil {
		return nil, err
	}
	if err := preflightBoltFreelist(path); err != nil {
		return nil, err
	}
	restoreNoFreelistSync := false
	if option := firstOpenOption(options); option != nil {
		restoreNoFreelistSync = option.NoFreelistSync
	}
	var identity FileIdentity
	db, err := openBoltSafely(path, 0o600, optionsForExistingNoInit(firstOpenOption(options), &identity))
	if err != nil {
		return nil, existingOpenError(path, err)
	}
	repo := &Repository{db: db, fileIdentity: identity}
	if err := repo.verifyExistingInitializedStructure(); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.NoFreelistSync = restoreNoFreelistSync
	return repo, nil
}

// OpenExistingReadOnly opens an existing root bbolt repository database for
// inspection. It never initializes or mutates the file.
func OpenExistingReadOnly(path string, options ...*bolt.Options) (*Repository, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: bbolt path is required", repository.ErrInvalidRecord)
	}
	if err := requireExistingRegularNonEmptyFile(path); err != nil {
		return nil, err
	}
	if err := preflightBoltPageHeaders(path); err != nil {
		return nil, err
	}
	if err := preflightBoltFreelist(path); err != nil {
		return nil, err
	}
	readOnlyOptions := bolt.Options{ReadOnly: true, Timeout: defaultOpenTimeout}
	if len(options) > 0 && options[0] != nil {
		readOnlyOptions = *options[0]
		readOnlyOptions.ReadOnly = true
	}
	var identity FileIdentity
	db, err := openBoltSafely(path, 0o600, optionsForExistingReadOnly(&readOnlyOptions, &identity))
	if err != nil {
		return nil, existingOpenError(path, err)
	}
	repo := &Repository{db: db, fileIdentity: identity}
	if err := repo.verifyExistingInitializedStructure(); err != nil {
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

func (r *Repository) verifyExistingInitializedStructure() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("%w: bbolt repository is not open", repository.ErrInvalidRecord)
	}
	return r.db.View(func(tx *bolt.Tx) error {
		if err := checkStructuralIntegrityTx(tx, r.fileIdentity); err != nil {
			return err
		}
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

func firstOpenOption(options []*bolt.Options) *bolt.Options {
	if len(options) == 0 {
		return nil
	}
	return options[0]
}

func optionsForCreate(options *bolt.Options, identity *FileIdentity) *bolt.Options {
	return optionsWithFileIdentityChecks(options, identity, true, false, false)
}

func optionsForExistingNoInit(options *bolt.Options, identity *FileIdentity) *bolt.Options {
	cloned := optionsWithFileIdentityChecks(options, identity, false, true, true)
	cloned.NoFreelistSync = true
	return cloned
}

func optionsForExistingReadOnly(options *bolt.Options, identity *FileIdentity) *bolt.Options {
	return optionsWithFileIdentityChecks(options, identity, false, true, true)
}

func optionsWithFileIdentityChecks(options *bolt.Options, identity *FileIdentity, createExclusive, existingOnly, rejectEmpty bool) *bolt.Options {
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
		if createExclusive {
			openFlag |= os.O_CREATE | os.O_EXCL
		}
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
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if !info.Mode().IsRegular() {
			_ = file.Close()
			return nil, fmt.Errorf("%w: admission repository is not a regular file: %s", repository.ErrInvalidRecord, path)
		}
		fileIdentity, err := fileIdentityFromFile(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if identity != nil {
			*identity = fileIdentity
		}
		return file, nil
	}
	return &cloned
}

func requireExistingRegularNonEmptyFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: admission repository is not a regular file: %s", repository.ErrInvalidRecord, path)
	}
	if info.Size() == 0 {
		return fmt.Errorf("%w: admission repository is zero-length: %s", repository.ErrInvalidRecord, path)
	}
	return nil
}

const (
	boltPageHeaderSize   = 16
	boltMetaPayloadSize  = 64
	boltMagic            = 0xED0CDAED
	boltVersion          = 2
	boltMinPageSize      = 512
	boltMaxPageSize      = 128 * 1024
	boltBranchPageFlag   = 0x01
	boltLeafPageFlag     = 0x02
	boltMetaPageFlag     = 0x04
	boltFreelistPageFlag = 0x10

	boltBranchPageElementSize    = 16
	boltLeafPageElementSize      = 16
	boltBucketHeaderSize         = 16
	boltBucketLeafFlag           = 0x01
	boltStructuralGraphMaxDepth  = 4096
	boltStructuralGraphPageFloor = 2
	boltNoFreelistID             = ^uint64(0)
)

type boltPreflightMeta struct {
	pageSize uint64
	root     uint64
	freelist uint64
	pgid     uint64
	txid     uint64
}

type boltPreflightPage struct {
	id       uint64
	flags    uint16
	count    uint16
	overflow uint32
}

type boltFreelistPageInfo struct {
	span      uint64
	count     uint64
	idsOffset uint64
}

func preflightBoltPageHeaders(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	meta, ok, err := readBoltPreflightMeta(file, size)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: bbolt meta pages are invalid: %s", repository.ErrCorruptRecord, path)
	}
	if meta.pgid < 2 {
		return fmt.Errorf("%w: bbolt high water mark %d is invalid: %s", repository.ErrCorruptRecord, meta.pgid, path)
	}
	if meta.pageSize == 0 || uint64(size)/meta.pageSize < meta.pgid {
		return fmt.Errorf("%w: bbolt file is truncated: pages=%d page_size=%d size=%d: %s", repository.ErrCorruptRecord, meta.pgid, meta.pageSize, size, path)
	}
	// Deliberately NOT a linear physical scan asserting page.id == pageID for every
	// page. bbolt does not guarantee that: overflow-continuation pages carry raw
	// payload with no self-identifying header, and freed pages retain stale
	// overflow/flags/id from a prior allocation. A physical linear scan therefore
	// spuriously rejects VALID databases — especially with small OS page sizes (e.g.
	// 4KiB on Linux), where the same data needs overflow spans that are absent on a
	// 16KiB darwin page. Sound structural validation is reachability-based and lives
	// elsewhere: meta pages are validated above and the freelist in
	// preflightBoltFreelist (both before open); openBoltSafely wraps bolt.Open in
	// fault recovery; and verifyExistingInitializedStructure -> checkStructuralIntegrityTx
	// -> preflightBoltBTreeGraphTx walks the live b-tree (correctly following overflow
	// spans) and cross-checks the freelist against reachable pages.
	return nil
}

func preflightBoltFreelist(path string) (err error) {
	previous := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(previous)
	defer func() {
		if recovered := recover(); recovered != nil {
			if boltPanicIsCorruption(recovered) {
				err = fmt.Errorf("%w: bbolt freelist preflight fault for %s: %v", repository.ErrCorruptRecord, path, recovered)
				return
			}
			panic(recovered)
		}
	}()

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size < 0 {
		return fmt.Errorf("%w: bbolt file has invalid size %d: %s", repository.ErrCorruptRecord, size, path)
	}
	meta, ok, err := readBoltPreflightMeta(file, size)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: bbolt meta pages are invalid: %s", repository.ErrCorruptRecord, path)
	}
	return validateBoltFreelistPage(file, meta, uint64(size), path)
}

func validateBoltFreelistPage(file *os.File, meta boltPreflightMeta, fileSize uint64, path string) error {
	if meta.freelist == boltNoFreelistID {
		return nil
	}
	if err := validateBoltFreelistPageID(meta, path); err != nil {
		return err
	}
	if meta.pageSize == 0 || fileSize/meta.pageSize < meta.pgid {
		return fmt.Errorf("%w: bbolt freelist preflight file is truncated: pages=%d page_size=%d size=%d: %s", repository.ErrCorruptRecord, meta.pgid, meta.pageSize, fileSize, path)
	}
	page, err := readBoltPreflightPage(file, meta.freelist, meta.pageSize)
	if err != nil {
		return fmt.Errorf("%w: bbolt freelist page %d header read failed: %v", repository.ErrCorruptRecord, meta.freelist, err)
	}
	if page.id != meta.freelist {
		return fmt.Errorf("%w: bbolt freelist page %d self-identifies as %d: %s", repository.ErrCorruptRecord, meta.freelist, page.id, path)
	}
	if page.flags != boltFreelistPageFlag {
		return fmt.Errorf("%w: bbolt freelist page %d has flags 0x%x, want freelist: %s", repository.ErrCorruptRecord, meta.freelist, page.flags, path)
	}
	span, err := validateBoltFreelistSpan(meta, page, path)
	if err != nil {
		return err
	}
	start, end, ok := checkedBoltDataRange(meta.freelist, meta.pageSize, span, fileSize)
	if !ok {
		return fmt.Errorf("%w: bbolt freelist page %d overflow %d is outside file data: %s", repository.ErrCorruptRecord, meta.freelist, page.overflow, path)
	}
	spanBytes := uint64(end - start)
	startOffset := uint64(start)
	info, err := validateBoltFreelistCount(meta, page, span, spanBytes, path, func() (uint64, error) {
		var buf [8]byte
		if _, err := file.ReadAt(buf[:], int64(startOffset+boltPageHeaderSize)); err != nil {
			return 0, fmt.Errorf("%w: bbolt freelist page %d extended count read failed: %v", repository.ErrCorruptRecord, meta.freelist, err)
		}
		return binary.LittleEndian.Uint64(buf[:]), nil
	})
	if err != nil {
		return err
	}
	return validateBoltFreelistIDs(meta, info, nil, path, func(index uint64) (uint64, error) {
		var buf [8]byte
		offset := startOffset + info.idsOffset + index*8
		if _, err := file.ReadAt(buf[:], int64(offset)); err != nil {
			return 0, fmt.Errorf("%w: bbolt freelist page %d entry %d read failed: %v", repository.ErrCorruptRecord, meta.freelist, index, err)
		}
		return binary.LittleEndian.Uint64(buf[:]), nil
	})
}

func validateBoltFreelistPageID(meta boltPreflightMeta, path string) error {
	if meta.freelist < boltStructuralGraphPageFloor || meta.freelist >= meta.pgid {
		return fmt.Errorf("%w: bbolt freelist page %d is out of range [2,%d): %s", repository.ErrCorruptRecord, meta.freelist, meta.pgid, path)
	}
	return nil
}

func validateBoltFreelistSpan(meta boltPreflightMeta, page boltPreflightPage, path string) (uint64, error) {
	span := uint64(page.overflow) + 1
	if span == 0 || span > meta.pgid || meta.freelist > meta.pgid-span {
		return 0, fmt.Errorf("%w: bbolt freelist page %d overflow %d exceeds high water mark %d: %s", repository.ErrCorruptRecord, meta.freelist, page.overflow, meta.pgid, path)
	}
	return span, nil
}

func validateBoltFreelistCount(meta boltPreflightMeta, page boltPreflightPage, span, spanBytes uint64, path string, readExtendedCount func() (uint64, error)) (boltFreelistPageInfo, error) {
	if spanBytes < boltPageHeaderSize {
		return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d span is shorter than a page header: %s", repository.ErrCorruptRecord, meta.freelist, path)
	}
	capacity := (spanBytes - boltPageHeaderSize) / 8
	count := uint64(page.count)
	requiredSlots := count
	idsOffset := uint64(boltPageHeaderSize)
	if page.count == 0xffff {
		if capacity == 0 {
			return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d extended count has no storage: %s", repository.ErrCorruptRecord, meta.freelist, path)
		}
		extendedCount, err := readExtendedCount()
		if err != nil {
			return boltFreelistPageInfo{}, err
		}
		count = extendedCount
		if count > uint64(^uint(0)>>1) {
			return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d extended count %d exceeds addressable memory: %s", repository.ErrCorruptRecord, meta.freelist, count, path)
		}
		if count == ^uint64(0) {
			return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d extended count overflows storage slots: %s", repository.ErrCorruptRecord, meta.freelist, path)
		}
		requiredSlots = count + 1
		idsOffset += 8
	}
	if requiredSlots > capacity {
		return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d count %d requires %d slots, capacity %d: %s", repository.ErrCorruptRecord, meta.freelist, count, requiredSlots, capacity, path)
	}
	if count > meta.pgid {
		return boltFreelistPageInfo{}, fmt.Errorf("%w: bbolt freelist page %d count %d exceeds high water mark %d: %s", repository.ErrCorruptRecord, meta.freelist, count, meta.pgid, path)
	}
	return boltFreelistPageInfo{
		span:      span,
		count:     count,
		idsOffset: idsOffset,
	}, nil
}

func validateBoltFreelistIDs(meta boltPreflightMeta, info boltFreelistPageInfo, reachable map[uint64]string, path string, readID func(index uint64) (uint64, error)) error {
	seen := make(map[uint64]struct{}, int(info.count))
	freelistEnd := meta.freelist + info.span
	for i := uint64(0); i < info.count; i++ {
		id, err := readID(i)
		if err != nil {
			return err
		}
		if id < boltStructuralGraphPageFloor || id >= meta.pgid {
			return fmt.Errorf("%w: bbolt freelist page %d entry %d page %d is out of range [2,%d): %s", repository.ErrCorruptRecord, meta.freelist, i, id, meta.pgid, path)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("%w: bbolt freelist page %d entry %d page %d is duplicated: %s", repository.ErrCorruptRecord, meta.freelist, i, id, path)
		}
		seen[id] = struct{}{}
		if id >= meta.freelist && id < freelistEnd {
			return fmt.Errorf("%w: bbolt freelist page %d entry %d lists freelist span page %d in [%d,%d): %s", repository.ErrCorruptRecord, meta.freelist, i, id, meta.freelist, freelistEnd, path)
		}
		if owner, ok := reachable[id]; ok {
			return fmt.Errorf("%w: bbolt freelist page %d entry %d page %d is reachable freed by %s: %s", repository.ErrCorruptRecord, meta.freelist, i, id, owner, path)
		}
	}
	return nil
}

func readBoltPreflightMeta(file *os.File, size int64) (boltPreflightMeta, bool, error) {
	return readBoltPreflightMetaForTx(file, size, -1)
}

func readBoltPreflightMetaForTx(file *os.File, size int64, txID int) (boltPreflightMeta, bool, error) {
	if meta, ok, err := readBoltPreflightMetaAt(file, 0, 0); err != nil {
		return boltPreflightMeta{}, false, err
	} else if ok {
		if second, secondOK, secondErr := readBoltPreflightMetaAt(file, 1, meta.pageSize); secondErr != nil {
			return boltPreflightMeta{}, false, secondErr
		} else if secondOK {
			if second.pageSize != meta.pageSize {
				return boltPreflightMeta{}, false, fmt.Errorf("%w: bbolt meta pages disagree on page size: page0=%d page1=%d", repository.ErrCorruptRecord, meta.pageSize, second.pageSize)
			}
			return selectBoltPreflightMeta(meta, second, txID), true, nil
		}
		if size >= 0 && uint64(size) < meta.pageSize+boltPageHeaderSize+boltMetaPayloadSize {
			return boltPreflightMeta{}, false, fmt.Errorf("%w: bbolt meta page 1 at offset %d exceeds file size %d", repository.ErrCorruptRecord, meta.pageSize, size)
		}
		return boltPreflightMeta{}, false, nil
	}
	return boltPreflightMeta{}, false, nil
}

func readBoltPreflightMetaAt(file *os.File, pageID, offset uint64) (boltPreflightMeta, bool, error) {
	buf := make([]byte, boltPageHeaderSize+boltMetaPayloadSize)
	n, err := file.ReadAt(buf, int64(offset))
	if err != nil && n != len(buf) {
		return boltPreflightMeta{}, false, nil
	}
	page := decodeBoltPreflightPage(buf)
	if page.id != pageID || page.flags != boltMetaPageFlag {
		return boltPreflightMeta{}, false, nil
	}
	meta := buf[boltPageHeaderSize:]
	if binary.LittleEndian.Uint32(meta[0:4]) != boltMagic || binary.LittleEndian.Uint32(meta[4:8]) != boltVersion {
		return boltPreflightMeta{}, false, nil
	}
	pageSize := uint64(binary.LittleEndian.Uint32(meta[8:12]))
	if !plausibleBoltPageSize(pageSize) {
		return boltPreflightMeta{}, false, nil
	}
	if !boltMetaChecksumMatches(meta) {
		return boltPreflightMeta{}, false, nil
	}
	return boltPreflightMeta{
		pageSize: pageSize,
		root:     binary.LittleEndian.Uint64(meta[16:24]),
		freelist: binary.LittleEndian.Uint64(meta[32:40]),
		pgid:     binary.LittleEndian.Uint64(meta[40:48]),
		txid:     binary.LittleEndian.Uint64(meta[48:56]),
	}, true, nil
}

func readBoltPreflightPage(file *os.File, pageID, pageSize uint64) (boltPreflightPage, error) {
	buf := make([]byte, boltPageHeaderSize)
	n, err := file.ReadAt(buf, int64(pageID*pageSize))
	if err != nil && n != len(buf) {
		return boltPreflightPage{}, err
	}
	if n != len(buf) {
		return boltPreflightPage{}, fmt.Errorf("short page header read: %d/%d", n, len(buf))
	}
	return decodeBoltPreflightPage(buf), nil
}

func decodeBoltPreflightPage(buf []byte) boltPreflightPage {
	return boltPreflightPage{
		id:       binary.LittleEndian.Uint64(buf[0:8]),
		flags:    binary.LittleEndian.Uint16(buf[8:10]),
		count:    binary.LittleEndian.Uint16(buf[10:12]),
		overflow: binary.LittleEndian.Uint32(buf[12:16]),
	}
}

func validBoltPageFlag(flag uint16) bool {
	switch flag {
	case boltBranchPageFlag, boltLeafPageFlag, boltMetaPageFlag, boltFreelistPageFlag:
		return true
	default:
		return false
	}
}

func plausibleBoltPageSize(size uint64) bool {
	return size >= boltMinPageSize && size <= boltMaxPageSize && size&(size-1) == 0
}

func boltMetaChecksumMatches(meta []byte) bool {
	if len(meta) < boltMetaPayloadSize {
		return false
	}
	hash := fnv.New64a()
	_, _ = hash.Write(meta[:56])
	return hash.Sum64() == binary.LittleEndian.Uint64(meta[56:64])
}

type boltStructuralGraphPreflight struct {
	path           string
	data           []byte
	pageSize       uint64
	hwm            uint64
	visited        map[uint64]string
	pageReferences uint64
	inlineBuckets  uint64
}

func preflightBoltBTreeGraphTx(tx *bolt.Tx, expectedIdentity FileIdentity) (err error) {
	if tx == nil || tx.DB() == nil {
		return nil
	}
	path := tx.DB().Path()
	previous := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(previous)
	defer func() {
		if recovered := recover(); recovered != nil {
			if boltPanicIsCorruption(recovered) {
				err = fmt.Errorf("%w: bbolt structural graph preflight fault for %s: %v", repository.ErrCorruptRecord, path, recovered)
				return
			}
			panic(recovered)
		}
	}()

	info := tx.DB().Info()
	if info == nil || info.Data == 0 {
		return fmt.Errorf("%w: bbolt structural graph preflight has no mmap data: %s", repository.ErrCorruptRecord, path)
	}
	meta, logicalSize, err := readBoltPreflightMetaForStructuralGraph(path, expectedIdentity, tx.ID())
	if err != nil {
		return err
	}

	data, releaseData, err := openBoltPreflightData(path, int64(logicalSize), expectedIdentity)
	if err != nil {
		return err
	}
	defer releaseData()

	meta, ok, err := readBoltPreflightMetaFromData(data, tx.ID())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: bbolt structural graph preflight could not read active meta: %s", repository.ErrCorruptRecord, path)
	}
	pageSize := meta.pageSize
	targetPgid := logicalSize / pageSize
	if meta.pgid != targetPgid {
		return fmt.Errorf("%w: bbolt structural graph preflight high water mark %d differs from tx pages %d: %s", repository.ErrCorruptRecord, meta.pgid, targetPgid, path)
	}
	if meta.pgid < boltStructuralGraphPageFloor {
		return fmt.Errorf("%w: bbolt structural graph preflight high water mark %d is invalid: %s", repository.ErrCorruptRecord, meta.pgid, path)
	}

	walk := boltStructuralGraphPreflight{
		path:     path,
		data:     data,
		pageSize: pageSize,
		hwm:      meta.pgid,
		visited:  make(map[uint64]string),
	}
	if err := walk.traversePage(meta.root, nil, 1); err != nil {
		return err
	}
	return walk.validateFreelist(meta)
}

func readBoltPreflightMetaForStructuralGraph(path string, expectedIdentity FileIdentity, txID int) (boltPreflightMeta, uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight open meta source for %s: %v", repository.ErrCorruptRecord, path, err)
	}
	defer file.Close()

	if expectedIdentity != (FileIdentity{}) {
		openedIdentity, err := fileIdentityFromFile(file)
		if err != nil {
			return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight stat meta source for %s: %v", repository.ErrCorruptRecord, path, err)
		}
		if openedIdentity != expectedIdentity {
			return boltPreflightMeta{}, 0, FileIdentityMismatchError{Path: path, Expected: expectedIdentity, Opened: openedIdentity}
		}
	}

	info, err := file.Stat()
	if err != nil {
		return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight stat meta source for %s: %v", repository.ErrCorruptRecord, path, err)
	}
	size := info.Size()
	if size < 0 {
		return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight file has invalid size %d: %s", repository.ErrCorruptRecord, size, path)
	}
	meta, ok, err := readBoltPreflightMetaForTx(file, size, txID)
	if err != nil {
		return boltPreflightMeta{}, 0, err
	}
	if !ok {
		return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight meta pages are invalid: %s", repository.ErrCorruptRecord, path)
	}
	if meta.pgid < boltStructuralGraphPageFloor {
		return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight high water mark %d is invalid: %s", repository.ErrCorruptRecord, meta.pgid, path)
	}
	fileSize := uint64(size)
	if meta.pageSize == 0 || fileSize/meta.pageSize < meta.pgid {
		return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight file is truncated: pages=%d page_size=%d size=%d: %s", repository.ErrCorruptRecord, meta.pgid, meta.pageSize, size, path)
	}
	if meta.pgid > ^uint64(0)/meta.pageSize {
		return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight size overflows: pages=%d page_size=%d: %s", repository.ErrCorruptRecord, meta.pgid, meta.pageSize, path)
	}
	logicalSize := meta.pgid * meta.pageSize
	if logicalSize > uint64(^uint(0)>>1) {
		return boltPreflightMeta{}, 0, fmt.Errorf("%w: bbolt structural graph preflight logical size %d exceeds addressable memory: %s", repository.ErrCorruptRecord, logicalSize, path)
	}
	return meta, logicalSize, nil
}

func readBoltPreflightMetaFromData(data []byte, txID int) (boltPreflightMeta, bool, error) {
	meta0, ok := decodeBoltPreflightMetaPage(data, 0)
	if !ok {
		return boltPreflightMeta{}, false, nil
	}
	start, end, ok := checkedBoltDataRange(1, meta0.pageSize, 1, uint64(len(data)))
	if !ok {
		return boltPreflightMeta{}, false, nil
	}
	meta1, ok := decodeBoltPreflightMetaPage(data[start:end], 1)
	if !ok {
		return boltPreflightMeta{}, false, nil
	}
	if meta1.pageSize != meta0.pageSize {
		return boltPreflightMeta{}, false, fmt.Errorf("%w: bbolt meta pages disagree on page size: page0=%d page1=%d", repository.ErrCorruptRecord, meta0.pageSize, meta1.pageSize)
	}
	return selectBoltPreflightMeta(meta0, meta1, txID), true, nil
}

func selectBoltPreflightMeta(meta0, meta1 boltPreflightMeta, txID int) boltPreflightMeta {
	if txID >= 0 {
		if meta0.txid == uint64(txID) {
			return meta0
		}
		if meta1.txid == uint64(txID) {
			return meta1
		}
	}
	if meta1.txid > meta0.txid {
		return meta1
	}
	return meta0
}

func decodeBoltPreflightMetaPage(pageBytes []byte, pageID uint64) (boltPreflightMeta, bool) {
	if len(pageBytes) < boltPageHeaderSize+boltMetaPayloadSize {
		return boltPreflightMeta{}, false
	}
	page := decodeBoltPreflightPage(pageBytes[:boltPageHeaderSize])
	if page.id != pageID || page.flags != boltMetaPageFlag {
		return boltPreflightMeta{}, false
	}
	meta := pageBytes[boltPageHeaderSize : boltPageHeaderSize+boltMetaPayloadSize]
	if binary.LittleEndian.Uint32(meta[0:4]) != boltMagic || binary.LittleEndian.Uint32(meta[4:8]) != boltVersion {
		return boltPreflightMeta{}, false
	}
	pageSize := uint64(binary.LittleEndian.Uint32(meta[8:12]))
	if !plausibleBoltPageSize(pageSize) {
		return boltPreflightMeta{}, false
	}
	if !boltMetaChecksumMatches(meta) {
		return boltPreflightMeta{}, false
	}
	return boltPreflightMeta{
		pageSize: pageSize,
		root:     binary.LittleEndian.Uint64(meta[16:24]),
		freelist: binary.LittleEndian.Uint64(meta[32:40]),
		pgid:     binary.LittleEndian.Uint64(meta[40:48]),
		txid:     binary.LittleEndian.Uint64(meta[48:56]),
	}, true
}

func (walk *boltStructuralGraphPreflight) traversePage(pgid uint64, stack []uint64, depth int) error {
	if pgid < boltStructuralGraphPageFloor || pgid >= walk.hwm {
		return fmt.Errorf("%w: bbolt structural graph preflight page %d is out of range [2,%d): %s", repository.ErrCorruptRecord, pgid, walk.hwm, walk.path)
	}
	if err := walk.requireDepth(depth, fmt.Sprintf("page %d", pgid)); err != nil {
		return err
	}
	for _, ancestor := range stack {
		if ancestor == pgid {
			return fmt.Errorf("%w: bbolt structural graph preflight cycle revisits page %d along stack %v: %s", repository.ErrCorruptRecord, pgid, append(stack, pgid), walk.path)
		}
	}
	if owner, ok := walk.visited[pgid]; ok {
		return fmt.Errorf("%w: bbolt structural graph preflight duplicate page reference %d previously claimed by %s: %s", repository.ErrCorruptRecord, pgid, owner, walk.path)
	}

	page, pageBytes, err := walk.readTreePage(pgid)
	if err != nil {
		return err
	}
	if err := walk.claimPageSpan(page); err != nil {
		return err
	}

	stack = append(stack, pgid)
	switch page.flags {
	case boltBranchPageFlag:
		return walk.scanBranchPage(page, pageBytes, stack, depth)
	case boltLeafPageFlag:
		return walk.scanLeafPage(page, pageBytes, stack, depth)
	default:
		return fmt.Errorf("%w: bbolt structural graph preflight page %d has invalid tree flags 0x%x: %s", repository.ErrCorruptRecord, pgid, page.flags, walk.path)
	}
}

func (walk *boltStructuralGraphPreflight) validateFreelist(meta boltPreflightMeta) error {
	if meta.freelist == boltNoFreelistID {
		return nil
	}
	if err := validateBoltFreelistPageID(meta, walk.path); err != nil {
		return err
	}
	start, headerEnd, ok := checkedBoltDataRange(meta.freelist, walk.pageSize, 1, uint64(len(walk.data)))
	if !ok || headerEnd-start < boltPageHeaderSize {
		return fmt.Errorf("%w: bbolt freelist page %d header is out of range: %s", repository.ErrCorruptRecord, meta.freelist, walk.path)
	}
	page := decodeBoltPreflightPage(walk.data[start : start+boltPageHeaderSize])
	if page.id != meta.freelist {
		return fmt.Errorf("%w: bbolt freelist page %d self-identifies as %d: %s", repository.ErrCorruptRecord, meta.freelist, page.id, walk.path)
	}
	if page.flags != boltFreelistPageFlag {
		return fmt.Errorf("%w: bbolt freelist page %d has flags 0x%x, want freelist: %s", repository.ErrCorruptRecord, meta.freelist, page.flags, walk.path)
	}
	span, err := validateBoltFreelistSpan(meta, page, walk.path)
	if err != nil {
		return err
	}
	start, end, ok := checkedBoltDataRange(meta.freelist, walk.pageSize, span, uint64(len(walk.data)))
	if !ok {
		return fmt.Errorf("%w: bbolt freelist page %d overflow %d is outside mapped data: %s", repository.ErrCorruptRecord, meta.freelist, page.overflow, walk.path)
	}
	pageBytes := walk.data[start:end]
	info, err := validateBoltFreelistCount(meta, page, span, uint64(len(pageBytes)), walk.path, func() (uint64, error) {
		return binary.LittleEndian.Uint64(pageBytes[boltPageHeaderSize : boltPageHeaderSize+8]), nil
	})
	if err != nil {
		return err
	}
	return validateBoltFreelistIDs(meta, info, walk.visited, walk.path, func(index uint64) (uint64, error) {
		offset := info.idsOffset + index*8
		return binary.LittleEndian.Uint64(pageBytes[offset : offset+8]), nil
	})
}

func (walk *boltStructuralGraphPreflight) readTreePage(pgid uint64) (boltPreflightPage, []byte, error) {
	start, headerEnd, ok := checkedBoltDataRange(pgid, walk.pageSize, 1, uint64(len(walk.data)))
	if !ok || headerEnd-start < boltPageHeaderSize {
		return boltPreflightPage{}, nil, fmt.Errorf("%w: bbolt structural graph preflight page %d header is out of range: %s", repository.ErrCorruptRecord, pgid, walk.path)
	}
	page := decodeBoltPreflightPage(walk.data[start : start+boltPageHeaderSize])
	if page.id != pgid {
		return boltPreflightPage{}, nil, fmt.Errorf("%w: bbolt structural graph preflight page %d self-identifies as %d: %s", repository.ErrCorruptRecord, pgid, page.id, walk.path)
	}
	if page.flags != boltBranchPageFlag && page.flags != boltLeafPageFlag {
		return boltPreflightPage{}, nil, fmt.Errorf("%w: bbolt structural graph preflight page %d has invalid tree flags 0x%x: %s", repository.ErrCorruptRecord, pgid, page.flags, walk.path)
	}
	span := uint64(page.overflow) + 1
	if span == 0 || span > walk.hwm || pgid > walk.hwm-span {
		return boltPreflightPage{}, nil, fmt.Errorf("%w: bbolt structural graph preflight page %d overflow %d exceeds high water mark %d: %s", repository.ErrCorruptRecord, pgid, page.overflow, walk.hwm, walk.path)
	}
	start, end, ok := checkedBoltDataRange(pgid, walk.pageSize, span, uint64(len(walk.data)))
	if !ok {
		return boltPreflightPage{}, nil, fmt.Errorf("%w: bbolt structural graph preflight page %d overflow %d is outside mapped data: %s", repository.ErrCorruptRecord, pgid, page.overflow, walk.path)
	}
	return page, walk.data[start:end], nil
}

func (walk *boltStructuralGraphPreflight) claimPageSpan(page boltPreflightPage) error {
	span := uint64(page.overflow) + 1
	if span == 0 || span > walk.hwm || page.id > walk.hwm-span {
		return fmt.Errorf("%w: bbolt structural graph preflight page %d overflow %d exceeds high water mark %d: %s", repository.ErrCorruptRecord, page.id, page.overflow, walk.hwm, walk.path)
	}
	if err := walk.consumePageReferences(span, fmt.Sprintf("page %d overflow span", page.id)); err != nil {
		return err
	}
	for i := uint64(0); i < span; i++ {
		id := page.id + i
		if owner, ok := walk.visited[id]; ok {
			return fmt.Errorf("%w: bbolt structural graph preflight duplicate page reference %d previously claimed by %s: %s", repository.ErrCorruptRecord, id, owner, walk.path)
		}
		walk.visited[id] = fmt.Sprintf("page %d", page.id)
	}
	return nil
}

func (walk *boltStructuralGraphPreflight) consumePageReferences(count uint64, owner string) error {
	if count == 0 {
		return nil
	}
	if count > walk.hwm || walk.pageReferences > walk.hwm-count {
		return fmt.Errorf("%w: bbolt structural graph preflight exceeded page-reference bound %d at %s: %s", repository.ErrCorruptRecord, walk.hwm, owner, walk.path)
	}
	walk.pageReferences += count
	return nil
}

func (walk *boltStructuralGraphPreflight) requireDepth(depth int, owner string) error {
	if depth <= 0 || depth > boltStructuralGraphMaxDepth {
		return fmt.Errorf("%w: bbolt structural graph preflight exceeded depth bound %d at %s: %s", repository.ErrCorruptRecord, boltStructuralGraphMaxDepth, owner, walk.path)
	}
	return nil
}

func (walk *boltStructuralGraphPreflight) scanBranchPage(page boltPreflightPage, pageBytes []byte, stack []uint64, depth int) error {
	tableEnd, err := validateBoltPageElementTable(page, pageBytes, boltBranchPageElementSize, "branch", walk.path)
	if err != nil {
		return err
	}
	var previousKey []byte
	for i := 0; i < int(page.count); i++ {
		elem := pageBytes[boltPageHeaderSize+i*boltBranchPageElementSize : boltPageHeaderSize+(i+1)*boltBranchPageElementSize]
		pos := binary.LittleEndian.Uint32(elem[0:4])
		ksize := binary.LittleEndian.Uint32(elem[4:8])
		child := binary.LittleEndian.Uint64(elem[8:16])
		keyStart, keyEnd, err := boltElementPayloadBounds(pageBytes, tableEnd, i, boltBranchPageElementSize, pos, ksize, 0)
		if err != nil {
			return fmt.Errorf("%w: bbolt structural graph preflight branch page %d element %d key bounds: %v: %s", repository.ErrCorruptRecord, page.id, i, err, walk.path)
		}
		key := pageBytes[keyStart:keyEnd]
		if previousKey != nil && bytes.Compare(previousKey, key) >= 0 {
			return fmt.Errorf("%w: bbolt structural graph preflight branch page %d key %d is not strictly ordered: %s", repository.ErrCorruptRecord, page.id, i, walk.path)
		}
		previousKey = key
		if err := walk.traversePage(child, stack, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func (walk *boltStructuralGraphPreflight) scanLeafPage(page boltPreflightPage, pageBytes []byte, stack []uint64, depth int) error {
	return walk.scanLeafElements(page, pageBytes, stack, depth, fmt.Sprintf("leaf page %d", page.id))
}

func (walk *boltStructuralGraphPreflight) scanInlineBucketPage(pageBytes []byte, stack []uint64, depth int, owner string) error {
	if err := walk.requireDepth(depth, "inline bucket "+owner); err != nil {
		return err
	}
	if err := walk.consumeInlineBucketReference("inline bucket " + owner); err != nil {
		return err
	}
	if len(pageBytes) < boltPageHeaderSize {
		return fmt.Errorf("%w: bbolt structural graph preflight inline bucket %s is shorter than a page header: %s", repository.ErrCorruptRecord, owner, walk.path)
	}
	page := decodeBoltPreflightPage(pageBytes[:boltPageHeaderSize])
	if page.flags != boltLeafPageFlag {
		return fmt.Errorf("%w: bbolt structural graph preflight inline bucket %s has page flags 0x%x, want leaf: %s", repository.ErrCorruptRecord, owner, page.flags, walk.path)
	}
	if page.overflow != 0 {
		return fmt.Errorf("%w: bbolt structural graph preflight inline bucket %s has overflow %d: %s", repository.ErrCorruptRecord, owner, page.overflow, walk.path)
	}
	return walk.scanLeafElements(page, pageBytes, stack, depth, "inline bucket "+owner)
}

func (walk *boltStructuralGraphPreflight) consumeInlineBucketReference(owner string) error {
	limit := walk.hwm
	if schemaFloor := uint64(len(bucketNames)); limit < schemaFloor {
		limit = schemaFloor
	}
	if walk.inlineBuckets >= limit {
		return fmt.Errorf("%w: bbolt structural graph preflight exceeded page-reference bound %d at %s: %s", repository.ErrCorruptRecord, limit, owner, walk.path)
	}
	walk.inlineBuckets++
	return nil
}

func (walk *boltStructuralGraphPreflight) scanLeafElements(page boltPreflightPage, pageBytes []byte, stack []uint64, depth int, label string) error {
	tableEnd, err := validateBoltPageElementTable(page, pageBytes, boltLeafPageElementSize, label, walk.path)
	if err != nil {
		return err
	}
	var previousKey []byte
	for i := 0; i < int(page.count); i++ {
		elem := pageBytes[boltPageHeaderSize+i*boltLeafPageElementSize : boltPageHeaderSize+(i+1)*boltLeafPageElementSize]
		flags := binary.LittleEndian.Uint32(elem[0:4])
		pos := binary.LittleEndian.Uint32(elem[4:8])
		ksize := binary.LittleEndian.Uint32(elem[8:12])
		vsize := binary.LittleEndian.Uint32(elem[12:16])
		keyStart, keyEnd, err := boltElementPayloadBounds(pageBytes, tableEnd, i, boltLeafPageElementSize, pos, ksize, vsize)
		if err != nil {
			return fmt.Errorf("%w: bbolt structural graph preflight %s element %d payload bounds: %v: %s", repository.ErrCorruptRecord, label, i, err, walk.path)
		}
		key := pageBytes[keyStart:keyEnd]
		if previousKey != nil && bytes.Compare(previousKey, key) >= 0 {
			return fmt.Errorf("%w: bbolt structural graph preflight %s key %d is not strictly ordered: %s", repository.ErrCorruptRecord, label, i, walk.path)
		}
		previousKey = key
		if flags&boltBucketLeafFlag == 0 {
			continue
		}
		value := pageBytes[keyEnd : keyEnd+int(vsize)]
		if len(value) < boltBucketHeaderSize {
			return fmt.Errorf("%w: bbolt structural graph preflight %s bucket element %d value is shorter than bucket header: %s", repository.ErrCorruptRecord, label, i, walk.path)
		}
		root := binary.LittleEndian.Uint64(value[0:8])
		if root == 0 {
			if len(value) == boltBucketHeaderSize {
				return fmt.Errorf("%w: bbolt structural graph preflight %s bucket element %d has no inline page: %s", repository.ErrCorruptRecord, label, i, walk.path)
			}
			if err := walk.scanInlineBucketPage(value[boltBucketHeaderSize:], stack, depth+1, fmt.Sprintf("%s element %d", label, i)); err != nil {
				return err
			}
			continue
		}
		if err := walk.traversePage(root, stack, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateBoltPageElementTable(page boltPreflightPage, pageBytes []byte, elemSize int, label, path string) (int, error) {
	tableSize := uint64(page.count) * uint64(elemSize)
	if tableSize > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("%w: bbolt structural graph preflight %s page %d element table is too large: %s", repository.ErrCorruptRecord, label, page.id, path)
	}
	need := uint64(boltPageHeaderSize) + tableSize
	if need < uint64(boltPageHeaderSize) || need > uint64(len(pageBytes)) {
		return 0, fmt.Errorf("%w: bbolt structural graph preflight %s page %d element table exceeds page bounds: %s", repository.ErrCorruptRecord, label, page.id, path)
	}
	return int(need), nil
}

func boltElementPayloadBounds(pageBytes []byte, tableEnd int, index int, elemSize int, pos, ksize, vsize uint32) (int, int, error) {
	if ksize == 0 {
		return 0, 0, fmt.Errorf("zero-length key")
	}
	elemStart := uint64(boltPageHeaderSize + index*elemSize)
	payloadStart := elemStart + uint64(pos)
	payloadSize := uint64(ksize) + uint64(vsize)
	payloadEnd := payloadStart + payloadSize
	if payloadStart < elemStart || payloadEnd < payloadStart || payloadEnd > uint64(len(pageBytes)) {
		return 0, 0, fmt.Errorf("payload range [%d,%d) exceeds page size %d", payloadStart, payloadEnd, len(pageBytes))
	}
	if payloadStart < uint64(tableEnd) {
		return 0, 0, fmt.Errorf("payload starts at %d before element table end %d", payloadStart, tableEnd)
	}
	keyEnd := payloadStart + uint64(ksize)
	if keyEnd < payloadStart || keyEnd > payloadEnd {
		return 0, 0, fmt.Errorf("key range [%d,%d) exceeds payload end %d", payloadStart, keyEnd, payloadEnd)
	}
	if payloadStart > uint64(^uint(0)>>1) || keyEnd > uint64(^uint(0)>>1) {
		return 0, 0, fmt.Errorf("payload range [%d,%d) exceeds addressable memory", payloadStart, keyEnd)
	}
	return int(payloadStart), int(keyEnd), nil
}

func checkedBoltDataRange(pgid, pageSize, span, dataLen uint64) (int, int, bool) {
	if pageSize == 0 || span == 0 {
		return 0, 0, false
	}
	if pgid > ^uint64(0)/pageSize || span > ^uint64(0)/pageSize {
		return 0, 0, false
	}
	start := pgid * pageSize
	size := span * pageSize
	if start > ^uint64(0)-size {
		return 0, 0, false
	}
	end := start + size
	if start > dataLen || end > dataLen {
		return 0, 0, false
	}
	if start > uint64(^uint(0)>>1) || end > uint64(^uint(0)>>1) {
		return 0, 0, false
	}
	return int(start), int(end), true
}

func boltPanicIsCorruption(recovered any) bool {
	if recovered == nil {
		return false
	}
	if _, ok := recovered.(interface {
		runtime.Error
		Addr() uintptr
	}); ok {
		return true
	}
	err, ok := recovered.(error)
	return ok && boltErrorIsCorruption(err)
}

func boltErrorIsCorruption(err error) bool {
	return errors.Is(err, bolt.ErrInvalid) ||
		errors.Is(err, bolt.ErrInvalidMapping) ||
		errors.Is(err, bolt.ErrVersionMismatch) ||
		errors.Is(err, bolt.ErrChecksum)
}

func boltCheckErrorIsCorruption(err error) bool {
	if err == nil {
		return false
	}
	if boltErrorIsCorruption(err) {
		return true
	}
	return !boltErrorIsOperational(err)
}

func boltErrorIsOperational(err error) bool {
	return errors.Is(err, bolt.ErrDatabaseNotOpen) ||
		errors.Is(err, bolt.ErrTimeout) ||
		errors.Is(err, bolt.ErrTxNotWritable) ||
		errors.Is(err, bolt.ErrTxClosed) ||
		errors.Is(err, bolt.ErrDatabaseReadOnly) ||
		errors.Is(err, bolt.ErrFreePagesNotLoaded)
}

func openBoltSafely(path string, mode os.FileMode, options *bolt.Options) (db *bolt.DB, err error) {
	previous := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(previous)
	defer func() {
		if recovered := recover(); recovered != nil {
			if db != nil {
				_ = db.Close()
				db = nil
			}
			if boltPanicIsCorruption(recovered) {
				err = fmt.Errorf("%w: bbolt open fault for %s: %v", repository.ErrCorruptRecord, path, recovered)
				return
			}
			panic(recovered)
		}
	}()
	return bolt.Open(path, mode, options)
}

func existingOpenError(path string, err error) error {
	if err == nil || errors.Is(err, os.ErrNotExist) || errors.Is(err, bolt.ErrTimeout) || errors.Is(err, repository.ErrInvalidRecord) || errors.Is(err, repository.ErrCorruptRecord) {
		return err
	}
	return fmt.Errorf("%w: structural bbolt open failed for %s: %v", repository.ErrCorruptRecord, path, err)
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
		return auditIntegrityTx(tx, r.fileIdentity)
	})
}

func auditIntegrityTx(tx *bolt.Tx, expectedIdentity FileIdentity) error {
	read := readTx{tx: tx}
	var findings []error
	if err := checkStructuralIntegrityTx(tx, expectedIdentity); err != nil {
		findings = append(findings, repository.NewIntegrityFinding("structure", "", err))
		return repository.NewIntegrityError(findings)
	}
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
		image := read.LoadJob(jobID)
		if err := repository.ValidateJobClosure(jobID, image, read.LookupRequest); err != nil {
			findings = append(findings, repository.NewIntegrityFinding(auditJobIntegrityFindingKind(image, err), jobID.String(), err))
		}
	}
	if err := read.auditBindingIndexEntries(); err != nil {
		findings = append(findings, repository.NewIntegrityFinding("binding_index", "", err))
	}
	return repository.NewIntegrityError(findings)
}

func auditJobIntegrityFindingKind(image repository.JobImage, err error) string {
	if auditJobProjectionRepairableFinding(image, err) {
		return "projection"
	}
	return "job"
}

func auditJobProjectionRepairableFinding(image repository.JobImage, err error) bool {
	if err == nil || image.Safety.State != repository.RecordValid {
		return false
	}
	record := image.Safety.Value
	if image.Binding.State != repository.RecordValid || image.Binding.Value.JobID != record.JobID {
		return false
	}
	if err := image.Binding.Value.Matches(record); err != nil {
		return false
	}
	switch image.Projection.State {
	case repository.RecordMissing:
		return strings.Contains(err.Error(), "has no projection")
	case repository.RecordCorrupt:
		var corrupt repository.CorruptRecordKindError
		return errors.As(err, &corrupt) && corrupt.Kind == "projection"
	case repository.RecordValid:
		if image.Projection.Value.JobID != record.JobID {
			return strings.Contains(err.Error(), "projection key mismatch")
		}
		return errors.Is(err, repository.ErrProjectionMismatch)
	default:
		return false
	}
}

func checkStructuralIntegrityTx(tx *bolt.Tx, expectedIdentity FileIdentity) (err error) {
	if tx == nil {
		return nil
	}
	path := ""
	if tx.DB() != nil {
		path = tx.DB().Path()
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if boltPanicIsCorruption(recovered) {
				err = fmt.Errorf("%w: bbolt structural integrity check panic for %s: %v", repository.ErrCorruptRecord, path, recovered)
				return
			}
			panic(recovered)
		}
	}()
	if err := preflightBoltBTreeGraphTx(tx, expectedIdentity); err != nil {
		return err
	}
	var findings []error
	for checkErr := range tx.Check() {
		if checkErr != nil {
			if !boltCheckErrorIsCorruption(checkErr) {
				return fmt.Errorf("bbolt structural integrity check failed for %s: %w", path, checkErr)
			}
			findings = append(findings, checkErr)
		}
	}
	if err := repository.NewIntegrityError(findings); err != nil {
		return fmt.Errorf("%w: bbolt structural integrity check failed for %s: %w", repository.ErrCorruptRecord, path, err)
	}
	return nil
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

func (r *Repository) InjectCorruptProjectionForTest(jobID model.JobID, diagnostic string) {
	if err := jobID.Validate(); err != nil {
		panic(err)
	}
	r.injectCorruptRecordForTest(bucketProjections, jobIDKey(jobID), "projection", diagnostic)
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

func (r *Repository) initializeFresh() error {
	return r.db.Update(func(tx *bolt.Tx) error {
		if !databaseEmpty(tx) {
			return fmt.Errorf("%w: admission repository create target is not fresh: %s", repository.ErrInvalidRecord, r.db.Path())
		}
		for _, name := range bucketNames {
			r.countOperationForTest("create_bucket")
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		if initializeFreshForTest != nil {
			if err := initializeFreshForTest(); err != nil {
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
			AdmissionRoot: repository.AdmissionRootMetadata{
				ContractVersion: repository.CurrentAdmissionContractVersion,
			},
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
	seen, err := tx.auditLiveJobIDSet()
	var findings []error
	if err != nil {
		findings = append(findings, err)
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
			findings = append(findings, err)
		}
	}
	return seen, repository.NewIntegrityError(findings)
}

func (tx readTx) auditLiveJobIDSet() (map[model.JobID]struct{}, error) {
	seen := map[model.JobID]struct{}{}
	var findings []error
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
			switch string(bucketName) {
			case string(bucketSafety):
				if _, diagnostic := decodeEnvelope(kindSafety, key, raw, validateSafety, revisionSafety); diagnostic != "" {
					findings = append(findings, repository.CorruptRecordError("safety", jobID.String(), diagnostic))
					seen[jobID] = struct{}{}
					return nil
				}
			case string(bucketProjections):
				if _, diagnostic := decodeEnvelope(kindProjection, key, raw, validateProjectionForLoad, revisionProjection); diagnostic != "" {
					findings = append(findings, repository.CorruptRecordError("projection", jobID.String(), diagnostic))
					seen[jobID] = struct{}{}
					return nil
				}
			case string(bucketQuarantine):
				if _, diagnostic := decodeEnvelope(kindQuarantine, key, raw, validateQuarantine, revisionQuarantine); diagnostic != "" {
					findings = append(findings, repository.CorruptRecordError("quarantine", jobID.String(), diagnostic))
					seen[jobID] = struct{}{}
					return nil
				}
			}
			if bytes.Equal(bucketName, bucketBindingIndex) {
				if err := tx.validateBindingIndexEntry(jobID, raw); err != nil {
					findings = append(findings, err)
					return nil
				}
			}
			seen[jobID] = struct{}{}
			return nil
		}); err != nil {
			return seen, err
		}
	}
	bindings := tx.tx.Bucket(bucketBindings)
	if bindings != nil {
		tx.count("foreach:bindings")
		if err := bindings.ForEach(func(key, raw []byte) error {
			binding, diagnostic := decodeEnvelope(kindBinding, key, raw, validateBinding, revisionBinding)
			if diagnostic == "" {
				if err := tx.validateBindingIndexForRequest(binding.RequestKey); err != nil {
					findings = append(findings, err)
				}
				seen[binding.JobID] = struct{}{}
			}
			return nil
		}); err != nil {
			return seen, err
		}
	}
	return seen, repository.NewIntegrityError(findings)
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
	var findings []error
	if err != nil {
		findings = append(findings, err)
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
			findings = append(findings, err)
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
	return requests, jobs, repository.NewIntegrityError(findings)
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
	canonical := tx.LookupRequest(indexedKey).Binding
	if canonical.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("binding", indexedKey.String(), canonical.Diagnostic)
	}
	if canonical.State != repository.RecordValid {
		return repository.CorruptRecordError("binding_index", binding.JobID.String(), "references missing binding")
	}
	if !reflect.DeepEqual(canonical.Value, binding) {
		return repository.CorruptRecordError("binding_index", binding.JobID.String(), "index entry does not match canonical binding")
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
	next := projection
	next.FinalAttemptStartedAt = clonePtr(projection.FinalAttemptStartedAt)
	next.FinalAttemptEndedAt = clonePtr(projection.FinalAttemptEndedAt)
	return next
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
	next.Outcome = cloneOutcomeFact(record.Outcome)
	next.Result = clonePtr(record.Result)
	next.Terminal = cloneTerminalCertificate(record.Terminal)
	next.FinalAttemptStartedAt = clonePtr(record.FinalAttemptStartedAt)
	next.FinalAttemptEndedAt = clonePtr(record.FinalAttemptEndedAt)
	return next
}

func cloneOutcomeFact(fact *model.OutcomeFact) *model.OutcomeFact {
	if fact == nil {
		return nil
	}
	copied := *fact
	copied.Contract = cloneContractStamp(fact.Contract)
	return &copied
}

func cloneTerminalCertificate(certificate *model.TerminalCertificate) *model.TerminalCertificate {
	if certificate == nil {
		return nil
	}
	copied := *certificate
	copied.Result = clonePtr(certificate.Result)
	copied.Contract = cloneContractStamp(certificate.Contract)
	return &copied
}

func cloneContractStamp(stamp *engine.ContractStamp) *engine.ContractStamp {
	if stamp == nil {
		return nil
	}
	copied := *stamp
	copied.Missing = append([]string(nil), stamp.Missing...)
	return &copied
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
	if err := model.ValidateFinalAttemptTiming(projection.FinalAttemptStartedAt, projection.FinalAttemptEndedAt); err != nil {
		return fmt.Errorf("%w: projection final attempt timing: %v", repository.ErrInvalidRecord, err)
	}
	if (projection.FinalAttemptStartedAt != nil || projection.FinalAttemptEndedAt != nil) && projection.Decision != model.DecisionTerminal {
		return fmt.Errorf("%w: projection final attempt timing requires terminal decision", repository.ErrInvalidRecord)
	}
	if err := model.ValidateFailureMetadata(projection.FailureClass, projection.FailureReason); err != nil {
		return fmt.Errorf("%w: projection failure metadata: %v", repository.ErrInvalidRecord, err)
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
