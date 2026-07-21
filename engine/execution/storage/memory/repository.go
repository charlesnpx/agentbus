package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

var nextMemoryDBUUID uint64

type Repository struct {
	mu    sync.Mutex
	state storeState
}

func NewRepository() *Repository {
	return &Repository{state: newStoreState()}
}

func NewRepositoryFromSnapshotBytes(data []byte) (*Repository, error) {
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	state, err := storeStateFromSnapshot(snap)
	if err != nil {
		return nil, err
	}
	return &Repository{state: state}, nil
}

func (r *Repository) View(ctx context.Context, fn func(repository.ReadTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(readTx{state: &r.state})
}

func (r *Repository) Update(ctx context.Context, fn func(repository.WriteTx) error) (commit repository.Commit, err error) {
	if err := ctx.Err(); err != nil {
		return repository.Commit{}, fmt.Errorf("%w: %w", repository.ErrDefinitelyNotCommitted, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return repository.Commit{}, fmt.Errorf("%w: %w", repository.ErrDefinitelyNotCommitted, err)
	}

	next := r.state.clone()
	tx := &writeTx{readTx: readTx{state: &next}}
	defer func() {
		if recovered := recover(); recovered != nil {
			commit = repository.Commit{Generation: r.state.generation}
			err = fmt.Errorf("%w: %w: %v", repository.ErrDefinitelyNotCommitted, repository.ErrTransactionPanic, recovered)
		}
	}()

	if err := fn(tx); err != nil {
		return repository.Commit{Generation: r.state.generation}, fmt.Errorf("%w: %w", repository.ErrDefinitelyNotCommitted, err)
	}
	if !tx.changed {
		return repository.Commit{Generation: r.state.generation}, nil
	}
	if err := next.validateForCommit(); err != nil {
		return repository.Commit{Generation: r.state.generation}, fmt.Errorf("%w: %w", repository.ErrDefinitelyNotCommitted, err)
	}
	next.advanceGeneration()
	r.state = next
	return repository.Commit{Generation: r.state.generation}, nil
}

func (r *Repository) SnapshotBytes() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, err := json.Marshal(snapshotState(r.state))
	if err != nil {
		panic(err)
	}
	return data
}

func (r *Repository) AnchorIdentity() (string, uint16, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.dbUUID == "" {
		return "", 0, fmt.Errorf("%w: db_uuid is missing", repository.ErrInvalidRecord)
	}
	if r.state.meta.state != repository.RecordValid {
		return "", 0, fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, r.state.meta.state)
	}
	return r.state.dbUUID, r.state.meta.value.SchemaVersion, nil
}

func (r *Repository) InjectCorruptSafetyForTest(jobID model.JobID, diagnostic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.safety[jobID] = corruptSlot[model.SafetyRecord](diagnostic)
}

func (r *Repository) InjectMissingSafetyForTest(jobID model.JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.state.safety, jobID)
}

func (r *Repository) InjectCorruptProjectionForTest(jobID model.JobID, diagnostic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.projections[jobID] = corruptSlot[model.JobProjection](diagnostic)
}

func (r *Repository) InjectMissingProjectionForTest(jobID model.JobID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.state.projections, jobID)
}

func (r *Repository) InjectCorruptBindingForTest(key model.RequestKey, diagnostic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.bindings[key] = corruptSlot[model.Binding](diagnostic)
}

func (r *Repository) InjectMissingBindingForTest(key model.RequestKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.state.bindings, key)
}

func (r *Repository) InjectProjectionForTest(projection model.JobProjection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.projections[projection.JobID] = validSlot(cloneProjection(projection))
}

func (r *Repository) InjectTombstoneForTest(tombstone repository.Tombstone) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.tombstones[tombstone.RequestKey] = validSlot(cloneTombstone(tombstone))
}

func (r *Repository) InjectCorruptMetaForTest(diagnostic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.meta = corruptSlot[repository.AuthorityMeta](diagnostic)
}

func (r *Repository) InjectMissingMetaForTest() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.meta = recordSlot[repository.AuthorityMeta]{}
}

func (r *Repository) InjectDBUUIDForTest(uuid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.dbUUID = uuid
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
	dbUUID          string
	generation      uint64
	nextJobSequence uint64
	meta            recordSlot[repository.AuthorityMeta]
	bindings        map[model.RequestKey]recordSlot[model.Binding]
	tombstones      map[model.RequestKey]recordSlot[repository.Tombstone]
	safety          map[model.JobID]recordSlot[model.SafetyRecord]
	projections     map[model.JobID]recordSlot[model.JobProjection]
	quarantines     map[model.JobID]recordSlot[repository.QuarantineRecord]
}

func newStoreState() storeState {
	state := storeState{
		dbUUID:          fmt.Sprintf("memory-db-%020d", atomic.AddUint64(&nextMemoryDBUUID, 1)),
		generation:      0,
		nextJobSequence: 1,
		bindings:        map[model.RequestKey]recordSlot[model.Binding]{},
		tombstones:      map[model.RequestKey]recordSlot[repository.Tombstone]{},
		safety:          map[model.JobID]recordSlot[model.SafetyRecord]{},
		projections:     map[model.JobID]recordSlot[model.JobProjection]{},
		quarantines:     map[model.JobID]recordSlot[repository.QuarantineRecord]{},
	}
	state.syncMeta()
	return state
}

func (s storeState) clone() storeState {
	return storeState{
		dbUUID:          s.dbUUID,
		generation:      s.generation,
		nextJobSequence: s.nextJobSequence,
		meta:            cloneSlot(s.meta, cloneMeta),
		bindings:        cloneMap(s.bindings, cloneBinding),
		tombstones:      cloneMap(s.tombstones, cloneTombstone),
		safety:          cloneMap(s.safety, cloneSafetyRecord),
		projections:     cloneMap(s.projections, cloneProjection),
		quarantines:     cloneMap(s.quarantines, cloneQuarantine),
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

func corruptRecordError(kind, key, diagnostic string) error {
	if diagnostic == "" {
		diagnostic = "corrupt"
	}
	return fmt.Errorf("%w: %s %s: %s", repository.ErrCorruptRecord, kind, key, diagnostic)
}

type snapshot struct {
	DBUUID          string                                       `json:"dbUUID"`
	Generation      uint64                                       `json:"generation"`
	NextJobSequence uint64                                       `json:"nextJobSequence"`
	Meta            repository.Record[repository.AuthorityMeta]  `json:"meta"`
	Bindings        []snapshotEntry[model.Binding]               `json:"bindings"`
	Tombstones      []snapshotEntry[repository.Tombstone]        `json:"tombstones"`
	Safety          []snapshotEntry[model.SafetyRecord]          `json:"safety"`
	Projections     []snapshotEntry[model.JobProjection]         `json:"projections"`
	Quarantines     []snapshotEntry[repository.QuarantineRecord] `json:"quarantines"`
}

type snapshotEntry[T any] struct {
	Key    string               `json:"key"`
	Record repository.Record[T] `json:"record"`
}

func snapshotState(state storeState) snapshot {
	return snapshot{
		DBUUID:          state.dbUUID,
		Generation:      state.generation,
		NextJobSequence: state.nextJobSequence,
		Meta:            state.metaRecord(),
		Bindings:        snapshotRequestMap(state.bindings, cloneBinding),
		Tombstones:      snapshotRequestMap(state.tombstones, cloneTombstone),
		Safety:          snapshotJobMap(state.safety, cloneSafetyRecord),
		Projections:     snapshotJobMap(state.projections, cloneProjection),
		Quarantines:     snapshotJobMap(state.quarantines, cloneQuarantine),
	}
}

func storeStateFromSnapshot(snap snapshot) (storeState, error) {
	if snap.DBUUID == "" {
		return storeState{}, fmt.Errorf("%w: snapshot.db_uuid is missing", repository.ErrInvalidRecord)
	}
	if snap.NextJobSequence == 0 {
		return storeState{}, fmt.Errorf("%w: snapshot.next_job_sequence is missing", repository.ErrInvalidRecord)
	}
	meta, err := slotFromRecord(snap.Meta, cloneMeta)
	if err != nil {
		return storeState{}, fmt.Errorf("snapshot.meta: %w", err)
	}
	state := storeState{
		dbUUID:          snap.DBUUID,
		generation:      snap.Generation,
		nextJobSequence: snap.NextJobSequence,
		meta:            meta,
		bindings:        map[model.RequestKey]recordSlot[model.Binding]{},
		tombstones:      map[model.RequestKey]recordSlot[repository.Tombstone]{},
		safety:          map[model.JobID]recordSlot[model.SafetyRecord]{},
		projections:     map[model.JobID]recordSlot[model.JobProjection]{},
		quarantines:     map[model.JobID]recordSlot[repository.QuarantineRecord]{},
	}
	for _, entry := range snap.Bindings {
		key, err := parseSnapshotRequestKey(entry.Key)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.binding %q: %w", entry.Key, err)
		}
		slot, err := slotFromRecord(entry.Record, cloneBinding)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.binding %q: %w", entry.Key, err)
		}
		state.bindings[key] = slot
	}
	for _, entry := range snap.Tombstones {
		key, err := parseSnapshotRequestKey(entry.Key)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.tombstone %q: %w", entry.Key, err)
		}
		slot, err := slotFromRecord(entry.Record, cloneTombstone)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.tombstone %q: %w", entry.Key, err)
		}
		state.tombstones[key] = slot
	}
	for _, entry := range snap.Safety {
		key, err := model.NewJobID(entry.Key)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.safety %q: %w", entry.Key, err)
		}
		slot, err := slotFromRecord(entry.Record, cloneSafetyRecord)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.safety %q: %w", entry.Key, err)
		}
		state.safety[key] = slot
	}
	for _, entry := range snap.Projections {
		key, err := model.NewJobID(entry.Key)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.projection %q: %w", entry.Key, err)
		}
		slot, err := slotFromRecord(entry.Record, cloneProjection)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.projection %q: %w", entry.Key, err)
		}
		state.projections[key] = slot
	}
	for _, entry := range snap.Quarantines {
		key, err := model.NewJobID(entry.Key)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.quarantine %q: %w", entry.Key, err)
		}
		slot, err := slotFromRecord(entry.Record, cloneQuarantine)
		if err != nil {
			return storeState{}, fmt.Errorf("snapshot.quarantine %q: %w", entry.Key, err)
		}
		state.quarantines[key] = slot
	}
	return state, nil
}

func slotFromRecord[T any](record repository.Record[T], clone func(T) T) (recordSlot[T], error) {
	switch record.State {
	case repository.RecordMissing:
		return recordSlot[T]{}, nil
	case repository.RecordValid:
		return validSlot(clone(record.Value)), nil
	case repository.RecordCorrupt:
		return corruptSlot[T](record.Diagnostic), nil
	default:
		return recordSlot[T]{}, fmt.Errorf("%w: unknown record state %d", repository.ErrInvalidRecord, record.State)
	}
}

func parseSnapshotRequestKey(value string) (model.RequestKey, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return model.RequestKey{}, fmt.Errorf("%w: request key %q is not workspace/request", repository.ErrInvalidRecord, value)
	}
	return model.NewRequestKey(parts[0], parts[1])
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
