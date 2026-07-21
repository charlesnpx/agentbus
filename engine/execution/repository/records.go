package repository

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type RecordState uint8

const (
	RecordMissing RecordState = iota
	RecordValid
	RecordCorrupt
)

func (state RecordState) String() string {
	switch state {
	case RecordMissing:
		return "missing"
	case RecordValid:
		return "valid"
	case RecordCorrupt:
		return "corrupt"
	default:
		return ""
	}
}

type Record[T any] struct {
	State      RecordState
	Value      T
	Diagnostic string
}

func MissingRecord[T any]() Record[T] {
	return Record[T]{State: RecordMissing}
}

func ValidRecord[T any](value T) Record[T] {
	return Record[T]{State: RecordValid, Value: value}
}

func CorruptRecord[T any](diagnostic string) Record[T] {
	return Record[T]{State: RecordCorrupt, Diagnostic: diagnostic}
}

func (record Record[T]) Valid() bool {
	return record.State == RecordValid
}

func (record Record[T]) Missing() bool {
	return record.State == RecordMissing
}

func (record Record[T]) Corrupt() bool {
	return record.State == RecordCorrupt
}

const (
	CurrentAuthorityMetaSchemaVersion = uint16(1)
	CurrentAdmissionContractVersion   = uint16(1)
)

type AdmissionRootMetadata struct {
	Activated       bool   `json:"activated"`
	ContractVersion uint16 `json:"contractVersion"`
	ActivatedAtGen  uint64 `json:"activatedAtGen"`
}

func (metadata AdmissionRootMetadata) Validate() error {
	if !metadata.Activated {
		if metadata.ActivatedAtGen != 0 {
			return fmt.Errorf("%w: admission_root.activated_at_gen must be zero before activation", ErrInvalidRecord)
		}
		return nil
	}
	if metadata.ContractVersion == 0 {
		return fmt.Errorf("%w: admission_root.contract_version is required after activation", ErrInvalidRecord)
	}
	if metadata.ActivatedAtGen == 0 {
		return fmt.Errorf("%w: admission_root.activated_at_gen is required after activation", ErrInvalidRecord)
	}
	return nil
}

type AuthorityMeta struct {
	SchemaVersion       uint16
	Generation          uint64
	NextJobSequence     uint64
	AdmissionRoot       AdmissionRootMetadata
	Sealed              bool
	SuccessorDomainUUID string
	SuccessorStateRoot  string
}

func (meta AuthorityMeta) Validate() error {
	if meta.SchemaVersion == 0 {
		return fmt.Errorf("%w: meta.schema_version is required", ErrInvalidRecord)
	}
	if meta.SchemaVersion != CurrentAuthorityMetaSchemaVersion {
		return fmt.Errorf("%w: meta.schema_version %d is unsupported", ErrInvalidRecord, meta.SchemaVersion)
	}
	if meta.NextJobSequence == 0 {
		return fmt.Errorf("%w: meta.next_job_sequence is required", ErrInvalidRecord)
	}
	if err := meta.AdmissionRoot.Validate(); err != nil {
		return err
	}
	if !meta.Sealed {
		if strings.TrimSpace(meta.SuccessorDomainUUID) != "" {
			return fmt.Errorf("%w: meta.successor_domain_uuid requires sealed root", ErrInvalidRecord)
		}
		if strings.TrimSpace(meta.SuccessorStateRoot) != "" {
			return fmt.Errorf("%w: meta.successor_state_root requires sealed root", ErrInvalidRecord)
		}
		return nil
	}
	if strings.TrimSpace(meta.SuccessorDomainUUID) == "" {
		return fmt.Errorf("%w: meta.successor_domain_uuid is required when sealed", ErrInvalidRecord)
	}
	return nil
}

func ValidateAuthorityMetaPut(current Record[AuthorityMeta], next AuthorityMeta, currentGeneration, currentNextJobSequence uint64, stats AuthorityRootStats) error {
	if err := next.Validate(); err != nil {
		return err
	}
	if current.State == RecordCorrupt {
		return fmt.Errorf("%w: meta is corrupt: %s", ErrCorruptRecord, current.Diagnostic)
	}
	if current.State != RecordValid && !stats.Empty() {
		return fmt.Errorf("%w: meta is %s on non-empty authority root: %s", ErrCorruptRecord, current.State, strings.Join(nonzeroAuthorityRootStats(stats), ", "))
	}
	if next.Generation != currentGeneration {
		return fmt.Errorf("%w: meta generation %d does not match current generation %d", ErrInvalidRecord, next.Generation, currentGeneration)
	}
	if next.NextJobSequence < currentNextJobSequence {
		return fmt.Errorf("%w: meta.next_job_sequence cannot move backwards", ErrInvalidRecord)
	}
	if current.State != RecordValid {
		return nil
	}
	// Admission-root metadata is one-way inside a live authority domain:
	// activation and sealing never clear, ActivatedAtGen never changes once set,
	// a declared/activated ContractVersion cannot be forged by later PutMeta,
	// and sealed successor identity is pinned forever once set.
	return validateAuthorityMetaTransition(current.Value, next, currentGeneration)
}

func validateAuthorityMetaTransition(current, next AuthorityMeta, currentGeneration uint64) error {
	if current.AdmissionRoot.Activated && !next.AdmissionRoot.Activated {
		return fmt.Errorf("%w: admission_root.activated is one-way and cannot be cleared", ErrInvalidRecord)
	}
	if current.Sealed && !next.Sealed {
		return fmt.Errorf("%w: meta.sealed is one-way and cannot be cleared", ErrInvalidRecord)
	}
	if current.Sealed && current.SuccessorDomainUUID != "" && next.SuccessorDomainUUID != current.SuccessorDomainUUID {
		return fmt.Errorf("%w: meta.successor_domain_uuid is immutable once set", ErrInvalidRecord)
	}
	if current.Sealed && current.SuccessorStateRoot != "" && next.SuccessorStateRoot != current.SuccessorStateRoot {
		return fmt.Errorf("%w: meta.successor_state_root is immutable once set", ErrInvalidRecord)
	}
	if current.AdmissionRoot.ActivatedAtGen != 0 && next.AdmissionRoot.ActivatedAtGen != current.AdmissionRoot.ActivatedAtGen {
		return fmt.Errorf("%w: admission_root.activated_at_gen is immutable once set", ErrInvalidRecord)
	}
	if current.AdmissionRoot.ContractVersion != 0 && next.AdmissionRoot.ContractVersion != current.AdmissionRoot.ContractVersion {
		return fmt.Errorf("%w: admission_root.contract_version is immutable once declared", ErrInvalidRecord)
	}
	if !current.AdmissionRoot.Activated && next.AdmissionRoot.Activated {
		committedGeneration := currentGeneration + 1
		if next.AdmissionRoot.ActivatedAtGen != committedGeneration {
			return fmt.Errorf("%w: admission_root.activated_at_gen %d does not match activation commit generation %d", ErrInvalidRecord, next.AdmissionRoot.ActivatedAtGen, committedGeneration)
		}
	}
	return nil
}

type AuthorityRootStats struct {
	Jobs                int `json:"jobs"`
	Bindings            int `json:"bindings"`
	Tombstones          int `json:"tombstones"`
	LaunchRecords       int `json:"launchRecords"`
	RecoveryObligations int `json:"recoveryObligations"`
}

func (stats AuthorityRootStats) Empty() bool {
	return stats.Jobs == 0 &&
		stats.Bindings == 0 &&
		stats.Tombstones == 0 &&
		stats.LaunchRecords == 0 &&
		stats.RecoveryObligations == 0
}

func nonzeroAuthorityRootStats(stats AuthorityRootStats) []string {
	var parts []string
	if stats.Jobs != 0 {
		parts = append(parts, fmt.Sprintf("jobs=%d", stats.Jobs))
	}
	if stats.Bindings != 0 {
		parts = append(parts, fmt.Sprintf("bindings=%d", stats.Bindings))
	}
	if stats.Tombstones != 0 {
		parts = append(parts, fmt.Sprintf("tombstones=%d", stats.Tombstones))
	}
	if stats.LaunchRecords != 0 {
		parts = append(parts, fmt.Sprintf("launch_records=%d", stats.LaunchRecords))
	}
	if stats.RecoveryObligations != 0 {
		parts = append(parts, fmt.Sprintf("recovery_obligations=%d", stats.RecoveryObligations))
	}
	if len(parts) == 0 {
		parts = append(parts, "empty")
	}
	return parts
}

type Tombstone struct {
	RequestKey        model.RequestKey
	JobID             model.JobID
	TaskIdentity      model.TaskIdentity
	ExpiredGeneration uint64
}

func (tombstone Tombstone) Validate() error {
	if err := tombstone.RequestKey.Validate(); err != nil {
		return fmt.Errorf("%w: tombstone.request_key: %v", ErrInvalidRecord, err)
	}
	if err := tombstone.JobID.Validate(); err != nil {
		return fmt.Errorf("%w: tombstone.job_id: %v", ErrInvalidRecord, err)
	}
	if err := tombstone.TaskIdentity.Validate(); err != nil {
		return fmt.Errorf("%w: tombstone.task_identity: %v", ErrInvalidRecord, err)
	}
	if tombstone.ExpiredGeneration == 0 {
		return fmt.Errorf("%w: tombstone.expired_generation is required", ErrInvalidRecord)
	}
	return nil
}

type QuarantineRecord struct {
	JobID      model.JobID
	Diagnostic string
	Generation uint64
}

func (record QuarantineRecord) Validate() error {
	if err := record.JobID.Validate(); err != nil {
		return fmt.Errorf("%w: quarantine.job_id: %v", ErrInvalidRecord, err)
	}
	if strings.TrimSpace(record.Diagnostic) == "" {
		return fmt.Errorf("%w: quarantine.diagnostic is required", ErrInvalidRecord)
	}
	if !utf8.ValidString(record.Diagnostic) || strings.ContainsAny(record.Diagnostic, "\x00\r\n") {
		return fmt.Errorf("%w: quarantine.diagnostic must be valid single-line UTF-8", ErrInvalidRecord)
	}
	if record.Generation == 0 {
		return fmt.Errorf("%w: quarantine.generation is required", ErrInvalidRecord)
	}
	return nil
}

type RequestImage struct {
	Binding   Record[model.Binding]
	Tombstone Record[Tombstone]
}

type JobImage struct {
	Binding    Record[model.Binding]
	Safety     Record[model.SafetyRecord]
	Projection Record[model.JobProjection]
	Quarantine Record[QuarantineRecord]
}

type JobFilter struct {
	BootID          model.BootID
	NonterminalOnly bool
}
