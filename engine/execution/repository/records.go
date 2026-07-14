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

const CurrentAuthorityMetaSchemaVersion = uint16(1)

type AuthorityMeta struct {
	SchemaVersion   uint16
	Generation      uint64
	NextJobSequence uint64
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
	return nil
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
