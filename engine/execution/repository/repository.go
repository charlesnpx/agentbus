package repository

import (
	"context"
	"errors"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

var (
	ErrCASMismatch        = errors.New("repository cas mismatch")
	ErrConflict           = errors.New("repository conflict")
	ErrCorruptRecord      = errors.New("repository corrupt record")
	ErrInvalidRecord      = errors.New("repository invalid record")
	ErrTransactionPanic   = errors.New("repository transaction panic")
	ErrProjectionMismatch = errors.New("repository projection mismatch")
	ErrAmbiguousCommit    = errors.New("repository ambiguous commit")
)

type Commit struct {
	Generation uint64
}

type Repository interface {
	View(context.Context, func(ReadTx) error) error
	Update(context.Context, func(WriteTx) error) (Commit, error)
}

type ReadTx interface {
	Meta() Record[AuthorityMeta]
	LookupRequest(model.RequestKey) RequestImage
	LoadJob(model.JobID) JobImage
	ListJobs(JobFilter) ([]JobImage, error)
	ListNonterminalByBoot(model.BootID) ([]JobImage, error)
}

type WriteTx interface {
	ReadTx
	AllocateJobID() (model.JobID, error)
	PutMeta(AuthorityMeta) error
	PutBinding(model.Binding) error
	PutSafety(model.SafetyRecord, uint64) error
	PutProjection(model.JobProjection) error
	PutQuarantine(QuarantineRecord) error
	PutTombstone(Tombstone) error
	DeleteLiveJob(model.JobID) error
}
