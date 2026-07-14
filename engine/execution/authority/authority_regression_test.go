package authority

import (
	"context"
	"errors"
	"testing"
)

var errNotImplemented = errors.New("admission authority skeleton: implemented in a later commit")

type AcceptanceCommand struct {
	WorkspaceKey                        string
	RequestID                           string
	TaskIdentity                        string
	InjectValidationFailureAfterBinding bool
}

type AcceptanceResult struct {
	JobID     string
	AttemptID string
	Epoch     int64
}

type TerminalCommand struct {
	JobID                   string
	AttemptID               string
	Epoch                   int64
	Outcome                 string
	RequestedProof          string
	InjectValidationFailure bool
}

type TerminalResult struct {
	JobID         string
	TerminalProof string
}

type AttemptRef struct {
	JobID     string
	AttemptID string
	Epoch     int64
}

type PermitCertainty string

const (
	PermitCertaintyUnknownMissing PermitCertainty = "unknown_missing"
	PermitCertaintyNeverPermitted PermitCertainty = "never_permitted"
	PermitCertaintyMaybePermitted PermitCertainty = "maybe_permitted"
	PermitCertaintyPermitConsumed PermitCertainty = "permit_consumed"
)

type Snapshot struct {
	JobCount              int
	BindingCount          int
	TerminalMutationCount int
}

type AdmissionAuthority interface {
	Accept(context.Context, AcceptanceCommand) (AcceptanceResult, error)
	PublishTerminal(context.Context, TerminalCommand) (TerminalResult, error)
	ClassifyPermitCertainty(context.Context, AttemptRef) (PermitCertainty, error)
	Snapshot(context.Context) (Snapshot, error)
}

type InMemoryAuthority struct{}

func NewInMemoryAuthority() *InMemoryAuthority {
	return &InMemoryAuthority{}
}

func (a *InMemoryAuthority) Accept(context.Context, AcceptanceCommand) (AcceptanceResult, error) {
	return AcceptanceResult{}, errNotImplemented
}

func (a *InMemoryAuthority) PublishTerminal(context.Context, TerminalCommand) (TerminalResult, error) {
	return TerminalResult{}, errNotImplemented
}

func (a *InMemoryAuthority) ClassifyPermitCertainty(context.Context, AttemptRef) (PermitCertainty, error) {
	return "", errNotImplemented
}

func (a *InMemoryAuthority) Snapshot(context.Context) (Snapshot, error) {
	return Snapshot{}, errNotImplemented
}

type legacyReady struct {
	bootID string
}

type Coordinator struct {
	authority AdmissionAuthority
	ready     legacyReady
}

func NewCoordinator(authority AdmissionAuthority, ready legacyReady) (*Coordinator, error) {
	if authority == nil {
		return nil, errors.New("admission authority is required")
	}
	if ready.bootID == "" {
		return nil, errors.New("ready capability is required")
	}
	return &Coordinator{authority: authority, ready: ready}, nil
}

func TestFailedAcceptanceLeavesNoJobOrBinding(t *testing.T) {
	authority := NewInMemoryAuthority()
	t.Skip("implemented in Commit 2: transactional acceptance rollback")

	_, err := authority.Accept(context.Background(), AcceptanceCommand{
		WorkspaceKey:                        "ws-rollback",
		RequestID:                           "req-rollback",
		TaskIdentity:                        "task-a",
		InjectValidationFailureAfterBinding: true,
	})
	if err == nil {
		t.Fatal("Accept succeeded, want validation failure")
	}
	snapshot, err := authority.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.JobCount != 0 || snapshot.BindingCount != 0 {
		t.Fatalf("snapshot after failed acceptance = jobs %d bindings %d, want 0/0", snapshot.JobCount, snapshot.BindingCount)
	}
}

func TestFailedTerminalValidationLeavesNoTerminalMutation(t *testing.T) {
	authority := NewInMemoryAuthority()
	t.Skip("implemented in Commit 2: terminal validation rollback")

	accepted, err := authority.Accept(context.Background(), AcceptanceCommand{
		WorkspaceKey: "ws-terminal-rollback",
		RequestID:    "req-terminal-rollback",
		TaskIdentity: "task-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := authority.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = authority.PublishTerminal(context.Background(), TerminalCommand{
		JobID:                   accepted.JobID,
		AttemptID:               accepted.AttemptID,
		Epoch:                   accepted.Epoch,
		Outcome:                 "canceled",
		RequestedProof:          "NeverPermittedAndRetired",
		InjectValidationFailure: true,
	})
	if err == nil {
		t.Fatal("PublishTerminal succeeded, want validation failure")
	}
	after, err := authority.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.TerminalMutationCount != before.TerminalMutationCount {
		t.Fatalf("terminal mutations = %d, want unchanged %d", after.TerminalMutationCount, before.TerminalMutationCount)
	}
}

func TestMissingIndependentProofCannotMeanNeverPermitted(t *testing.T) {
	authority := NewInMemoryAuthority()
	t.Skip("implemented in Commit 2: explicit missing/corrupt proof classification")

	certainty, err := authority.ClassifyPermitCertainty(context.Background(), AttemptRef{
		JobID:     "job-missing-proof",
		AttemptID: "attempt-1",
		Epoch:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if certainty == PermitCertaintyNeverPermitted {
		t.Fatal("missing proof classified as never permitted")
	}
	if certainty != PermitCertaintyUnknownMissing {
		t.Fatalf("certainty = %s, want %s", certainty, PermitCertaintyUnknownMissing)
	}
}

func TestCoordinatorConstructibleAgainstAuthorityFake(t *testing.T) {
	var fake AdmissionAuthority = fakeAuthority{}
	t.Skip("implemented in Commit 3: coordinator accepts AdmissionAuthority")

	coordinator, err := NewCoordinator(fake, legacyReady{bootID: "boot-1"})
	if err != nil {
		t.Fatal(err)
	}
	if coordinator.authority == nil {
		t.Fatal("coordinator was not wired to the authority")
	}
}

type fakeAuthority struct{}

func (fakeAuthority) Accept(context.Context, AcceptanceCommand) (AcceptanceResult, error) {
	return AcceptanceResult{}, errors.New("not called")
}

func (fakeAuthority) PublishTerminal(context.Context, TerminalCommand) (TerminalResult, error) {
	return TerminalResult{}, errors.New("not called")
}

func (fakeAuthority) ClassifyPermitCertainty(context.Context, AttemptRef) (PermitCertainty, error) {
	return "", errors.New("not called")
}

func (fakeAuthority) Snapshot(context.Context) (Snapshot, error) {
	return Snapshot{}, errors.New("not called")
}
