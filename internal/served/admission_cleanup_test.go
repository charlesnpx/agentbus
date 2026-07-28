package served

import (
	"context"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestAuthorityStatusAndResultExposeDerivedCleanupDisposition(t *testing.T) {
	ctx := context.Background()
	ready := newServedCleanupReady(t, "cleanup-disposition")
	accepted := acceptServedCleanupJob(t, ctx, ready, "cleanup-disposition")
	record := terminalizeServedCleanupOrphaned(t, ctx, ready, accepted)

	image, err := ready.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	status, ok, err := authorityStatusFromImage(image)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("authorityStatusFromImage returned ok=false")
	}
	if status.State != engine.StateOrphaned || status.CleanupDisposition != model.CleanupDispositionUnresolved.String() {
		t.Fatalf("status = %+v, want orphaned unresolved cleanup", status)
	}

	server := &Server{admissionReady: ready, admissionInstance: &admissionInstance{}}
	result, ok, errObj := server.authorityResult(record.JobID.String())
	if errObj != nil {
		t.Fatalf("authorityResult error = %+v", errObj)
	}
	if !ok {
		t.Fatal("authorityResult returned ok=false")
	}
	if result.State != engine.StateOrphaned || result.CleanupDisposition != model.CleanupDispositionUnresolved.String() {
		t.Fatalf("result = %+v, want orphaned unresolved cleanup", result)
	}
}

func newServedCleanupReady(t *testing.T, name string) *authority.Ready {
	t.Helper()
	_, verifier := custodian.NewAttestationChannel()
	bootstrapper, err := authority.NewBootstrapper(memory.NewRepository(), authority.WithQuiescenceVerifier(verifier))
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-served-"+name, "owner-served-"+name)
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Begin(context.Background(), boot)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

func acceptServedCleanupJob(t *testing.T, ctx context.Context, ready *authority.Ready, name string) authority.AcceptResult {
	t.Helper()
	key, err := model.NewRequestKey("workspace-"+name, "request-"+name)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ready.Accept(ctx, authority.AcceptRequest{
		RequestKey:   key,
		TaskIdentity: model.NewSHA256TaskIdentity([]byte("task-" + name)),
		SessionID:    "session-" + name,
	})
	if err != nil {
		t.Fatal(err)
	}
	return accepted
}

func terminalizeServedCleanupOrphaned(t *testing.T, ctx context.Context, ready *authority.Ready, accepted authority.AcceptResult) model.SafetyRecord {
	t.Helper()
	ref := accepted.Record.Attempt.Ref
	ordinal := model.LaunchOrdinalOne
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: ordinal})
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, ordinal, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ready.AllocateGrant(ctx, ref, ordinal); err != nil {
		t.Fatal(err)
	}
	finalized, err := ready.Finalize(ctx, accepted.Record.JobID, ref, model.TerminalIntent{
		Outcome: model.OutcomeOrphaned,
		Cause:   model.CauseDaemonRestartedAfterAuthorization,
	})
	if err != nil {
		t.Fatal(err)
	}
	return finalized.Record
}
