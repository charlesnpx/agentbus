package served

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestServedCoordinatorRecordsQuiescenceBeforeLatchTripOnCleanupFailure(t *testing.T) {
	ctx := context.Background()
	cleanupErr := errors.New("served containment cleanup failed")
	events := &servedCoordinatorEventLog{}
	repo := memory.NewRepository()
	issuer, verifier := custodian.NewAttestationChannel()
	bootstrapper, err := authority.NewBootstrapper(repo, authority.WithQuiescenceVerifier(verifier))
	if err != nil {
		t.Fatal(err)
	}
	owner := model.OwnerID("owner-served-containment-order")
	boot, err := model.NewBootRef("boot-served-containment-order", string(owner))
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ready.AcceptAndClaim(ctx, authority.AcceptRequest{
		RequestKey:   model.RequestKey{WorkspaceKey: "workspace-served-containment-order", RequestID: "request-served-containment-order"},
		TaskIdentity: model.NewSHA256TaskIdentity([]byte("served-containment-order")),
		Mode:         model.ModeIdentifiedFenced,
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	launchKey := model.LaunchKey{Attempt: accepted.Record.Attempt.Ref, Ordinal: model.LaunchOrdinalOne}
	group := admissionTestGroup(launchKey)
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, err := ready.CommitGrant(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.LaunchOrdinalOne, "permit-served-containment-order"); err != nil {
		t.Fatal(err)
	}
	launchController, err := launch.New(servedCoordinatorOrderLaunchAuthority{}, servedCoordinatorOrderCustodian{
		issuer:     issuer,
		cleanupErr: cleanupErr,
		events:     events,
	})
	if err != nil {
		t.Fatal(err)
	}
	latch := NewSafetyLatch()
	coord, err := coordinator.New(&servedCoordinatorOrderAuthority{
		ready:  ready,
		latch:  latch,
		events: events,
	}, servedCoordinatorLaunchContainment{controller: launchController}, servedCoordinatorOrderResults{}, owner)
	if err != nil {
		t.Fatal(err)
	}

	err = coord.Cancel(ctx, accepted.Record.JobID, nil)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("Cancel error = %v, want cleanup failure", err)
	}
	select {
	case <-latch.Done():
	default:
		t.Fatal("safety latch was not tripped")
	}
	wantEvents := "contain,record_quiescence,latch_trip"
	if got := strings.Join(events.snapshot(), ","); got != wantEvents {
		t.Fatalf("events = %s, want %s", got, wantEvents)
	}
	var record model.SafetyRecord
	if err := repo.View(ctx, func(tx repository.ReadTx) error {
		image := tx.LoadJob(accepted.Record.JobID)
		if image.Safety.State != repository.RecordValid {
			return fmt.Errorf("safety state = %s", image.Safety.State)
		}
		record = image.Safety.Value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Quiescence == nil {
		t.Fatalf("launch quiescence = %+v, want recorded before latch trip", first)
	}
}

type servedCoordinatorEventLog struct {
	events []string
}

func (log *servedCoordinatorEventLog) add(event string) {
	log.events = append(log.events, event)
}

func (log *servedCoordinatorEventLog) snapshot() []string {
	return append([]string(nil), log.events...)
}

type servedCoordinatorOrderAuthority struct {
	ready  *authority.Ready
	latch  *SafetyLatch
	events *servedCoordinatorEventLog
}

func (a *servedCoordinatorOrderAuthority) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordQuiescence(ctx, jobID, ordinal, verified)
	step, err := admissionStepResult(applied, err)
	if err == nil {
		a.events.add("record_quiescence")
	}
	return step, err
}

func (a *servedCoordinatorOrderAuthority) RequestCancel(ctx context.Context, jobID model.JobID) (coordinator.StepResult, error) {
	applied, err := a.ready.RequestCancel(ctx, jobID)
	return admissionStepResult(applied, err)
}

func (a *servedCoordinatorOrderAuthority) RecordOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, outcome model.Outcome) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordOutcome(ctx, jobID, ref, outcome)
	return admissionStepResult(applied, err)
}

func (a *servedCoordinatorOrderAuthority) RecordResult(ctx context.Context, jobID model.JobID, ref model.AttemptRef, receipt model.ResultReceipt) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordResult(ctx, jobID, ref, receipt)
	return admissionStepResult(applied, err)
}

func (a *servedCoordinatorOrderAuthority) Finalize(ctx context.Context, jobID model.JobID, ref model.AttemptRef, intent model.TerminalIntent) (coordinator.StepResult, error) {
	applied, err := a.ready.Finalize(ctx, jobID, ref, intent)
	return admissionStepResult(applied, err)
}

func (a *servedCoordinatorOrderAuthority) Snapshot(ctx context.Context, jobID model.JobID) (coordinator.JobSnapshot, error) {
	image, err := a.ready.LoadJob(ctx, jobID)
	if err != nil {
		return coordinator.JobSnapshot{}, err
	}
	if image.Safety.State != repository.RecordValid {
		return coordinator.JobSnapshot{}, fmt.Errorf("safety state = %s", image.Safety.State)
	}
	if image.Projection.State != repository.RecordValid {
		return coordinator.JobSnapshot{}, fmt.Errorf("projection state = %s", image.Projection.State)
	}
	return coordinator.JobSnapshot{Record: image.Safety.Value, Projection: image.Projection.Value}, nil
}

func (a *servedCoordinatorOrderAuthority) RecoveryPlan(ctx context.Context, jobID model.JobID, trigger model.RecoveryTrigger) (model.RecoveryPlan, error) {
	snapshot, err := a.Snapshot(ctx, jobID)
	if err != nil {
		return model.RecoveryPlan{}, err
	}
	return model.PlanRecovery(snapshot.Record, trigger)
}

func (a *servedCoordinatorOrderAuthority) HasOwnedWork(context.Context) (bool, error) {
	return true, nil
}

func (a *servedCoordinatorOrderAuthority) FailStop(ctx context.Context, err error) error {
	var stopErr error
	if err == nil {
		stopErr = a.ready.FailStop(ctx, "")
	} else {
		stopErr = a.ready.FailStop(ctx, err.Error())
	}
	if stopErr == nil {
		a.events.add("latch_trip")
		a.latch.Trip(err)
	}
	return stopErr
}

type servedCoordinatorOrderCustodian struct {
	issuer     custodian.AttestationIssuer
	cleanupErr error
	events     *servedCoordinatorEventLog
}

func (c servedCoordinatorOrderCustodian) Prepare(context.Context, command.ExecSpec, model.LaunchKey) (launch.PreparedProcess, error) {
	return nil, errors.New("unexpected prepare")
}

func (c servedCoordinatorOrderCustodian) ContainAndVerify(_ context.Context, group model.GroupRef, _ custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	c.events.add("contain")
	verified, err := c.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: group, Method: model.QuiescenceTermKill})
	if err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return verified, custodian.CleanupStatus{Err: c.cleanupErr}, nil
}

type servedCoordinatorOrderLaunchAuthority struct{}

func (servedCoordinatorOrderLaunchAuthority) BindGroup(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.GroupRef) (launch.DurabilityOutcome, error) {
	return launch.CommittedAndAnchored, nil
}

func (servedCoordinatorOrderLaunchAuthority) AllocateGrant(context.Context, model.AttemptRef, model.LaunchOrdinal) (model.LaunchGrant, launch.DurabilityOutcome, error) {
	return model.LaunchGrant{}, launch.CommittedAndAnchored, nil
}

func (servedCoordinatorOrderLaunchAuthority) RecordReleaseOutcome(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.LaunchReleaseOutcome) (launch.DurabilityOutcome, error) {
	return launch.CommittedAndAnchored, nil
}

func (servedCoordinatorOrderLaunchAuthority) RecordRelease(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.ChildIdentity, model.Evidence) (launch.DurabilityOutcome, error) {
	return launch.CommittedAndAnchored, nil
}

func (servedCoordinatorOrderLaunchAuthority) RecordQuiescence(context.Context, model.JobID, model.LaunchOrdinal, custodian.VerifiedQuiescence) (launch.DurabilityOutcome, error) {
	return launch.CommittedAndAnchored, nil
}

func (servedCoordinatorOrderLaunchAuthority) FailStop(context.Context, error) error {
	return nil
}

type servedCoordinatorOrderResults struct{}

func (servedCoordinatorOrderResults) Publish(context.Context, model.JobID, []byte) (model.ResultReceipt, error) {
	return model.ResultReceipt{}, errors.New("unexpected result publish")
}

func (servedCoordinatorOrderResults) Verify(context.Context, model.ResultRef) (model.ResultReceipt, error) {
	return model.ResultReceipt{}, errors.New("unexpected result verify")
}

type servedCoordinatorOrderRunning struct{}

func (servedCoordinatorOrderRunning) Ref() model.GroupRef {
	return model.GroupRef{}
}

func (servedCoordinatorOrderRunning) Stdin() io.WriteCloser {
	return nil
}

func (servedCoordinatorOrderRunning) Stdout() io.ReadCloser {
	return nil
}

func (servedCoordinatorOrderRunning) Stderr() io.ReadCloser {
	return nil
}

func (servedCoordinatorOrderRunning) WaitAndVerify(context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	return command.ExitObservation{}, custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, errors.New("unexpected wait")
}

func (servedCoordinatorOrderRunning) ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, errors.New("unexpected running contain")
}
