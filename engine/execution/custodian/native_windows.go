//go:build windows

package custodian

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

var (
	ErrNativeCustodianUnavailable        = errors.New("native custodian unavailable")
	ErrPhysicalContainment               = errors.New("physical containment failed")
	ErrNativeRuntimeSelfTestRequired     = errors.New("native runtime self-test required")
	ErrRetainedContainmentUnavailable    = errors.New("retained containment unavailable")
	ErrRetainedObjectReacquireUnresolved = errors.New("retained object reacquire unresolved")
	ErrNativeRuntimeUnsupported          = errors.New("native runtime unsupported")
	ErrNativeRuntimeSelfTestRetry        = errors.New("native runtime self-test retryable")
	ErrNativeRuntimeSelfTestUnsafe       = errors.New("native runtime self-test unsafe")
)

// Windows deliberately reports unsupported rather than treating a Windows job
// object as equivalent to the Unix process-group custody protocol. The latter
// cannot establish this package's required leader identity and group-wide
// absence proof at the same seam.
func windowsNativeRuntimeUnsupported() error {
	return fmt.Errorf("%w: Windows has no implementation of the required process-group custody protocol", ErrNativeRuntimeUnsupported)
}

type PhysicalOutcomeKind string

const (
	PhysicalOutcomeAbsent     PhysicalOutcomeKind = "absent"
	PhysicalOutcomeUnprovable PhysicalOutcomeKind = "unprovable"
)

// PhysicalOutcome is deliberately not a quiescence attestation. It reports
// only the local physical result of attempting to make one exact process group
// absent.
type PhysicalOutcome struct {
	Kind     PhysicalOutcomeKind
	Group    model.GroupRef
	Method   model.QuiescenceMethod
	Reason   containment.UnprovableReason
	Decision model.ContainmentDecision
	Err      error
}

func (outcome PhysicalOutcome) Absent() bool {
	return outcome.Kind == PhysicalOutcomeAbsent
}

func (outcome PhysicalOutcome) Unprovable() bool {
	return outcome.Kind == PhysicalOutcomeUnprovable
}

type RetainedObjectReacquireUnresolvedError struct {
	Group model.GroupRef
	Cause error
}

func (err RetainedObjectReacquireUnresolvedError) Error() string {
	if err.Cause == nil {
		return fmt.Sprintf("%s: retained_id=%q", ErrRetainedObjectReacquireUnresolved, err.Group.RetainedID)
	}
	return fmt.Sprintf("%s: retained_id=%q: %v", ErrRetainedObjectReacquireUnresolved, err.Group.RetainedID, err.Cause)
}

func (err RetainedObjectReacquireUnresolvedError) Is(target error) bool {
	return target == ErrRetainedObjectReacquireUnresolved
}

func (err RetainedObjectReacquireUnresolvedError) Unwrap() error {
	return err.Cause
}

type NativeOptions struct {
	AgentbusPath      string
	MonitorCommand    parklaunch.CommandSpec
	ContainmentParams containment.Params
	WorkerEnv         []string
	WorkerDir         string
}

type NativeLaunchSpec struct {
	Exec      command.ExecSpec
	CustodyID model.CustodyID
	LaunchKey model.LaunchKey
}

// NativeCustodian has no constructible Windows implementation. Its methods
// remain present so embedded consumers compile on Windows and receive the
// typed unsupported error rather than a false custody success.
type NativeCustodian struct{}

func NewNativeCustodian(NativeOptions) (*NativeCustodian, error) {
	return nil, windowsNativeRuntimeUnsupported()
}

func NewNativeRuntime(NativeOptions) (Runtime, error) {
	err := windowsNativeRuntimeUnsupported()
	return NewUnavailableRuntime(err), err
}

func (*NativeCustodian) processCustodian() {}

func (*NativeCustodian) ActiveCustodyCount() int {
	return 0
}

func (*NativeCustodian) Prepare(context.Context, command.ExecSpec, model.LaunchKey) (PreparedProcess, error) {
	return nil, windowsNativeRuntimeUnsupported()
}

func (*NativeCustodian) Launch(context.Context, NativeLaunchSpec) (*NativeRunningProcess, error) {
	return nil, windowsNativeRuntimeUnsupported()
}

func (custodian *NativeCustodian) ContainPhysical(_ context.Context, group model.GroupRef) PhysicalOutcome {
	return windowsUnsupportedPhysicalOutcome(group)
}

func (custodian *NativeCustodian) ContainAndVerify(ctx context.Context, group model.GroupRef, _ QuiescenceCause) (VerifiedQuiescence, CleanupStatus, error) {
	return VerifiedQuiescence{}, CleanupStatus{}, custodian.ContainPhysical(ctx, group).Err
}

func (*NativeCustodian) AbandonUnresolvedCustody(context.Context, model.GroupRef) error {
	return windowsNativeRuntimeUnsupported()
}

func (*NativeCustodian) Close() error {
	return windowsNativeRuntimeUnsupported()
}

func (*NativeCustodian) SelfTest(context.Context, AttestationVerifier) SupportAssessment {
	return SupportAssessment{
		Class:       SupportUnsupported,
		Cause:       windowsNativeRuntimeUnsupported(),
		Attempts:    1,
		CleanupSafe: true,
	}
}

type NativeRunningProcess struct {
	group model.GroupRef
}

func (*NativeRunningProcess) runningProcess() {}

func (process *NativeRunningProcess) Ref() model.GroupRef {
	if process == nil {
		return model.GroupRef{}
	}
	return process.group
}

func (*NativeRunningProcess) Stdin() io.WriteCloser {
	return nil
}

func (*NativeRunningProcess) Stdout() io.ReadCloser {
	return nil
}

func (*NativeRunningProcess) Stderr() io.ReadCloser {
	return nil
}

func (*NativeRunningProcess) Wait(context.Context) (command.ExitObservation, error) {
	return command.ExitObservation{}, windowsNativeRuntimeUnsupported()
}

func (process *NativeRunningProcess) WaitAndVerify(ctx context.Context) (command.ExitObservation, VerifiedQuiescence, CleanupStatus, error) {
	_, err := process.Wait(ctx)
	return command.ExitObservation{}, VerifiedQuiescence{}, CleanupStatus{}, err
}

func (*NativeRunningProcess) WaitContained() bool {
	return false
}

func (process *NativeRunningProcess) ContainPhysical(ctx context.Context) PhysicalOutcome {
	return windowsUnsupportedPhysicalOutcome(process.Ref())
}

func (process *NativeRunningProcess) ContainAndVerify(ctx context.Context, _ QuiescenceCause) (VerifiedQuiescence, CleanupStatus, error) {
	return VerifiedQuiescence{}, CleanupStatus{}, process.ContainPhysical(ctx).Err
}

func (*NativeRunningProcess) AbandonUnresolvedCustody(context.Context) error {
	return windowsNativeRuntimeUnsupported()
}

func windowsUnsupportedPhysicalOutcome(group model.GroupRef) PhysicalOutcome {
	return PhysicalOutcome{
		Kind:     PhysicalOutcomeUnprovable,
		Group:    group,
		Reason:   containment.ReasonAuthorizationUnprovable,
		Decision: model.Unprovable,
		Err:      windowsNativeRuntimeUnsupported(),
	}
}

type RealContainment struct {
	Params         containment.Params
	Witness        containment.ContinuityWitness
	RetainedObject containment.RetainedGroupObject

	TolerateUnleasedCleanupSkip bool
}

func (RealContainment) Contain(context.Context, model.GroupRef) error {
	return windowsNativeRuntimeUnsupported()
}

func (RealContainment) ContainWithWaitBeforeAbsenceProof(context.Context, model.GroupRef, func()) error {
	return windowsNativeRuntimeUnsupported()
}

func (RealContainment) BindContainmentTarget(context.Context, model.GroupRef) (parklaunch.Containment, error) {
	return nil, windowsNativeRuntimeUnsupported()
}

func NewInheritedMonitorContainment(params containment.Params, _ int) RealContainment {
	return RealContainment{Params: params, TolerateUnleasedCleanupSkip: true}
}
