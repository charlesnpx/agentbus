//go:build darwin || linux

package custodian

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
	"github.com/charlesnpx/agentbus/internal/parkproto"
	"github.com/charlesnpx/agentbus/internal/procgroup"
)

var (
	ErrNativeCustodianUnavailable = errors.New("native custodian unavailable")
	ErrPhysicalContainment        = errors.New("physical containment failed")
)

type PhysicalOutcomeKind string

const (
	PhysicalOutcomeAbsent     PhysicalOutcomeKind = "absent"
	PhysicalOutcomeUnprovable PhysicalOutcomeKind = "unprovable"
)

// PhysicalOutcome is deliberately not a quiescence attestation. It reports only
// the local physical result of attempting to make one exact process group
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

type NativeOptions struct {
	AgentbusPath      string
	MonitorCommand    parklaunch.CommandSpec
	ContainmentParams containment.Params
	WorkerEnv         []string
	WorkerDir         string
}

type NativeLaunchSpec struct {
	Exec          command.ExecSpec
	CustodyID     model.CustodyID
	LaunchKey     model.LaunchKey
	LogicalGrant  model.LaunchGrant
	ReleaseSecret model.ReleaseSecret
}

type NativeCustodian struct {
	options NativeOptions

	mu      sync.Mutex
	running map[string]*NativeRunningProcess
}

func NewNativeCustodian(options NativeOptions) (*NativeCustodian, error) {
	if err := validateNativeOptions(options); err != nil {
		return nil, err
	}
	return &NativeCustodian{
		options: options,
		running: make(map[string]*NativeRunningProcess),
	}, nil
}

// NewNativeRuntime exposes only the physical S3B-A custodian plus support
// state. It intentionally does not return a Runtime verifier bundle or any
// proof issuer; S3B-B owns proof minting.
func NewNativeRuntime(options NativeOptions) (*NativeCustodian, Support, error) {
	native, err := NewNativeCustodian(options)
	support := Support{
		ParkedExec:             err == nil,
		VerifiedContainment:    err == nil,
		ImplementationCompiled: true,
		RuntimeProbePassed:     err == nil,
		FeatureConfigured:      false,
		FeatureAdvertised:      false,
		RuntimeProbeResult:     err,
		Platform:               runtime.GOOS,
		Reason:                 err,
	}
	if err == nil {
		support.RuntimeProbeResult = nil
	}
	if _, supportErr := NewSupport(support); supportErr != nil {
		return nil, Support{}, supportErr
	}
	return native, support, err
}

func (custodian *NativeCustodian) Launch(ctx context.Context, spec NativeLaunchSpec) (*NativeRunningProcess, error) {
	if custodian == nil {
		return nil, fmt.Errorf("%w: custodian is nil", ErrNativeCustodianUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	parkSpec, err := custodian.parklaunchSpec(spec)
	if err != nil {
		return nil, err
	}
	handle, err := parklaunch.Launch(ctx, parkSpec)
	if err != nil {
		return nil, err
	}
	retention, err := newLeaderRetention(handle)
	if err != nil {
		cleanupErr := cleanupLaunchedHandle(ctx, handle, custodian.options.ContainmentParams)
		return nil, errors.Join(err, cleanupErr)
	}
	running := &NativeRunningProcess{
		custodian: custodian,
		handle:    handle,
		group:     handle.GroupRef,
		leader:    retention,
	}
	custodian.mu.Lock()
	custodian.running[groupKey(handle.GroupRef)] = running
	custodian.mu.Unlock()
	return running, nil
}

func (custodian *NativeCustodian) ContainAndVerify(ctx context.Context, group model.GroupRef) PhysicalOutcome {
	if custodian == nil {
		return unprovablePhysical(group, containment.ReasonInvalidInput, "", fmt.Errorf("%w: custodian is nil", ErrNativeCustodianUnavailable))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := group.Validate(); err != nil {
		return unprovablePhysical(group, containment.ReasonInvalidInput, "", err)
	}
	running := custodian.lookup(group)
	var witness containment.ContinuityWitness
	if running != nil && running.leader != nil && running.leader.unreapedFor(group) {
		witness = running.leader
	}
	var retained *leaderRetention
	if running != nil {
		retained = running.leader
	}
	outcome := containPhysical(ctx, group, custodian.options.ContainmentParams, witness, retained)
	if outcome.Unprovable() {
		return outcome
	}
	if running != nil {
		if err := running.finalizeAfterAbsence(ctx); err != nil {
			return unprovablePhysical(group, containment.ReasonProbeUnprovable, outcome.Decision, err)
		}
	}
	return outcome
}

func (custodian *NativeCustodian) lookup(group model.GroupRef) *NativeRunningProcess {
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	running := custodian.running[groupKey(group)]
	if running == nil || !running.group.Equal(group) {
		return nil
	}
	return running
}

func (custodian *NativeCustodian) forget(group model.GroupRef) {
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	delete(custodian.running, groupKey(group))
}

func (custodian *NativeCustodian) parklaunchSpec(spec NativeLaunchSpec) (parklaunch.Spec, error) {
	execSpec, err := parkprotoExecSpec(spec.Exec)
	if err != nil {
		return parklaunch.Spec{}, err
	}
	workerEnv := custodian.options.WorkerEnv
	if workerEnv == nil {
		workerEnv = os.Environ()
	}
	return parklaunch.Spec{
		AgentbusPath:         custodian.options.AgentbusPath,
		ExecSpec:             execSpec,
		CustodyID:            spec.CustodyID,
		LaunchKey:            spec.LaunchKey,
		LogicalGrant:         spec.LogicalGrant,
		ReleaseSecret:        spec.ReleaseSecret,
		Containment:          RealContainment{Params: custodian.options.ContainmentParams},
		Monitor:              &parklaunch.MonitorProcessSpec{Command: custodian.options.MonitorCommand},
		RetainLeaderUnreaped: true,
		WorkerEnv:            workerEnv,
		WorkerDir:            custodian.options.WorkerDir,
	}, nil
}

func validateNativeOptions(options NativeOptions) error {
	if options.AgentbusPath == "" {
		return fmt.Errorf("%w: agentbus path is required", ErrNativeCustodianUnavailable)
	}
	if !filepath.IsAbs(options.AgentbusPath) {
		return fmt.Errorf("%w: agentbus path must be absolute", ErrNativeCustodianUnavailable)
	}
	if options.MonitorCommand.Path == "" {
		return fmt.Errorf("%w: monitor command path is required", ErrNativeCustodianUnavailable)
	}
	if !filepath.IsAbs(options.MonitorCommand.Path) {
		return fmt.Errorf("%w: monitor command path must be absolute", ErrNativeCustodianUnavailable)
	}
	if err := options.ContainmentParams.Validate(); err != nil {
		return err
	}
	return nil
}

func (spec NativeLaunchSpec) Validate() error {
	if len(spec.Exec.Argv) == 0 || spec.Exec.Argv[0] == "" {
		return fmt.Errorf("%w: exec argv is required", ErrInvalidSupport)
	}
	if !filepath.IsAbs(spec.Exec.Argv[0]) {
		return fmt.Errorf("%w: native exec argv[0] must be absolute", ErrInvalidSupport)
	}
	if err := spec.CustodyID.Validate(); err != nil {
		return err
	}
	if err := spec.LaunchKey.Validate(); err != nil {
		return err
	}
	if err := spec.LogicalGrant.Validate(); err != nil {
		return err
	}
	if !spec.LogicalGrant.Attempt.Equal(spec.LaunchKey.Attempt) || spec.LogicalGrant.Ordinal != spec.LaunchKey.Ordinal {
		return fmt.Errorf("%w: logical grant does not match launch key", ErrInvalidSupport)
	}
	return spec.ReleaseSecret.Validate()
}

func parkprotoExecSpec(spec command.ExecSpec) (parkproto.ExecSpec, error) {
	env := spec.Env
	if env == nil {
		env = os.Environ()
	}
	out := parkproto.ExecSpec{
		Path: spec.Argv[0],
		Argv: append([]string(nil), spec.Argv...),
		Env:  append([]string(nil), env...),
		Dir:  spec.Dir,
	}
	if err := out.Validate(); err != nil {
		return parkproto.ExecSpec{}, err
	}
	return out, nil
}

type NativeRunningProcess struct {
	custodian *NativeCustodian
	handle    *parklaunch.ParkedHandle
	group     model.GroupRef
	leader    *leaderRetention

	finalOnce sync.Once
	finalErr  error
}

func (process *NativeRunningProcess) Ref() model.GroupRef {
	if process == nil {
		return model.GroupRef{}
	}
	return process.group
}

func (process *NativeRunningProcess) Stdin() io.WriteCloser {
	if process == nil || process.handle == nil {
		return nil
	}
	return process.handle.Stdin
}

func (process *NativeRunningProcess) Stdout() io.ReadCloser {
	if process == nil || process.handle == nil {
		return nil
	}
	return process.handle.Stdout
}

func (process *NativeRunningProcess) Stderr() io.ReadCloser {
	if process == nil || process.handle == nil {
		return nil
	}
	return process.handle.Stderr
}

func (process *NativeRunningProcess) Wait(ctx context.Context) (command.ExitObservation, error) {
	if process == nil || process.handle == nil {
		return command.ExitObservation{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	state, err := waitHandleState(ctx, process.handle)
	observation := exitObservationForState(state)
	if absent, _ := independentlyAbsent(process.group, process.leader); absent {
		if finalizeErr := process.finalizeAfterAbsence(ctx); err == nil {
			err = finalizeErr
		}
	}
	return observation, err
}

func (process *NativeRunningProcess) ContainAndVerify(ctx context.Context) PhysicalOutcome {
	if process == nil || process.custodian == nil {
		return unprovablePhysical(model.GroupRef{}, containment.ReasonInvalidInput, "", fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable))
	}
	return process.custodian.ContainAndVerify(ctx, process.group)
}

func (process *NativeRunningProcess) finalizeAfterAbsence(ctx context.Context) error {
	if process == nil {
		return nil
	}
	process.finalOnce.Do(func() {
		if process.handle != nil {
			if !process.handle.LeaderReaped() {
				_, _ = waitHandleState(ctx, process.handle)
			}
			_ = closeNativeProcessFiles(process.handle)
			if process.handle.Monitor != nil {
				process.finalErr = errors.Join(process.finalErr, process.handle.Monitor.Stop(ctx))
			}
		}
		if process.leader != nil {
			process.finalErr = errors.Join(process.finalErr, process.leader.close())
		}
		if process.custodian != nil {
			process.custodian.forget(process.group)
		}
	})
	return process.finalErr
}

type RealContainment struct {
	Params  containment.Params
	Witness containment.ContinuityWitness
}

func (containment RealContainment) Contain(ctx context.Context, group model.GroupRef) error {
	outcome := containPhysical(ctx, group, containment.Params, containment.Witness, nil)
	if outcome.Absent() {
		return nil
	}
	if outcome.Err != nil {
		return fmt.Errorf("%w: %s: %v", ErrPhysicalContainment, outcome.Reason, outcome.Err)
	}
	return fmt.Errorf("%w: %s", ErrPhysicalContainment, outcome.Reason)
}

func containPhysical(ctx context.Context, group model.GroupRef, params containment.Params, witness containment.ContinuityWitness, retained *leaderRetention) PhysicalOutcome {
	engine := containment.Engine{
		Observer:   containment.RealObserver{},
		Signaler:   containment.RealSignaler{},
		Clock:      containment.RealClock{},
		Continuity: witness,
	}
	outcome := engine.Contain(ctx, group, params)
	if outcome.Unprovable() {
		if unprovableMayStillResolveAbsent(outcome.Reason) {
			if absent, err := independentlyAbsent(group, retained); err == nil && absent {
				return PhysicalOutcome{
					Kind:     PhysicalOutcomeAbsent,
					Group:    group,
					Method:   methodForDecision(outcome.Decision),
					Decision: outcome.Decision,
				}
			}
			if waitGroupHasNoLiveMembers(ctx, group) {
				return PhysicalOutcome{
					Kind:     PhysicalOutcomeAbsent,
					Group:    group,
					Method:   methodForDecision(outcome.Decision),
					Decision: outcome.Decision,
				}
			}
		}
		if retainedGroupAbsentExceptLeader(ctx, group, retained, outcome.Reason) {
			return PhysicalOutcome{
				Kind:     PhysicalOutcomeAbsent,
				Group:    group,
				Method:   model.QuiescenceTermKill,
				Decision: outcome.Decision,
			}
		}
		return PhysicalOutcome{
			Kind:     PhysicalOutcomeUnprovable,
			Group:    group,
			Reason:   outcome.Reason,
			Decision: outcome.Decision,
			Err:      outcome.Err,
		}
	}
	absent, err := independentlyAbsent(group, retained)
	if err != nil {
		return unprovablePhysical(group, containment.ReasonProbeUnprovable, outcome.Decision, err)
	}
	if !absent {
		return unprovablePhysical(group, containment.ReasonProbeContradictedObserver, outcome.Decision, nil)
	}
	return PhysicalOutcome{
		Kind:     PhysicalOutcomeAbsent,
		Group:    group,
		Method:   methodForDecision(outcome.Decision),
		Decision: outcome.Decision,
	}
}

func methodForDecision(decision model.ContainmentDecision) model.QuiescenceMethod {
	if decision == model.AlreadyAbsent {
		return model.QuiescenceAlreadyAbsent
	}
	return model.QuiescenceTermKill
}

func unprovablePhysical(group model.GroupRef, reason containment.UnprovableReason, decision model.ContainmentDecision, err error) PhysicalOutcome {
	return PhysicalOutcome{
		Kind:     PhysicalOutcomeUnprovable,
		Group:    group,
		Reason:   reason,
		Decision: decision,
		Err:      err,
	}
}

func independentlyAbsent(group model.GroupRef, retained *leaderRetention) (bool, error) {
	if err := group.Validate(); err != nil {
		return false, err
	}
	claim, err := procgroup.NewGroupClaim(group.PGID, group.KernelDomain())
	if err != nil {
		return false, err
	}
	switch procgroup.ClassifyGroup(claim) {
	case model.GroupAbsent:
		return true, nil
	case model.GroupLive:
		if retained != nil && retained.unreapedFor(group) {
			return groupHasNoMembersExceptLeader(group)
		}
		return false, nil
	default:
		return false, fmt.Errorf("independent group observation is %s", procgroup.ClassifyGroup(claim))
	}
}

func unprovableMayStillResolveAbsent(reason containment.UnprovableReason) bool {
	switch reason {
	case containment.ReasonSignalUnprovable,
		containment.ReasonProbeUnprovable,
		containment.ReasonProbeContradictedObserver,
		containment.ReasonAbsenceDeadlineExceeded:
		return true
	default:
		return false
	}
}

func waitGroupHasNoLiveMembers(ctx context.Context, group model.GroupRef) bool {
	deadline := time.Now().Add(3 * time.Second)
	for {
		absent, err := groupHasNoLiveMembers(group)
		if err == nil && absent {
			return true
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return false
			}
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func retainedGroupAbsentExceptLeader(ctx context.Context, group model.GroupRef, retained *leaderRetention, reason containment.UnprovableReason) bool {
	switch reason {
	case containment.ReasonProbeUnprovable, containment.ReasonProbeContradictedObserver, containment.ReasonAbsenceDeadlineExceeded:
	default:
		return false
	}
	if retained == nil || !retained.unreapedFor(group) {
		return false
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		absent, err := groupHasNoMembersExceptLeader(group)
		if err == nil && absent {
			return true
		}
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return false
			}
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func cleanupLaunchedHandle(ctx context.Context, handle *parklaunch.ParkedHandle, params containment.Params) error {
	if handle == nil {
		return nil
	}
	var out error
	out = errors.Join(out, RealContainment{Params: params}.Contain(ctx, handle.GroupRef))
	_, waitErr := waitHandleState(ctx, handle)
	out = errors.Join(out, waitErr)
	out = errors.Join(out, closeNativeProcessFiles(handle))
	if handle.Monitor != nil {
		out = errors.Join(out, handle.Monitor.Stop(ctx))
	}
	return out
}

func closeNativeProcessFiles(handle *parklaunch.ParkedHandle) error {
	if handle == nil {
		return nil
	}
	var err error
	if handle.Stdin != nil {
		err = errors.Join(err, handle.Stdin.Close())
	}
	if handle.Stdout != nil {
		err = errors.Join(err, handle.Stdout.Close())
	}
	if handle.Stderr != nil {
		err = errors.Join(err, handle.Stderr.Close())
	}
	return err
}

func waitHandleState(ctx context.Context, handle *parklaunch.ParkedHandle) (*os.ProcessState, error) {
	type result struct {
		state *os.ProcessState
		err   error
	}
	done := make(chan result, 1)
	go func() {
		state, err := handle.WaitState()
		done <- result{state: state, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-done:
		return result.state, result.err
	}
}

func exitObservationForState(state *os.ProcessState) command.ExitObservation {
	observation := command.ExitObservation{}
	if state == nil {
		return observation
	}
	observation.Exited = state.Exited()
	observation.Code = state.ExitCode()
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		observation.Signal = status.Signal().String()
	}
	return observation
}

type leaderRetention struct {
	mu        sync.Mutex
	group     model.GroupRef
	handle    *parklaunch.ParkedHandle
	platform  nativeLeaderPlatformHandle
	heldSince time.Time
}

func newLeaderRetention(handle *parklaunch.ParkedHandle) (*leaderRetention, error) {
	if handle == nil {
		return nil, fmt.Errorf("%w: parked handle is nil", ErrNativeCustodianUnavailable)
	}
	if err := handle.GroupRef.Validate(); err != nil {
		return nil, err
	}
	platform, err := openNativeLeaderPlatformHandle(handle.GroupRef.Leader.PID)
	if err != nil {
		return nil, err
	}
	return &leaderRetention{
		group:     handle.GroupRef,
		handle:    handle,
		platform:  platform,
		heldSince: time.Now(),
	}, nil
}

func (retention *leaderRetention) BeginGroupContinuity(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, observedAt time.Time) containment.GroupContinuity {
	_ = ctx
	if retention == nil || !retention.unreapedFor(target) || observedAt.Before(retention.heldSince) {
		return brokenContinuity{}
	}
	if observation.Group != model.GroupLive || observation.Leader != model.ProcessIdentityMatching {
		return brokenContinuity{}
	}
	return &leaderContinuity{retention: retention, begin: observedAt}
}

func (retention *leaderRetention) unreapedFor(target model.GroupRef) bool {
	if retention == nil || retention.handle == nil {
		return false
	}
	retention.mu.Lock()
	defer retention.mu.Unlock()
	if !retention.group.Equal(target) {
		return false
	}
	if retention.handle.LeaderReaped() {
		return false
	}
	return retention.platform.held()
}

func (retention *leaderRetention) close() error {
	if retention == nil {
		return nil
	}
	retention.mu.Lock()
	defer retention.mu.Unlock()
	return retention.platform.close()
}

type leaderContinuity struct {
	retention *leaderRetention
	begin     time.Time
}

func (continuity *leaderContinuity) ConfirmContinuouslyLive(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, begin, end time.Time) containment.GroupContinuityEvidence {
	_ = ctx
	if continuity == nil || continuity.retention == nil {
		return containment.GroupContinuityEvidence{}
	}
	if begin.Before(continuity.begin) || end.Before(begin) {
		return containment.GroupContinuityEvidence{}
	}
	if observation.Group != model.GroupLive || !continuity.retention.unreapedFor(target) {
		return containment.GroupContinuityEvidence{}
	}
	evidence, err := containment.NewGroupContinuityEvidence(target, begin, end)
	if err != nil {
		return containment.GroupContinuityEvidence{}
	}
	return evidence
}

type brokenContinuity struct{}

func (brokenContinuity) ConfirmContinuouslyLive(context.Context, model.GroupRef, model.ContainmentObservation, time.Time, time.Time) containment.GroupContinuityEvidence {
	return containment.GroupContinuityEvidence{}
}

func groupKey(group model.GroupRef) string {
	return group.HostBootID + "\x00" +
		group.PIDNamespaceID + "\x00" +
		strconv.Itoa(int(group.PIDNamespaceState)) + "\x00" +
		strconv.Itoa(group.PGID) + "\x00" +
		group.Leader.HighResStartToken + "\x00" +
		string(group.CustodyID)
}
