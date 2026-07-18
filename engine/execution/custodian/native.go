//go:build darwin || linux

package custodian

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	"golang.org/x/sys/unix"
)

var (
	ErrNativeCustodianUnavailable = errors.New("native custodian unavailable")
	ErrPhysicalContainment        = errors.New("physical containment failed")
	ErrNativePreparedBoundary     = errors.New("native prepared custody requires launch-controller wiring")
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

type quiescenceAttestationIssuer interface {
	AttestQuiescence(PhysicalQuiescence) (VerifiedQuiescence, error)
}

type NativeOptions struct {
	AgentbusPath      string
	MonitorCommand    parklaunch.CommandSpec
	ContainmentParams containment.Params
	WorkerEnv         []string
	WorkerDir         string

	newLeaderRetention func(model.GroupRef) (*leaderRetention, error)
	newRetainedGroup   func() (containment.RetainedGroupObject, error)
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
	issuer  quiescenceAttestationIssuer

	mu        sync.Mutex
	running   map[string]*NativeRunningProcess
	finalized map[string]*NativeRunningProcess
	closed    bool

	retainedMu    sync.Mutex
	retainedGroup containment.RetainedGroupObject
}

type nativeContainmentBackend interface {
	retainedID() string
	retainLeaderUnreaped() bool
	beforeMonitorBind(context.Context, model.GroupRef) (model.GroupRef, error)
	beforeRelease(context.Context, model.GroupRef) error
	witness() containment.ContinuityWitness
	witnessAcquired() bool
	retainedObject() containment.RetainedGroupObject
	attachHandle(*parklaunch.ParkedHandle)
	leaderRetention() *leaderRetention
	close(context.Context) error
}

func NewNativeCustodian(options NativeOptions) (*NativeCustodian, error) {
	if err := validateNativeOptions(options); err != nil {
		return nil, err
	}
	return &NativeCustodian{
		options:   options,
		running:   make(map[string]*NativeRunningProcess),
		finalized: make(map[string]*NativeRunningProcess),
	}, nil
}

func NewNativeRuntime(options NativeOptions) (Runtime, error) {
	issuer, verifier := NewAttestationChannel()
	native, err := NewNativeCustodian(options)
	if native != nil {
		native.issuer = issuer
	}
	probeErr := err
	if probeErr == nil {
		probeErr = probeNativeRuntime(options)
	}
	support := Support{
		ParkedExec:             err == nil,
		VerifiedContainment:    probeErr == nil,
		ImplementationCompiled: true,
		RuntimeProbePassed:     probeErr == nil,
		FeatureConfigured:      false,
		FeatureAdvertised:      false,
		RuntimeProbeResult:     probeErr,
		Platform:               runtime.GOOS,
		Reason:                 probeErr,
	}
	if probeErr == nil {
		support.RuntimeProbeResult = nil
	}
	if _, supportErr := NewSupport(support); supportErr != nil {
		if native != nil {
			_ = native.Close()
		}
		return Runtime{}, supportErr
	}
	process := ProcessCustodian(UnavailableCustodian{})
	if probeErr == nil {
		process = native
	} else if native != nil {
		_ = native.Close()
	}
	return Runtime{
		process:  process,
		verifier: verifier,
		support:  support,
	}, probeErr
}

func (custodian *NativeCustodian) processCustodian() {}

func (custodian *NativeCustodian) ActiveCustodyCount() int {
	if custodian == nil {
		return 0
	}
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	return len(custodian.running)
}

func (custodian *NativeCustodian) Prepare(context.Context, command.ExecSpec, model.LaunchKey) (PreparedProcess, error) {
	return nil, fmt.Errorf("%w: Prepare/Release is owned by S4 launch-controller wiring", ErrNativePreparedBoundary)
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
	custodian.mu.Lock()
	if custodian.closed {
		custodian.mu.Unlock()
		return nil, fmt.Errorf("%w: custodian is closed", ErrNativeCustodianUnavailable)
	}
	custodian.mu.Unlock()
	backend, err := newNativeContainmentBackend(ctx, custodian)
	if err != nil {
		return nil, err
	}
	launchContainment := &nativeLaunchContainment{
		params:         custodian.options.ContainmentParams,
		retainedObject: backend.retainedObject(),
	}
	syncLaunchContainment := func() {
		if witness := backend.witness(); witness != nil {
			launchContainment.setWitness(witness)
		}
		if retainedObject := backend.retainedObject(); retainedObject != nil {
			launchContainment.setRetainedObject(retainedObject)
		}
	}
	parkSpec, err := custodian.parklaunchSpec(spec, launchContainment, backend, syncLaunchContainment)
	if err != nil {
		_ = backend.close(ctx)
		return nil, err
	}
	handle, err := parklaunch.Launch(ctx, parkSpec)
	if err != nil {
		return nil, errors.Join(err, backend.close(ctx))
	}
	if !backend.witnessAcquired() {
		err := fmt.Errorf("%w: containment continuity witness was not acquired before release", ErrNativeCustodianUnavailable)
		cleanupErr := cleanupLaunchedHandle(ctx, handle, custodian.options.ContainmentParams)
		return nil, errors.Join(err, cleanupErr, backend.close(ctx))
	}
	backend.attachHandle(handle)
	running := &NativeRunningProcess{
		custodian:   custodian,
		handle:      handle,
		group:       handle.GroupRef,
		leader:      backend.leaderRetention(),
		containment: backend,
	}
	custodian.mu.Lock()
	if custodian.closed {
		custodian.mu.Unlock()
		cleanupErr := cleanupLaunchedHandle(ctx, handle, custodian.options.ContainmentParams)
		return nil, errors.Join(fmt.Errorf("%w: custodian is closed", ErrNativeCustodianUnavailable), cleanupErr, backend.close(ctx))
	}
	key := groupKey(handle.GroupRef)
	if custodian.running == nil {
		custodian.running = make(map[string]*NativeRunningProcess)
	}
	delete(custodian.finalized, key)
	custodian.running[key] = running
	custodian.mu.Unlock()
	return running, nil
}

func (custodian *NativeCustodian) Close() error {
	if custodian == nil {
		return nil
	}
	custodian.mu.Lock()
	if len(custodian.running) != 0 {
		custodian.mu.Unlock()
		return fmt.Errorf("%w: cannot close custodian with running processes", ErrNativeCustodianUnavailable)
	}
	custodian.closed = true
	custodian.mu.Unlock()

	custodian.retainedMu.Lock()
	retainedGroup := custodian.retainedGroup
	custodian.retainedGroup = nil
	custodian.retainedMu.Unlock()
	closer, ok := retainedGroup.(interface{ Close() error })
	if !ok || closer == nil {
		return nil
	}
	return closer.Close()
}

func (custodian *NativeCustodian) sharedRetainedGroup(factory func() (containment.RetainedGroupObject, error)) (containment.RetainedGroupObject, error) {
	if custodian == nil {
		return nil, fmt.Errorf("%w: custodian is nil", ErrNativeCustodianUnavailable)
	}
	if factory == nil {
		return nil, fmt.Errorf("%w: retained-object factory is nil", ErrNativeCustodianUnavailable)
	}
	custodian.mu.Lock()
	closed := custodian.closed
	custodian.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("%w: custodian is closed", ErrNativeCustodianUnavailable)
	}
	custodian.retainedMu.Lock()
	defer custodian.retainedMu.Unlock()
	if custodian.retainedGroup != nil {
		return custodian.retainedGroup, nil
	}
	manager, err := factory()
	if err != nil {
		return nil, err
	}
	if manager == nil {
		return nil, fmt.Errorf("%w: retained-object manager is nil", ErrNativeCustodianUnavailable)
	}
	custodian.retainedGroup = manager
	return manager, nil
}

func (custodian *NativeCustodian) ContainPhysical(ctx context.Context, group model.GroupRef) PhysicalOutcome {
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
	if running != nil {
		return running.containAndVerify(ctx)
	}
	finalized := custodian.lookupFinalized(group)
	if finalized != nil {
		return finalized.containAndVerify(ctx)
	}
	return containPhysical(ctx, group, custodian.options.ContainmentParams, nil, nil)
}

func (custodian *NativeCustodian) ContainAndVerify(ctx context.Context, group model.GroupRef, cause QuiescenceCause) (VerifiedQuiescence, error) {
	_ = cause
	if custodian == nil {
		return VerifiedQuiescence{}, fmt.Errorf("%w: custodian is nil", ErrNativeCustodianUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := group.Validate(); err != nil {
		return VerifiedQuiescence{}, err
	}
	running := custodian.lookup(group)
	if running != nil {
		return running.ContainAndVerify(ctx, cause)
	}
	finalized := custodian.lookupFinalized(group)
	if finalized != nil {
		return finalized.ContainAndVerify(ctx, cause)
	}
	return attestPhysicalOutcome(custodian.issuer, custodian.containRecoveredPhysical(ctx, group))
}

func (custodian *NativeCustodian) containRecoveredPhysical(ctx context.Context, group model.GroupRef) PhysicalOutcome {
	requiresRetained, err := model.ContainmentRequiresRetainedObject(group)
	if err != nil {
		return unprovablePhysical(group, containment.ReasonInvalidInput, "", err)
	}
	if !requiresRetained {
		return containPhysical(ctx, group, custodian.options.ContainmentParams, nil, nil)
	}
	factory := platformRetainedGroupFactory(custodian.options.newRetainedGroup)
	if factory == nil {
		return unprovablePhysical(group, containment.ReasonAuthorizationUnprovable, model.Unprovable, fmt.Errorf("%w: required retained object acquisition provider is missing", ErrNativeCustodianUnavailable))
	}
	manager, err := custodian.sharedRetainedGroup(factory)
	if err != nil {
		return unprovablePhysical(group, containment.ReasonAuthorizationUnprovable, model.Unprovable, err)
	}
	capability, err := manager.AcquireRetainedGroup(ctx, group, time.Now())
	if err != nil {
		if retainedGroupMissing(err) {
			if proofErr := proveRetainedGroupAbsent(ctx, manager, group); proofErr == nil {
				return PhysicalOutcome{
					Kind:     PhysicalOutcomeAbsent,
					Group:    group,
					Method:   model.QuiescenceAlreadyAbsent,
					Decision: model.AlreadyAbsent,
				}
			} else {
				return unprovablePhysical(group, containment.ReasonAuthorizationUnprovable, model.Unprovable, errors.Join(
					fmt.Errorf("%w: retained group missing without same-domain absence proof", ErrNativeCustodianUnavailable),
					err,
					proofErr,
				))
			}
		}
		return unprovablePhysical(group, containment.ReasonAuthorizationUnprovable, model.Unprovable, err)
	}
	if capability == nil {
		return unprovablePhysical(group, containment.ReasonAuthorizationUnprovable, model.Unprovable, fmt.Errorf("%w: retained object acquisition returned nil capability", ErrNativeCustodianUnavailable))
	}
	defer capability.Release()

	var witness containment.ContinuityWitness
	if continuity, ok := capability.(containment.ContinuityWitness); ok {
		witness = continuity
	}
	return containPhysical(ctx, group, custodian.options.ContainmentParams, witness, recoveredRetainedObject{capability: capability})
}

func retainedGroupMissing(err error) bool {
	return errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ESTALE)
}

type retainedGroupAbsenceProver interface {
	ProveRetainedGroupAbsent(ctx context.Context, target model.GroupRef) error
}

func proveRetainedGroupAbsent(ctx context.Context, manager containment.RetainedGroupObject, group model.GroupRef) error {
	prover, ok := manager.(retainedGroupAbsenceProver)
	if !ok || prover == nil {
		return fmt.Errorf("%w: retained group manager cannot prove same-domain absence", ErrNativeCustodianUnavailable)
	}
	return prover.ProveRetainedGroupAbsent(ctx, group)
}

type recoveredRetainedObject struct {
	capability containment.RetainedGroupCapability
}

func (object recoveredRetainedObject) AcquireRetainedGroup(_ context.Context, target model.GroupRef, _ time.Time) (containment.RetainedGroupCapability, error) {
	if object.capability == nil {
		return nil, fmt.Errorf("%w: recovered retained capability is nil", ErrNativeCustodianUnavailable)
	}
	identity := object.capability.Identity()
	if err := identity.KernelDomainID.Validate(); err != nil {
		return nil, err
	}
	if identity.RetainedID != target.RetainedID || !identity.KernelDomainID.ProvablySame(target.KernelDomain()) {
		return nil, fmt.Errorf("%w: recovered retained capability identity mismatch", ErrNativeCustodianUnavailable)
	}
	return object.capability, nil
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

func (custodian *NativeCustodian) lookupFinalized(group model.GroupRef) *NativeRunningProcess {
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	finalized := custodian.finalized[groupKey(group)]
	if finalized == nil || !finalized.group.Equal(group) {
		return nil
	}
	return finalized
}

func (custodian *NativeCustodian) forget(group model.GroupRef) {
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	delete(custodian.running, groupKey(group))
}

func (custodian *NativeCustodian) cacheFinalized(process *NativeRunningProcess) {
	if custodian == nil || process == nil {
		return
	}
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	key := groupKey(process.group)
	if custodian.finalized == nil {
		custodian.finalized = make(map[string]*NativeRunningProcess)
	}
	custodian.finalized[key] = process
	if custodian.running[key] == process {
		delete(custodian.running, key)
	}
}

func (custodian *NativeCustodian) parklaunchSpec(spec NativeLaunchSpec, launchContainment parklaunch.Containment, backend nativeContainmentBackend, syncLaunchContainment func()) (parklaunch.Spec, error) {
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
		Containment:          launchContainment,
		Monitor:              &parklaunch.MonitorProcessSpec{Command: custodian.options.MonitorCommand},
		RetainedID:           backend.retainedID(),
		RetainLeaderUnreaped: backend.retainLeaderUnreaped(),
		BeforeMonitorBind: func(ctx context.Context, group model.GroupRef) (model.GroupRef, error) {
			bound, err := backend.beforeMonitorBind(ctx, group)
			if err != nil {
				return model.GroupRef{}, err
			}
			if syncLaunchContainment != nil {
				syncLaunchContainment()
			}
			return bound, nil
		},
		BeforeRelease: func(ctx context.Context, group model.GroupRef) error {
			if err := backend.beforeRelease(ctx, group); err != nil {
				return err
			}
			if syncLaunchContainment != nil {
				syncLaunchContainment()
			}
			return nil
		},
		WorkerEnv: workerEnv,
		WorkerDir: custodian.options.WorkerDir,
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

func probeNativeRuntime(options NativeOptions) error {
	return probeNativeRuntimePlatform(options)
}

func probeNativeLeaderContainment(options NativeOptions) error {
	if err := probeNativeLeaderPlatform(); err != nil {
		return fmt.Errorf("%w: leader continuity probe: %v", ErrNativeCustodianUnavailable, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.Command("/bin/sh", "-c", "trap '' TERM; while :; do sleep 1; done")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: start containment probe: %v", ErrNativeCustodianUnavailable, err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()
	group, err := probeGroupRef(ctx, cmd.Process.Pid)
	if err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-waitDone
		return err
	}
	retention, err := newLeaderRetentionForGroup(group)
	if err != nil {
		_ = syscall.Kill(-group.PGID, syscall.SIGKILL)
		<-waitDone
		return err
	}
	defer retention.close()

	params := options.ContainmentParams
	if params.GracePeriod == 0 {
		params.GracePeriod = 20 * time.Millisecond
	}
	if params.PollInterval == 0 {
		params.PollInterval = 20 * time.Millisecond
	}
	if params.PollTimeout == 0 {
		params.PollTimeout = 2 * time.Second
	}
	outcome := containPhysical(ctx, group, params, retention, nil)
	if !outcome.Absent() {
		_ = syscall.Kill(-group.PGID, syscall.SIGKILL)
		<-waitDone
		if outcome.Err != nil {
			return fmt.Errorf("%w: containment probe %s: %v", ErrNativeCustodianUnavailable, outcome.Reason, outcome.Err)
		}
		return fmt.Errorf("%w: containment probe outcome=%s reason=%s", ErrNativeCustodianUnavailable, outcome.Kind, outcome.Reason)
	}
	<-waitDone
	return nil
}

func probeGroupRef(ctx context.Context, leaderPID int) (model.GroupRef, error) {
	var leader procgroup.ProcessClaim
	var err error
	deadline := time.Now().Add(2 * time.Second)
	for {
		leader, err = procgroup.ReadProcessClaim(leaderPID)
		if err == nil && leader.PGID == leader.PID {
			break
		}
		if !time.Now().Before(deadline) {
			if err != nil {
				return model.GroupRef{}, err
			}
			return model.GroupRef{}, fmt.Errorf("probe leader pgid=%d pid=%d", leader.PGID, leader.PID)
		}
		if sleepErr := sleepContext(ctx, 10*time.Millisecond); sleepErr != nil {
			return model.GroupRef{}, sleepErr
		}
	}
	monitor, err := procgroup.ReadProcessClaim(os.Getpid())
	if err != nil {
		return model.GroupRef{}, err
	}
	attempt := model.AttemptRef{JobID: "job-native-probe", AttemptID: "attempt-native-probe", Epoch: 1}
	return model.GroupRef{
		Version:   1,
		CustodyID: "custody-native-probe",
		Launch: model.LaunchKey{
			Attempt: attempt,
			Ordinal: model.LaunchOrdinalOne,
		},
		HostBootID:          leader.KernelDomainID.HostBootID,
		PIDNamespaceID:      leader.KernelDomainID.PIDNamespaceID,
		PIDNamespaceState:   leader.KernelDomainID.PIDNamespaceState,
		RetainedDomainID:    leader.KernelDomainID.RetainedDomainID,
		RetainedDomainState: leader.KernelDomainID.RetainedDomainState,
		PGID:                leader.PGID,
		Leader: model.ProcessIdentity{
			PID:               leader.PID,
			HighResStartToken: leader.StartToken.String(),
		},
		Monitor: model.ProcessIdentity{
			PID:               monitor.PID,
			HighResStartToken: monitor.StartToken.String(),
		},
		RetainedID: nativeProbeRetainedID(leader, monitor),
	}, nil
}

func nativeProbeRetainedID(leader, monitor procgroup.ProcessClaim) string {
	hash := sha256.New()
	fmt.Fprintf(hash, "native-probe-retained-v1\x00%d\x00%s\x00%s\x00%s\x00%d\x00%s",
		leader.PID,
		leader.StartToken,
		leader.KernelDomainID.HostBootID,
		leader.KernelDomainID.PIDNamespaceID,
		monitor.PID,
		monitor.StartToken,
	)
	return "native-probe-retained-sha256-" + hex.EncodeToString(hash.Sum(nil))
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
	custodian   *NativeCustodian
	handle      *parklaunch.ParkedHandle
	group       model.GroupRef
	leader      *leaderRetention
	containment nativeContainmentBackend

	lifecycleMu  sync.Mutex
	finalized    bool
	finalOutcome PhysicalOutcome
	finalExit    command.ExitObservation
	finalWaitErr error
	finalErr     error

	finalAttestationAttempted bool
	finalAttestation          VerifiedQuiescence
	finalAttestationErr       error
}

func (process *NativeRunningProcess) runningProcess() {}

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
	process.lifecycleMu.Lock()
	defer process.lifecycleMu.Unlock()
	return process.handle.Stdin
}

func (process *NativeRunningProcess) Stdout() io.ReadCloser {
	if process == nil || process.handle == nil {
		return nil
	}
	process.lifecycleMu.Lock()
	defer process.lifecycleMu.Unlock()
	return process.handle.Stdout
}

func (process *NativeRunningProcess) Stderr() io.ReadCloser {
	if process == nil || process.handle == nil {
		return nil
	}
	process.lifecycleMu.Lock()
	defer process.lifecycleMu.Unlock()
	return process.handle.Stderr
}

func (process *NativeRunningProcess) continuityWitness() containment.ContinuityWitness {
	if process == nil {
		return nil
	}
	if process.containment != nil {
		if witness := process.containment.witness(); witness != nil {
			return witness
		}
	}
	return process.leader
}

func (process *NativeRunningProcess) retainedObject() containment.RetainedGroupObject {
	if process == nil || process.containment == nil {
		return nil
	}
	return process.containment.retainedObject()
}

func (process *NativeRunningProcess) shouldContainResidualGroup() (bool, error) {
	if process == nil {
		return false, fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable)
	}
	if process.leader != nil {
		return true, nil
	}
	return model.ContainmentRequiresRetainedObject(process.group)
}

func (process *NativeRunningProcess) Wait(ctx context.Context) (command.ExitObservation, error) {
	if process == nil || process.handle == nil {
		return command.ExitObservation{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		var waitHandle nativeLeaderPlatformHandle
		haveWaitHandle := false
		process.lifecycleMu.Lock()
		if process.finalized {
			exit, err := process.finalizedWaitResultLocked()
			process.lifecycleMu.Unlock()
			return exit, err
		}
		if process.leader != nil {
			cloned, err := process.leader.cloneExitNotification()
			if err != nil {
				process.lifecycleMu.Unlock()
				return process.finalizedWaitResultOrError(err)
			}
			waitHandle = cloned
			haveWaitHandle = true
		}
		process.lifecycleMu.Unlock()
		if haveWaitHandle {
			if err := waitHandle.waitExited(ctx); err != nil {
				_ = waitHandle.close()
				return process.finalizedWaitResultOrError(err)
			}
			_ = waitHandle.close()
		} else {
			select {
			case <-process.handle.Done():
			case <-ctx.Done():
				return process.finalizedWaitResultOrError(ctx.Err())
			}
		}

		process.lifecycleMu.Lock()
		if process.finalized {
			exit, err := process.finalizedWaitResultLocked()
			process.lifecycleMu.Unlock()
			return exit, err
		}
		outcome := PhysicalOutcome{
			Kind:     PhysicalOutcomeAbsent,
			Group:    process.group,
			Method:   model.QuiescenceAlreadyAbsent,
			Decision: model.AlreadyAbsent,
		}
		var exit command.ExitObservation
		var err error
		containResidual, err := process.shouldContainResidualGroup()
		if err != nil {
			process.lifecycleMu.Unlock()
			return command.ExitObservation{}, err
		}
		if process.leader != nil {
			outcome, exit, err = process.finalizeIfSoleLeaderLocked(ctx, outcome, 0)
		} else {
			outcome, exit, err = process.finalizeCompletedWorkerLocked(ctx, outcome)
		}
		if outcome.Absent() || err != nil {
			if outcome.Absent() {
				err = errors.Join(process.finalWaitErr, err)
			}
			process.lifecycleMu.Unlock()
			return exit, err
		}
		process.lifecycleMu.Unlock()
		if !containResidual {
			if err := sleepContext(ctx, 20*time.Millisecond); err != nil {
				return command.ExitObservation{}, err
			}
			continue
		}
		contained := process.containAndVerify(ctx)
		if !contained.Absent() {
			return process.finalizedWaitResultOrError(physicalOutcomeError(contained))
		}
		return process.finalizedWaitResultOrError(nil)
	}
}

func (process *NativeRunningProcess) WaitAndVerify(ctx context.Context) (command.ExitObservation, VerifiedQuiescence, error) {
	if process == nil {
		return command.ExitObservation{}, VerifiedQuiescence{}, fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable)
	}
	exit, waitErr := process.Wait(ctx)
	process.lifecycleMu.Lock()
	if !process.finalized {
		process.lifecycleMu.Unlock()
		return exit, VerifiedQuiescence{}, waitErr
	}
	verified, attestErr := process.finalAttestationLocked()
	process.lifecycleMu.Unlock()
	return exit, verified, errors.Join(waitErr, attestErr)
}

func (process *NativeRunningProcess) WaitContained() bool {
	if process == nil {
		return false
	}
	process.lifecycleMu.Lock()
	defer process.lifecycleMu.Unlock()
	return process.finalized && process.finalOutcome.Method == model.QuiescenceTermKill
}

func (process *NativeRunningProcess) finalizedWaitResultLocked() (command.ExitObservation, error) {
	return process.finalExit, errors.Join(process.finalWaitErr, process.finalErr)
}

func (process *NativeRunningProcess) finalizedWaitResultOrError(err error) (command.ExitObservation, error) {
	process.lifecycleMu.Lock()
	defer process.lifecycleMu.Unlock()
	if process.finalized {
		return process.finalizedWaitResultLocked()
	}
	return command.ExitObservation{}, err
}

func (process *NativeRunningProcess) ContainPhysical(ctx context.Context) PhysicalOutcome {
	if process == nil || process.custodian == nil {
		return unprovablePhysical(model.GroupRef{}, containment.ReasonInvalidInput, "", fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable))
	}
	return process.containAndVerify(ctx)
}

func (process *NativeRunningProcess) ContainAndVerify(ctx context.Context, cause QuiescenceCause) (VerifiedQuiescence, error) {
	_ = cause
	if process == nil || process.custodian == nil {
		return VerifiedQuiescence{}, fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable)
	}
	outcome := process.containAndVerify(ctx)
	if !outcome.Absent() {
		return VerifiedQuiescence{}, physicalOutcomeError(outcome)
	}
	process.lifecycleMu.Lock()
	defer process.lifecycleMu.Unlock()
	if process.finalized {
		return process.finalAttestationLocked()
	}
	return attestPhysicalOutcome(process.custodian.issuer, outcome)
}

func (process *NativeRunningProcess) containAndVerify(ctx context.Context) PhysicalOutcome {
	if process == nil {
		return unprovablePhysical(model.GroupRef{}, containment.ReasonInvalidInput, "", fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	process.lifecycleMu.Lock()
	defer process.lifecycleMu.Unlock()
	if process.finalized {
		return process.finalOutcome
	}
	outcome := containPhysical(ctx, process.group, process.custodian.options.ContainmentParams, process.continuityWitness(), process.retainedObject())
	if outcome.Unprovable() {
		if !mayFinalizeSoleLeaderAfterUnprovable(outcome) {
			return outcome
		}
		finalOutcome, _, _ := process.finalizeIfSoleLeaderLocked(ctx, PhysicalOutcome{
			Kind:     PhysicalOutcomeAbsent,
			Group:    process.group,
			Method:   model.QuiescenceTermKill,
			Decision: outcome.Decision,
		}, 0)
		if finalOutcome.Absent() {
			return finalOutcome
		}
		return outcome
	}
	finalOutcome, _, err := process.finalizeAbsentLocked(ctx, outcome)
	if err != nil && !process.finalized {
		return unprovablePhysical(process.group, containment.ReasonProbeUnprovable, outcome.Decision, err)
	}
	return finalOutcome
}

func mayFinalizeSoleLeaderAfterUnprovable(outcome PhysicalOutcome) bool {
	return outcome.Unprovable() &&
		outcome.Reason == containment.ReasonAbsenceDeadlineExceeded &&
		outcome.Decision == model.SignalDirectly &&
		outcome.Err == nil
}

func (process *NativeRunningProcess) finalizeIfSoleLeaderLocked(ctx context.Context, outcome PhysicalOutcome, waitForSole time.Duration) (PhysicalOutcome, command.ExitObservation, error) {
	if process == nil {
		return PhysicalOutcome{}, command.ExitObservation{}, fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable)
	}
	if process.finalized {
		return process.finalOutcome, process.finalExit, process.finalErr
	}
	if process.leader == nil || !process.leader.unreapedFor(process.group) {
		return PhysicalOutcome{}, command.ExitObservation{}, nil
	}
	deadline := time.Now().Add(waitForSole)
	for {
		soleLeader, err := groupHasNoMembersExceptLeader(process.group)
		if err != nil {
			return PhysicalOutcome{}, command.ExitObservation{}, err
		}
		if soleLeader {
			break
		}
		if waitForSole <= 0 || !time.Now().Before(deadline) {
			return PhysicalOutcome{}, command.ExitObservation{}, nil
		}
		if err := sleepContext(ctx, 20*time.Millisecond); err != nil {
			return PhysicalOutcome{}, command.ExitObservation{}, err
		}
	}
	exit, waitErr, err := process.reapLeaderLocked(ctx)
	if err != nil {
		return PhysicalOutcome{}, command.ExitObservation{}, err
	}
	absent, err := stableIndependentAbsent(ctx, process.group)
	if err != nil {
		return PhysicalOutcome{}, exit, err
	}
	if !absent {
		return PhysicalOutcome{}, exit, nil
	}
	finalOutcome, err := process.cacheFinalLocked(ctx, outcome, exit, waitErr)
	return finalOutcome, exit, err
}

func (process *NativeRunningProcess) finalizeCompletedWorkerLocked(ctx context.Context, outcome PhysicalOutcome) (PhysicalOutcome, command.ExitObservation, error) {
	if process == nil {
		return PhysicalOutcome{}, command.ExitObservation{}, fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable)
	}
	if process.finalized {
		return process.finalOutcome, process.finalExit, process.finalErr
	}
	if process.handle == nil {
		return PhysicalOutcome{}, command.ExitObservation{}, fmt.Errorf("%w: parked handle is nil", ErrNativeCustodianUnavailable)
	}
	select {
	case <-process.handle.Done():
	default:
		return PhysicalOutcome{}, command.ExitObservation{}, nil
	}
	requiresRetained, err := model.ContainmentRequiresRetainedObject(process.group)
	if err != nil {
		return PhysicalOutcome{}, command.ExitObservation{}, err
	}
	if requiresRetained {
		empty, err := process.retainedObjectEmptyLocked(ctx)
		if err != nil {
			return PhysicalOutcome{}, command.ExitObservation{}, err
		}
		if !empty {
			return PhysicalOutcome{}, command.ExitObservation{}, nil
		}
		return process.finalizeAbsentLocked(ctx, outcome)
	}
	absent, err := stableIndependentAbsent(ctx, process.group)
	if err != nil {
		return PhysicalOutcome{}, command.ExitObservation{}, err
	}
	if !absent {
		return PhysicalOutcome{}, command.ExitObservation{}, nil
	}
	return process.finalizeAbsentLocked(ctx, outcome)
}

func (process *NativeRunningProcess) finalizeAbsentLocked(ctx context.Context, outcome PhysicalOutcome) (PhysicalOutcome, command.ExitObservation, error) {
	if process == nil {
		return PhysicalOutcome{}, command.ExitObservation{}, fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable)
	}
	if process.finalized {
		return process.finalOutcome, process.finalExit, process.finalErr
	}
	requiresRetained, err := model.ContainmentRequiresRetainedObject(process.group)
	if err != nil {
		return PhysicalOutcome{}, command.ExitObservation{}, err
	}
	if !requiresRetained {
		absent, err := stableIndependentAbsent(ctx, process.group)
		if err != nil {
			return PhysicalOutcome{}, command.ExitObservation{}, err
		}
		if !absent {
			return PhysicalOutcome{}, command.ExitObservation{}, fmt.Errorf("target group is not stably absent")
		}
	} else {
		empty, err := process.retainedObjectEmptyLocked(ctx)
		if err != nil {
			return PhysicalOutcome{}, command.ExitObservation{}, err
		}
		if !empty {
			return PhysicalOutcome{}, command.ExitObservation{}, fmt.Errorf("target retained group is not empty")
		}
	}
	var exit command.ExitObservation
	var waitErr error
	exit, waitErr, err = process.workerExitLocked(ctx)
	if err != nil {
		return PhysicalOutcome{}, command.ExitObservation{}, err
	}
	finalOutcome, err := process.cacheFinalLocked(ctx, outcome, exit, waitErr)
	return finalOutcome, exit, err
}

func (process *NativeRunningProcess) retainedObjectEmptyLocked(ctx context.Context) (bool, error) {
	if process == nil {
		return false, fmt.Errorf("%w: running process is nil", ErrNativeCustodianUnavailable)
	}
	retainedObject := process.retainedObject()
	if retainedObject == nil {
		return false, fmt.Errorf("%w: required retained object acquisition provider is missing", ErrNativeCustodianUnavailable)
	}
	capability, err := retainedObject.AcquireRetainedGroup(ctx, process.group, time.Now())
	if err != nil {
		return false, err
	}
	if capability == nil {
		return false, fmt.Errorf("%w: retained object acquisition returned nil capability", ErrNativeCustodianUnavailable)
	}
	defer capability.Release()
	identity := capability.Identity()
	if identity.RetainedID != process.group.RetainedID || !identity.KernelDomainID.ProvablySame(process.group.KernelDomain()) {
		return false, fmt.Errorf("%w: retained object identity mismatch", ErrNativeCustodianUnavailable)
	}
	membership, err := capability.Membership(ctx)
	if err != nil {
		return false, err
	}
	if membership == containment.RetainedMembershipPresent {
		return false, nil
	}
	if membership != containment.RetainedMembershipEmpty {
		return false, fmt.Errorf("%w: retained object membership is unknown", ErrNativeCustodianUnavailable)
	}
	held, err := capability.StillHeld(ctx)
	if err != nil {
		return false, err
	}
	if !held {
		return false, fmt.Errorf("%w: retained object is no longer held", ErrNativeCustodianUnavailable)
	}
	return true, nil
}

func (process *NativeRunningProcess) reapLeaderLocked(ctx context.Context) (command.ExitObservation, error, error) {
	if process == nil || process.handle == nil || process.handle.LeaderReaped() {
		return command.ExitObservation{}, nil, nil
	}
	if process.leader != nil {
		if err := process.leader.waitExited(ctx); err != nil {
			return command.ExitObservation{}, nil, err
		}
	}
	state, waitErr := process.handle.WaitState()
	return exitObservationForState(state), waitErr, nil
}

func (process *NativeRunningProcess) workerExitLocked(ctx context.Context) (command.ExitObservation, error, error) {
	if process == nil || process.handle == nil {
		return command.ExitObservation{}, nil, nil
	}
	if process.leader != nil && !process.handle.LeaderReaped() {
		return process.reapLeaderLocked(ctx)
	}
	select {
	case <-process.handle.Done():
	default:
		return command.ExitObservation{}, nil, nil
	}
	state, waitErr := process.handle.WaitState()
	return exitObservationForState(state), waitErr, nil
}

func (process *NativeRunningProcess) cacheFinalLocked(ctx context.Context, outcome PhysicalOutcome, exit command.ExitObservation, waitErr error) (PhysicalOutcome, error) {
	if process.finalized {
		return process.finalOutcome, process.finalErr
	}
	var cleanupErr error
	if process.handle != nil {
		cleanupErr = errors.Join(cleanupErr, closeNativeProcessFiles(process.handle))
		if process.handle.Monitor != nil {
			cleanupErr = errors.Join(cleanupErr, process.handle.Monitor.Stop(ctx))
		}
	}
	if process.leader != nil {
		if process.containment == nil {
			cleanupErr = errors.Join(cleanupErr, process.leader.close())
		}
	}
	if process.containment != nil {
		cleanupErr = errors.Join(cleanupErr, process.containment.close(ctx))
	}
	process.finalized = true
	process.finalOutcome = outcome
	process.finalExit = exit
	process.finalWaitErr = waitErr
	process.finalErr = cleanupErr
	if process.custodian != nil {
		process.custodian.cacheFinalized(process)
	}
	return outcome, cleanupErr
}

func (process *NativeRunningProcess) finalAttestationLocked() (VerifiedQuiescence, error) {
	if process == nil || !process.finalized {
		return VerifiedQuiescence{}, fmt.Errorf("%w: process has no final physical outcome", ErrNativeCustodianUnavailable)
	}
	if process.finalAttestationAttempted {
		return process.finalAttestation, process.finalAttestationErr
	}
	process.finalAttestationAttempted = true
	if process.custodian == nil {
		process.finalAttestationErr = fmt.Errorf("%w: running process has no custodian", ErrNativeCustodianUnavailable)
		return process.finalAttestation, process.finalAttestationErr
	}
	process.finalAttestation, process.finalAttestationErr = attestPhysicalOutcome(process.custodian.issuer, process.finalOutcome)
	return process.finalAttestation, process.finalAttestationErr
}

type RealContainment struct {
	Params         containment.Params
	Witness        containment.ContinuityWitness
	RetainedObject containment.RetainedGroupObject
}

func (real RealContainment) Contain(ctx context.Context, group model.GroupRef) error {
	bound, err := platformRealContainment(ctx, real, group)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPhysicalContainment, err)
	}
	outcome := containPhysical(ctx, group, bound.Params, bound.Witness, bound.RetainedObject)
	if outcome.Absent() {
		return nil
	}
	if outcome.Err != nil {
		return fmt.Errorf("%w: %s: %v", ErrPhysicalContainment, outcome.Reason, outcome.Err)
	}
	return fmt.Errorf("%w: %s", ErrPhysicalContainment, outcome.Reason)
}

func (real RealContainment) BindContainmentTarget(ctx context.Context, group model.GroupRef) (parklaunch.Containment, error) {
	return platformBindContainmentTarget(ctx, real, group)
}

type nativeLaunchContainment struct {
	mu             sync.RWMutex
	params         containment.Params
	witness        containment.ContinuityWitness
	retainedObject containment.RetainedGroupObject
}

func (native *nativeLaunchContainment) setWitness(witness containment.ContinuityWitness) {
	native.mu.Lock()
	defer native.mu.Unlock()
	native.witness = witness
}

func (native *nativeLaunchContainment) setRetainedObject(retainedObject containment.RetainedGroupObject) {
	native.mu.Lock()
	defer native.mu.Unlock()
	native.retainedObject = retainedObject
}

func (native *nativeLaunchContainment) Contain(ctx context.Context, group model.GroupRef) error {
	native.mu.RLock()
	params := native.params
	witness := native.witness
	retainedObject := native.retainedObject
	native.mu.RUnlock()
	return RealContainment{Params: params, Witness: witness, RetainedObject: retainedObject}.Contain(ctx, group)
}

func containPhysical(ctx context.Context, group model.GroupRef, params containment.Params, witness containment.ContinuityWitness, retainedObject containment.RetainedGroupObject) PhysicalOutcome {
	var retainedSignals *retainedSignalRecorder
	if retainedObject != nil {
		retainedSignals = &retainedSignalRecorder{}
		retainedObject = retainedSignalTrackingObject{
			inner:    retainedObject,
			recorder: retainedSignals,
		}
	}
	engine := containment.Engine{
		Observer:       nativeObserverFor(witness),
		Signaler:       nativeSignalerFor(witness),
		Clock:          containment.RealClock{},
		Continuity:     witness,
		RetainedObject: retainedObject,
	}
	outcome := engine.Contain(ctx, group, params)
	if outcome.Unprovable() {
		return PhysicalOutcome{
			Kind:     PhysicalOutcomeUnprovable,
			Group:    group,
			Reason:   outcome.Reason,
			Decision: outcome.Decision,
			Err:      outcome.Err,
		}
	}
	requiresRetained, err := model.ContainmentRequiresRetainedObject(group)
	if err != nil {
		return unprovablePhysical(group, containment.ReasonInvalidInput, outcome.Decision, err)
	}
	if requiresRetained {
		method := methodForDecision(outcome.Decision)
		if retainedSignals.termKillDelivered() {
			method = model.QuiescenceTermKill
		}
		return PhysicalOutcome{
			Kind:     PhysicalOutcomeAbsent,
			Group:    group,
			Method:   method,
			Decision: outcome.Decision,
		}
	}
	absent, err := stableIndependentAbsent(ctx, group)
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

type retainedSignalRecorder struct {
	mu            sync.Mutex
	termDelivered bool
	killDelivered bool
}

func (recorder *retainedSignalRecorder) recordTerm(result containment.SignalResult, err error) {
	if recorder == nil || err != nil || result != containment.SignalDelivered {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.termDelivered = true
}

func (recorder *retainedSignalRecorder) recordKill(result containment.SignalResult, err error) {
	if recorder == nil || err != nil || result != containment.SignalDelivered {
		return
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.killDelivered = true
}

func (recorder *retainedSignalRecorder) termKillDelivered() bool {
	if recorder == nil {
		return false
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.termDelivered && recorder.killDelivered
}

type retainedSignalTrackingObject struct {
	inner    containment.RetainedGroupObject
	recorder *retainedSignalRecorder
}

func (object retainedSignalTrackingObject) AcquireRetainedGroup(ctx context.Context, target model.GroupRef, acquiredAt time.Time) (containment.RetainedGroupCapability, error) {
	if object.inner == nil {
		return nil, fmt.Errorf("%w: retained object is nil", ErrNativeCustodianUnavailable)
	}
	capability, err := object.inner.AcquireRetainedGroup(ctx, target, acquiredAt)
	if err != nil {
		return nil, err
	}
	if capability == nil {
		return nil, nil
	}
	return retainedSignalTrackingCapability{
		RetainedGroupCapability: capability,
		recorder:                object.recorder,
	}, nil
}

type retainedSignalTrackingCapability struct {
	containment.RetainedGroupCapability
	recorder *retainedSignalRecorder
}

func (capability retainedSignalTrackingCapability) SignalTerm(ctx context.Context) (containment.SignalResult, error) {
	if capability.RetainedGroupCapability == nil {
		return containment.SignalUnprovable, fmt.Errorf("%w: retained capability is nil", ErrNativeCustodianUnavailable)
	}
	result, err := capability.RetainedGroupCapability.SignalTerm(ctx)
	capability.recorder.recordTerm(result, err)
	return result, err
}

func (capability retainedSignalTrackingCapability) Kill(ctx context.Context) (containment.SignalResult, error) {
	if capability.RetainedGroupCapability == nil {
		return containment.SignalUnprovable, fmt.Errorf("%w: retained capability is nil", ErrNativeCustodianUnavailable)
	}
	result, err := capability.RetainedGroupCapability.Kill(ctx)
	capability.recorder.recordKill(result, err)
	return result, err
}

func nativeObserverFor(witness containment.ContinuityWitness) containment.Observer {
	observer := nativeContainmentObserver{}
	if retention, ok := witness.(*leaderRetention); ok {
		observer.retention = retention
	}
	return observer
}

type nativeContainmentObserver struct {
	retention *leaderRetention
}

func (observer nativeContainmentObserver) ObserveGroup(ctx context.Context, target model.GroupRef) (model.ContainmentObservation, error) {
	observation, err := containment.RealObserver{}.ObserveGroup(ctx, target)
	if err != nil {
		return model.ContainmentObservation{}, err
	}
	if observation.Group != model.GroupLive || observation.Leader != model.ProcessIdentityMatching {
		return observation, nil
	}
	leader, err := observeNativeLeader(target)
	if err != nil {
		observation.Leader = model.ProcessIdentityUnknown
		return observation, nil
	}
	if leader.Identity != model.ProcessIdentityMatching {
		observation.Leader = leader.Identity
		return observation, nil
	}
	if leader.RunState == procgroup.ProcessRunStateRunning {
		return observation, nil
	}
	if leader.RunState == procgroup.ProcessRunStateZombie && observer.retention != nil && observer.retention.unreapedFor(target) {
		return observation, nil
	}
	observation.Leader = model.ProcessIdentityUnknown
	return observation, nil
}

func observeNativeLeader(target model.GroupRef) (procgroup.ProcessObservation, error) {
	claim, err := procgroup.NewProcessClaim(
		target.Leader.PID,
		target.PGID,
		procgroup.StartToken(target.Leader.HighResStartToken),
		target.KernelDomain(),
	)
	if err != nil {
		return procgroup.ProcessObservation{}, err
	}
	return procgroup.ObserveProcess(claim), nil
}

func nativeSignalerFor(witness containment.ContinuityWitness) containment.Signaler {
	signaler := nativeContainmentSignaler{}
	if retention, ok := witness.(*leaderRetention); ok {
		signaler.retention = retention
	}
	return signaler
}

type nativeContainmentSignaler struct {
	retention *leaderRetention
}

// nativeContainmentSignaler keeps shared containment fail-closed while smoothing
// native owner facts: kill(0) EPERM proves group existence, and a signal EPERM is
// non-fatal only when the parent-held leader is the sole remaining group member.
func (signaler nativeContainmentSignaler) SignalGroup(ctx context.Context, target model.GroupRef, signal containment.Signal) (containment.SignalResult, error) {
	result, err := containment.RealSignaler{}.SignalGroup(ctx, target, signal)
	if err == nil {
		return result, nil
	}
	if errors.Is(err, unix.EPERM) && signaler.retention != nil && signaler.retention.unreapedFor(target) {
		soleLeader, soleErr := groupHasNoMembersExceptLeader(target)
		if soleErr == nil && soleLeader {
			return containment.SignalDelivered, nil
		}
	}
	return result, err
}

func (nativeContainmentSignaler) ProbeGroup(ctx context.Context, target model.GroupRef) (containment.ProbeResult, error) {
	result, err := containment.RealSignaler{}.ProbeGroup(ctx, target)
	if errors.Is(err, unix.EPERM) {
		return containment.ProbeLive, nil
	}
	return result, err
}

func methodForDecision(decision model.ContainmentDecision) model.QuiescenceMethod {
	if decision == model.AlreadyAbsent {
		return model.QuiescenceAlreadyAbsent
	}
	return model.QuiescenceTermKill
}

func attestPhysicalOutcome(issuer quiescenceAttestationIssuer, outcome PhysicalOutcome) (VerifiedQuiescence, error) {
	if !outcome.Absent() {
		return VerifiedQuiescence{}, physicalOutcomeError(outcome)
	}
	if issuer == nil {
		return VerifiedQuiescence{}, ErrInvalidAttestation
	}
	return issuer.AttestQuiescence(PhysicalQuiescence{
		Group:  outcome.Group,
		Method: outcome.Method,
	})
}

func physicalOutcomeError(outcome PhysicalOutcome) error {
	if outcome.Err != nil {
		return outcome.Err
	}
	if outcome.Kind == "" {
		return fmt.Errorf("%w: missing physical outcome", ErrPhysicalContainment)
	}
	if outcome.Reason != "" {
		return fmt.Errorf("%w: outcome=%s reason=%s", ErrPhysicalContainment, outcome.Kind, outcome.Reason)
	}
	return fmt.Errorf("%w: outcome=%s", ErrPhysicalContainment, outcome.Kind)
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

func independentlyAbsent(group model.GroupRef) (bool, error) {
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
		return false, nil
	default:
		return false, fmt.Errorf("independent group observation is %s", procgroup.ClassifyGroup(claim))
	}
}

func stableIndependentAbsent(ctx context.Context, group model.GroupRef) (bool, error) {
	absent, err := independentlyAbsent(group)
	if err != nil || !absent {
		return absent, err
	}
	if err := sleepContext(ctx, 20*time.Millisecond); err != nil {
		return false, err
	}
	return independentlyAbsent(group)
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cleanupLaunchedHandle(ctx context.Context, handle *parklaunch.ParkedHandle, params containment.Params) error {
	if handle == nil {
		return nil
	}
	var out error
	out = errors.Join(out, RealContainment{Params: params}.Contain(ctx, handle.GroupRef))
	_, waitErr := handle.WaitState()
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
	stdin, stdout, stderr := handle.Stdin, handle.Stdout, handle.Stderr
	handle.Stdin, handle.Stdout, handle.Stderr = nil, nil, nil
	var err error
	err = errors.Join(err, closeNativeProcessFile(stdin))
	err = errors.Join(err, closeNativeProcessFile(stdout))
	err = errors.Join(err, closeNativeProcessFile(stderr))
	return err
}

func closeNativeProcessFile(file *os.File) error {
	if file == nil {
		return nil
	}
	err := file.Close()
	if errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
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

func newLeaderRetentionForGroup(group model.GroupRef) (*leaderRetention, error) {
	if err := validateLeaderContinuityTarget(group); err != nil {
		return nil, err
	}
	platform, err := openNativeLeaderPlatformHandle(group.Leader.PID)
	if err != nil {
		return nil, err
	}
	if err := validateLeaderContinuityTarget(group); err != nil {
		_ = platform.close()
		return nil, err
	}
	return &leaderRetention{
		group:     group,
		platform:  platform,
		heldSince: time.Now(),
	}, nil
}

func validateLeaderContinuityTarget(group model.GroupRef) error {
	if err := group.Validate(); err != nil {
		return err
	}
	claim, err := procgroup.ReadProcessClaim(group.Leader.PID)
	if err != nil {
		return err
	}
	if claim.PID != group.Leader.PID ||
		claim.PGID != group.PGID ||
		claim.StartToken.String() != group.Leader.HighResStartToken ||
		!claim.KernelDomainID.Equal(group.KernelDomain()) {
		return fmt.Errorf("%w: leader identity changed before continuity handle acquisition", ErrNativeCustodianUnavailable)
	}
	return nil
}

func (retention *leaderRetention) attachHandle(handle *parklaunch.ParkedHandle) {
	if retention == nil {
		return
	}
	retention.mu.Lock()
	defer retention.mu.Unlock()
	retention.handle = handle
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

// unreapedFor is true only for genuine non-reaping parent ownership: this
// process must hold the ParkedHandle for the exact leader and must not have
// called Wait/WaitState. A pidfd or kqueue opened by a non-parent is only an
// exit-notification handle; it does not stop PID/PGID reuse after the real parent
// reaps the zombie.
//
// Ledger: robust after-daemon-death containment of TERM-ignoring descendants
// needs a reuse-proof group lifetime object, such as Linux cgroup v2
// cgroup.procs/cgroup.kill. That mechanism is forward-tracked and intentionally
// not approximated with pidfd/kqueue retention here.
func (retention *leaderRetention) unreapedFor(target model.GroupRef) bool {
	if retention == nil {
		return false
	}
	retention.mu.Lock()
	defer retention.mu.Unlock()
	if !retention.group.Equal(target) {
		return false
	}
	if retention.handle == nil || retention.handle.LeaderReaped() {
		return false
	}
	return retention.platform.held()
}

func (retention *leaderRetention) cloneExitNotification() (nativeLeaderPlatformHandle, error) {
	if retention == nil {
		return nativeLeaderPlatformHandle{}, fmt.Errorf("%w: leader retention is nil", ErrNativeCustodianUnavailable)
	}
	retention.mu.Lock()
	defer retention.mu.Unlock()
	if !retention.platform.held() {
		return nativeLeaderPlatformHandle{}, fmt.Errorf("%w: leader continuity handle is closed", ErrNativeCustodianUnavailable)
	}
	return retention.platform.clone()
}

func (retention *leaderRetention) waitExited(ctx context.Context) error {
	waitHandle, err := retention.cloneExitNotification()
	if err != nil {
		return err
	}
	defer waitHandle.close()
	return waitHandle.waitExited(ctx)
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
