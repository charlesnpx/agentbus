// Package parklaunch provides the parent-side parked-worker launch primitive.
package parklaunch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parkproto"
	"github.com/charlesnpx/agentbus/internal/procgroup"
)

const (
	workerControlReadFD  = 3
	workerControlWriteFD = 4
	workerBootstrapFD    = 5
)

var (
	ErrInvalidSpec              = errors.New("invalid park launch spec")
	ErrIdentityMismatch         = errors.New("parked worker identity mismatch")
	ErrChannelLostBeforeRelease = errors.New("parked worker channel lost before release")
	ErrReleaseAlreadySent       = errors.New("parked worker release already sent")
	ErrReleaseAck               = errors.New("parked worker release ack failed")
	ErrMonitorNotArmed          = errors.New("parklaunch monitor not armed")
)

// Containment is the narrow cleanup dependency injected by S3A/S3B later. It
// must not mint quiescence proofs or write durable state from this package.
type Containment interface {
	Contain(context.Context, model.GroupRef) error
}

// Spec describes one parent-side parked-worker launch.
type Spec struct {
	AgentbusPath  string
	ExecSpec      parkproto.ExecSpec
	CustodyID     model.CustodyID
	LaunchKey     model.LaunchKey
	LogicalGrant  model.LaunchGrant
	ReleaseSecret model.ReleaseSecret
	Containment   Containment
	Monitor       *MonitorProcessSpec

	WorkerEnv []string
	WorkerDir string

	identity identityReader
	hooks    launchHooks
}

type identityReader interface {
	ReadProcessClaim(pid int) (procgroup.ProcessClaim, error)
	ClassifyProcess(procgroup.ProcessClaim) model.ProcessIdentityObservation
	ClassifyGroup(procgroup.GroupClaim) model.GroupExistenceObservation
}

type nativeIdentityReader struct{}

func (nativeIdentityReader) ReadProcessClaim(pid int) (procgroup.ProcessClaim, error) {
	return procgroup.ReadProcessClaim(pid)
}

func (nativeIdentityReader) ClassifyProcess(claim procgroup.ProcessClaim) model.ProcessIdentityObservation {
	return procgroup.ClassifyProcess(claim)
}

func (nativeIdentityReader) ClassifyGroup(claim procgroup.GroupClaim) model.GroupExistenceObservation {
	return procgroup.ClassifyGroup(claim)
}

type launchHooks struct {
	afterPipesCreated   func(launchPipeSnapshot) error
	afterMonitorStarted func(*MonitorProcess) error
	afterWorkerStarted  func(int) error
	beforeRelease       func(launchControlSnapshot) error
}

type launchPipeSnapshot struct {
	ControlWriteFD   int
	BootstrapWriteFD int
}

type launchControlSnapshot struct {
	ControlWrite *os.File
	ControlRead  *os.File
}

// ParkedHandle is returned after the worker has acknowledged the one release
// and exec'd the backend with the same PID/group identity.
type ParkedHandle struct {
	GroupRef model.GroupRef
	Stdin    *os.File
	Stdout   *os.File
	Stderr   *os.File
	Monitor  *MonitorProcess

	cmd      *exec.Cmd
	done     <-chan error
	releaser *releaseGate
}

func (handle *ParkedHandle) Done() <-chan error {
	return handle.done
}

func (handle *ParkedHandle) Wait() error {
	if handle == nil || handle.done == nil {
		return nil
	}
	return <-handle.done
}

// Release is intentionally one-use. Launch already sent the release; calling
// this method again returns ErrReleaseAlreadySent without writing the channel.
func (handle *ParkedHandle) Release(context.Context) error {
	if handle == nil || handle.releaser == nil {
		return ErrReleaseAlreadySent
	}
	return handle.releaser.send(nil, parkproto.Release{})
}

type releaseGate struct {
	mu   sync.Mutex
	sent bool
}

func (gate *releaseGate) send(writer *parkproto.Writer, release parkproto.Release) error {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.sent {
		return ErrReleaseAlreadySent
	}
	gate.sent = true
	if writer == nil {
		return ErrReleaseAlreadySent
	}
	seq, err := writer.WriteRelease(release)
	if err != nil {
		return err
	}
	if seq != release.Binding.Sequence {
		return fmt.Errorf("release sequence = %d, want %d", seq, release.Binding.Sequence)
	}
	return nil
}

// Launch starts a parked worker in a fresh process group, verifies its kernel
// identity independently, sends exactly one release, waits for ReleaseAck, and
// returns the backend stdio streams. It does not wait for, reconcile, or attest
// backend completion.
func Launch(ctx context.Context, spec Spec) (*ParkedHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if spec.identity == nil {
		spec.identity = nativeIdentityReader{}
	}
	if spec.WorkerEnv == nil {
		spec.WorkerEnv = os.Environ()
	}
	if err := validateSpec(spec); err != nil {
		return nil, err
	}

	pipes, err := openLaunchPipes()
	if err != nil {
		return nil, err
	}
	defer pipes.closeParentOnReturn()
	if spec.hooks.afterPipesCreated != nil {
		snapshot := launchPipeSnapshot{
			ControlWriteFD:   int(pipes.toWorkerWrite.Fd()),
			BootstrapWriteFD: int(pipes.bootstrapWrite.Fd()),
		}
		if err := spec.hooks.afterPipesCreated(snapshot); err != nil {
			return nil, err
		}
	}

	monitor, err := StartMonitorProcess(ctx, *spec.Monitor)
	if err != nil {
		return nil, fmt.Errorf("start monitor: %w", err)
	}
	launchSucceeded := false
	defer func() {
		if !launchSucceeded {
			_ = closeMonitorTarget(monitor)
			_ = closeMonitorReady(monitor)
			_ = waitMonitorOrKill(monitor)
			_ = closeMonitorDaemonControl(monitor)
		}
	}()
	if spec.hooks.afterMonitorStarted != nil {
		if err := spec.hooks.afterMonitorStarted(monitor); err != nil {
			return nil, err
		}
	}

	cmd := exec.Command(spec.AgentbusPath,
		"internal-parked-worker",
		"--control-read-fd", fmt.Sprint(workerControlReadFD),
		"--control-write-fd", fmt.Sprint(workerControlWriteFD),
		"--bootstrap-fd", fmt.Sprint(workerBootstrapFD),
	)
	cmd.Env = append([]string(nil), spec.WorkerEnv...)
	cmd.Dir = spec.WorkerDir
	cmd.Stdin = pipes.backendStdinRead
	cmd.Stdout = pipes.backendStdoutWrite
	cmd.Stderr = pipes.backendStderrWrite
	cmd.ExtraFiles = []*os.File{pipes.toWorkerRead, pipes.fromWorkerWrite, pipes.bootstrapRead}
	cmd.SysProcAttr = newProcessGroupSysProcAttr()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start parked worker: %w", err)
	}
	pipes.closeWorkerCopiesInParent()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	started := true
	var group model.GroupRef
	released := false
	preIdentityAbort := func(cause error) error {
		cleanupCtx, cancel := cleanupContext()
		defer cancel()
		if err := terminateStartedProcess(cleanupCtx, cmd.Process, done); err != nil {
			return errors.Join(cause, fmt.Errorf("terminate parked worker before identity: %w", err))
		}
		return cause
	}
	defer func() {
		if started && !released {
			_ = pipes.closeControl()
		}
	}()
	if spec.hooks.afterWorkerStarted != nil {
		if err := spec.hooks.afterWorkerStarted(cmd.Process.Pid); err != nil {
			return nil, preIdentityAbort(err)
		}
	}

	workerClaim, err := waitProcessGroupLeaderClaim(ctx, spec.identity, cmd.Process.Pid)
	if err != nil {
		return nil, preIdentityAbort(fmt.Errorf("%w: read worker identity before bootstrap: %v", ErrIdentityMismatch, err))
	}
	monitorClaim, err := waitProcessGroupLeaderClaim(ctx, spec.identity, monitor.PID)
	if err != nil {
		return nil, preIdentityAbort(fmt.Errorf("read monitor identity: %w", err))
	}
	if workerClaim.PGID != workerClaim.PID {
		return nil, preIdentityAbort(fmt.Errorf("%w: worker pgid=%d pid=%d", ErrIdentityMismatch, workerClaim.PGID, workerClaim.PID))
	}
	if monitorClaim.PGID != monitorClaim.PID {
		return nil, preIdentityAbort(fmt.Errorf("monitor pgid=%d pid=%d", monitorClaim.PGID, monitorClaim.PID))
	}
	if monitorClaim.PGID == workerClaim.PGID {
		return nil, preIdentityAbort(fmt.Errorf("monitor joined target process group %d", workerClaim.PGID))
	}

	group = groupRefFromClaims(workerClaim, monitorClaim, spec.CustodyID, spec.LaunchKey)
	expectation, err := releaseExpectation(spec, group)
	if err != nil {
		return nil, failClosed(spec.Containment, group, err)
	}
	if err := writeReleaseExpectation(pipes.bootstrapWrite, expectation); err != nil {
		return nil, failClosed(spec.Containment, group, fmt.Errorf("write release expectation bootstrap: %w", err))
	}
	_ = pipes.bootstrapWrite.Close()
	pipes.bootstrapWrite = nil

	if err := monitor.BindTarget(group); err != nil {
		return nil, failClosed(spec.Containment, group, fmt.Errorf("%w: bind monitor target: %v", ErrMonitorNotArmed, err))
	}
	if err := monitor.WaitReady(ctx); err != nil {
		return nil, failClosed(spec.Containment, group, err)
	}

	reader := parkproto.NewReader(pipes.fromWorkerRead)
	received, err := readParkFrame(ctx, reader)
	if err != nil {
		return nil, failClosed(spec.Containment, group, fmt.Errorf("%w: read identity report: %v", ErrChannelLostBeforeRelease, err))
	}
	report, ok := received.Message.(parkproto.IdentityReport)
	if !ok {
		return nil, failClosed(spec.Containment, group, fmt.Errorf("%w: first worker frame was %T", ErrIdentityMismatch, received.Message))
	}
	if err := verifyIdentityReport(spec.identity, report, workerClaim, group); err != nil {
		return nil, failClosed(spec.Containment, group, err)
	}
	if spec.hooks.beforeRelease != nil {
		if err := spec.hooks.beforeRelease(launchControlSnapshot{ControlWrite: pipes.toWorkerWrite, ControlRead: pipes.fromWorkerRead}); err != nil {
			return nil, failClosed(spec.Containment, group, err)
		}
	}
	if err := verifyMonitorReadyIdentity(spec.identity, monitor, monitorClaim, group); err != nil {
		return nil, failClosed(spec.Containment, group, err)
	}

	writer := parkproto.NewWriter(pipes.toWorkerWrite)
	release := releaseForReport(expectation, group, spec.ExecSpec, report)
	releaser := &releaseGate{}
	if err := releaser.send(writer, release); err != nil {
		return nil, failClosed(spec.Containment, group, fmt.Errorf("%w: %v", ErrChannelLostBeforeRelease, err))
	}
	ackFrame, err := readParkFrame(ctx, reader)
	if err != nil {
		return nil, failClosed(spec.Containment, group, fmt.Errorf("%w: %v", ErrReleaseAck, err))
	}
	ack, ok := ackFrame.Message.(parkproto.ReleaseAck)
	if !ok {
		return nil, failClosed(spec.Containment, group, fmt.Errorf("%w: got %T", ErrReleaseAck, ackFrame.Message))
	}
	if ackFrame.Sequence != 2 || ack.AcceptedSequence != release.Binding.Sequence {
		return nil, failClosed(spec.Containment, group, fmt.Errorf("%w: sequence=%d accepted=%d", ErrReleaseAck, ackFrame.Sequence, ack.AcceptedSequence))
	}

	released = true
	pipes.closeWorkerControlInParent()
	handle := &ParkedHandle{
		GroupRef: group,
		Stdin:    pipes.backendStdinWrite,
		Stdout:   pipes.backendStdoutRead,
		Stderr:   pipes.backendStderrRead,
		Monitor:  monitor,
		cmd:      cmd,
		done:     done,
		releaser: releaser,
	}
	pipes.disownBackendParentFiles()
	launchSucceeded = true
	return handle, nil
}

func validateSpec(spec Spec) error {
	if spec.AgentbusPath == "" {
		return fmt.Errorf("%w: agentbus path is required", ErrInvalidSpec)
	}
	if spec.Monitor == nil {
		return fmt.Errorf("%w: monitor is required", ErrInvalidSpec)
	}
	if spec.Containment == nil {
		return fmt.Errorf("%w: containment is required", ErrInvalidSpec)
	}
	if err := spec.ExecSpec.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}
	if err := spec.CustodyID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}
	if err := spec.LaunchKey.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}
	if err := spec.LogicalGrant.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}
	if !spec.LogicalGrant.Attempt.Equal(spec.LaunchKey.Attempt) || spec.LogicalGrant.Ordinal != spec.LaunchKey.Ordinal {
		return fmt.Errorf("%w: logical grant does not match launch key", ErrInvalidSpec)
	}
	if err := spec.ReleaseSecret.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSpec, err)
	}
	return nil
}

func releaseExpectation(spec Spec, group model.GroupRef) (parkproto.ReleaseExpectation, error) {
	execDigest, err := parkproto.DigestExecSpec(spec.ExecSpec)
	if err != nil {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("digest exec spec: %w", err)
	}
	groupDigest, err := parkproto.DigestGroupRef(group)
	if err != nil {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("digest group ref: %w", err)
	}
	expectation := parkproto.ReleaseExpectation{Binding: parkproto.ReleaseBinding{
		ProtocolVersion:     parkproto.Version,
		Sequence:            1,
		CustodyID:           spec.CustodyID,
		LaunchKey:           spec.LaunchKey,
		GroupRefDigest:      groupDigest,
		LogicalGrant:        spec.LogicalGrant,
		ReleaseSecret:       spec.ReleaseSecret,
		ImmutableExecDigest: execDigest,
	}}
	if err := expectation.Validate(); err != nil {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("release expectation: %w", err)
	}
	return expectation, nil
}

func writeReleaseExpectation(file *os.File, expectation parkproto.ReleaseExpectation) error {
	raw, err := json.Marshal(expectation)
	if err != nil {
		return err
	}
	defer zeroBytes(raw)
	if _, err := file.Write(raw); err != nil {
		return err
	}
	return nil
}

func releaseForReport(expectation parkproto.ReleaseExpectation, group model.GroupRef, execSpec parkproto.ExecSpec, report parkproto.IdentityReport) parkproto.Release {
	binding := expectation.Binding
	binding.ParkInstanceID = report.ParkInstanceID
	binding.StartToken = report.StartToken
	return parkproto.Release{
		Binding:          binding,
		ExpectedGroupRef: group,
		ExecSpec:         execSpec,
	}
}

func verifyIdentityReport(reader identityReader, report parkproto.IdentityReport, initial procgroup.ProcessClaim, group model.GroupRef) error {
	if err := report.Validate(); err != nil {
		return fmt.Errorf("%w: invalid report: %v", ErrIdentityMismatch, err)
	}
	if report.PID != initial.PID || report.PGID != initial.PGID || report.StartToken != initial.StartToken || !report.KernelDomainID.Equal(initial.KernelDomainID) {
		return fmt.Errorf("%w: report does not match pre-bootstrap kernel read", ErrIdentityMismatch)
	}
	claim, err := procgroup.NewProcessClaim(report.PID, report.PGID, report.StartToken, report.KernelDomainID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIdentityMismatch, err)
	}
	if observed := reader.ClassifyProcess(claim); observed != model.ProcessIdentityMatching {
		return fmt.Errorf("%w: process observation=%s", ErrIdentityMismatch, observed)
	}
	reread, err := reader.ReadProcessClaim(report.PID)
	if err != nil {
		return fmt.Errorf("%w: re-read process: %v", ErrIdentityMismatch, err)
	}
	if reread.PID != report.PID || reread.PGID != report.PGID || reread.StartToken != report.StartToken || !reread.KernelDomainID.Equal(report.KernelDomainID) {
		return fmt.Errorf("%w: re-read claim differs", ErrIdentityMismatch)
	}
	groupClaim, err := procgroup.NewGroupClaim(group.PGID, group.KernelDomain())
	if err != nil {
		return fmt.Errorf("%w: group claim: %v", ErrIdentityMismatch, err)
	}
	if observed := reader.ClassifyGroup(groupClaim); observed != model.GroupLive {
		return fmt.Errorf("%w: group observation=%s", ErrIdentityMismatch, observed)
	}
	if group.PGID != report.PGID || group.Leader.PID != report.PID || group.Leader.HighResStartToken != report.StartToken.String() {
		return fmt.Errorf("%w: group ref does not bind reported leader", ErrIdentityMismatch)
	}
	return nil
}

func verifyMonitorReadyIdentity(reader identityReader, monitor *MonitorProcess, initial procgroup.ProcessClaim, group model.GroupRef) error {
	if monitor == nil {
		return fmt.Errorf("%w: monitor process is nil", ErrMonitorNotArmed)
	}
	select {
	case <-monitor.wait.exited:
		return fmt.Errorf("%w: monitor exited before final identity check: %v", ErrMonitorNotArmed, monitor.wait.Err())
	default:
	}
	reread, err := reader.ReadProcessClaim(monitor.PID)
	if err != nil {
		return fmt.Errorf("%w: re-read monitor identity: %v", ErrIdentityMismatch, err)
	}
	if reread.PID != initial.PID || reread.PGID != initial.PGID || reread.StartToken != initial.StartToken || !reread.KernelDomainID.Equal(initial.KernelDomainID) {
		return fmt.Errorf("%w: monitor re-read claim differs", ErrIdentityMismatch)
	}
	if reread.PGID != reread.PID {
		return fmt.Errorf("%w: monitor pgid=%d pid=%d", ErrIdentityMismatch, reread.PGID, reread.PID)
	}
	if reread.PGID == group.PGID {
		return fmt.Errorf("%w: monitor joined target process group %d", ErrIdentityMismatch, group.PGID)
	}
	if observed := reader.ClassifyProcess(reread); observed != model.ProcessIdentityMatching {
		return fmt.Errorf("%w: monitor process observation=%s", ErrIdentityMismatch, observed)
	}
	groupClaim, err := procgroup.NewGroupClaim(reread.PGID, reread.KernelDomainID)
	if err != nil {
		return fmt.Errorf("%w: monitor group claim: %v", ErrIdentityMismatch, err)
	}
	if observed := reader.ClassifyGroup(groupClaim); observed != model.GroupLive {
		return fmt.Errorf("%w: monitor group observation=%s", ErrIdentityMismatch, observed)
	}
	if group.Monitor.PID != reread.PID || group.Monitor.HighResStartToken != reread.StartToken.String() {
		return fmt.Errorf("%w: group ref does not bind monitor identity", ErrIdentityMismatch)
	}
	select {
	case <-monitor.wait.exited:
		return fmt.Errorf("%w: monitor exited after final identity check: %v", ErrMonitorNotArmed, monitor.wait.Err())
	default:
	}
	return nil
}

func groupRefFromClaims(worker, monitor procgroup.ProcessClaim, custodyID model.CustodyID, launchKey model.LaunchKey) model.GroupRef {
	return model.GroupRef{
		Version:           1,
		CustodyID:         custodyID,
		Launch:            launchKey,
		HostBootID:        worker.KernelDomainID.HostBootID,
		PIDNamespaceID:    worker.KernelDomainID.PIDNamespaceID,
		PIDNamespaceState: worker.KernelDomainID.PIDNamespaceState,
		PGID:              worker.PGID,
		Leader: model.ProcessIdentity{
			PID:               worker.PID,
			HighResStartToken: worker.StartToken.String(),
		},
		Monitor: model.ProcessIdentity{
			PID:               monitor.PID,
			HighResStartToken: monitor.StartToken.String(),
		},
	}
}

func waitProcessGroupLeaderClaim(ctx context.Context, reader identityReader, pid int) (procgroup.ProcessClaim, error) {
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for {
		claim, err := reader.ReadProcessClaim(pid)
		if err == nil && claim.PGID == claim.PID {
			return claim, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("pgid=%d pid=%d", claim.PGID, claim.PID)
		}
		if time.Now().After(deadline) {
			return procgroup.ProcessClaim{}, lastErr
		}
		select {
		case <-ctx.Done():
			return procgroup.ProcessClaim{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func readParkFrame(ctx context.Context, reader *parkproto.Reader) (parkproto.Received, error) {
	type readResult struct {
		received parkproto.Received
		err      error
	}
	done := make(chan readResult, 1)
	go func() {
		received, err := reader.Read()
		done <- readResult{received: received, err: err}
	}()
	select {
	case result := <-done:
		return result.received, result.err
	case <-ctx.Done():
		return parkproto.Received{}, ctx.Err()
	}
}

func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

func failClosed(containment Containment, group model.GroupRef, cause error) error {
	if group.Validate() == nil && containment != nil {
		ctx, cancel := cleanupContext()
		defer cancel()
		if err := containment.Contain(ctx, group); err != nil {
			return errors.Join(cause, fmt.Errorf("contain target group: %w", err))
		}
	}
	return cause
}

func terminateStartedProcess(ctx context.Context, process *os.Process, done <-chan error) error {
	if process == nil || done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	default:
	}
	_ = process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		timer.Stop()
		return nil
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
	}
	_ = process.Kill()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func zeroBytes(raw []byte) {
	for i := range raw {
		raw[i] = 0
	}
}

func discardAndClose(file *os.File) {
	if file != nil {
		_, _ = io.Copy(io.Discard, file)
		_ = file.Close()
	}
}
