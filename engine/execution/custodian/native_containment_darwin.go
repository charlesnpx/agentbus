//go:build darwin

package custodian

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
)

const (
	darwinPlatformSelfTestTimeout = 5 * time.Second
	darwinPlatformProbeSleep      = "/bin/sleep"
)

var darwinPlatformSelfTest = struct {
	once sync.Once
	err  error
}{}

type leaderNativeContainmentBackend struct {
	factory   func(model.GroupRef) (*leaderRetention, error)
	retention *leaderRetention
}

func newNativeContainmentBackend(ctx context.Context, custodian *NativeCustodian) (nativeContainmentBackend, error) {
	if custodian.options.newRetainedGroup != nil {
		return newRetainedNativeContainmentBackend(ctx, custodian, platformRetainedGroupFactory(custodian.options.newRetainedGroup))
	}
	factory := custodian.options.newLeaderRetention
	if factory == nil {
		factory = newLeaderRetentionForGroup
	}
	return &leaderNativeContainmentBackend{factory: factory}, nil
}

func platformRetainedGroupFactory(factory func() (containment.RetainedGroupObject, error)) func() (containment.RetainedGroupObject, error) {
	return factory
}

func (backend *leaderNativeContainmentBackend) retainedID() string {
	return ""
}

func (backend *leaderNativeContainmentBackend) retainLeaderUnreaped() bool {
	return true
}

func (backend *leaderNativeContainmentBackend) beforeMonitorBind(_ context.Context, group model.GroupRef) (model.GroupRef, error) {
	if backend == nil || backend.factory == nil {
		return model.GroupRef{}, fmt.Errorf("%w: leader containment backend is nil", ErrNativeCustodianUnavailable)
	}
	backend.retention = nil
	retention, err := backend.factory(group)
	if err != nil {
		return model.GroupRef{}, err
	}
	backend.retention = retention
	return group, nil
}

func (backend *leaderNativeContainmentBackend) beforeRelease(_ context.Context, group model.GroupRef) error {
	if backend == nil || backend.retention == nil {
		return fmt.Errorf("%w: leader retention was not acquired before release", ErrNativeCustodianUnavailable)
	}
	if !backend.retention.group.Equal(group) {
		return fmt.Errorf("%w: leader retention group mismatch before release", ErrNativeCustodianUnavailable)
	}
	return nil
}

func (backend *leaderNativeContainmentBackend) witness() containment.ContinuityWitness {
	if backend == nil || backend.retention == nil {
		return nil
	}
	return backend.retention
}

func (backend *leaderNativeContainmentBackend) witnessAcquired() bool {
	return backend != nil && backend.retention != nil
}

func (backend *leaderNativeContainmentBackend) retainedObject() containment.RetainedGroupObject {
	return nil
}

func (backend *leaderNativeContainmentBackend) monitorLeafFile(context.Context) (*os.File, error) {
	return nil, nil
}

func (backend *leaderNativeContainmentBackend) attachHandle(handle *parklaunch.ParkedHandle) {
	if backend == nil || backend.retention == nil {
		return
	}
	backend.retention.attachHandle(handle)
}

func (backend *leaderNativeContainmentBackend) leaderRetention() *leaderRetention {
	if backend == nil {
		return nil
	}
	return backend.retention
}

func (backend *leaderNativeContainmentBackend) close(context.Context) error {
	if backend == nil || backend.retention == nil {
		return nil
	}
	return backend.retention.close()
}

func platformRealContainment(_ context.Context, real RealContainment, _ model.GroupRef) (RealContainment, error) {
	return real, nil
}

func platformBindContainmentTarget(_ context.Context, real RealContainment, group model.GroupRef) (parklaunch.Containment, error) {
	retention, err := newLeaderRetentionForGroup(group)
	if err != nil {
		return nil, err
	}
	return RealContainment{Params: real.Params, Witness: retention}, nil
}

func prepareNativeRuntimePlatformOptions(options NativeOptions) (NativeOptions, func() error, error) {
	return options, nil, nil
}

func nativeRuntimePlatformSelfTestEnabled() bool {
	return nativeRuntimeDarwinPlatformSelfTest() == nil
}

func nativeRuntimePlatformUnsupportedCause() error {
	if err := nativeRuntimeDarwinPlatformSelfTest(); err != nil {
		return fmt.Errorf("%w: %w", ErrNativeRuntimeUnsupported, err)
	}
	return nil
}

func nativeRuntimePlatformUnsupportedError(err error) bool {
	return errors.Is(err, ErrNativeRuntimeUnsupported)
}

func nativeRuntimeDarwinPlatformSelfTest() error {
	darwinPlatformSelfTest.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), darwinPlatformSelfTestTimeout)
		defer cancel()
		darwinPlatformSelfTest.err = runDarwinPlatformSelfTest(ctx)
	})
	return darwinPlatformSelfTest.err
}

func runDarwinPlatformSelfTest(ctx context.Context) error {
	if err := probeDarwinPlatformProcessGroup(ctx, "setpgid", &syscall.SysProcAttr{Setpgid: true}, unix.SIGTERM); err != nil {
		return fmt.Errorf("darwin setpgid process-group supervision probe: %w", err)
	}
	if err := probeDarwinPlatformProcessGroup(ctx, "setsid", &syscall.SysProcAttr{Setsid: true}, unix.SIGKILL); err != nil {
		return fmt.Errorf("darwin setsid process-group supervision probe: %w", err)
	}
	return nil
}

func probeDarwinPlatformProcessGroup(ctx context.Context, name string, attr *syscall.SysProcAttr, signal syscall.Signal) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.Command(darwinPlatformProbeSleep, "60")
	cmd.SysProcAttr = attr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s probe: %w", name, err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
		close(waitDone)
	}()

	pgid := cmd.Process.Pid
	waited := false
	defer func() {
		if waited {
			return
		}
		err = errors.Join(err, cleanupDarwinPlatformProbeProcess(pgid, waitDone))
	}()

	leaderClaim, err := waitDarwinPlatformProbeLeaderClaim(ctx, cmd.Process.Pid)
	if err != nil {
		return err
	}
	if leaderClaim.PGID != leaderClaim.PID {
		return fmt.Errorf("%s probe pgid=%d pid=%d, want process-group leader", name, leaderClaim.PGID, leaderClaim.PID)
	}
	group, err := darwinPlatformProbeGroupRef(leaderClaim, name)
	if err != nil {
		return err
	}
	retention, err := newLeaderRetentionForGroup(group)
	if err != nil {
		return fmt.Errorf("acquire kqueue leader retention: %w", err)
	}
	defer func() {
		err = errors.Join(err, retention.close())
	}()

	if err := unix.Kill(-group.PGID, signal); err != nil && !errors.Is(err, unix.ESRCH) {
		return fmt.Errorf("signal %s probe group %s: %w", name, signal, err)
	}
	if err := retention.waitExited(ctx); err != nil {
		return fmt.Errorf("wait %s probe kqueue NOTE_EXIT: %w", name, err)
	}
	waitErr := waitDarwinPlatformProbeProcess(ctx, waitDone)
	waited = true
	if waitErr != nil {
		return fmt.Errorf("wait %s probe process: %w", name, waitErr)
	}
	absent, err := stableIndependentAbsent(ctx, group)
	if err != nil {
		return fmt.Errorf("prove %s probe group absent: %w", name, err)
	}
	if !absent {
		return fmt.Errorf("%s probe group is not stably absent after %s and wait", name, signal)
	}
	return nil
}

func waitDarwinPlatformProbeLeaderClaim(ctx context.Context, pid int) (procgroup.ProcessClaim, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return procgroup.ProcessClaim{}, err
		}
		claim, err := procgroup.ReadProcessClaim(pid)
		if err == nil {
			return claim, nil
		}
		if !errors.Is(err, procgroup.ErrProcessMissing) {
			return procgroup.ProcessClaim{}, err
		}
		if err := sleepContext(ctx, 10*time.Millisecond); err != nil {
			return procgroup.ProcessClaim{}, err
		}
	}
}

func darwinPlatformProbeGroupRef(leader procgroup.ProcessClaim, name string) (model.GroupRef, error) {
	monitor, err := procgroup.ReadProcessClaim(os.Getpid())
	if err != nil {
		return model.GroupRef{}, fmt.Errorf("read monitor process claim: %w", err)
	}
	group := model.GroupRef{
		Version:             1,
		CustodyID:           model.CustodyID("custody-darwin-platform-self-test-" + name),
		Launch:              darwinPlatformProbeLaunchKey(name),
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
	}
	if err := group.Validate(); err != nil {
		return model.GroupRef{}, err
	}
	return group, nil
}

func darwinPlatformProbeLaunchKey(name string) model.LaunchKey {
	return model.LaunchKey{
		Attempt: model.AttemptRef{
			JobID:     model.JobID("job-darwin-platform-self-test-" + name),
			AttemptID: model.AttemptID("attempt-darwin-platform-self-test-" + name),
			Epoch:     1,
		},
		Ordinal: model.LaunchOrdinalOne,
	}
}

func waitDarwinPlatformProbeProcess(ctx context.Context, waitDone <-chan error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case err, ok := <-waitDone:
		if !ok {
			return nil
		}
		if err == nil {
			return nil
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cleanupDarwinPlatformProbeProcess(pgid int, waitDone <-chan error) error {
	if pgid <= 1 {
		return nil
	}
	killErr := unix.Kill(-pgid, unix.SIGKILL)
	if errors.Is(killErr, unix.ESRCH) {
		killErr = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return errors.Join(killErr, waitDarwinPlatformProbeProcess(ctx, waitDone))
}
