package parklaunch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parkproto"
)

const (
	MonitorDaemonControlFD = 3
	MonitorTargetFD        = 4
	MonitorReadyFD         = 5
)

// MonitorProcessSpec describes a one-shot monitor process. The command must
// call RunMonitorFromFDs with MonitorDaemonControlFD, MonitorTargetFD, and
// MonitorReadyFD.
type MonitorProcessSpec struct {
	Command CommandSpec
	Stderr  io.Writer
}

type CommandSpec struct {
	Path string
	Args []string
	Env  []string
	Dir  string
}

type MonitorProcess struct {
	PID int

	DaemonControlWrite *os.File

	cmd         *exec.Cmd
	targetWrite *os.File
	readyRead   *os.File
	wait        *processWait
	done        <-chan error
	bindOnce    sync.Once
	bindErr     error
}

func (process *MonitorProcess) Done() <-chan error {
	return process.done
}

func (process *MonitorProcess) Wait() error {
	if process == nil || process.done == nil {
		return nil
	}
	return <-process.done
}

func (process *MonitorProcess) BindTarget(group model.GroupRef) error {
	if process == nil {
		return fmt.Errorf("monitor process is nil")
	}
	process.bindOnce.Do(func() {
		if err := group.Validate(); err != nil {
			process.bindErr = err
			return
		}
		raw, err := json.Marshal(group)
		if err != nil {
			process.bindErr = err
			return
		}
		if _, err := process.targetWrite.Write(raw); err != nil {
			process.bindErr = err
			return
		}
		process.bindErr = process.targetWrite.Close()
		process.targetWrite = nil
	})
	return process.bindErr
}

// StartMonitorProcess starts a one-shot monitor helper in its own process
// group. It creates the daemon control pipe itself, gives only the read end to
// the monitor, and returns the CLOEXEC writer to the daemon owner.
func StartMonitorProcess(ctx context.Context, spec MonitorProcessSpec) (*MonitorProcess, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if spec.Command.Path == "" {
		return nil, fmt.Errorf("%w: monitor command path is required", ErrInvalidSpec)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	daemonRead, daemonWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	targetRead, targetWrite, err := os.Pipe()
	if err != nil {
		_ = daemonRead.Close()
		_ = daemonWrite.Close()
		return nil, err
	}
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		_ = daemonRead.Close()
		_ = daemonWrite.Close()
		_ = targetRead.Close()
		_ = targetWrite.Close()
		return nil, err
	}
	setCloseOnExec(daemonRead)
	setCloseOnExec(daemonWrite)
	setCloseOnExec(targetRead)
	setCloseOnExec(targetWrite)
	setCloseOnExec(readyRead)
	setCloseOnExec(readyWrite)

	args := spec.Command.Args
	if len(args) == 0 {
		args = []string{spec.Command.Path}
	}
	env := spec.Command.Env
	if env == nil {
		env = os.Environ()
	}
	cmd := exec.Command(spec.Command.Path, args[1:]...)
	cmd.Args = append([]string(nil), args...)
	cmd.Env = append([]string(nil), env...)
	cmd.Dir = spec.Command.Dir
	cmd.Stderr = spec.Stderr
	cmd.ExtraFiles = []*os.File{daemonRead, targetRead, readyWrite}
	cmd.SysProcAttr = newProcessGroupSysProcAttr()
	if err := cmd.Start(); err != nil {
		_ = daemonRead.Close()
		_ = daemonWrite.Close()
		_ = targetRead.Close()
		_ = targetWrite.Close()
		_ = readyRead.Close()
		_ = readyWrite.Close()
		return nil, err
	}
	_ = daemonRead.Close()
	_ = targetRead.Close()
	_ = readyWrite.Close()

	wait := startProcessWait(cmd)
	return &MonitorProcess{
		PID:                cmd.Process.Pid,
		DaemonControlWrite: daemonWrite,
		cmd:                cmd,
		targetWrite:        targetWrite,
		readyRead:          readyRead,
		wait:               wait,
		done:               wait.done,
	}, nil
}

func (process *MonitorProcess) WaitReady(ctx context.Context) error {
	if process == nil {
		return fmt.Errorf("%w: monitor process is nil", ErrMonitorNotArmed)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if process.readyRead == nil {
		return nil
	}
	readyRead := process.readyRead
	readDone := make(chan error, 1)
	go func() {
		var ack [1]byte
		if _, err := io.ReadFull(readyRead, ack[:]); err != nil {
			readDone <- err
			return
		}
		if ack[0] != '1' {
			readDone <- fmt.Errorf("unexpected readiness byte %q", ack[0])
			return
		}
		readDone <- nil
	}()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	defer func() {
		_ = readyRead.Close()
		if process.readyRead == readyRead {
			process.readyRead = nil
		}
	}()
	select {
	case err := <-readDone:
		if err != nil {
			return fmt.Errorf("%w: read readiness ack: %v", ErrMonitorNotArmed, err)
		}
		return nil
	case <-process.wait.exited:
		return fmt.Errorf("%w: monitor exited before readiness: %v", ErrMonitorNotArmed, process.wait.Err())
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrMonitorNotArmed, ctx.Err())
	case <-timer.C:
		return fmt.Errorf("%w: timeout waiting for readiness ack", ErrMonitorNotArmed)
	}
}

type MonitorRunSpec struct {
	DaemonControl io.Reader
	Ready         io.Writer
	Target        model.GroupRef
	Containment   Containment
}

// RunMonitor waits for daemon-channel EOF, invokes the injected containment
// once for the exact target GroupRef, then exits. It never mints proofs or
// writes durable state.
func RunMonitor(ctx context.Context, spec MonitorRunSpec) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if spec.DaemonControl == nil {
		return fmt.Errorf("%w: monitor daemon control reader is required", ErrInvalidSpec)
	}
	if spec.Containment == nil {
		return fmt.Errorf("%w: monitor containment is required", ErrInvalidSpec)
	}
	if err := spec.Target.Validate(); err != nil {
		return fmt.Errorf("%w: monitor target: %v", ErrInvalidSpec, err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, spec.DaemonControl)
		done <- err
	}()
	if spec.Ready != nil {
		if _, err := spec.Ready.Write([]byte{'1'}); err != nil {
			return fmt.Errorf("write monitor readiness: %w", err)
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			return fmt.Errorf("read daemon control: %w", err)
		}
		return spec.Containment.Contain(ctx, spec.Target)
	}
}

func RunMonitorFromFDs(ctx context.Context, daemonFD, targetFD, readyFD int, containment Containment) error {
	daemonFile := os.NewFile(uintptr(daemonFD), "agentbus-parklaunch-monitor-daemon")
	if daemonFile == nil {
		return fmt.Errorf("open daemon fd %d", daemonFD)
	}
	defer daemonFile.Close()
	setCloseOnExec(daemonFile)
	targetFile := os.NewFile(uintptr(targetFD), "agentbus-parklaunch-monitor-target")
	if targetFile == nil {
		return fmt.Errorf("open target fd %d", targetFD)
	}
	defer targetFile.Close()
	setCloseOnExec(targetFile)
	readyFile := os.NewFile(uintptr(readyFD), "agentbus-parklaunch-monitor-ready")
	if readyFile == nil {
		return fmt.Errorf("open ready fd %d", readyFD)
	}
	defer readyFile.Close()
	setCloseOnExec(readyFile)
	target, err := readMonitorTarget(targetFile)
	if err != nil {
		return err
	}
	return RunMonitor(ctx, MonitorRunSpec{
		DaemonControl: daemonFile,
		Ready:         readyFile,
		Target:        target,
		Containment:   containment,
	})
}

func readMonitorTarget(r io.Reader) (model.GroupRef, error) {
	raw, err := io.ReadAll(io.LimitReader(r, parkproto.MaxFrameSize+1))
	if err != nil {
		return model.GroupRef{}, fmt.Errorf("read monitor target: %w", err)
	}
	if len(raw) == 0 {
		return model.GroupRef{}, fmt.Errorf("monitor target is empty")
	}
	if len(raw) > parkproto.MaxFrameSize {
		return model.GroupRef{}, fmt.Errorf("monitor target is too large")
	}
	var target model.GroupRef
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&target); err != nil {
		return model.GroupRef{}, fmt.Errorf("parse monitor target: %w", err)
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return model.GroupRef{}, fmt.Errorf("parse monitor target: trailing JSON value")
		}
		return model.GroupRef{}, fmt.Errorf("parse monitor target: %w", err)
	}
	if err := target.Validate(); err != nil {
		return model.GroupRef{}, fmt.Errorf("monitor target: %w", err)
	}
	return target, nil
}

func closeMonitorTarget(process *MonitorProcess) error {
	if process == nil || process.targetWrite == nil {
		return nil
	}
	err := process.targetWrite.Close()
	process.targetWrite = nil
	return err
}

func closeMonitorReady(process *MonitorProcess) error {
	if process == nil || process.readyRead == nil {
		return nil
	}
	err := process.readyRead.Close()
	process.readyRead = nil
	return err
}

func closeMonitorDaemonControl(process *MonitorProcess) error {
	if process == nil || process.DaemonControlWrite == nil {
		return nil
	}
	err := process.DaemonControlWrite.Close()
	process.DaemonControlWrite = nil
	return err
}

func waitMonitorOrKill(process *MonitorProcess) error {
	if process == nil || process.cmd == nil || process.cmd.Process == nil {
		return nil
	}
	select {
	case err := <-process.done:
		return err
	default:
		_ = process.cmd.Process.Kill()
		return <-process.done
	}
}

type processWait struct {
	done   chan error
	exited chan struct{}
	mu     sync.Mutex
	err    error
}

func startProcessWait(cmd *exec.Cmd) *processWait {
	wait := &processWait{
		done:   make(chan error, 1),
		exited: make(chan struct{}),
	}
	go func() {
		err := cmd.Wait()
		wait.mu.Lock()
		wait.err = err
		wait.mu.Unlock()
		wait.done <- err
		close(wait.exited)
	}()
	return wait
}

func (wait *processWait) Err() error {
	wait.mu.Lock()
	defer wait.mu.Unlock()
	return wait.err
}
