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

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parkproto"
)

const (
	MonitorDaemonControlFD = 3
	MonitorTargetFD        = 4
)

// MonitorProcessSpec describes a one-shot monitor process. The command must
// call RunMonitorFromFDs with MonitorDaemonControlFD and MonitorTargetFD.
type MonitorProcessSpec struct {
	Command           CommandSpec
	DaemonControlRead *os.File
	Stderr            io.Writer
}

type CommandSpec struct {
	Path string
	Args []string
	Env  []string
	Dir  string
}

type MonitorProcess struct {
	PID int

	cmd         *exec.Cmd
	targetWrite *os.File
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
// group. The daemon control read file is consumed by this call.
func StartMonitorProcess(ctx context.Context, spec MonitorProcessSpec) (*MonitorProcess, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if spec.Command.Path == "" {
		return nil, fmt.Errorf("%w: monitor command path is required", ErrInvalidSpec)
	}
	if spec.DaemonControlRead == nil {
		return nil, fmt.Errorf("%w: monitor daemon control read fd is required", ErrInvalidSpec)
	}
	targetRead, targetWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	setCloseOnExec(spec.DaemonControlRead)
	setCloseOnExec(targetRead)
	setCloseOnExec(targetWrite)

	args := spec.Command.Args
	if len(args) == 0 {
		args = []string{spec.Command.Path}
	}
	env := spec.Command.Env
	if env == nil {
		env = os.Environ()
	}
	cmd := exec.CommandContext(ctx, spec.Command.Path, args[1:]...)
	cmd.Args = append([]string(nil), args...)
	cmd.Env = append([]string(nil), env...)
	cmd.Dir = spec.Command.Dir
	cmd.Stderr = spec.Stderr
	cmd.ExtraFiles = []*os.File{spec.DaemonControlRead, targetRead}
	cmd.SysProcAttr = newProcessGroupSysProcAttr()
	if err := cmd.Start(); err != nil {
		_ = targetRead.Close()
		_ = targetWrite.Close()
		return nil, err
	}
	_ = spec.DaemonControlRead.Close()
	_ = targetRead.Close()

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	return &MonitorProcess{
		PID:         cmd.Process.Pid,
		cmd:         cmd,
		targetWrite: targetWrite,
		done:        done,
	}, nil
}

type MonitorRunSpec struct {
	DaemonControl io.Reader
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

func RunMonitorFromFDs(ctx context.Context, daemonFD, targetFD int, containment Containment) error {
	daemonFile := os.NewFile(uintptr(daemonFD), "agentbus-parklaunch-monitor-daemon")
	if daemonFile == nil {
		return fmt.Errorf("open daemon fd %d", daemonFD)
	}
	defer daemonFile.Close()
	targetFile := os.NewFile(uintptr(targetFD), "agentbus-parklaunch-monitor-target")
	if targetFile == nil {
		return fmt.Errorf("open target fd %d", targetFD)
	}
	defer targetFile.Close()
	target, err := readMonitorTarget(targetFile)
	if err != nil {
		return err
	}
	return RunMonitor(ctx, MonitorRunSpec{
		DaemonControl: daemonFile,
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
