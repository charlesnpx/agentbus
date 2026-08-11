//go:build windows

package parklaunch

import (
	"fmt"
	"os"
	"syscall"
)

// Windows has no equivalent to the Unix inherited-FD process-group protocol
// used by parked workers. These definitions preserve the package API for
// cross-compilation; platformParkLaunchSupported rejects every launch before
// any worker or monitor can be started.
type launchPipes struct {
	toWorkerRead  *os.File
	toWorkerWrite *os.File

	fromWorkerRead  *os.File
	fromWorkerWrite *os.File

	bootstrapRead  *os.File
	bootstrapWrite *os.File

	backendStdinRead   *os.File
	backendStdinWrite  *os.File
	backendStdoutRead  *os.File
	backendStdoutWrite *os.File
	backendStderrRead  *os.File
	backendStderrWrite *os.File
}

func openLaunchPipes() (*launchPipes, error) {
	return nil, platformParkLaunchSupported()
}

func (pipes *launchPipes) setParentCLOEXEC() {}

func (pipes *launchPipes) closeWorkerCopiesInParent() {}

func (pipes *launchPipes) closeWorkerControlInParent() {}

func (pipes *launchPipes) closeControl() error {
	return nil
}

func (pipes *launchPipes) disownBackendParentFiles() {}

func (pipes *launchPipes) closeParentOnReturn() {}

func (pipes *launchPipes) closeAll() {}

func closeFile(file **os.File) {
	_ = closeFileErr(file)
}

func closeFileErr(file **os.File) error {
	if file == nil || *file == nil {
		return nil
	}
	err := (*file).Close()
	*file = nil
	return err
}

func setCloseOnExec(*os.File) {}

func newProcessGroupSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{}
}

func platformParkLaunchSupported() error {
	return fmt.Errorf("%w: Windows cannot provide Unix process-group identity, inherited-FD, and group-containment semantics", ErrPlatformUnsupported)
}

func signalStartedProcess(process *os.Process) error {
	if process == nil {
		return nil
	}
	return process.Kill()
}
