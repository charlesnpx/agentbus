//go:build darwin || linux

package parklaunch

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

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
	var out launchPipes
	var err error
	if out.toWorkerRead, out.toWorkerWrite, err = os.Pipe(); err != nil {
		return nil, err
	}
	if out.fromWorkerRead, out.fromWorkerWrite, err = os.Pipe(); err != nil {
		out.closeAll()
		return nil, err
	}
	if out.bootstrapRead, out.bootstrapWrite, err = os.Pipe(); err != nil {
		out.closeAll()
		return nil, err
	}
	if out.backendStdinRead, out.backendStdinWrite, err = os.Pipe(); err != nil {
		out.closeAll()
		return nil, err
	}
	if out.backendStdoutRead, out.backendStdoutWrite, err = os.Pipe(); err != nil {
		out.closeAll()
		return nil, err
	}
	if out.backendStderrRead, out.backendStderrWrite, err = os.Pipe(); err != nil {
		out.closeAll()
		return nil, err
	}
	out.setParentCLOEXEC()
	return &out, nil
}

func (pipes *launchPipes) setParentCLOEXEC() {
	for _, file := range []*os.File{
		pipes.toWorkerRead, pipes.toWorkerWrite,
		pipes.fromWorkerRead, pipes.fromWorkerWrite,
		pipes.bootstrapRead, pipes.bootstrapWrite,
		pipes.backendStdinRead, pipes.backendStdinWrite,
		pipes.backendStdoutRead, pipes.backendStdoutWrite,
		pipes.backendStderrRead, pipes.backendStderrWrite,
	} {
		setCloseOnExec(file)
	}
}

func (pipes *launchPipes) closeWorkerCopiesInParent() {
	closeFile(&pipes.toWorkerRead)
	closeFile(&pipes.fromWorkerWrite)
	closeFile(&pipes.bootstrapRead)
	closeFile(&pipes.backendStdinRead)
	closeFile(&pipes.backendStdoutWrite)
	closeFile(&pipes.backendStderrWrite)
}

func (pipes *launchPipes) closeWorkerControlInParent() {
	closeFile(&pipes.toWorkerWrite)
	closeFile(&pipes.fromWorkerRead)
	closeFile(&pipes.bootstrapWrite)
}

func (pipes *launchPipes) closeControl() error {
	var err error
	err = errors.Join(err, closeFileErr(&pipes.toWorkerWrite))
	err = errors.Join(err, closeFileErr(&pipes.fromWorkerRead))
	err = errors.Join(err, closeFileErr(&pipes.bootstrapWrite))
	return err
}

func (pipes *launchPipes) disownBackendParentFiles() {
	pipes.backendStdinWrite = nil
	pipes.backendStdoutRead = nil
	pipes.backendStderrRead = nil
}

func (pipes *launchPipes) closeParentOnReturn() {
	pipes.closeAll()
}

func (pipes *launchPipes) closeAll() {
	closeFile(&pipes.toWorkerRead)
	closeFile(&pipes.toWorkerWrite)
	closeFile(&pipes.fromWorkerRead)
	closeFile(&pipes.fromWorkerWrite)
	closeFile(&pipes.bootstrapRead)
	closeFile(&pipes.bootstrapWrite)
	closeFile(&pipes.backendStdinRead)
	closeFile(&pipes.backendStdinWrite)
	closeFile(&pipes.backendStdoutRead)
	closeFile(&pipes.backendStdoutWrite)
	closeFile(&pipes.backendStderrRead)
	closeFile(&pipes.backendStderrWrite)
}

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

func setCloseOnExec(file *os.File) {
	if file == nil {
		return
	}
	unix.CloseOnExec(int(file.Fd()))
}

func newProcessGroupSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
