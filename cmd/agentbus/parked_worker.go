package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"syscall"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parkproto"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
)

const internalParkedWorkerCommand = "internal-parked-worker"

type parkedWorkerOptions struct {
	controlReadFD  int
	controlWriteFD int
	bootstrapFD    int
}

func (a *app) runInternalParkedWorker(args []string, errOut io.Writer) int {
	opts, err := parseParkedWorkerOptions(args, errOut)
	if err != nil {
		return commandError(errOut, err)
	}
	if err := runParkedWorker(opts); err != nil {
		return commandError(errOut, err)
	}
	return 0
}

func parseParkedWorkerOptions(args []string, errOut io.Writer) (parkedWorkerOptions, error) {
	fs := flag.NewFlagSet("agentbus "+internalParkedWorkerCommand, flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.Usage = func() {
		fmt.Fprintln(errOut, "agentbus: internal command")
	}
	controlReadFD := fs.Int("control-read-fd", -1, "internal control read fd")
	controlWriteFD := fs.Int("control-write-fd", -1, "internal control write fd")
	bootstrapFD := fs.Int("bootstrap-fd", -1, "internal bootstrap fd")
	if err := fs.Parse(args); err != nil {
		return parkedWorkerOptions{}, err
	}
	if fs.NArg() != 0 {
		return parkedWorkerOptions{}, fmt.Errorf("%s does not accept positional arguments", internalParkedWorkerCommand)
	}
	if *controlReadFD < 3 {
		return parkedWorkerOptions{}, fmt.Errorf("control read fd must be >= 3")
	}
	if *controlWriteFD < 3 {
		return parkedWorkerOptions{}, fmt.Errorf("control write fd must be >= 3")
	}
	if *bootstrapFD < 3 {
		return parkedWorkerOptions{}, fmt.Errorf("bootstrap fd must be >= 3")
	}
	if *bootstrapFD == *controlReadFD || *bootstrapFD == *controlWriteFD {
		return parkedWorkerOptions{}, fmt.Errorf("bootstrap fd must be distinct from control fds")
	}
	return parkedWorkerOptions{
		controlReadFD:  *controlReadFD,
		controlWriteFD: *controlWriteFD,
		bootstrapFD:    *bootstrapFD,
	}, nil
}

func readReleaseExpectationFromBootstrapFD(fd int) (parkproto.ReleaseExpectation, error) {
	file := os.NewFile(uintptr(fd), "agentbus-park-bootstrap")
	if file == nil {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("open bootstrap fd %d", fd)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, parkproto.MaxFrameSize+1))
	if err != nil {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("read release expectation bootstrap: %w", err)
	}
	defer zeroBytes(raw)
	if len(raw) == 0 {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("release expectation bootstrap is empty")
	}
	if len(raw) > parkproto.MaxFrameSize {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("release expectation bootstrap is too large")
	}
	var expectation parkproto.ReleaseExpectation
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&expectation); err != nil {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("parse release expectation bootstrap: %w", err)
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return parkproto.ReleaseExpectation{}, fmt.Errorf("parse release expectation bootstrap: trailing JSON value")
		}
		return parkproto.ReleaseExpectation{}, fmt.Errorf("parse release expectation bootstrap: %w", err)
	}
	return expectation, nil
}

func runParkedWorker(opts parkedWorkerOptions) error {
	if err := setCloseOnExecForInheritedFDs(opts.controlReadFD, opts.controlWriteFD, opts.bootstrapFD); err != nil {
		return fmt.Errorf("prepare inherited fds: %w", err)
	}
	expectationTemplate, err := readReleaseExpectationFromBootstrapFD(opts.bootstrapFD)
	if err != nil {
		return err
	}
	controlRead, controlWrite, err := openControlFiles(opts.controlReadFD, opts.controlWriteFD)
	if err != nil {
		return err
	}
	controlClosed := false
	defer func() {
		if !controlClosed {
			_ = closeControlFiles(controlRead, controlWrite)
		}
	}()

	report, err := currentParkedWorkerIdentity()
	if err != nil {
		return fmt.Errorf("read worker identity: %w", err)
	}
	expectation := bindReportedIdentity(expectationTemplate, report)
	if err := expectation.Validate(); err != nil {
		return fmt.Errorf("release expectation: %w", err)
	}
	writer := parkproto.NewWriter(controlWrite)
	if _, err := writer.WriteIdentityReport(report); err != nil {
		return fmt.Errorf("write identity report: %w", err)
	}

	reader := parkproto.NewReader(controlRead)
	received, err := reader.Read()
	if err != nil {
		return fmt.Errorf("read release: %w", err)
	}
	release, ok := received.Message.(parkproto.Release)
	if !ok {
		return fmt.Errorf("%w: expected Release, got %T", parkproto.ErrMalformed, received.Message)
	}
	if err := release.ValidateFor(received.Sequence, expectation); err != nil {
		return err
	}
	if err := validateReleaseGroupMatchesReport(release.ExpectedGroupRef, report); err != nil {
		return err
	}
	if _, err := writer.WriteReleaseAck(parkproto.ReleaseAck{AcceptedSequence: received.Sequence}); err != nil {
		return fmt.Errorf("write release ack: %w", err)
	}
	if err := closeControlFiles(controlRead, controlWrite); err != nil {
		return fmt.Errorf("close control fds: %w", err)
	}
	controlClosed = true

	if release.ExecSpec.Dir != "" {
		if err := os.Chdir(release.ExecSpec.Dir); err != nil {
			return fmt.Errorf("chdir for backend exec: %w", err)
		}
	}
	return syscall.Exec(release.ExecSpec.Path, release.ExecSpec.Argv, release.ExecSpec.Env)
}

func currentParkedWorkerIdentity() (parkproto.IdentityReport, error) {
	claim, err := procgroup.ReadProcessClaim(os.Getpid())
	if err != nil {
		return parkproto.IdentityReport{}, err
	}
	parkInstanceID, err := parkproto.NewParkInstanceID()
	if err != nil {
		return parkproto.IdentityReport{}, err
	}
	return parkproto.IdentityReportFromClaim(claim, parkInstanceID), nil
}

func validateReleaseGroupMatchesReport(groupRef model.GroupRef, report parkproto.IdentityReport) error {
	if groupRef.PGID != report.PGID {
		return fmt.Errorf("%w: group pgid %d does not match worker pgid %d", parkproto.ErrBinding, groupRef.PGID, report.PGID)
	}
	if !groupRef.KernelDomain().Equal(report.KernelDomainID) {
		return fmt.Errorf("%w: group kernel domain does not match worker domain", parkproto.ErrBinding)
	}
	if groupRef.Leader.PID != report.PID || groupRef.Leader.HighResStartToken != report.StartToken.String() {
		return fmt.Errorf("%w: group leader does not match worker identity", parkproto.ErrBinding)
	}
	return nil
}

func openControlFiles(readFD, writeFD int) (*os.File, *os.File, error) {
	unix.CloseOnExec(readFD)
	readFile := os.NewFile(uintptr(readFD), "agentbus-park-control-read")
	if readFile == nil {
		return nil, nil, fmt.Errorf("open control read fd %d", readFD)
	}
	if readFD == writeFD {
		return readFile, readFile, nil
	}
	unix.CloseOnExec(writeFD)
	writeFile := os.NewFile(uintptr(writeFD), "agentbus-park-control-write")
	if writeFile == nil {
		_ = readFile.Close()
		return nil, nil, fmt.Errorf("open control write fd %d", writeFD)
	}
	return readFile, writeFile, nil
}

func setCloseOnExecForInheritedFDs(controlReadFD, controlWriteFD, bootstrapFD int) error {
	keep := map[int]struct{}{
		controlReadFD:  {},
		controlWriteFD: {},
		bootstrapFD:    {},
	}
	return setCloseOnExecForAllOpenFDsExcept(keep)
}

func setCloseOnExecForAllOpenFDsExcept(keep map[int]struct{}) error {
	for _, dir := range []string{"/proc/self/fd", "/dev/fd"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read %s: %w", dir, err)
		}
		return setCloseOnExecForFDEntries(entries, keep)
	}
	return scanCloseOnExecForOpenFDs(keep)
}

func setCloseOnExecForFDEntries(entries []os.DirEntry, keep map[int]struct{}) error {
	var out error
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd < 3 {
			continue
		}
		if _, ok := keep[fd]; ok {
			continue
		}
		if err := setCloseOnExecIfOpen(fd); err != nil {
			out = errors.Join(out, fmt.Errorf("fd %d: %w", fd, err))
		}
	}
	return out
}

func scanCloseOnExecForOpenFDs(keep map[int]struct{}) error {
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return fmt.Errorf("read fd limit: %w", err)
	}
	var out error
	for fd := 3; uint64(fd) < limit.Cur; fd++ {
		if _, ok := keep[fd]; ok {
			continue
		}
		if err := setCloseOnExecIfOpen(fd); err != nil {
			out = errors.Join(out, fmt.Errorf("fd %d: %w", fd, err))
		}
	}
	return out
}

func setCloseOnExecIfOpen(fd int) error {
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		if errors.Is(err, unix.EBADF) {
			return nil
		}
		return err
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		if errors.Is(err, unix.EBADF) {
			return nil
		}
		return err
	}
	return nil
}

func zeroBytes(raw []byte) {
	for i := range raw {
		raw[i] = 0
	}
}

func bindReportedIdentity(expectation parkproto.ReleaseExpectation, report parkproto.IdentityReport) parkproto.ReleaseExpectation {
	expectation.Binding.ParkInstanceID = report.ParkInstanceID
	expectation.Binding.StartToken = report.StartToken
	return expectation
}

func closeControlFiles(readFile, writeFile *os.File) error {
	var err error
	if readFile != nil {
		err = errors.Join(err, readFile.Close())
	}
	if writeFile != nil && writeFile != readFile {
		err = errors.Join(err, writeFile.Close())
	}
	return err
}
