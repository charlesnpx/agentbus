package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parkproto"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
)

const internalParkedWorkerCommand = "internal-parked-worker"

type parkedWorkerOptions struct {
	controlReadFD       int
	controlWriteFD      int
	expectationTemplate parkproto.ReleaseExpectation
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
	encodedExpectation := fs.String("release-expectation", "", "base64 JSON release expectation")
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
	expectation, err := decodeReleaseExpectation(*encodedExpectation)
	if err != nil {
		return parkedWorkerOptions{}, err
	}
	return parkedWorkerOptions{
		controlReadFD:       *controlReadFD,
		controlWriteFD:      *controlWriteFD,
		expectationTemplate: expectation,
	}, nil
}

func decodeReleaseExpectation(encoded string) (parkproto.ReleaseExpectation, error) {
	if encoded == "" {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("release expectation is required")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("decode release expectation: %w", err)
	}
	var expectation parkproto.ReleaseExpectation
	if err := json.Unmarshal(raw, &expectation); err != nil {
		return parkproto.ReleaseExpectation{}, fmt.Errorf("parse release expectation: %w", err)
	}
	return expectation, nil
}

func runParkedWorker(opts parkedWorkerOptions) error {
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
	expectation := bindReportedIdentity(opts.expectationTemplate, report)
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
