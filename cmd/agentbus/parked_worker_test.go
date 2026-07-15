//go:build darwin || linux

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parkproto"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
)

const (
	parkedWorkerHelperEnv  = "AGENTBUS_TEST_HELPER"
	parkedWorkerHelperMode = "parked-worker"
	parkedBackendMode      = "parked-backend"
	workerControlReadFD    = 3
	workerControlWriteFD   = 4
	workerBootstrapFD      = 5
	workerExtraInheritedFD = 6
	workerDuplicatedFD     = 7
)

func TestInternalParkedWorkerHiddenFromHelp(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	code, stdout, stderr := runTestCLI(t, a, []string{"--help"}, "")
	if code != 0 {
		t.Fatalf("help exit = %d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, internalParkedWorkerCommand) {
		t.Fatalf("root help exposes hidden command:\n%s", stdout)
	}
}

func TestParkedWorkerReleaseGateExecPIDAndFDs(t *testing.T) {
	harness := startParkedWorkerHarness(t, newBackendFixtureSpec(t, backendFixtureOptions{
		MarkerPath: t.TempDir() + "/backend-started",
		ResultPath: t.TempDir() + "/backend-result.json",
		ClosedFDs:  []int{workerControlReadFD, workerControlWriteFD},
	}))
	report := harness.readIdentity(t)
	assertFileAbsentFor(t, harness.backend.MarkerPath, 150*time.Millisecond)

	release := harness.releaseForReport(t, report, nil)
	harness.writeRelease(t, release)
	harness.readReleaseAck(t)
	harness.waitSuccess(t)

	if _, err := os.Stat(harness.backend.MarkerPath); err != nil {
		t.Fatalf("backend marker was not written after release: %v", err)
	}
	result := readBackendFixtureResult(t, harness.backend.ResultPath)
	if result.PID != report.PID {
		t.Fatalf("backend pid = %d, want stable worker pid %d", result.PID, report.PID)
	}
	if len(result.OpenFDs) != 0 {
		t.Fatalf("backend inherited control fds: %v", result.OpenFDs)
	}
}

func TestParkedWorkerClosesUnexpectedInheritedFDsBeforeBackendExec(t *testing.T) {
	harness := startParkedWorkerHarnessWithOptions(t, newBackendFixtureSpec(t, backendFixtureOptions{
		MarkerPath: t.TempDir() + "/backend-started",
		ResultPath: t.TempDir() + "/backend-result.json",
		ClosedFDs:  append(defaultClosedWorkerFDs(), workerExtraInheritedFD, workerDuplicatedFD),
	}), parkedWorkerHarnessOptions{includeUnexpectedInheritedFDs: true})
	report := harness.readIdentity(t)

	release := harness.releaseForReport(t, report, nil)
	harness.writeRelease(t, release)
	harness.readReleaseAck(t)
	harness.waitSuccess(t)

	result := readBackendFixtureResult(t, harness.backend.ResultPath)
	if len(result.OpenFDs) != 0 {
		t.Fatalf("backend inherited unexpected fds: %v", result.OpenFDs)
	}
}

func TestParkedWorkerReleaseExpectationSecretNotInArgvOrEnv(t *testing.T) {
	harness := startParkedWorkerHarness(t, newBackendFixtureSpec(t, backendFixtureOptions{
		MarkerPath: t.TempDir() + "/backend-started",
		ResultPath: t.TempDir() + "/backend-result.json",
		ClosedFDs:  defaultClosedWorkerFDs(),
	}))
	report := harness.readIdentity(t)

	secret := string(harness.expectation.Binding.ReleaseSecret)
	for _, arg := range harness.cmd.Args {
		if strings.Contains(arg, secret) {
			t.Fatalf("release secret leaked into argv element %q", arg)
		}
	}
	for _, env := range harness.cmd.Env {
		if strings.Contains(env, secret) {
			t.Fatalf("release secret leaked into env element %q", env)
		}
	}
	assertProcessCommandLineOmits(t, harness.cmd.Process.Pid, secret)

	release := harness.releaseForReport(t, report, nil)
	harness.writeRelease(t, release)
	harness.readReleaseAck(t)
	harness.waitSuccess(t)
}

func TestParkedWorkerReleaseIsOneUse(t *testing.T) {
	harness := startParkedWorkerHarness(t, newBackendFixtureSpec(t, backendFixtureOptions{
		MarkerPath: t.TempDir() + "/backend-started",
		ResultPath: t.TempDir() + "/backend-result.json",
		ClosedFDs:  []int{workerControlReadFD, workerControlWriteFD},
	}))
	report := harness.readIdentity(t)
	release := harness.releaseForReport(t, report, nil)
	harness.writeRelease(t, release)
	harness.readReleaseAck(t)
	harness.waitSuccess(t)

	if err := harness.writeReleaseExpectingError(release); err == nil {
		t.Fatal("second release write succeeded after backend exec; want closed control channel")
	}
	result := readBackendFixtureResult(t, harness.backend.ResultPath)
	if result.PID != report.PID {
		t.Fatalf("backend pid = %d, want %d", result.PID, report.PID)
	}
}

func TestParkedWorkerRejectsReleaseCapturedForFreshParkInstance(t *testing.T) {
	dir := t.TempDir()
	backend := newBackendFixtureSpec(t, backendFixtureOptions{
		MarkerPath: filepath.Join(dir, "backend-started"),
		ResultPath: filepath.Join(dir, "backend-result.json"),
		ClosedFDs:  []int{workerControlReadFD, workerControlWriteFD},
	})
	first := startParkedWorkerHarness(t, backend)
	firstReport := first.readIdentity(t)
	captured := first.releaseForReport(t, firstReport, nil)
	first.writeRelease(t, captured)
	first.readReleaseAck(t)
	first.waitSuccess(t)

	second := startParkedWorkerHarness(t, backend)
	secondReport := second.readIdentity(t)
	if secondReport.ParkInstanceID == firstReport.ParkInstanceID {
		t.Fatalf("fresh worker reused park instance id %q", secondReport.ParkInstanceID)
	}
	second.writeRelease(t, captured)
	second.waitFailure(t, "park protocol release binding mismatch")

	result := readBackendFixtureResult(t, backend.ResultPath)
	if result.PID != firstReport.PID {
		t.Fatalf("backend pid after replay = %d, want original pid %d", result.PID, firstReport.PID)
	}
}

func TestParkedWorkerRejectsWrongReleaseBinding(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*testing.T, *parkproto.Release)
	}{
		{
			name: "wrong release secret",
			mutate: func(_ *testing.T, release *parkproto.Release) {
				release.Binding.ReleaseSecret = "release-secret-wrong"
			},
		},
		{
			name: "wrong group digest",
			mutate: func(_ *testing.T, release *parkproto.Release) {
				release.Binding.GroupRefDigest = "sha256:" + strings.Repeat("0", 64)
			},
		},
		{
			name: "wrong group identity",
			mutate: func(t *testing.T, release *parkproto.Release) {
				release.ExpectedGroupRef.HostBootID += "-other"
				groupDigest, err := parkproto.DigestGroupRef(release.ExpectedGroupRef)
				if err != nil {
					t.Fatal(err)
				}
				release.Binding.GroupRefDigest = groupDigest
			},
		},
		{
			name: "self-consistent wrong monitor",
			mutate: func(t *testing.T, release *parkproto.Release) {
				release.ExpectedGroupRef.Monitor = model.ProcessIdentity{
					PID:               release.ExpectedGroupRef.Monitor.PID + 1,
					HighResStartToken: release.ExpectedGroupRef.Monitor.HighResStartToken + "-other",
				}
				groupDigest, err := parkproto.DigestGroupRef(release.ExpectedGroupRef)
				if err != nil {
					t.Fatal(err)
				}
				release.Binding.GroupRefDigest = groupDigest
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			harness := startParkedWorkerHarness(t, newBackendFixtureSpec(t, backendFixtureOptions{
				MarkerPath: t.TempDir() + "/backend-started",
				ResultPath: t.TempDir() + "/backend-result.json",
				ClosedFDs:  []int{workerControlReadFD, workerControlWriteFD},
			}))
			report := harness.readIdentity(t)
			release := harness.releaseForReport(t, report, tt.mutate)
			harness.writeRelease(t, release)
			harness.waitFailure(t, "park protocol release binding mismatch")
			assertFileAbsent(t, harness.backend.MarkerPath)
		})
	}
}

func TestParkedWorkerRejectsInvalidProtocolFrames(t *testing.T) {
	for _, tt := range []struct {
		name       string
		frame      func(t *testing.T) []byte
		wantStderr string
		closeInput bool
	}{
		{
			name: "version mismatch",
			frame: func(t *testing.T) []byte {
				return mustParkedWorkerFrame(t, parkproto.Version+1, 1, dummyIdentityReport())
			},
			wantStderr: "park protocol version mismatch",
		},
		{
			name: "malformed",
			frame: func(t *testing.T) []byte {
				return rawParkedWorkerPayload([]byte("{"))
			},
			wantStderr: "park protocol malformed frame",
		},
		{
			name: "out of order",
			frame: func(t *testing.T) []byte {
				return mustParkedWorkerFrame(t, parkproto.Version, 2, dummyIdentityReport())
			},
			wantStderr: "park protocol sequence error",
		},
		{
			name: "oversized",
			frame: func(t *testing.T) []byte {
				return oversizedParkedWorkerFrame()
			},
			wantStderr: "park protocol oversized frame",
		},
		{
			name: "truncated",
			frame: func(t *testing.T) []byte {
				raw := mustParkedWorkerFrame(t, parkproto.Version, 1, dummyIdentityReport())
				return raw[:len(raw)-2]
			},
			wantStderr: "park protocol truncated frame",
			closeInput: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			harness := startParkedWorkerHarness(t, newBackendFixtureSpec(t, backendFixtureOptions{
				MarkerPath: t.TempDir() + "/backend-started",
				ResultPath: t.TempDir() + "/backend-result.json",
				ClosedFDs:  []int{workerControlReadFD, workerControlWriteFD},
			}))
			_ = harness.readIdentity(t)
			harness.writeRawControlFrame(t, tt.frame(t))
			if tt.closeInput {
				_ = harness.controlIn.Close()
			}
			harness.waitFailure(t, tt.wantStderr)
			assertFileAbsent(t, harness.backend.MarkerPath)
		})
	}
}

func TestParkedWorkerHelperProcess(t *testing.T) {
	if os.Getenv(parkedWorkerHelperEnv) != parkedWorkerHelperMode {
		return
	}
	args, ok := helperArgs()
	if !ok {
		os.Exit(97)
	}
	code := (&app{}).run(t.Context(), args, os.Stdin, os.Stdout, os.Stderr)
	os.Exit(code)
}

func TestParkedBackendFixtureHelperProcess(t *testing.T) {
	if os.Getenv(parkedWorkerHelperEnv) != parkedBackendMode {
		return
	}
	args, ok := helperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runParkedBackendFixture(args))
}

type backendFixtureOptions struct {
	MarkerPath string
	ResultPath string
	ClosedFDs  []int
}

type backendFixtureSpec struct {
	parkproto.ExecSpec
	backendFixtureOptions
}

type backendFixtureResult struct {
	PID     int   `json:"pid"`
	OpenFDs []int `json:"openFds"`
}

func newBackendFixtureSpec(t *testing.T, opts backendFixtureOptions) backendFixtureSpec {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if opts.MarkerPath == "" || opts.ResultPath == "" {
		t.Fatalf("backend fixture marker and result paths are required")
	}
	opts.ClosedFDs = requireDefaultClosedWorkerFDs(opts.ClosedFDs)
	closedFDs := make([]string, 0, len(opts.ClosedFDs))
	for _, fd := range opts.ClosedFDs {
		closedFDs = append(closedFDs, strconv.Itoa(fd))
	}
	argv := []string{
		exe,
		"-test.run=TestParkedBackendFixtureHelperProcess",
		"--",
		"--marker", opts.MarkerPath,
		"--result", opts.ResultPath,
		"--closed-fds", strings.Join(closedFDs, ","),
	}
	return backendFixtureSpec{
		ExecSpec: parkproto.ExecSpec{
			Path: exe,
			Argv: argv,
			Env:  []string{parkedWorkerHelperEnv + "=" + parkedBackendMode},
			Dir:  filepath.Dir(exe),
		},
		backendFixtureOptions: opts,
	}
}

func runParkedBackendFixture(args []string) int {
	fs := flag.NewFlagSet("parked-backend-fixture", flag.ContinueOnError)
	fs.SetOutput(ioDiscard{})
	markerPath := fs.String("marker", "", "marker path")
	resultPath := fs.String("result", "", "result path")
	closedFDCSV := fs.String("closed-fds", "", "fds that must be closed")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *markerPath == "" || *resultPath == "" {
		return 2
	}
	result := backendFixtureResult{PID: os.Getpid(), OpenFDs: openFDs(parseFDList(*closedFDCSV))}
	if err := os.WriteFile(*markerPath, []byte("started\n"), 0o600); err != nil {
		return 3
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return 4
	}
	if err := os.WriteFile(*resultPath, append(raw, '\n'), 0o600); err != nil {
		return 5
	}
	return 0
}

type parkedWorkerHarness struct {
	cmd         *exec.Cmd
	toWorker    *parkproto.Writer
	fromWorker  *parkproto.Reader
	controlIn   *os.File
	controlOut  *os.File
	waitCh      chan error
	stderr      *bytes.Buffer
	backend     backendFixtureSpec
	expectation parkproto.ReleaseExpectation
	waitOnce    sync.Once
	waitErr     error
}

type parkedWorkerHarnessOptions struct {
	includeUnexpectedInheritedFDs bool
}

func startParkedWorkerHarness(t *testing.T, backend backendFixtureSpec) *parkedWorkerHarness {
	t.Helper()
	return startParkedWorkerHarnessWithOptions(t, backend, parkedWorkerHarnessOptions{})
}

func startParkedWorkerHarnessWithOptions(t *testing.T, backend backendFixtureSpec, opts parkedWorkerHarnessOptions) *parkedWorkerHarness {
	t.Helper()

	releaseRead, releaseWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	reportRead, reportWrite, err := os.Pipe()
	if err != nil {
		_ = releaseRead.Close()
		_ = releaseWrite.Close()
		t.Fatal(err)
	}
	bootstrapRead, bootstrapWrite, err := os.Pipe()
	if err != nil {
		_ = releaseRead.Close()
		_ = releaseWrite.Close()
		_ = reportRead.Close()
		_ = reportWrite.Close()
		t.Fatal(err)
	}
	extraFiles, closeExtraFiles := childExtraFiles(t, releaseRead, reportWrite, bootstrapRead, opts.includeUnexpectedInheritedFDs)
	t.Cleanup(closeExtraFiles)

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd := exec.Command(exe,
		"-test.run=TestParkedWorkerHelperProcess",
		"--",
		internalParkedWorkerCommand,
		"--control-read-fd", strconv.Itoa(workerControlReadFD),
		"--control-write-fd", strconv.Itoa(workerControlWriteFD),
		"--bootstrap-fd", strconv.Itoa(workerBootstrapFD),
	)
	cmd.Env = []string{parkedWorkerHelperEnv + "=" + parkedWorkerHelperMode}
	cmd.ExtraFiles = extraFiles
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		_ = bootstrapRead.Close()
		_ = bootstrapWrite.Close()
		t.Fatal(err)
	}
	_ = releaseRead.Close()
	_ = reportWrite.Close()
	_ = bootstrapRead.Close()
	closeExtraFiles()

	expectation := releaseExpectationForProcess(t, backend.ExecSpec, cmd.Process.Pid)
	writeReleaseExpectationBootstrap(t, bootstrapWrite, expectation)
	_ = bootstrapWrite.Close()

	harness := &parkedWorkerHarness{
		cmd:         cmd,
		toWorker:    parkproto.NewWriter(releaseWrite),
		fromWorker:  parkproto.NewReader(reportRead),
		controlIn:   releaseWrite,
		controlOut:  reportRead,
		waitCh:      make(chan error, 1),
		stderr:      &stderr,
		backend:     backend,
		expectation: expectation,
	}
	go func() {
		harness.waitCh <- cmd.Wait()
	}()
	t.Cleanup(func() {
		_ = releaseWrite.Close()
		_ = reportRead.Close()
		done := make(chan struct{})
		go func() {
			_ = harness.waitProcess()
			close(done)
		}()
		select {
		case <-done:
			return
		case <-time.After(50 * time.Millisecond):
		}
		_ = cmd.Process.Kill()
		<-done
	})
	return harness
}

func (h *parkedWorkerHarness) readIdentity(t *testing.T) parkproto.IdentityReport {
	t.Helper()
	received, err := h.readFromWorker(t, "identity report")
	if err != nil {
		t.Fatalf("read identity report: %v stderr=%s", err, h.stderr.String())
	}
	report, ok := received.Message.(parkproto.IdentityReport)
	if !ok {
		t.Fatalf("message=%T, want IdentityReport", received.Message)
	}
	if received.Sequence != 1 {
		t.Fatalf("identity sequence=%d, want 1", received.Sequence)
	}
	if report.PID <= 0 || report.PGID != report.PID {
		t.Fatalf("worker identity pid=%d pgid=%d, want process group leader", report.PID, report.PGID)
	}
	return report
}

func (h *parkedWorkerHarness) releaseForReport(t *testing.T, report parkproto.IdentityReport, mutate func(*testing.T, *parkproto.Release)) parkproto.Release {
	t.Helper()
	binding := bindReportedIdentity(h.expectation, report).Binding
	groupRef := groupRefFromReport(report, binding.CustodyID, binding.LaunchKey)
	groupDigest, err := parkproto.DigestGroupRef(groupRef)
	if err != nil {
		t.Fatal(err)
	}
	binding.GroupRefDigest = groupDigest
	release := parkproto.Release{
		Binding:          binding,
		ExpectedGroupRef: groupRef,
		ExecSpec:         h.backend.ExecSpec,
	}
	if mutate != nil {
		mutate(t, &release)
	}
	return release
}

func (h *parkedWorkerHarness) writeRelease(t *testing.T, release parkproto.Release) {
	t.Helper()
	if seq, err := h.toWorker.WriteRelease(release); err != nil || seq == 0 {
		t.Fatalf("write release seq=%d err=%v stderr=%s", seq, err, h.stderr.String())
	}
}

func (h *parkedWorkerHarness) writeReleaseExpectingError(release parkproto.Release) error {
	_, err := h.toWorker.WriteRelease(release)
	return err
}

func (h *parkedWorkerHarness) writeRawControlFrame(t *testing.T, raw []byte) {
	t.Helper()
	if _, err := h.controlIn.Write(raw); err != nil {
		t.Fatalf("write raw control frame: %v", err)
	}
	_ = h.controlIn.Close()
}

func (h *parkedWorkerHarness) readReleaseAck(t *testing.T) {
	t.Helper()
	received, err := h.readFromWorker(t, "release ack")
	if err != nil {
		t.Fatalf("read release ack: %v stderr=%s", err, h.stderr.String())
	}
	ack, ok := received.Message.(parkproto.ReleaseAck)
	if !ok {
		t.Fatalf("message=%T, want ReleaseAck", received.Message)
	}
	if received.Sequence != 2 || ack.AcceptedSequence != 1 {
		t.Fatalf("ack sequence=%d accepted=%d, want seq=2 accepted=1", received.Sequence, ack.AcceptedSequence)
	}
}

func (h *parkedWorkerHarness) readFromWorker(t *testing.T, label string) (parkproto.Received, error) {
	t.Helper()
	type result struct {
		received parkproto.Received
		err      error
	}
	ch := make(chan result, 1)
	go func() {
		received, err := h.fromWorker.Read()
		ch <- result{received: received, err: err}
	}()
	select {
	case result := <-ch:
		return result.received, result.err
	case <-time.After(3 * time.Second):
		return parkproto.Received{}, fmt.Errorf("timeout waiting for %s", label)
	}
}

func (h *parkedWorkerHarness) waitSuccess(t *testing.T) {
	t.Helper()
	err := h.wait(t)
	if err != nil {
		t.Fatalf("worker/backend exit error=%v stderr=%s", err, h.stderr.String())
	}
}

func (h *parkedWorkerHarness) waitFailure(t *testing.T, stderrFragment string) {
	t.Helper()
	err := h.wait(t)
	if err == nil {
		t.Fatalf("worker succeeded; want failure containing %q", stderrFragment)
	}
	if !strings.Contains(h.stderr.String(), stderrFragment) {
		t.Fatalf("stderr=%q, want fragment %q", h.stderr.String(), stderrFragment)
	}
}

func (h *parkedWorkerHarness) wait(t *testing.T) error {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_ = h.waitProcess()
		close(done)
	}()
	select {
	case <-done:
		return h.waitErr
	case <-time.After(5 * time.Second):
		_ = h.cmd.Process.Kill()
		_ = h.waitProcess()
		t.Fatalf("worker timed out; kill wait=%v stderr=%s", h.waitErr, h.stderr.String())
		return h.waitErr
	}
}

func (h *parkedWorkerHarness) waitProcess() error {
	h.waitOnce.Do(func() {
		h.waitErr = <-h.waitCh
	})
	return h.waitErr
}

func releaseExpectationTemplate(t *testing.T, execSpec parkproto.ExecSpec) parkproto.ReleaseExpectation {
	t.Helper()
	execDigest, err := parkproto.DigestExecSpec(execSpec)
	if err != nil {
		t.Fatal(err)
	}
	attempt := model.AttemptRef{JobID: "job-parked-worker", AttemptID: "attempt-1", Epoch: 1}
	boot := model.BootRef{BootID: "boot-1", OwnerID: "owner-1"}
	return parkproto.ReleaseExpectation{Binding: parkproto.ReleaseBinding{
		ProtocolVersion:     parkproto.Version,
		Sequence:            1,
		CustodyID:           "custody-1",
		LaunchKey:           model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne},
		LogicalGrant:        model.LaunchGrant{Attempt: attempt, Ordinal: model.LaunchOrdinalOne, Nonce: "nonce-1", GrantedBy: boot},
		ReleaseSecret:       "release-secret-1",
		ImmutableExecDigest: execDigest,
	}}
}

func releaseExpectationForProcess(t *testing.T, execSpec parkproto.ExecSpec, pid int) parkproto.ReleaseExpectation {
	t.Helper()
	expectation := releaseExpectationTemplate(t, execSpec)
	claim := readProcessClaimForTest(t, pid)
	groupRef := groupRefFromClaim(claim, expectation.Binding.CustodyID, expectation.Binding.LaunchKey)
	groupDigest, err := parkproto.DigestGroupRef(groupRef)
	if err != nil {
		t.Fatal(err)
	}
	expectation.Binding.GroupRefDigest = groupDigest
	return expectation
}

func readProcessClaimForTest(t *testing.T, pid int) procgroup.ProcessClaim {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		claim, err := procgroup.ReadProcessClaim(pid)
		if err == nil && claim.PGID == pid {
			return claim
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("pgid=%d, want %d", claim.PGID, pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("read child process claim: %v", lastErr)
	return procgroup.ProcessClaim{}
}

func writeReleaseExpectationBootstrap(t *testing.T, file *os.File, expectation parkproto.ReleaseExpectation) {
	t.Helper()
	raw, err := json.Marshal(expectation)
	if err != nil {
		t.Fatal(err)
	}
	n, err := file.Write(raw)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(raw) {
		t.Fatalf("bootstrap write length = %d, want %d", n, len(raw))
	}
}

func groupRefFromReport(report parkproto.IdentityReport, custodyID model.CustodyID, launchKey model.LaunchKey) model.GroupRef {
	return model.GroupRef{
		Version:           1,
		CustodyID:         custodyID,
		Launch:            launchKey,
		HostBootID:        report.KernelDomainID.HostBootID,
		PIDNamespaceID:    report.KernelDomainID.PIDNamespaceID,
		PIDNamespaceState: report.KernelDomainID.PIDNamespaceState,
		PGID:              report.PGID,
		Leader:            model.ProcessIdentity{PID: report.PID, HighResStartToken: report.StartToken.String()},
		Monitor:           model.ProcessIdentity{PID: report.PID, HighResStartToken: report.StartToken.String()},
	}
}

func groupRefFromClaim(claim procgroup.ProcessClaim, custodyID model.CustodyID, launchKey model.LaunchKey) model.GroupRef {
	return model.GroupRef{
		Version:           1,
		CustodyID:         custodyID,
		Launch:            launchKey,
		HostBootID:        claim.KernelDomainID.HostBootID,
		PIDNamespaceID:    claim.KernelDomainID.PIDNamespaceID,
		PIDNamespaceState: claim.KernelDomainID.PIDNamespaceState,
		PGID:              claim.PGID,
		Leader:            model.ProcessIdentity{PID: claim.PID, HighResStartToken: claim.StartToken.String()},
		Monitor:           model.ProcessIdentity{PID: claim.PID, HighResStartToken: claim.StartToken.String()},
	}
}

func childExtraFiles(t *testing.T, controlRead, controlWrite, bootstrapRead *os.File, includeUnexpectedInheritedFDs bool) ([]*os.File, func()) {
	t.Helper()
	files := []*os.File{controlRead, controlWrite, bootstrapRead}
	var closeFiles []*os.File
	if includeUnexpectedInheritedFDs {
		extraFile, err := os.CreateTemp(t.TempDir(), "extra-inherited-fd")
		if err != nil {
			t.Fatal(err)
		}
		duplicatedFD, err := unix.Dup(int(controlWrite.Fd()))
		if err != nil {
			_ = extraFile.Close()
			t.Fatal(err)
		}
		duplicatedFile := os.NewFile(uintptr(duplicatedFD), "duplicated-control-write")
		files = append(files, extraFile, duplicatedFile)
		closeFiles = append(closeFiles, extraFile, duplicatedFile)
	}
	return files, func() {
		for _, file := range closeFiles {
			_ = file.Close()
		}
	}
}

func defaultClosedWorkerFDs() []int {
	return []int{workerControlReadFD, workerControlWriteFD, workerBootstrapFD}
}

func requireDefaultClosedWorkerFDs(fds []int) []int {
	out := append([]int(nil), fds...)
	for _, fd := range defaultClosedWorkerFDs() {
		if !containsFD(out, fd) {
			out = append(out, fd)
		}
	}
	return out
}

func containsFD(fds []int, want int) bool {
	for _, fd := range fds {
		if fd == want {
			return true
		}
	}
	return false
}

func readBackendFixtureResult(t *testing.T, path string) backendFixtureResult {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result backendFixtureResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func helperArgs() ([]string, bool) {
	for i, arg := range os.Args {
		if arg == "--" {
			return os.Args[i+1:], true
		}
	}
	return nil, false
}

func parseFDList(raw string) []int {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	fds := make([]int, 0, len(parts))
	for _, part := range parts {
		fd, err := strconv.Atoi(part)
		if err == nil {
			fds = append(fds, fd)
		}
	}
	return fds
}

func openFDs(fds []int) []int {
	var open []int
	for _, fd := range fds {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
			open = append(open, fd)
		} else if !errors.Is(err, unix.EBADF) {
			open = append(open, fd)
		}
	}
	return open
}

func assertProcessCommandLineOmits(t *testing.T, pid int, secret string) {
	t.Helper()
	if raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil {
		if strings.Contains(string(bytes.ReplaceAll(raw, []byte{0}, []byte{' '})), secret) {
			t.Fatalf("release secret leaked into /proc cmdline")
		}
		return
	}
	output, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if err != nil {
		t.Logf("process command line inspection unavailable: %v", err)
		return
	}
	if strings.Contains(string(output), secret) {
		t.Fatalf("release secret leaked into process command line: %s", output)
	}
}

func assertFileAbsentFor(t *testing.T, path string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		assertFileAbsent(t, path)
		time.Sleep(10 * time.Millisecond)
	}
}

func assertFileAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("file exists unexpectedly: %s", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", path, err)
	}
}

func dummyIdentityReport() parkproto.IdentityReport {
	return parkproto.IdentityReport{
		ParkInstanceID: "park-instance-dummy",
		PID:            101,
		PGID:           101,
		StartToken:     "start-101",
		KernelDomainID: model.KernelDomainID{
			HostBootID:        "boot-1",
			PIDNamespaceState: model.PIDNamespaceNotApplicable,
		},
	}
}

func mustParkedWorkerFrame(t *testing.T, version uint16, sequence uint64, message parkproto.Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := parkproto.WriteFrame(&buf, version, sequence, message); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func rawParkedWorkerPayload(payload []byte) []byte {
	var buf bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	buf.Write(header[:])
	buf.Write(payload)
	return buf.Bytes()
}

func oversizedParkedWorkerFrame() []byte {
	var buf bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], parkproto.MaxFrameSize+1)
	buf.Write(header[:])
	buf.WriteString("x")
	return buf.Bytes()
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
