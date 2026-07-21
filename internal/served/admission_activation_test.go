package served

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestActivatedRootSupportLossFailsStartupWithoutDowngrade(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name         string
		class        custodian.SupportClass
		cleanupSafe  bool
		retryProbe   bool
		wantAttempts int
		wantFailStop bool
	}{
		{name: "unsupported", class: custodian.SupportUnsupported, cleanupSafe: true, wantAttempts: 1},
		{name: "unsafe", class: custodian.SupportUnsafe, cleanupSafe: false, wantAttempts: 1, wantFailStop: true},
		{name: "retryable exhausted", class: custodian.SupportRetryable, cleanupSafe: true, retryProbe: true, wantAttempts: admissionSupportMaxAttempts},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend("fake")
			rootServer, root, cwd := newUnstartedTestServer(t, backend)
			launcher := newAdmissionFakeLaunchCustodian(t)
			enableTestAdmission(t, rootServer, launcher)
			if err := rootServer.closeServeAdmission(); err != nil {
				t.Fatal(err)
			}
			before, err := authority.InspectAdmissionRoot(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			if !before.ActivationMetadata.Activated {
				t.Fatal("test root was not activated")
			}

			restart := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
			restartLauncher := newAdmissionFakeLaunchCustodian(t)
			configureAdmissionSupport(t, restart, restartLauncher, tt.class, tt.cleanupSafe, tt.retryProbe)
			listenCalled := false
			restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
				listenCalled = true
				return nil, socketFileIdentity{}, errors.New("listener must not open for activated support loss")
			}

			err = restart.Serve(ctx)
			if listenCalled {
				t.Fatal("Serve opened a listener after activated root support loss")
			}
			var diagnostic AdmissionSupportDiagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("Serve error = %v, want AdmissionSupportDiagnostic", err)
			}
			if diagnostic.Assessment.Class != tt.class {
				t.Fatalf("support class = %s, want %s", diagnostic.Assessment.Class, tt.class)
			}
			if diagnostic.Assessment.Attempts != tt.wantAttempts {
				t.Fatalf("attempts = %d, want %d", diagnostic.Assessment.Attempts, tt.wantAttempts)
			}
			if tt.wantFailStop && !errors.Is(err, ErrSafetyFailStopped) {
				t.Fatalf("unsafe error = %v, want safety fail-stop", err)
			}
			after, err := authority.InspectAdmissionRoot(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			if after.ActivationMetadata != before.ActivationMetadata {
				t.Fatalf("activation metadata after failed startup = %+v, want %+v", after.ActivationMetadata, before.ActivationMetadata)
			}
			if tt.wantFailStop {
				if !after.FailStopped || !strings.Contains(after.FailStopReason, "support class=unsafe") {
					t.Fatalf("fail-stop inspection = %+v, want retained unsafe reason", after)
				}
				blockedRestart := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
				configureAdmissionSupport(t, blockedRestart, newAdmissionFakeLaunchCustodian(t), custodian.SupportAvailable, true, false)
				blockedRestart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
					return nil, socketFileIdentity{}, errors.New("listener must not open before fail-stop diagnosis")
				}
				err = blockedRestart.Serve(ctx)
				if !errors.Is(err, ErrSafetyFailStopped) || !strings.Contains(err.Error(), after.FailStopReason) {
					t.Fatalf("restart after fail-stop error = %v, want retained safety fail-stop reason %q", err, after.FailStopReason)
				}
				if _, err := authority.ClearAdmissionFailStop(ctx, root, authority.ClearFailStopOptions{}); !errors.Is(err, authority.ErrClearFailStopConfirmationRequired) {
					t.Fatalf("clear fail-stop without acknowledgement error = %v, want ErrClearFailStopConfirmationRequired", err)
				}
				report, err := authority.ClearAdmissionFailStop(ctx, root, authority.ClearFailStopOptions{AcknowledgeUnsafeDiagnosis: true})
				if err != nil {
					t.Fatal(err)
				}
				if !report.Cleared || report.ClearedReason != after.FailStopReason || report.Inspection.FailStopped {
					t.Fatalf("clear fail-stop report = %+v, want cleared retained reason", report)
				}
				clearedRestart := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
				configureAdmissionSupport(t, clearedRestart, newAdmissionFakeLaunchCustodian(t), custodian.SupportAvailable, true, false)
				listenErr := errors.New("listener reached after fail-stop clear")
				clearedRestart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
					return nil, socketFileIdentity{}, listenErr
				}
				err = clearedRestart.Serve(ctx)
				if !errors.Is(err, listenErr) {
					t.Fatalf("restart after clear error = %v, want listener reached", err)
				}
			}
		})
	}
}

func TestUnsafeSupportFailStopPersistenceFailureReportsBothCauses(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	server, _, cwd := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}

	persistErr := errors.New("anchor write failed")
	anchorStore.FailNextForTest(authority.AnchorFailStop, persistErr)
	restart := newTestServerAtRoot(t, server.stateRoot, cwd, newFakeBackend("fake"))
	restart.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, repo, io.NopCloser(strings.NewReader("")), nil
	}
	configureAdmissionSupport(t, restart, newAdmissionFakeLaunchCustodian(t), custodian.SupportUnsafe, false, false)

	err := restart.Serve(ctx)
	if !errors.Is(err, authority.ErrFailStopRecord) || !errors.Is(err, persistErr) || !errors.Is(err, ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("Serve error = %v, want unsafe support and fail-stop persistence failure", err)
	}
	var diagnostic AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Serve error = %v, want AdmissionSupportDiagnostic", err)
	}
	if diagnostic.FailStopped {
		t.Fatalf("diagnostic FailStopped = true after failed persistence: %+v", diagnostic)
	}
	snapshot := admissionAnchorSnapshot(t, anchorStore)
	if snapshot.Phase == "fail_stopped" {
		t.Fatalf("anchor snapshot = %+v, want fail-stop not persisted", snapshot)
	}
}

func TestActivatedRootRejectsLegacySubmitBeforeBackendStart(t *testing.T) {
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "legacy"},
	}))
	if outcome.err == nil {
		t.Fatalf("legacy submit result = %+v, want rejection", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectLegacyDowngrade)
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 before legacy downgrade rejection", got)
	}
}

func TestActivatedRootContractVersionMismatchFailsStartup(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	server, _, cwd := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}

	badRepo := repositoryWithForgedContractVersion(t, repo, authority.CurrentAdmissionContractVersion+1)
	restart := newTestServerAtRoot(t, server.stateRoot, cwd, newFakeBackend("fake"))
	restart.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(badRepo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, badRepo, io.NopCloser(strings.NewReader("")), nil
	}
	configureTestAdmissionRuntime(t, restart, newAdmissionFakeLaunchCustodian(t), true)
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener must not open for contract mismatch")
	}
	err := restart.Serve(ctx)
	if !errors.Is(err, authority.ErrAdmissionContractMismatch) {
		t.Fatalf("Serve contract mismatch error = %v, want ErrAdmissionContractMismatch", err)
	}
}

func repositoryWithForgedContractVersion(t *testing.T, repo *memory.Repository, version uint16) *memory.Repository {
	t.Helper()
	type memorySnapshot struct {
		DBUUID          string                                      `json:"dbUUID"`
		Generation      uint64                                      `json:"generation"`
		NextJobSequence uint64                                      `json:"nextJobSequence"`
		Meta            repository.Record[repository.AuthorityMeta] `json:"meta"`
		Bindings        json.RawMessage                             `json:"bindings"`
		Tombstones      json.RawMessage                             `json:"tombstones"`
		Safety          json.RawMessage                             `json:"safety"`
		Projections     json.RawMessage                             `json:"projections"`
		Quarantines     json.RawMessage                             `json:"quarantines"`
	}
	var snapshot memorySnapshot
	if err := json.Unmarshal(repo.SnapshotBytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Meta.Value.AdmissionRoot.ContractVersion = version
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	badRepo, err := memory.NewRepositoryFromSnapshotBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	return badRepo
}

func TestRecoveryOnlyFinalizesActivatedObligationsWithoutSocket(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend("fake")
	server, root, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	accepted, err := server.admissionReady.Accept(ctx, authority.AcceptRequest{
		RequestKey:   model.RequestKey{WorkspaceKey: "workspace-recovery-only", RequestID: "request-recovery-only"},
		TaskIdentity: model.NewSHA256TaskIdentity([]byte("recovery-only")),
		Mode:         model.ModeIdentifiedFenced,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}

	recoveryServer := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, recoveryServer, newAdmissionFakeLaunchCustodian(t), true)
	report, err := recoveryServer.recoverAdmissionRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Mode != AdmissionRecoveryOnly.String() || report.FinalizedJobs != 1 {
		t.Fatalf("recovery report = %+v, want recovery-only finalizing one job", report)
	}
	if _, err := os.Stat(filepath.Join(root, protocol.SocketName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket stat error = %v, want not exist", err)
	}
	inspection, err := authority.InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Counts.RecoveryObligations != 0 {
		t.Fatalf("recovery obligations = %d, want 0 after finalizing %s", inspection.Counts.RecoveryObligations, accepted.Record.JobID)
	}
}

func TestActivatedStartupRecoversBeforeSupportPolicyAndListen(t *testing.T) {
	ctx := context.Background()
	rootServer, root, cwd := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, rootServer, launcher)
	if _, err := rootServer.admissionReady.Accept(ctx, authority.AcceptRequest{
		RequestKey:   model.RequestKey{WorkspaceKey: "workspace-order", RequestID: "request-order"},
		TaskIdentity: model.NewSHA256TaskIdentity([]byte("order")),
		Mode:         model.ModeIdentifiedFenced,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rootServer.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}

	restart := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
	restartLauncher := newAdmissionFakeLaunchCustodian(t)
	available := admissionSupportForClass(t, custodian.SupportAvailable, true, 1)
	restart.admissionRuntime = &servedAdmissionRuntime{
		runtime:         custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
		launchCustodian: restartLauncher,
		supportProbe: func(context.Context) custodian.Support {
			restart.recordStartupEvent("support")
			return available
		},
		verifierOverride: restartLauncher.verifier,
	}
	restart.admissionStartupHooks = admissionStartupHooks{
		AfterMetadataRead: func(authority.AdmissionRootMetadata) { restart.recordStartupEvent("metadata") },
		BeforeRecovery:    func() { restart.recordStartupEvent("before-recovery") },
		AfterRecovery:     func() { restart.recordStartupEvent("after-recovery") },
		BeforeSupportAssessment: func() {
			restart.recordStartupEvent("before-support")
		},
		AfterSupportAssessment: func(custodian.Support) { restart.recordStartupEvent("after-support") },
		BeforePolicyInstall:    func() { restart.recordStartupEvent("policy") },
	}
	listenErr := errors.New("listener reached")
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		restart.recordStartupEvent("listen")
		return nil, socketFileIdentity{}, listenErr
	}

	err := restart.Serve(ctx)
	if !errors.Is(err, listenErr) {
		t.Fatalf("Serve error = %v, want %v", err, listenErr)
	}
	want := []string{"metadata", "before-recovery", "after-recovery", "before-support", "support", "after-support", "policy", "listen"}
	if got := restart.startupEvents(); !reflect.DeepEqual(got, want) {
		t.Fatalf("startup order = %v, want %v", got, want)
	}
}

func TestSequentialServeReprobesSupportAndClosesRuntimePerServe(t *testing.T) {
	ctx := context.Background()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	available := admissionSupportForClass(t, custodian.SupportAvailable, true, 1)
	unsupported := admissionSupportForClass(t, custodian.SupportUnsupported, true, 1)

	var factoryCalls atomic.Int64
	var selfTests atomic.Int64
	var closes atomic.Int64
	server.admissionRuntimeFactory = func(*Server) *servedAdmissionRuntime {
		factoryCalls.Add(1)
		launcher := newAdmissionFakeLaunchCustodian(t)
		return &servedAdmissionRuntime{
			runtime:         custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
			launchCustodian: launcher,
			supportProbe: func(context.Context) custodian.Support {
				if selfTests.Add(1) == 1 {
					return available
				}
				return unsupported
			},
			verifierOverride: launcher.verifier,
			closeHook: func() error {
				closes.Add(1)
				return nil
			},
		}
	}

	listenErr := errors.New("first listen reached")
	var listenCalls atomic.Int64
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		if listenCalls.Add(1) == 1 {
			return nil, socketFileIdentity{}, listenErr
		}
		return nil, socketFileIdentity{}, errors.New("listener must not open after support loss")
	}

	if err := server.Serve(ctx); !errors.Is(err, listenErr) {
		t.Fatalf("first Serve error = %v, want %v", err, listenErr)
	}
	if server.admissionRuntime != nil {
		t.Fatal("admissionRuntime retained after first Serve close")
	}
	err := server.Serve(ctx)
	if !errors.Is(err, ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("second Serve error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if got := factoryCalls.Load(); got != 2 {
		t.Fatalf("runtime factory calls = %d, want 2", got)
	}
	if got := selfTests.Load(); got != 2 {
		t.Fatalf("self-test calls = %d, want 2", got)
	}
	if got := closes.Load(); got != 2 {
		t.Fatalf("runtime closes = %d, want 2", got)
	}
	if got := listenCalls.Load(); got != 1 {
		t.Fatalf("listener calls = %d, want only first Serve to listen", got)
	}
}

func TestBootstrapAdmissionProbeFailureClosesRuntime(t *testing.T) {
	ctx := context.Background()
	server, _, _ := newUnstartedTestServer(t, &nilProbeBackend{fakeBackend: newFakeBackend("fake")})
	var closes atomic.Int64
	server.admissionRuntime = closeCountingServedAdmissionRuntime(t, &closes, admissionSupportForClass(t, custodian.SupportAvailable, true, 1))

	err := server.bootstrapAdmission(ctx)
	if err == nil || !strings.Contains(err.Error(), "nil probed backend") {
		t.Fatalf("bootstrapAdmission error = %v, want nil probed backend", err)
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("runtime closes = %d, want 1", got)
	}
	if server.admissionRuntime != nil {
		t.Fatal("admissionRuntime retained after probe failure")
	}
}

func TestBootstrapAdmissionFactoryFailureClosesRuntime(t *testing.T) {
	ctx := context.Background()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	var closes atomic.Int64
	server.admissionRuntime = closeCountingServedAdmissionRuntime(t, &closes, admissionSupportForClass(t, custodian.SupportAvailable, true, 1))
	factoryErr := errors.New("bootstrapper factory failed")
	server.admissionBootstrapperFactory = func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		return nil, nil, nil, factoryErr
	}

	err := server.bootstrapAdmission(ctx)
	if !errors.Is(err, factoryErr) {
		t.Fatalf("bootstrapAdmission error = %v, want factory error", err)
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("runtime closes = %d, want 1", got)
	}
	if server.admissionRuntime != nil {
		t.Fatal("admissionRuntime retained after factory failure")
	}
}

func TestRecoveryOnlyClosesRuntimeOnSuccessAndFailure(t *testing.T) {
	ctx := context.Background()

	successServer, successRoot, successCWD := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, successServer, newAdmissionFakeLaunchCustodian(t))
	if err := successServer.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	successRecovery := newTestServerAtRoot(t, successRoot, successCWD, newFakeBackend("fake"))
	var successCloses atomic.Int64
	successRecovery.admissionRuntime = closeCountingServedAdmissionRuntime(t, &successCloses, admissionSupportForClass(t, custodian.SupportAvailable, true, 1))
	if _, err := successRecovery.recoverAdmissionRoot(ctx); err != nil {
		t.Fatalf("recovery-only success error = %v", err)
	}
	if got := successCloses.Load(); got != 1 {
		t.Fatalf("success runtime closes = %d, want 1", got)
	}
	if successRecovery.admissionRuntime != nil {
		t.Fatal("admissionRuntime retained after recovery-only success")
	}

	failureServer, failureRoot, failureCWD := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, failureServer, newAdmissionFakeLaunchCustodian(t))
	if err := failureServer.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	failureRecovery := newTestServerAtRoot(t, failureRoot, failureCWD, newFakeBackend("fake"))
	var failureCloses atomic.Int64
	failureRecovery.admissionRuntime = closeCountingServedAdmissionRuntime(t, &failureCloses, admissionSupportForClass(t, custodian.SupportUnsupported, true, 1))
	_, err := failureRecovery.recoverAdmissionRoot(ctx)
	if !errors.Is(err, ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("recovery-only failure error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if got := failureCloses.Load(); got != 1 {
		t.Fatalf("failure runtime closes = %d, want 1", got)
	}
	if failureRecovery.admissionRuntime != nil {
		t.Fatal("admissionRuntime retained after recovery-only failure")
	}
}

func TestServeRejectsConsumedInjectedRuntime(t *testing.T) {
	ctx := context.Background()
	var closes atomic.Int64
	runtime := custodian.NewUnavailableRuntimeForTest(custodian.ErrSupervisorUnavailable, func() error {
		closes.Add(1)
		return nil
	})
	server := newUnstartedTestServerWithRuntime(t, runtime)
	listenErr := errors.New("listen reached")
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, listenErr
	}

	if err := server.Serve(ctx); !errors.Is(err, listenErr) {
		t.Fatalf("first Serve error = %v, want listen error", err)
	}
	if !runtime.Consumed() {
		t.Fatal("runtime was not marked consumed after first Serve close")
	}
	err := server.Serve(ctx)
	if !errors.Is(err, ErrRuntimeConsumed) {
		t.Fatalf("second Serve error = %v, want ErrRuntimeConsumed", err)
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("runtime closes = %d, want 1", got)
	}
}

func TestServeAllowsRepeatedUnavailableRuntime(t *testing.T) {
	ctx := context.Background()
	server := newUnstartedTestServerWithRuntime(t, custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable))
	var calls atomic.Int64
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, fmt.Errorf("listen reached %d", calls.Add(1))
	}

	if err := server.Serve(ctx); err == nil || !strings.Contains(err.Error(), "listen reached 1") {
		t.Fatalf("first Serve error = %v, want first listen error", err)
	}
	if err := server.Serve(ctx); err == nil || !strings.Contains(err.Error(), "listen reached 2") {
		t.Fatalf("second Serve error = %v, want second listen error", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("listener calls = %d, want 2", got)
	}
}

func TestSealedRootServeFailsTypedBeforeListen(t *testing.T) {
	ctx := context.Background()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.SealAdmissionRoot(ctx, root, authority.SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: filepath.Join(t.TempDir(), "new-domain")}); err != nil {
		t.Fatal(err)
	}

	restart := newTestServerAtRoot(t, root, shortTempDir(t), newFakeBackend("fake"))
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener must not open for sealed root")
	}
	err := restart.Serve(ctx)
	if !errors.Is(err, authority.ErrRootSealed) {
		t.Fatalf("Serve sealed root error = %v, want ErrRootSealed", err)
	}
}

type nilProbeBackend struct {
	*fakeBackend
}

func (b *nilProbeBackend) ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error) {
	return nil, nil
}

func closeCountingServedAdmissionRuntime(t *testing.T, closes *atomic.Int64, support custodian.Support) *servedAdmissionRuntime {
	t.Helper()
	launcher := newAdmissionFakeLaunchCustodian(t)
	return &servedAdmissionRuntime{
		runtime: custodian.NewUnavailableRuntimeForTest(custodian.ErrSupervisorUnavailable, func() error {
			closes.Add(1)
			return nil
		}),
		launchCustodian:  launcher,
		supportOverride:  &support,
		verifierOverride: launcher.verifier,
	}
}

func newUnstartedTestServerWithRuntime(t *testing.T, runtime custodian.Runtime) *Server {
	t.Helper()
	server, err := New(Config{
		StateRoot:    shortTempDir(t),
		CWD:          shortTempDir(t),
		Token:        "test-token",
		Backends:     []engine.Backend{newFakeBackend("fake")},
		ProcessTable: mapProcessTable{entries: map[int]engine.ProcessInfo{os.Getpid(): {PID: os.Getpid(), StartTime: "daemon-start"}}},
		IdleTimeout:  -1,
		Runtime:      runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func configureAdmissionSupport(t *testing.T, server *Server, launcher *admissionFakeLaunchCustodian, class custodian.SupportClass, cleanupSafe bool, retryProbe bool) {
	t.Helper()
	support := admissionSupportForClass(t, class, cleanupSafe, 1)
	runtime := &servedAdmissionRuntime{
		runtime:          custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
		launchCustodian:  launcher,
		supportOverride:  &support,
		verifierOverride: launcher.verifier,
	}
	if retryProbe {
		var calls atomic.Int64
		runtime.supportOverride = nil
		runtime.supportProbe = func(context.Context) custodian.Support {
			calls.Add(1)
			return admissionSupportForClass(t, class, cleanupSafe, 1)
		}
	}
	server.admissionRuntime = runtime
}

func admissionSupportForClass(t *testing.T, class custodian.SupportClass, cleanupSafe bool, attempts int) custodian.Support {
	t.Helper()
	if class == custodian.SupportAvailable {
		support, err := custodian.NewSupport(custodian.Support{
			ParkedExec:             true,
			VerifiedContainment:    true,
			ImplementationCompiled: true,
			RuntimeProbePassed:     true,
			FeatureConfigured:      true,
			FeatureAdvertised:      true,
			Platform:               "test",
			Assessment: custodian.SupportAssessment{
				Class:       custodian.SupportAvailable,
				Attempts:    attempts,
				CleanupSafe: true,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return support
	}
	cause := fmt.Errorf("test support %s", class)
	support, err := custodian.NewSupport(custodian.Support{
		Assessment: custodian.SupportAssessment{
			Class:       class,
			Cause:       cause,
			Attempts:    attempts,
			CleanupSafe: cleanupSafe,
		},
		ImplementationCompiled: true,
		RuntimeProbePassed:     false,
		RuntimeProbeResult:     cause,
		Platform:               "test",
		Reason:                 cause,
	})
	if err != nil {
		t.Fatal(err)
	}
	return support
}

func newTestServerAtRoot(t *testing.T, root, cwd string, backend engine.Backend) *Server {
	t.Helper()
	server, err := New(Config{
		StateRoot:    root,
		CWD:          cwd,
		Token:        "test-token",
		Backends:     []engine.Backend{backend},
		ProcessTable: mapProcessTable{entries: map[int]engine.ProcessInfo{os.Getpid(): {PID: os.Getpid(), StartTime: "daemon-start"}}},
		IdleTimeout:  -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func (s *Server) recordStartupEvent(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = map[string]*sessionState{}
	}
	s.sessions["startup-event-"+fmt.Sprintf("%03d", len(s.sessions))] = &sessionState{id: event}
}

func (s *Server) startupEvents() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]string, 0)
	for i := 0; ; i++ {
		state := s.sessions[fmt.Sprintf("startup-event-%03d", i)]
		if state == nil {
			return events
		}
		events = append(events, state.id)
	}
}
