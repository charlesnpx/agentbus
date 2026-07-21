package served

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
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
		})
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
	server, root, cwd := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	repo, err := bboltrepo.NewRepository(filepath.Join(root, authority.AdmissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Update(ctx, func(tx repository.WriteTx) error {
		metaRecord := tx.Meta()
		if metaRecord.State != repository.RecordValid {
			t.Fatalf("meta state = %s", metaRecord.State)
		}
		meta := metaRecord.Value
		meta.AdmissionRoot.ContractVersion = authority.CurrentAdmissionContractVersion + 1
		return tx.PutMeta(meta)
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	restart := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, restart, newAdmissionFakeLaunchCustodian(t), true)
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener must not open for contract mismatch")
	}
	err = restart.Serve(ctx)
	if !errors.Is(err, authority.ErrAdmissionContractMismatch) {
		t.Fatalf("Serve contract mismatch error = %v, want ErrAdmissionContractMismatch", err)
	}
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

func TestSealedRootServeFailsTypedBeforeListen(t *testing.T) {
	ctx := context.Background()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.SealAdmissionRoot(ctx, root, authority.SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true}); err != nil {
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
