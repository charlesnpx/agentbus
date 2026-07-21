package served

import (
	"context"
	"io"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
)

func RecoverAdmissionRoot(ctx context.Context, cfg Config) (AdmissionRecoveryReport, error) {
	server, err := New(cfg)
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	return server.recoverAdmissionRoot(ctx)
}

func (server *Server) recoverAdmissionRoot(ctx context.Context) (AdmissionRecoveryReport, error) {
	if server == nil {
		return AdmissionRecoveryReport{}, authority.ErrNotReady
	}
	server.ensureSafetyLatch()
	server.admissionStateMu.Lock()
	runtime := server.admissionRuntime
	if runtime == nil {
		runtime = newServedAdmissionRuntime(server)
		server.admissionRuntime = runtime
	}
	if runtime.consumed() {
		server.admissionStateMu.Unlock()
		return AdmissionRecoveryReport{}, ErrRuntimeConsumed
	}
	server.admissionStateMu.Unlock()
	defer func() {
		_ = runtime.close()
		server.admissionStateMu.Lock()
		if server.admissionRuntime == runtime {
			server.admissionRuntime = nil
		}
		server.admissionStateMu.Unlock()
	}()

	factory := server.admissionBootstrapperFactory
	if factory == nil {
		factory = openAdmissionBootstrapper
	}
	bootstrapper, _, closer, err := factory(ctx, server)
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	defer closeRecoveryOnlyRepository(closer)

	boot, err := server.admissionDaemonBoot()
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	metadata, err := session.RootMetadata(ctx)
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	if err := authority.ValidateAdmissionRootContract(metadata); err != nil {
		return AdmissionRecoveryReport{}, err
	}
	support := server.assessAdmissionSupportWithRetry(ctx, runtime)
	if !strictSupportAvailable(support) {
		diagnostic := newAdmissionSupportDiagnostic(metadata, support.Assessment, support.Assessment.Class == custodian.SupportRetryable)
		logAdmissionSupportDiagnostic(diagnostic)
		return AdmissionRecoveryReport{}, diagnostic
	}
	report, err := recoverAdmissionBeforeReadyReport(ctx, session, runtime.launchPort(), server.safetyLatch)
	if err != nil {
		return report, err
	}
	report.Mode = AdmissionRecoveryOnly.String()
	if _, err := session.SealReady(ctx); err != nil {
		return report, err
	}
	return report, nil
}

func closeRecoveryOnlyRepository(closer io.Closer) {
	if closer != nil {
		_ = closer.Close()
	}
}
