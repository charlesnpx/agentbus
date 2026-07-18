package served

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type servedAdmissionSupervisor struct {
	runtime          custodian.Runtime
	launchCustodian  launch.CustodianPort
	supportOverride  *custodian.Support
	verifierOverride custodian.AttestationVerifier
}

func newServedAdmissionSupervisor(_ *Server) *servedAdmissionSupervisor {
	return &servedAdmissionSupervisor{
		runtime: custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
	}
}

func (s *servedAdmissionSupervisor) SetBoot(model.BootRef) {
}

func (s *servedAdmissionSupervisor) verifiedContainmentSupported(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("%w: admission supervisor is nil", custodian.ErrSupervisorUnavailable)
	}
	support := s.support()
	if support.VerifiedContainment {
		return nil
	}
	if support.Reason != nil {
		return support.Reason
	}
	return fmt.Errorf("%w: verified containment unsupported", custodian.ErrSupervisorUnavailable)
}

func (s *servedAdmissionSupervisor) support() custodian.Support {
	if s == nil {
		return custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable).Support()
	}
	if s.supportOverride != nil {
		return *s.supportOverride
	}
	return s.runtime.Support()
}

func (s *servedAdmissionSupervisor) quiescenceVerifier() custodian.AttestationVerifier {
	if s == nil {
		return custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable).Verifier()
	}
	if s.verifierOverride != (custodian.AttestationVerifier{}) {
		return s.verifierOverride
	}
	return s.runtime.Verifier()
}

func (s *servedAdmissionSupervisor) launchPort() launch.CustodianPort {
	if s == nil {
		return runtimeLaunchCustodian{runtime: custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable)}
	}
	if s.launchCustodian != nil {
		return s.launchCustodian
	}
	return runtimeLaunchCustodian{runtime: s.runtime}
}

type runtimeLaunchCustodian struct {
	runtime custodian.Runtime
}

func (c runtimeLaunchCustodian) Prepare(ctx context.Context, spec command.ExecSpec, key model.LaunchKey) (launch.PreparedProcess, error) {
	prepared, err := c.runtime.Process().Prepare(ctx, spec, key)
	if err != nil {
		return nil, err
	}
	return runtimePreparedProcess{prepared: prepared}, nil
}

func (c runtimeLaunchCustodian) ContainAndVerify(ctx context.Context, group model.GroupRef, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error) {
	return c.runtime.Process().ContainAndVerify(ctx, group, cause)
}

type runtimePreparedProcess struct {
	prepared custodian.PreparedProcess
}

func (p runtimePreparedProcess) Ref() model.GroupRef {
	if p.prepared == nil {
		return model.GroupRef{}
	}
	return p.prepared.Ref()
}

func (p runtimePreparedProcess) Release(ctx context.Context, token custodian.GrantToken) (launch.RunningProcess, error) {
	running, err := p.prepared.Release(ctx, token)
	if err != nil {
		return nil, err
	}
	streaming, ok := running.(interface {
		Ref() model.GroupRef
		Stdin() io.WriteCloser
		Stdout() io.ReadCloser
		Stderr() io.ReadCloser
		WaitAndVerify(context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, error)
		ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error)
	})
	if !ok {
		return nil, fmt.Errorf("%w: running process does not expose command streams", custodian.ErrSupervisorUnavailable)
	}
	return runtimeRunningProcess{running: streaming}, nil
}

func (p runtimePreparedProcess) AbortAndVerify(ctx context.Context) (custodian.VerifiedQuiescence, error) {
	if p.prepared == nil {
		return custodian.VerifiedQuiescence{}, custodian.ErrSupervisorUnavailable
	}
	return p.prepared.AbortAndVerify(ctx)
}

type runtimeRunningProcess struct {
	running interface {
		Ref() model.GroupRef
		Stdin() io.WriteCloser
		Stdout() io.ReadCloser
		Stderr() io.ReadCloser
		WaitAndVerify(context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, error)
		ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error)
	}
}

func (p runtimeRunningProcess) Ref() model.GroupRef {
	return p.running.Ref()
}

func (p runtimeRunningProcess) Stdin() io.WriteCloser {
	return p.running.Stdin()
}

func (p runtimeRunningProcess) Stdout() io.ReadCloser {
	return p.running.Stdout()
}

func (p runtimeRunningProcess) Stderr() io.ReadCloser {
	return p.running.Stderr()
}

func (p runtimeRunningProcess) WaitAndVerify(ctx context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, error) {
	return p.running.WaitAndVerify(ctx)
}

func (p runtimeRunningProcess) ContainAndVerify(ctx context.Context, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error) {
	return p.running.ContainAndVerify(ctx, cause)
}

func (p runtimeRunningProcess) WaitContained() bool {
	reporter, ok := p.running.(interface {
		WaitContained() bool
	})
	return ok && reporter.WaitContained()
}

func (s *servedAdmissionSupervisor) Register(model.JobID, engine.Backend, engine.SessionOpts) error {
	return custodian.ErrSupervisorUnavailable
}

func (s *servedAdmissionSupervisor) AttachActive(model.JobID, *activeJob) {
}

func (s *servedAdmissionSupervisor) Started(model.JobID, model.LaunchOrdinal) (engine.Session, string, error) {
	return nil, "", custodian.ErrSupervisorUnavailable
}

func (s *servedAdmissionSupervisor) Prepare(context.Context, coordinator.LaunchPlan) (coordinator.PreparedSupervisor, error) {
	return coordinator.PreparedSupervisor{}, custodian.ErrSupervisorUnavailable
}

func (s *servedAdmissionSupervisor) SendPermit(context.Context, coordinator.PreparedSupervisor, model.LaunchGrant) error {
	return custodian.ErrSupervisorUnavailable
}

func (s *servedAdmissionSupervisor) ObserveLaunch(context.Context, coordinator.PreparedSupervisor, model.LaunchGrant) (coordinator.LaunchObservation, error) {
	return coordinator.LaunchObservation{}, custodian.ErrSupervisorUnavailable
}

func (s *servedAdmissionSupervisor) VerifyQuiescence(context.Context, coordinator.PreparedSupervisor, model.LaunchReleaseFact) (verified custodian.VerifiedQuiescence, err error) {
	return verified, custodian.ErrSupervisorUnavailable
}

func (s *servedAdmissionSupervisor) Contain(ctx context.Context, prepared coordinator.PreparedSupervisor) (verified custodian.VerifiedQuiescence, err error) {
	return s.launchPort().ContainAndVerify(ctx, prepared.Group, custodian.QuiescenceCauseContain)
}

func (s *servedAdmissionSupervisor) Retire(ctx context.Context, prepared coordinator.PreparedSupervisor) (verified custodian.VerifiedQuiescence, err error) {
	return s.launchPort().ContainAndVerify(ctx, prepared.Group, custodian.QuiescenceCauseRecovery)
}

type servedResultPublisher struct {
	server *Server
}

func (p servedResultPublisher) Publish(ctx context.Context, jobID model.JobID, payload []byte) (model.ResultReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ResultReceipt{}, err
	}
	store := p.server.storeForJob(jobID.String())
	if store == nil {
		return model.ResultReceipt{}, fmt.Errorf("job store not found for %s", jobID)
	}
	info, err := store.WriteResult(jobID.String(), payload, p.server.inlineResultCap)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	return model.ResultReceipt{
		JobID: jobID,
		Result: model.ResultRef{
			Path:   info.ResultPath,
			Digest: info.SHA256,
			Bytes:  info.Bytes,
		},
		DirSynced: model.Evidence{
			Kind:   "served_result",
			Detail: "directory_synced",
		},
	}, nil
}

func (p servedResultPublisher) Verify(ctx context.Context, result model.ResultRef) (model.ResultReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ResultReceipt{}, err
	}
	raw, err := os.ReadFile(result.Path)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != result.Digest {
		return model.ResultReceipt{}, fmt.Errorf("result digest = %s, want %s", got, result.Digest)
	}
	if int64(len(raw)) != result.Bytes {
		return model.ResultReceipt{}, fmt.Errorf("result bytes = %d, want %d", len(raw), result.Bytes)
	}
	jobID, err := jobIDFromResultPath(result.Path)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	return model.ResultReceipt{
		JobID:  jobID,
		Result: result,
		DirSynced: model.Evidence{
			Kind:   "served_result",
			Detail: "directory_synced",
		},
	}, nil
}

func jobIDFromResultPath(path string) (model.JobID, error) {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	if ext != "" {
		base = base[:len(base)-len(ext)]
	}
	return model.NewJobID(base)
}

func (s *Server) markAdmissionJob(jobID string) {
	s.mu.Lock()
	s.admissionJobs[jobID] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) isAdmissionJob(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.admissionJobs[jobID]
	return ok
}

func (s *Server) completeAdmissionRun(run jobRun, state engine.JobState, text string) error {
	if s.admissionCoordinator == nil {
		return nil
	}
	jobID, err := model.NewJobID(run.jobID)
	if err != nil {
		return err
	}
	snapshot, err := s.admissionCoordinator.Snapshot(context.Background(), jobID)
	if err == nil && snapshot.Record.Terminal != nil {
		return nil
	}
	outcome, ok := admissionOutcomeForState(state)
	if !ok {
		return fmt.Errorf("cannot complete admission job %s with state %s", run.jobID, state)
	}
	return s.admissionCoordinator.Complete(context.Background(), jobID, outcome, []byte(text), nil)
}

func admissionOutcomeForState(state engine.JobState) (model.Outcome, bool) {
	switch state {
	case engine.StateCompleted:
		return model.OutcomeCompleted, true
	case engine.StateCompletedNoncompliant:
		return model.OutcomeCompletedNoncompliant, true
	case engine.StateFailed:
		return model.OutcomeFailed, true
	case engine.StateTimedOut:
		return model.OutcomeTimedOut, true
	case engine.StateCanceled:
		return model.OutcomeCanceled, true
	case engine.StateInterrupted:
		return model.OutcomeInterrupted, true
	case engine.StateReaped:
		return model.OutcomeReaped, true
	case engine.StateQuarantined:
		return model.OutcomeQuarantined, true
	default:
		return 0, false
	}
}

type admissionStartupRecoveryJob struct {
	record model.SafetyRecord
	plan   model.RecoveryPlan
}

func admissionStartupRecoveryJobs(ctx context.Context, repo repository.Repository, boot model.BootRef) ([]admissionStartupRecoveryJob, error) {
	if repo == nil {
		return nil, errors.New("admission recovery repository is nil")
	}
	var jobs []admissionStartupRecoveryJob
	if err := repo.View(ctx, func(tx repository.ReadTx) error {
		images, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		for _, image := range images {
			switch image.Safety.State {
			case repository.RecordCorrupt:
				return fmt.Errorf("%w: safety: %s", repository.ErrCorruptRecord, image.Safety.Diagnostic)
			case repository.RecordValid:
				record := image.Safety.Value
				if record.Terminal != nil || record.AdmittedBy.BootID == boot.BootID {
					continue
				}
				plan, err := model.PlanRecovery(record, model.RecoveryStartupLoss)
				if err != nil {
					return err
				}
				jobs = append(jobs, admissionStartupRecoveryJob{record: record, plan: plan})
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return jobs, nil
}

func admissionPreparedFromRecord(record model.SafetyRecord, ordinal model.LaunchOrdinal) (coordinator.PreparedSupervisor, error) {
	launch, ok := record.Attempt.Launches.Get(ordinal)
	if !ok || launch.Group == nil {
		return coordinator.PreparedSupervisor{}, fmt.Errorf("group reference is not bound for %s ordinal %s", record.JobID, ordinal)
	}
	prepared := coordinator.PreparedSupervisor{Ref: record.Attempt.Ref, Ordinal: ordinal, Group: *launch.Group}
	if err := prepared.ValidateFor(record.Attempt.Ref); err != nil {
		return coordinator.PreparedSupervisor{}, err
	}
	return prepared, nil
}

func syncFileAndParent(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncParentDir(path)
}

func syncParentDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
