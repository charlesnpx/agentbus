package served

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type servedAdmissionSupervisor struct {
	runtime admissionCustodianRuntime
}

type admissionCustodianRuntime interface {
	Support() custodian.Support
	Verifier() custodian.AttestationVerifier
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
	if s.runtime == nil {
		return fmt.Errorf("%w: admission runtime is nil", custodian.ErrSupervisorUnavailable)
	}
	support := s.runtime.Support()
	if support.AdvertisedAvailable() {
		return nil
	}
	if support.Reason != nil {
		return support.Reason
	}
	if support.RuntimeProbeResult != nil {
		return support.RuntimeProbeResult
	}
	return fmt.Errorf("%w: verified containment unsupported", custodian.ErrSupervisorUnavailable)
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

func (s *servedAdmissionSupervisor) Contain(context.Context, coordinator.PreparedSupervisor) (verified custodian.VerifiedQuiescence, err error) {
	return verified, custodian.ErrSupervisorUnavailable
}

func (s *servedAdmissionSupervisor) Retire(context.Context, coordinator.PreparedSupervisor) (verified custodian.VerifiedQuiescence, err error) {
	return verified, custodian.ErrSupervisorUnavailable
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
