package served

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type servedAdmissionSupervisor struct {
	server *Server

	mu       sync.Mutex
	launches map[model.JobID]*servedAdmissionLaunch
}

type servedAdmissionLaunch struct {
	mu       sync.Mutex
	plan     coordinator.LaunchPlan
	identity model.SupervisorIdentity
	backend  engine.Backend
	opts     engine.SessionOpts
	permit   *model.LaunchGrant
	session  engine.Session
	child    model.ChildIdentity
	active   *activeJob
}

func newServedAdmissionSupervisor(server *Server) *servedAdmissionSupervisor {
	return &servedAdmissionSupervisor{
		server:   server,
		launches: make(map[model.JobID]*servedAdmissionLaunch),
	}
}

func (s *servedAdmissionSupervisor) Register(jobID model.JobID, backend engine.Backend, opts engine.SessionOpts) error {
	if s == nil {
		return errors.New("admission supervisor is nil")
	}
	if err := jobID.Validate(); err != nil {
		return err
	}
	if backend == nil {
		return errors.New("backend is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	launch := s.launches[jobID]
	if launch == nil {
		launch = &servedAdmissionLaunch{backend: backend, opts: opts}
		s.launches[jobID] = launch
		return nil
	}
	launch.mu.Lock()
	defer launch.mu.Unlock()
	if launch.session != nil {
		return fmt.Errorf("admission launch already started for %s", jobID)
	}
	launch.backend = backend
	launch.opts = opts
	return nil
}

func (s *servedAdmissionSupervisor) AttachActive(jobID model.JobID, active *activeJob) {
	launch := s.launch(jobID)
	if launch == nil {
		return
	}
	launch.mu.Lock()
	launch.active = active
	launch.mu.Unlock()
}

func (s *servedAdmissionSupervisor) Started(jobID model.JobID) (engine.Session, string, error) {
	launch := s.launch(jobID)
	if launch == nil {
		return nil, "", fmt.Errorf("admission launch %s is not registered", jobID)
	}
	launch.mu.Lock()
	defer launch.mu.Unlock()
	if launch.session == nil {
		return nil, "", fmt.Errorf("admission launch %s has not started", jobID)
	}
	sessionID := launch.plan.SessionID
	if id := launch.session.ID(); id != "" {
		sessionID = id
	}
	return launch.session, sessionID, nil
}

func (s *servedAdmissionSupervisor) Prepare(_ context.Context, plan coordinator.LaunchPlan) (coordinator.PreparedSupervisor, error) {
	if err := plan.JobID.Validate(); err != nil {
		return coordinator.PreparedSupervisor{}, err
	}
	if err := plan.Ref.Validate(); err != nil {
		return coordinator.PreparedSupervisor{}, err
	}
	launch := s.launch(plan.JobID)
	if launch == nil {
		return coordinator.PreparedSupervisor{}, fmt.Errorf("admission launch %s is not registered", plan.JobID)
	}
	identity := model.SupervisorIdentity{
		PGID:               os.Getpid(),
		LeaderPID:          os.Getpid(),
		HighResStartToken:  "served-" + plan.Ref.AttemptID.String(),
		PlatformRetainedID: plan.JobID.String(),
	}
	if err := identity.Validate(); err != nil {
		return coordinator.PreparedSupervisor{}, err
	}
	prepared := coordinator.PreparedSupervisor{Ref: plan.Ref, Identity: identity}
	if err := prepared.ValidateFor(plan.Ref); err != nil {
		return coordinator.PreparedSupervisor{}, err
	}
	launch.mu.Lock()
	launch.plan = plan
	launch.identity = identity
	launch.mu.Unlock()
	return prepared, nil
}

func (s *servedAdmissionSupervisor) SendPermit(_ context.Context, prepared coordinator.PreparedSupervisor, grant model.LaunchGrant) error {
	if err := prepared.ValidateFor(grant.Attempt); err != nil {
		return err
	}
	launch := s.launch(grant.Attempt.JobID)
	if launch == nil {
		return fmt.Errorf("admission launch %s is not registered", grant.Attempt.JobID)
	}
	launch.mu.Lock()
	defer launch.mu.Unlock()
	if launch.permit != nil {
		if *launch.permit == grant {
			return nil
		}
		return fmt.Errorf("admission launch %s already has a different permit", grant.Attempt.JobID)
	}
	copied := grant
	launch.permit = &copied
	return nil
}

func (s *servedAdmissionSupervisor) ObserveLaunch(ctx context.Context, prepared coordinator.PreparedSupervisor, grant model.LaunchGrant) (coordinator.LaunchObservation, error) {
	if err := prepared.ValidateFor(grant.Attempt); err != nil {
		return coordinator.LaunchObservation{}, err
	}
	launch := s.launch(grant.Attempt.JobID)
	if launch == nil {
		return coordinator.LaunchObservation{}, fmt.Errorf("admission launch %s is not registered", grant.Attempt.JobID)
	}
	launch.mu.Lock()
	defer launch.mu.Unlock()
	if launch.permit == nil || *launch.permit != grant {
		return coordinator.LaunchObservation{}, fmt.Errorf("admission launch %s has no matching permit", grant.Attempt.JobID)
	}
	if launch.session == nil {
		session, err := launch.backend.Start(ctx, launch.opts)
		if err != nil {
			return coordinator.LaunchObservation{}, err
		}
		launch.session = session
	}
	if launch.child.PID == 0 {
		child := model.ChildIdentity{
			PID:               os.Getpid(),
			HighResStartToken: fmt.Sprintf("served-child-%s-%s", grant.Attempt.AttemptID, grant.Ordinal),
		}
		if err := child.Validate(); err != nil {
			return coordinator.LaunchObservation{}, err
		}
		launch.child = child
	}
	return coordinator.LaunchObservation{
		Ordinal: grant.Ordinal,
		Child:   launch.child,
		Evidence: model.Evidence{
			Kind:   "served_launch",
			Detail: "backend_session_started",
		},
	}, nil
}

func (s *servedAdmissionSupervisor) VerifyQuiescence(_ context.Context, prepared coordinator.PreparedSupervisor, consumed model.LaunchConsumed) (model.QuiescenceReceipt, error) {
	if err := prepared.ValidateFor(consumed.Attempt); err != nil {
		return model.QuiescenceReceipt{}, err
	}
	return model.QuiescenceReceipt{
		Attempt: consumed.Attempt,
		Ordinal: consumed.Ordinal,
		Child:   consumed.Child,
		ChildExited: model.Evidence{
			Kind:   "served_quiescence",
			Detail: "child_exited",
		},
		GroupEmpty: model.Evidence{
			Kind:   "served_quiescence",
			Detail: "group_empty",
		},
	}, nil
}

func (s *servedAdmissionSupervisor) Contain(ctx context.Context, prepared coordinator.PreparedSupervisor) (model.ContainmentReceipt, error) {
	if err := prepared.ValidateFor(prepared.Ref); err != nil {
		return model.ContainmentReceipt{}, err
	}
	if launch := s.launch(prepared.Ref.JobID); launch != nil {
		launch.mu.Lock()
		active := launch.active
		session := launch.session
		launch.mu.Unlock()
		if active != nil {
			active.requestTerminal(engine.StateCanceled)
			if active.cancel != nil {
				active.cancel()
			}
		}
		if session != nil {
			_ = session.Interrupt(ctx)
		}
	}
	return model.ContainmentReceipt{
		Attempt:    prepared.Ref,
		Supervisor: prepared.Identity,
		Signal: model.Evidence{
			Kind:   "served_contain",
			Detail: "cancel_requested",
		},
		Verification: model.Evidence{
			Kind:   "served_contain",
			Detail: "containment_verified",
		},
	}, nil
}

func (s *servedAdmissionSupervisor) Retire(_ context.Context, prepared coordinator.PreparedSupervisor) (model.RetirementReceipt, error) {
	if err := prepared.ValidateFor(prepared.Ref); err != nil {
		return model.RetirementReceipt{}, err
	}
	if launch := s.launch(prepared.Ref.JobID); launch != nil {
		launch.mu.Lock()
		active := launch.active
		launch.mu.Unlock()
		if active != nil && active.cancel != nil {
			active.cancel()
		}
	}
	return model.RetirementReceipt{
		Attempt:    prepared.Ref,
		Supervisor: prepared.Identity,
		ControlClosed: model.Evidence{
			Kind:   "served_retire",
			Detail: "control_closed",
		},
		WorkerExited: model.Evidence{
			Kind:   "served_retire",
			Detail: "worker_exited",
		},
		GroupEmpty: model.Evidence{
			Kind:   "served_retire",
			Detail: "group_empty",
		},
	}, nil
}

func (s *servedAdmissionSupervisor) launch(jobID model.JobID) *servedAdmissionLaunch {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.launches[jobID]
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

func admissionPreparedFromRecord(record model.SafetyRecord) (coordinator.PreparedSupervisor, error) {
	if record.Attempt.Supervisor == nil {
		return coordinator.PreparedSupervisor{}, fmt.Errorf("supervisor identity is not bound for %s", record.JobID)
	}
	prepared := coordinator.PreparedSupervisor{Ref: record.Attempt.Ref, Identity: *record.Attempt.Supervisor}
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
