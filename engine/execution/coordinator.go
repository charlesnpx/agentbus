package execution

import "fmt"

type Coordinator struct {
	Store        *MemoryAdmissionStore
	BootID       string
	OwnerID      string
	FailStopping bool
	obligations  map[string]CoordinatorObligation
	nextAttempt  int
	nextProcess  int
}

func NewCoordinator(store *MemoryAdmissionStore, bootID, ownerID string) *Coordinator {
	if store == nil {
		store = NewMemoryAdmissionStore()
	}
	if bootID == "" {
		bootID = "boot-1"
	}
	if ownerID == "" {
		ownerID = "owner-1"
	}
	return &Coordinator{
		Store:       store,
		BootID:      bootID,
		OwnerID:     ownerID,
		obligations: map[string]CoordinatorObligation{},
		nextAttempt: 1,
		nextProcess: 100,
	}
}

func (c *Coordinator) Obligations() map[string]CoordinatorObligation {
	out := make(map[string]CoordinatorObligation, len(c.obligations))
	for k, v := range c.obligations {
		out[k] = v
	}
	return out
}

func (c *Coordinator) Check() error {
	return CheckInvariants(InvariantView{
		Store:         c.Store,
		Obligations:   c.Obligations(),
		FailStopping:  c.FailStopping,
		CurrentBootID: c.BootID,
	})
}

func (c *Coordinator) Submit(req SubmitRequest, injector *FailureInjector) (ResolveResult, error) {
	if req.Mode == "" {
		req.Mode = ModeIdentifiedFenced
	}
	if req.Mode == ModeLegacyUnfenced {
		return ResolveResult{}, protocolError(CodeLegacyUnfenced, "", "legacy unfenced path is outside fenced model")
	}
	if req.Mode == ModeIdentifiedFenced {
		if existing, ok, err := c.Store.ResolveExisting(req); err != nil {
			return existing, err
		} else if ok {
			return existing, nil
		}
	}

	if req.JobID == "" {
		req.JobID = c.Store.AllocateJobID()
	}
	if req.AttemptID == "" {
		req.AttemptID = c.nextAttemptID()
	}
	if req.Epoch == 0 {
		req.Epoch = 1
	}
	req.BootID = c.BootID
	req.OwnerID = c.OwnerID
	req.LaunchSpec.WorkspaceKey = req.WorkspaceKey
	req.LaunchSpec.RequestID = req.RequestID
	req.LaunchSpec.Fingerprint = req.Fingerprint.normalized()

	c.obligations[req.JobID] = CoordinatorObligation{
		JobID:      req.JobID,
		LaunchSpec: req.LaunchSpec,
		Mode:       req.Mode,
		Committed:  false,
	}
	if err := injector.Fail(FailBeforeCommit); err != nil {
		delete(c.obligations, req.JobID)
		return ResolveResult{}, err
	}
	result, err := c.Store.ResolveOrAccept(req)
	if err != nil {
		delete(c.obligations, req.JobID)
		return ResolveResult{}, err
	}
	obligation := c.obligations[req.JobID]
	obligation.Committed = true
	c.obligations[req.JobID] = obligation
	if err := injector.Fail(FailAfterCommit); err != nil {
		c.FailStopping = true
		return result, err
	}
	if err := injector.Fail(FailPostCommitPreRunnable); err != nil {
		c.FailStopping = true
		return result, err
	}
	return result, nil
}

func (c *Coordinator) SubmitLegacyFenced(req SubmitRequest, injector *FailureInjector) (ResolveResult, error) {
	req.Mode = ModeLegacyFenced
	if err := injector.Fail(FailBeforeSideEffect); err != nil {
		return ResolveResult{}, err
	}
	c.Store.noteModeledSideEffect()
	group := c.nextGroupRef()
	if err := injector.Fail(FailAfterSideEffect); err != nil {
		// The worker was prepared before any durable job existed; synchronous
		// reap is modeled by not creating an aggregate or launch side effect.
		return ResolveResult{}, err
	}
	req.PreparedSupervisor = &group
	return c.Submit(req, injector)
}

func (c *Coordinator) Acknowledge(jobID string) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	_, err := c.Store.Acknowledge(jobID, job.AttemptID, job.Epoch)
	return err
}

func (c *Coordinator) RejectUnacknowledged(jobID string) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	_, err := c.Store.RejectUnacknowledged(jobID, job.AttemptID, job.Epoch)
	if err == nil {
		delete(c.obligations, jobID)
	}
	return err
}

func (c *Coordinator) PrepareSupervisor(jobID string, injector *FailureInjector) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := injector.Fail(FailBeforeSideEffect); err != nil {
		return err
	}
	c.Store.noteModeledSideEffect()
	group := c.nextGroupRef()
	if err := injector.Fail(FailAfterSideEffect); err != nil {
		c.FailStopping = true
		return err
	}
	_, err := c.Store.RecordSupervisor(jobID, job.AttemptID, job.Epoch, group)
	return err
}

func (c *Coordinator) GrantPermit(jobID string, launchOrdinal int, nonce string, injector *FailureInjector) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := injector.Fail(FailBeforeCAS); err != nil {
		return err
	}
	if _, err := c.Store.GrantPermit(jobID, job.AttemptID, job.Epoch, launchOrdinal, nonce); err != nil {
		return err
	}
	if err := injector.Fail(FailAfterCAS); err != nil {
		return err
	}
	if err := injector.Fail(FailBeforeSideEffect); err != nil {
		return err
	}
	c.Store.noteModeledSideEffect()
	if err := injector.Fail(FailAfterSideEffect); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) Start(jobID string, injector *FailureInjector) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	for _, point := range []Failpoint{
		FailExecDeathBeforeFork,
		FailExecDeathAfterForkBeforeExec,
		FailExecDeathAfterExecBeforeStart,
		FailExecDeathAfterStartBeforeCAS,
	} {
		if err := injector.Fail(point); err != nil {
			if recErr := c.LiveSupervisorLoss(jobID); recErr != nil {
				c.FailStopping = true
				return recErr
			}
			return err
		}
	}
	c.Store.noteModeledSideEffect()
	child := ChildRef{PID: c.nextPID(), HighResStartToken: fmt.Sprintf("child-token-%s", jobID)}
	if _, err := c.Store.RecordStarted(jobID, job.AttemptID, job.Epoch, job.LaunchOrdinal, child); err != nil {
		return err
	}
	if err := injector.Fail(FailAfterCAS); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) Complete(jobID string, outcome Outcome) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if _, err := c.Store.RecordOutcome(jobID, job.AttemptID, job.Epoch, outcome); err != nil {
		return err
	}
	if job.PermitMaybeSent && !job.Contained {
		if _, err := c.Store.RecordLaunchQuiescent(jobID, job.AttemptID, job.Epoch, job.LaunchOrdinal); err != nil {
			return err
		}
	}
	if outcome == OutcomeCompleted || outcome == OutcomeCompletedNoncompliant {
		if _, err := c.Store.BeginResultPublication(jobID, "results/"+jobID+".txt", "sha256:"+jobID, int64(len(jobID))); err != nil {
			return err
		}
	}
	if _, err := c.Store.PublishTerminal(jobID, job.AttemptID, job.Epoch, outcome, ProofCleanQuiescentOutcomeAndRetired); err != nil {
		return err
	}
	delete(c.obligations, jobID)
	return nil
}

func (c *Coordinator) Cancel(jobID string) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	updated, err := c.Store.RequestCancel(jobID)
	if err != nil {
		return err
	}
	if updated.PermitMaybeSent {
		if err := c.LiveSupervisorLoss(jobID); err != nil {
			return err
		}
		job, _ = c.Store.GetJob(jobID)
		if job.Outcome == OutcomeReaped {
			job.Outcome = OutcomeCanceled
			c.Store.jobs[jobID].Outcome = OutcomeCanceled
			c.Store.jobs[jobID].TerminalReason = "canceled_after_permit"
		}
		return nil
	}
	if _, err := c.Store.RecordOutcome(jobID, job.AttemptID, job.Epoch, OutcomeCanceled); err != nil {
		return err
	}
	if _, err := c.Store.PublishTerminal(jobID, job.AttemptID, job.Epoch, OutcomeCanceled, ProofNeverPermittedAndRetired); err != nil {
		return err
	}
	delete(c.obligations, jobID)
	return nil
}

func (c *Coordinator) LiveSupervisorLoss(jobID string) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if job.Terminal() {
		return nil
	}
	if _, err := c.Store.BeginReconciliation(jobID, job.AttemptID, job.Epoch); err != nil {
		return err
	}
	if _, err := c.Store.RecordContained(jobID, job.AttemptID, job.Epoch, Evidence{Kind: "verified_absent", Detail: "live supervisor loss"}); err != nil {
		return err
	}
	outcome := OutcomeReaped
	reason := "daemon_restarted"
	if job.Decision == DecisionCancelRequested {
		outcome = OutcomeCanceled
		reason = "canceled_after_permit"
	}
	if _, err := c.Store.RecordOutcome(jobID, job.AttemptID, job.Epoch, outcome); err != nil {
		return err
	}
	if _, err := c.Store.PublishTerminal(jobID, job.AttemptID, job.Epoch, outcome, ProofContained); err != nil {
		return err
	}
	c.Store.jobs[jobID].TerminalReason = reason
	delete(c.obligations, jobID)
	return nil
}

func (c *Coordinator) StartupReconcile() error {
	jobs, err := c.Store.ListPriorBootNonterminal(c.BootID)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.PermitMaybeSent || job.PermitState == PermitGranted {
			if _, err := c.Store.BeginReconciliation(job.JobID, job.AttemptID, job.Epoch); err != nil {
				return err
			}
			if _, err := c.Store.RecordContained(job.JobID, job.AttemptID, job.Epoch, Evidence{Kind: "verified_absent", Detail: "startup reconciliation"}); err != nil {
				return err
			}
			if _, err := c.Store.RecordOutcome(job.JobID, job.AttemptID, job.Epoch, OutcomeReaped); err != nil {
				return err
			}
			if _, err := c.Store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, OutcomeReaped, ProofContained); err != nil {
				return err
			}
			c.Store.jobs[job.JobID].TerminalReason = "daemon_restarted"
			continue
		}
		if _, err := c.Store.RecordOutcome(job.JobID, job.AttemptID, job.Epoch, OutcomeFailed); err != nil {
			return err
		}
		if _, err := c.Store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, OutcomeFailed, ProofNeverPermittedAndRetired); err != nil {
			return err
		}
		c.Store.jobs[job.JobID].TerminalReason = "daemon_restarted_before_launch"
	}
	return nil
}

func (c *Coordinator) HasOwnedWork() bool {
	for _, job := range c.Store.jobs {
		if job.BootID == c.BootID && !job.Terminal() {
			return true
		}
	}
	return false
}

func (c *Coordinator) nextAttemptID() string {
	id := fmt.Sprintf("attempt-%d", c.nextAttempt)
	c.nextAttempt++
	return id
}

func (c *Coordinator) nextGroupRef() GroupRef {
	pid := c.nextPID()
	return GroupRef{PGID: pid, LeaderPID: pid, HighResStartToken: fmt.Sprintf("start-token-%d", pid)}
}

func (c *Coordinator) nextPID() int {
	c.nextProcess++
	return c.nextProcess
}
