package execution

import "fmt"

type Coordinator struct {
	Store                           *MemoryAdmissionStore
	BootID                          string
	OwnerID                         string
	FailStopping                    bool
	LifecycleState                  CoordinatorLifecycleState
	allowPreRunnable                bool
	allowCurrentBootOrphanReconcile bool
	obligations                     map[string]CoordinatorObligation
	preparedRetirements             map[string]PreparedSupervisorRetirement
	legacyUnfenced                  map[string]LegacyUnfencedRun
	nextAttempt                     int
	nextProcess                     int
	nextLegacyRunID                 int
}

type LegacyUnfencedRun struct {
	RunID        string
	WorkspaceKey string
	Started      bool
	Acknowledged bool
	Retired      bool
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
		Store:               store,
		BootID:              bootID,
		OwnerID:             ownerID,
		LifecycleState:      CoordinatorLifecycleNotReady,
		obligations:         map[string]CoordinatorObligation{},
		preparedRetirements: map[string]PreparedSupervisorRetirement{},
		legacyUnfenced:      map[string]LegacyUnfencedRun{},
		nextAttempt:         1,
		nextProcess:         100,
		nextLegacyRunID:     1,
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
		Store:                           c.Store,
		Obligations:                     c.Obligations(),
		FailStopping:                    c.FailStopping,
		CurrentBootID:                   c.BootID,
		LifecycleState:                  c.LifecycleState,
		AllowPreRunnable:                c.allowPreRunnable,
		AllowCurrentBootOrphanReconcile: c.allowCurrentBootOrphanReconcile,
	})
}

func (c *Coordinator) Submit(req SubmitRequest, injector *FailureInjector) (ResolveResult, error) {
	if err := c.rejectIfNotRunning(); err != nil {
		return ResolveResult{}, err
	}
	if req.Mode == "" {
		req.Mode = ModeIdentifiedFenced
	}
	if req.Mode == ModeLegacyUnfenced {
		return ResolveResult{}, protocolError(CodeLegacyUnfenced, "", "legacy unfenced path uses SubmitLegacyUnfenced")
	}
	if err := c.requireLegacyFencedPrepared(req); err != nil {
		return ResolveResult{}, c.checkBoundary(err)
	}
	if req.Mode == ModeIdentifiedFenced {
		if existing, ok, err := c.Store.ResolveExisting(req); err != nil {
			return existing, c.checkBoundary(err)
		} else if ok {
			return existing, c.checkBoundary(nil)
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
	req.Fingerprint = CurrentFingerprint(requestRawTask(req))
	spec, err := materializeLaunchSpec(req, req.Fingerprint)
	if err != nil {
		if req.PreparedSupervisor != nil {
			if retireErr := c.retirePendingPrepared(req.JobID, injector); retireErr != nil {
				c.beginFailStop()
				return ResolveResult{}, c.checkBoundary(fmt.Errorf("%w; prepared supervisor retirement: %v", err, retireErr))
			}
		}
		return ResolveResult{}, c.checkBoundary(err)
	}
	req.LaunchSpec = spec

	c.obligations[req.JobID] = CoordinatorObligation{
		JobID:              req.JobID,
		LaunchSpec:         req.LaunchSpec,
		Mode:               req.Mode,
		State:              ObligationPending,
		PreparedSupervisor: req.PreparedSupervisor,
	}
	if err := c.inject(injector, FailAdmissionBeforeCommit); err != nil {
		if retireErr := c.retirePendingPrepared(req.JobID, injector); retireErr != nil {
			c.beginFailStop()
			return ResolveResult{}, c.checkBoundary(retireErr)
		}
		delete(c.obligations, req.JobID)
		return ResolveResult{}, err
	}
	result, err := c.Store.ResolveOrAccept(req)
	if err != nil {
		if retireErr := c.retirePendingPrepared(req.JobID, injector); retireErr != nil {
			c.beginFailStop()
			return ResolveResult{}, c.checkBoundary(fmt.Errorf("%w; prepared supervisor retirement: %v", err, retireErr))
		}
		delete(c.obligations, req.JobID)
		return ResolveResult{}, c.checkBoundary(err)
	}
	c.setObligationState(req.JobID, ObligationCommitted)
	if err := c.injectPreRunnable(injector, FailPostCommitPreRunnable); err != nil {
		c.beginFailStop()
		return result, c.checkBoundary(err)
	}
	if err := c.injectPreRunnable(injector, FailAdmissionAfterCommit); err != nil {
		c.beginFailStop()
		return result, c.checkBoundary(err)
	}
	c.setObligationState(req.JobID, ObligationRunnable)
	return result, c.checkBoundary(nil)
}

func (c *Coordinator) SubmitLegacyFenced(req SubmitRequest, injector *FailureInjector) (ResolveResult, error) {
	if err := c.rejectIfNotRunning(); err != nil {
		return ResolveResult{}, err
	}
	req.Mode = ModeLegacyFenced
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
	req.Fingerprint = CurrentFingerprint(requestRawTask(req))
	spec, err := materializeLaunchSpec(req, req.Fingerprint)
	if err != nil {
		return ResolveResult{}, c.checkBoundary(err)
	}
	req.LaunchSpec = spec
	c.obligations[req.JobID] = CoordinatorObligation{JobID: req.JobID, LaunchSpec: spec, Mode: req.Mode, State: ObligationPending}
	if err := c.inject(injector, FailLegacySupervisorPrepareBefore); err != nil {
		delete(c.obligations, req.JobID)
		return ResolveResult{}, err
	}
	c.Store.noteModeledSideEffect()
	group := c.nextGroupRef()
	obligation := c.obligations[req.JobID]
	obligation.PreparedSupervisor = &group
	c.obligations[req.JobID] = obligation
	if err := c.inject(injector, FailLegacySupervisorPrepareAfter); err != nil {
		if retireErr := c.retirePendingPrepared(req.JobID, injector); retireErr != nil {
			c.beginFailStop()
			return ResolveResult{}, c.checkBoundary(retireErr)
		}
		delete(c.obligations, req.JobID)
		return ResolveResult{}, err
	}
	req.PreparedSupervisor = &group
	return c.Submit(req, injector)
}

func (c *Coordinator) SubmitLegacyUnfenced(req SubmitRequest, responseDelivered bool, injector *FailureInjector) (LegacyUnfencedRun, error) {
	if err := c.rejectIfNotRunning(); err != nil {
		return LegacyUnfencedRun{}, err
	}
	req.Mode = ModeLegacyUnfenced
	run := LegacyUnfencedRun{
		RunID:        fmt.Sprintf("legacy-unfenced-%d", c.nextLegacyRunID),
		WorkspaceKey: req.WorkspaceKey,
	}
	c.nextLegacyRunID++
	if err := c.inject(injector, FailLegacyUnfencedStartBefore); err != nil {
		return run, err
	}
	c.Store.noteModeledSideEffect()
	run.Started = true
	if err := c.inject(injector, FailLegacyUnfencedStartAfter); err != nil {
		run.Retired = true
		return run, protocolError(CodeLegacyUnfenced, "", "legacy unfenced Start failed before job acceptance")
	}
	if !responseDelivered {
		run.Retired = true
		return run, protocolError(CodeLegacyUnfenced, "", "legacy unfenced response was not acknowledged")
	}
	run.Acknowledged = true
	c.legacyUnfenced[run.RunID] = run
	return run, c.checkBoundary(nil)
}

func (c *Coordinator) Acknowledge(jobID string) error {
	return c.AcknowledgeWithInjector(jobID, nil)
}

func (c *Coordinator) AcknowledgeWithInjector(jobID string, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	return c.casStep(injector, FailAcknowledgeBeforeCAS, FailAcknowledgeAfterCAS, func() error {
		_, err := c.Store.Acknowledge(jobID, job.AttemptID, job.Epoch)
		return err
	})
}

func (c *Coordinator) RejectUnacknowledged(jobID string) error {
	return c.RejectUnacknowledgedWithInjector(jobID, nil)
}

func (c *Coordinator) RejectUnacknowledgedWithInjector(jobID string, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := c.retireSupervisor(job, injector); err != nil {
		return err
	}
	err := c.casStep(injector, FailRejectBeforeCAS, FailRejectAfterCAS, func() error {
		_, err := c.Store.RejectUnacknowledged(jobID, job.AttemptID, job.Epoch)
		return err
	})
	if err == nil {
		delete(c.obligations, jobID)
	}
	return err
}

func (c *Coordinator) PrepareSupervisor(jobID string, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := c.inject(injector, FailSupervisorRecordBeforeCAS); err != nil {
		return err
	}
	if err := c.inject(injector, FailSupervisorPrepareBefore); err != nil {
		return err
	}
	c.Store.noteModeledSideEffect()
	group := c.nextGroupRef()
	if err := c.inject(injector, FailSupervisorPrepareAfter); err != nil {
		c.beginFailStop()
		return c.checkBoundary(err)
	}
	err := c.StoreOp(FailSupervisorRecordAfterCAS, injector, func() error {
		_, err := c.Store.RecordSupervisor(jobID, job.AttemptID, job.Epoch, group)
		return err
	})
	return err
}

func (c *Coordinator) GrantPermit(jobID string, launchOrdinal int, nonce string, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := c.casStep(injector, FailGrantPermitBeforeCAS, FailGrantPermitAfterCAS, func() error {
		_, err := c.Store.GrantPermit(jobID, job.AttemptID, job.Epoch, launchOrdinal, nonce)
		return err
	}); err != nil {
		return err
	}
	if err := c.inject(injector, FailPermitSendBeforeSideEffect); err != nil {
		return err
	}
	c.Store.noteModeledSideEffect()
	if err := c.inject(injector, FailPermitSendAfterSideEffect); err != nil {
		return err
	}
	return c.casStep(injector, FailPermitMaybeSentBeforeCAS, FailPermitMaybeSentAfterCAS, func() error {
		_, err := c.Store.RecordPermitMaybeSent(jobID, job.AttemptID, job.Epoch, launchOrdinal)
		return err
	})
}

func (c *Coordinator) Start(jobID string, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	postPermit := executionUncertain(&job)
	if err := c.inject(injector, FailExecDeathBeforeFork); err != nil {
		return c.reconcilePostPermitError(jobID, postPermit, err)
	}
	if err := c.casStep(injector, FailExecForkBeforeCAS, FailExecForkAfterCAS, func() error {
		_, err := c.Store.RecordExecForked(jobID, job.AttemptID, job.Epoch)
		return err
	}); err != nil {
		return c.reconcilePostPermitError(jobID, postPermit, err)
	}
	c.setStartPhase(jobID, "forked")
	if err := c.inject(injector, FailExecDeathAfterForkBeforeExec); err != nil {
		return c.reconcilePostPermitError(jobID, postPermit, err)
	}
	if err := c.casStep(injector, FailExecBeforeCAS, FailExecAfterCAS, func() error {
		_, err := c.Store.RecordExeced(jobID, job.AttemptID, job.Epoch)
		return err
	}); err != nil {
		return c.reconcilePostPermitError(jobID, postPermit, err)
	}
	c.setStartPhase(jobID, "execed")
	if err := c.inject(injector, FailExecDeathAfterExecBeforeStart); err != nil {
		return c.reconcilePostPermitError(jobID, postPermit, err)
	}
	c.Store.noteModeledSideEffect()
	child := ChildRef{PID: job.Supervisor.LeaderPID, HighResStartToken: job.Supervisor.HighResStartToken}
	if err := c.casStep(injector, FailBackendStartedBeforeCAS, FailBackendStartedAfterCAS, func() error {
		_, err := c.Store.RecordBackendStarted(jobID, job.AttemptID, job.Epoch, child)
		return err
	}); err != nil {
		return c.reconcilePostPermitError(jobID, postPermit, err)
	}
	c.setStartPhase(jobID, "backend_started")
	if err := c.inject(injector, FailExecDeathAfterStartBeforeCAS); err != nil {
		return c.reconcilePostPermitError(jobID, postPermit, err)
	}
	err := c.casStep(injector, FailRecordStartedBeforeCAS, FailRecordStartedAfterCAS, func() error {
		_, err := c.Store.RecordStarted(jobID, job.AttemptID, job.Epoch, job.LaunchOrdinal, child)
		return err
	})
	if err == nil {
		c.setStartPhase(jobID, "record_started")
	}
	return c.reconcilePostPermitError(jobID, postPermit, err)
}

func (c *Coordinator) Complete(jobID string, outcome Outcome) error {
	return c.CompleteWithInjector(jobID, outcome, nil)
}

func (c *Coordinator) CompleteWithInjector(jobID string, outcome Outcome, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := c.casStep(injector, FailOutcomeBeforeCAS, FailOutcomeAfterCAS, func() error {
		_, err := c.Store.RecordOutcome(jobID, job.AttemptID, job.Epoch, outcome)
		return err
	}); err != nil {
		return err
	}
	job, _ = c.Store.GetJob(jobID)
	if job.PermitState == PermitConsumed {
		if err := c.casStep(injector, FailLaunchExitBeforeCAS, FailLaunchExitAfterCAS, func() error {
			_, err := c.Store.RecordLaunchExitEvidence(jobID, job.AttemptID, job.Epoch, job.LaunchOrdinal,
				Evidence{Kind: "child_exit", Detail: "outcome observed"},
				Evidence{Kind: "group_empty", Detail: "outcome observed"})
			return err
		}); err != nil {
			return err
		}
		if err := c.casStep(injector, FailLaunchQuiescentBeforeCAS, FailLaunchQuiescentAfterCAS, func() error {
			_, err := c.Store.RecordLaunchQuiescent(jobID, job.AttemptID, job.Epoch, job.LaunchOrdinal)
			return err
		}); err != nil {
			return err
		}
	}
	job, _ = c.Store.GetJob(jobID)
	if err := c.retireSupervisor(job, injector); err != nil {
		return err
	}
	if outcome == OutcomeCompleted || outcome == OutcomeCompletedNoncompliant {
		result := ResultRef{Path: "results/" + jobID + ".txt", Digest: CurrentFingerprint("result:" + jobID).Value, Bytes: int64(len("result:" + jobID))}
		if err := c.publishResult(jobID, job.AttemptID, job.Epoch, result, injector); err != nil {
			return err
		}
	}
	err := c.casStep(injector, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
		_, err := c.Store.PublishTerminal(jobID, job.AttemptID, job.Epoch, outcome, ProofCleanQuiescentOutcomeAndRetired)
		return err
	})
	if err == nil {
		delete(c.obligations, jobID)
	}
	return err
}

func (c *Coordinator) Cancel(jobID string) error {
	return c.CancelWithInjector(jobID, nil)
}

func (c *Coordinator) CancelWithInjector(jobID string, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := c.casStep(injector, FailCancelBeforeCAS, FailCancelAfterCAS, func() error {
		_, err := c.Store.RequestCancel(jobID)
		return err
	}); err != nil {
		return err
	}
	updated, _ := c.Store.GetJob(jobID)
	if executionUncertain(&updated) {
		return c.LiveSupervisorLossWithInjector(jobID, injector)
	}
	if err := c.casStep(injector, FailOutcomeBeforeCAS, FailOutcomeAfterCAS, func() error {
		_, err := c.Store.RecordOutcome(jobID, job.AttemptID, job.Epoch, OutcomeCanceled)
		return err
	}); err != nil {
		return err
	}
	job, _ = c.Store.GetJob(jobID)
	if err := c.retireSupervisor(job, injector); err != nil {
		return err
	}
	err := c.casStep(injector, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
		_, err := c.Store.PublishTerminal(jobID, job.AttemptID, job.Epoch, OutcomeCanceled, ProofNeverPermittedAndRetired)
		return err
	})
	if err == nil {
		delete(c.obligations, jobID)
	}
	return err
}

func (c *Coordinator) LiveSupervisorLoss(jobID string) error {
	return c.LiveSupervisorLossWithInjector(jobID, nil)
}

func (c *Coordinator) LiveSupervisorLossWithInjector(jobID string, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if job.Terminal() {
		return c.checkBoundary(nil)
	}
	if err := c.casStep(injector, FailReconciliationBeforeCAS, FailReconciliationAfterCAS, func() error {
		_, err := c.Store.BeginReconciliation(jobID, job.AttemptID, job.Epoch)
		return err
	}); err != nil {
		return c.failStopAfterReconciliationError(jobID, err)
	}
	if err := c.contain(jobID, injector, "live supervisor loss"); err != nil {
		return c.failStopAfterReconciliationError(jobID, err)
	}
	job, _ = c.Store.GetJob(jobID)
	outcome := OutcomeReaped
	reason := "daemon_restarted"
	if job.Decision == DecisionCancelRequested {
		outcome = OutcomeCanceled
		reason = "canceled_after_permit"
	}
	if err := c.casStep(injector, FailOutcomeBeforeCAS, FailOutcomeAfterCAS, func() error {
		_, err := c.Store.RecordOutcome(jobID, job.AttemptID, job.Epoch, outcome)
		return err
	}); err != nil {
		return c.failStopAfterReconciliationError(jobID, err)
	}
	if err := c.casStep(injector, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
		_, err := c.Store.PublishTerminal(jobID, job.AttemptID, job.Epoch, outcome, ProofContained)
		return err
	}); err != nil {
		return c.failStopAfterReconciliationError(jobID, err)
	}
	c.Store.jobs[jobID].TerminalReason = reason
	delete(c.obligations, jobID)
	return c.checkBoundary(nil)
}

func (c *Coordinator) StartupReconcile() (err error) {
	if err := c.rejectIfFailStopped(); err != nil {
		return err
	}
	if c.LifecycleState == CoordinatorLifecycleRunning {
		return c.checkBoundary(nil)
	}
	if c.LifecycleState == CoordinatorLifecycleReconciling {
		return protocolError(CodePreconditionFailed, "", "startup reconciliation already in progress")
	}
	c.LifecycleState = CoordinatorLifecycleReconciling
	defer func() {
		if err != nil && c.LifecycleState != CoordinatorLifecycleFailStopped {
			c.LifecycleState = CoordinatorLifecycleNotReady
		}
	}()
	if err := c.validateStartupAnchors(); err != nil {
		return err
	}
	if err := c.reconcileCurrentBootOrphans(); err != nil {
		return err
	}
	jobs, err := c.Store.ListPriorBootNonterminal(c.BootID)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if done, err := c.publishContainedStartupTerminal(job, "daemon_restarted"); done || err != nil {
			if err != nil {
				return err
			}
			continue
		}
		if executionUncertain(&job) {
			if err := c.casStep(nil, FailReconciliationBeforeCAS, FailReconciliationAfterCAS, func() error {
				_, err := c.Store.BeginReconciliation(job.JobID, job.AttemptID, job.Epoch)
				return err
			}); err != nil {
				return err
			}
			if err := c.contain(job.JobID, nil, "startup reconciliation"); err != nil {
				return err
			}
			if err := c.casStep(nil, FailOutcomeBeforeCAS, FailOutcomeAfterCAS, func() error {
				_, err := c.Store.RecordOutcome(job.JobID, job.AttemptID, job.Epoch, OutcomeReaped)
				return err
			}); err != nil {
				return err
			}
			if err := c.casStep(nil, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
				_, err := c.Store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, OutcomeReaped, ProofContained)
				return err
			}); err != nil {
				return err
			}
			c.Store.jobs[job.JobID].TerminalReason = "daemon_restarted"
			continue
		}
		if err := c.casStep(nil, FailOutcomeBeforeCAS, FailOutcomeAfterCAS, func() error {
			_, err := c.Store.RecordOutcome(job.JobID, job.AttemptID, job.Epoch, OutcomeFailed)
			return err
		}); err != nil {
			return err
		}
		refreshed, _ := c.Store.GetJob(job.JobID)
		if err := c.retireSupervisor(refreshed, nil); err != nil {
			return err
		}
		if err := c.casStep(nil, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
			_, err := c.Store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, OutcomeFailed, ProofNeverPermittedAndRetired)
			return err
		}); err != nil {
			return err
		}
		c.Store.jobs[job.JobID].TerminalReason = "daemon_restarted_before_launch"
	}
	c.LifecycleState = CoordinatorLifecycleRunning
	if err = c.checkBoundary(nil); err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) Expire(workspaceKey, requestID string, injector *FailureInjector) (string, error) {
	if err := c.rejectIfNotRunning(); err != nil {
		return "", err
	}
	var expired string
	err := c.casStep(injector, FailExpireBeforeCAS, FailExpireAfterCAS, func() error {
		id, err := c.Store.Expire(workspaceKey, requestID)
		expired = id
		return err
	})
	return expired, err
}

func (c *Coordinator) MarkCorrupt(jobID string, permitMaybe, identityTrustworthy bool, diagnostic string, injector *FailureInjector) error {
	if err := c.rejectIfNotRunning(); err != nil {
		return err
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if !identityTrustworthy {
		c.beginFailStop()
		return protocolError(CodeCorruptFatal, jobID, "containment identity is untrustworthy")
	}
	if permitMaybe {
		if err := c.contain(jobID, injector, "corrupt quarantine"); err != nil {
			return err
		}
	} else if err := c.retireSupervisor(job, injector); err != nil {
		return err
	}
	job, _ = c.Store.GetJob(jobID)
	if err := c.casStep(injector, FailCorruptBeforeCAS, FailCorruptAfterCAS, func() error {
		_, err := c.Store.MarkCorrupt(jobID, permitMaybe, identityTrustworthy, diagnostic)
		return err
	}); err != nil {
		return err
	}
	proof := ProofNeverPermittedAndRetired
	if permitMaybe {
		proof = ProofContained
	}
	err := c.casStep(injector, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
		_, err := c.Store.PublishTerminal(jobID, job.AttemptID, job.Epoch, OutcomeQuarantined, proof)
		return err
	})
	if err == nil {
		delete(c.obligations, jobID)
	}
	return err
}

func (c *Coordinator) HasOwnedWork() bool {
	if c.LifecycleState != CoordinatorLifecycleRunning {
		return false
	}
	for _, job := range c.Store.jobs {
		if job.BootID == c.BootID && !job.Terminal() {
			return true
		}
	}
	return false
}

func (c *Coordinator) reconcileCurrentBootOrphans() error {
	var jobs []Aggregate
	for _, job := range c.Store.jobs {
		if job.BootID == c.BootID && !job.Terminal() && job.Mode != ModeLegacyUnfenced && !c.hasCommittedObligationFor(job.copy()) {
			jobs = append(jobs, job.copy())
		}
	}
	for _, job := range jobs {
		if err := c.withCurrentBootOrphanReconcile(func() error {
			return c.terminalizeForStartup(job, "current_boot_orphan")
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) validateStartupAnchors() error {
	if c.Store == nil {
		return protocolError(CodePreconditionFailed, "", "missing admission store")
	}
	for key, binding := range c.Store.bindings {
		if bindingKey(binding.WorkspaceKey, binding.RequestID) != key {
			return fmt.Errorf("binding %q does not match workspace/request anchor", key)
		}
		if err := validateWorkspaceRequest(binding.WorkspaceKey, binding.RequestID, true); err != nil {
			return fmt.Errorf("binding %q is invalid: %w", key, err)
		}
		if !binding.Fingerprint.supported() {
			return protocolError(CodeRequestFingerprintUnsupported, binding.JobID, "recorded fingerprint is unsupported")
		}
		if _, ok := c.Store.tombstones[key]; ok {
			return fmt.Errorf("binding %q also has tombstone", key)
		}
		job, ok := c.Store.jobs[binding.JobID]
		if !ok {
			return protocolError(CodePreconditionFailed, binding.JobID, "binding references missing job")
		}
		if job.WorkspaceKey != binding.WorkspaceKey || job.RequestID != binding.RequestID || !job.Fingerprint.Equal(binding.Fingerprint) {
			return fmt.Errorf("binding %q does not match aggregate identity", key)
		}
		if accepted := c.Store.acceptedKeys[key]; accepted != "" && accepted != binding.JobID {
			return fmt.Errorf("request key %q reaccepted as %s after %s", key, binding.JobID, accepted)
		}
	}
	for key, tombstone := range c.Store.tombstones {
		if bindingKey(tombstone.WorkspaceKey, tombstone.RequestID) != key {
			return fmt.Errorf("tombstone %q does not match workspace/request anchor", key)
		}
		if err := validateWorkspaceRequest(tombstone.WorkspaceKey, tombstone.RequestID, true); err != nil {
			return fmt.Errorf("tombstone %q is invalid: %w", key, err)
		}
		if !tombstone.Fingerprint.supported() {
			return protocolError(CodeRequestFingerprintUnsupported, tombstone.JobID, "recorded tombstone fingerprint is unsupported")
		}
		if accepted := c.Store.acceptedKeys[key]; accepted != "" && accepted != tombstone.JobID {
			return fmt.Errorf("request key %q tombstone changed job from %s to %s", key, accepted, tombstone.JobID)
		}
	}
	return nil
}

func (c *Coordinator) terminalizeForStartup(job Aggregate, reasonPrefix string) error {
	if done, err := c.publishContainedStartupTerminal(job, reasonPrefix+"_contained"); done || err != nil {
		return err
	}
	if executionUncertain(&job) {
		if err := c.casStep(nil, FailReconciliationBeforeCAS, FailReconciliationAfterCAS, func() error {
			_, err := c.Store.BeginReconciliation(job.JobID, job.AttemptID, job.Epoch)
			return err
		}); err != nil {
			return err
		}
		if err := c.contain(job.JobID, nil, reasonPrefix); err != nil {
			return err
		}
		if err := c.casStep(nil, FailOutcomeBeforeCAS, FailOutcomeAfterCAS, func() error {
			_, err := c.Store.RecordOutcome(job.JobID, job.AttemptID, job.Epoch, OutcomeReaped)
			return err
		}); err != nil {
			return err
		}
		if err := c.casStep(nil, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
			_, err := c.Store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, OutcomeReaped, ProofContained)
			return err
		}); err != nil {
			return err
		}
		c.Store.jobs[job.JobID].TerminalReason = reasonPrefix + "_contained"
		return nil
	}
	if err := c.casStep(nil, FailOutcomeBeforeCAS, FailOutcomeAfterCAS, func() error {
		_, err := c.Store.RecordOutcome(job.JobID, job.AttemptID, job.Epoch, OutcomeFailed)
		return err
	}); err != nil {
		return err
	}
	refreshed, _ := c.Store.GetJob(job.JobID)
	if err := c.retireSupervisor(refreshed, nil); err != nil {
		return err
	}
	if err := c.casStep(nil, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
		_, err := c.Store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, OutcomeFailed, ProofNeverPermittedAndRetired)
		return err
	}); err != nil {
		return err
	}
	c.Store.jobs[job.JobID].TerminalReason = reasonPrefix + "_before_launch"
	return nil
}

func (c *Coordinator) publishContainedStartupTerminal(job Aggregate, reason string) (bool, error) {
	if !job.Contained || !job.Retired || !terminalOutcome(job.Outcome) {
		return false, nil
	}
	if err := c.casStep(nil, FailTerminalBeforeCAS, FailTerminalAfterCAS, func() error {
		_, err := c.Store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, job.Outcome, ProofContained)
		return err
	}); err != nil {
		return true, err
	}
	c.Store.jobs[job.JobID].TerminalReason = reason
	return true, nil
}

func (c *Coordinator) hasCommittedObligationFor(job Aggregate) bool {
	obligation, ok := c.obligations[job.JobID]
	return ok &&
		obligation.committed() &&
		obligation.Mode == job.Mode &&
		obligation.LaunchSpec == job.LaunchSpec
}

func (c *Coordinator) requireLegacyFencedPrepared(req SubmitRequest) error {
	if req.Mode != ModeLegacyFenced {
		return nil
	}
	if req.PreparedSupervisor == nil || !req.PreparedSupervisor.Valid() {
		return protocolError(CodePreconditionFailed, req.JobID, "legacy fenced admission requires prepared supervisor")
	}
	obligation, ok := c.obligations[req.JobID]
	if !ok || obligation.PreparedSupervisor == nil || !obligation.PreparedSupervisor.Valid() {
		return protocolError(CodePreconditionFailed, req.JobID, "legacy fenced admission requires pending retirement obligation")
	}
	if obligation.Retired {
		return protocolError(CodePreconditionFailed, req.JobID, "legacy fenced prepared supervisor was already retired")
	}
	if !sameGroupRef(*obligation.PreparedSupervisor, *req.PreparedSupervisor) {
		return protocolError(CodePreconditionFailed, req.JobID, "legacy fenced prepared supervisor does not match retirement obligation")
	}
	return nil
}

func sameGroupRef(a, b GroupRef) bool {
	if a.PGID != b.PGID ||
		a.LeaderPID != b.LeaderPID ||
		a.HighResStartToken != b.HighResStartToken ||
		a.PlatformRetainedID != b.PlatformRetainedID ||
		len(a.KnownChildRefs) != len(b.KnownChildRefs) {
		return false
	}
	for i := range a.KnownChildRefs {
		if a.KnownChildRefs[i] != b.KnownChildRefs[i] {
			return false
		}
	}
	return true
}

func (c *Coordinator) withCurrentBootOrphanReconcile(fn func() error) error {
	c.allowCurrentBootOrphanReconcile = true
	defer func() {
		c.allowCurrentBootOrphanReconcile = false
	}()
	return fn()
}

func (c *Coordinator) beginFailStop() {
	c.FailStopping = true
	c.LifecycleState = CoordinatorLifecycleFailStopped
}

func (c *Coordinator) rejectIfFailStopped() error {
	if c.FailStopping || c.LifecycleState == CoordinatorLifecycleFailStopped {
		return protocolError(CodePreconditionFailed, "", "coordinator is fail-stopped; create a new boot coordinator and run StartupReconcile")
	}
	return nil
}

func (c *Coordinator) rejectIfNotRunning() error {
	if err := c.rejectIfFailStopped(); err != nil {
		return err
	}
	if c.LifecycleState != CoordinatorLifecycleRunning && c.storeIsPristine() {
		if err := c.StartupReconcile(); err != nil {
			return err
		}
	}
	if c.LifecycleState != CoordinatorLifecycleRunning {
		return protocolError(CodePreconditionFailed, "", "coordinator is not ready; run StartupReconcile")
	}
	return nil
}

func (c *Coordinator) reconcilePostPermitError(jobID string, postPermit bool, err error) error {
	if err == nil {
		return nil
	}
	if !postPermit {
		return err
	}
	if recErr := c.LiveSupervisorLossWithInjector(jobID, nil); recErr != nil {
		return c.failStopAfterReconciliationError(jobID, fmt.Errorf("%w; post-permit reconciliation: %v", err, recErr))
	}
	return c.checkBoundary(err)
}

func (c *Coordinator) failStopAfterReconciliationError(jobID string, err error) error {
	if err == nil {
		return nil
	}
	job, ok := c.Store.GetJob(jobID)
	if !ok || !job.Terminal() {
		c.beginFailStop()
	}
	return c.checkBoundary(err)
}

func (c *Coordinator) storeIsPristine() bool {
	return c.Store != nil && len(c.Store.jobs) == 0 && len(c.Store.bindings) == 0 && len(c.Store.tombstones) == 0
}

func (c *Coordinator) publishResult(jobID, attemptID string, epoch int64, result ResultRef, injector *FailureInjector) error {
	if err := c.casStep(injector, FailResultPublicationBeforeCAS, FailResultPublicationAfterCAS, func() error {
		_, err := c.Store.BeginResultPublication(jobID, result.Path, result.Digest, result.Bytes)
		return err
	}); err != nil {
		return err
	}
	if err := c.sideEffectStep(injector, FailResultTempWriteBefore, FailResultTempWriteAfter, func() error {
		return c.Store.RecordResultTempWritten(result.Path, result.Digest, result.Bytes)
	}); err != nil {
		return err
	}
	if err := c.sideEffectStep(injector, FailResultFsyncTempBefore, FailResultFsyncTempAfter, func() error {
		return c.Store.RecordResultTempSynced(result.Path)
	}); err != nil {
		return err
	}
	if err := c.sideEffectStep(injector, FailResultCloseBefore, FailResultCloseAfter, func() error {
		return c.Store.RecordResultClosed(result.Path)
	}); err != nil {
		return err
	}
	if err := c.sideEffectStep(injector, FailResultRenameBefore, FailResultRenameAfter, func() error {
		return c.Store.RecordResultRenamed(result.Path)
	}); err != nil {
		return err
	}
	return c.sideEffectStep(injector, FailResultDirFsyncBefore, FailResultDirFsyncAfter, func() error {
		return c.Store.RecordResultDirSynced(result.Path)
	})
}

func (c *Coordinator) contain(jobID string, injector *FailureInjector, detail string) error {
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := c.sideEffectStep(injector, FailContainmentSignalBefore, FailContainmentSignalAfter, func() error {
		return nil
	}); err != nil {
		return err
	}
	if _, err := c.Store.RecordContainmentSignaled(jobID, job.AttemptID, job.Epoch, Evidence{Kind: "containment_signal", Detail: detail}); err != nil {
		return c.checkBoundary(err)
	}
	if err := c.sideEffectStep(injector, FailContainmentVerifyBefore, FailContainmentVerifyAfter, func() error {
		return nil
	}); err != nil {
		return err
	}
	if _, err := c.Store.RecordContainmentVerified(jobID, job.AttemptID, job.Epoch, Evidence{Kind: "verified_group_empty", Detail: detail}); err != nil {
		return c.checkBoundary(err)
	}
	return c.casStep(injector, FailContainmentRecordBeforeCAS, FailContainmentRecordAfterCAS, func() error {
		_, err := c.Store.RecordContained(jobID, job.AttemptID, job.Epoch, Evidence{Kind: "verified_absent", Detail: detail})
		return err
	})
}

func (c *Coordinator) retireSupervisor(job Aggregate, injector *FailureInjector) error {
	if job.Retired {
		return c.checkBoundary(nil)
	}
	if err := c.sideEffectStep(injector, FailRetirementCloseBefore, FailRetirementCloseAfter, func() error {
		return nil
	}); err != nil {
		return err
	}
	if _, err := c.Store.RecordRetirementStarted(job.JobID, job.AttemptID, job.Epoch, Evidence{Kind: "control_closed", Detail: "retirement"}); err != nil {
		return c.checkBoundary(err)
	}
	if err := c.sideEffectStep(injector, FailRetirementWaitBefore, FailRetirementWaitAfter, func() error {
		return nil
	}); err != nil {
		return err
	}
	if _, err := c.Store.RecordRetirementWorkerExited(job.JobID, job.AttemptID, job.Epoch, Evidence{Kind: "worker_exit", Detail: "retirement"}); err != nil {
		return c.checkBoundary(err)
	}
	if err := c.sideEffectStep(injector, FailRetirementVerifyBefore, FailRetirementVerifyAfter, func() error {
		return nil
	}); err != nil {
		return err
	}
	return c.casStep(injector, FailRetirementRecordBeforeCAS, FailRetirementRecordAfterCAS, func() error {
		_, err := c.Store.RecordRetirementGroupEmpty(job.JobID, job.AttemptID, job.Epoch, Evidence{Kind: "group_empty", Detail: "retirement"})
		return err
	})
}

func (c *Coordinator) retirePendingPrepared(jobID string, injector *FailureInjector) error {
	obligation, ok := c.obligations[jobID]
	if !ok || obligation.PreparedSupervisor == nil {
		return nil
	}
	if obligation.Retired && obligation.PreparedRetirement.Verified() {
		return nil
	}
	if err := c.sideEffectStep(injector, FailRetirementCloseBefore, FailRetirementCloseAfter, func() error {
		obligation.PreparedRetirement.ControlClosed = Evidence{Kind: "control_closed", Detail: "prepared supervisor retirement"}
		c.obligations[jobID] = obligation
		return nil
	}); err != nil {
		return err
	}
	obligation = c.obligations[jobID]
	if err := c.sideEffectStep(injector, FailRetirementWaitBefore, FailRetirementWaitAfter, func() error {
		obligation.PreparedRetirement.WorkerExited = Evidence{Kind: "worker_exit", Detail: "prepared supervisor retirement"}
		c.obligations[jobID] = obligation
		return nil
	}); err != nil {
		return err
	}
	obligation = c.obligations[jobID]
	if err := c.sideEffectStep(injector, FailRetirementVerifyBefore, FailRetirementVerifyAfter, func() error {
		obligation.PreparedRetirement.GroupEmpty = Evidence{Kind: "group_empty", Detail: "prepared supervisor retirement"}
		obligation.Retired = true
		c.obligations[jobID] = obligation
		c.preparedRetirements[jobID] = obligation.PreparedRetirement
		return nil
	}); err != nil {
		return err
	}
	obligation = c.obligations[jobID]
	if !obligation.PreparedRetirement.Verified() {
		return protocolError(CodePreconditionFailed, jobID, "prepared supervisor retirement evidence is incomplete")
	}
	obligation.Retired = true
	c.obligations[jobID] = obligation
	c.preparedRetirements[jobID] = obligation.PreparedRetirement
	return nil
}

func (c *Coordinator) setObligationState(jobID string, state ObligationState) {
	obligation := c.obligations[jobID]
	obligation.State = state
	obligation.Committed = state == ObligationCommitted || state == ObligationRunnable
	c.obligations[jobID] = obligation
}

func (c *Coordinator) setStartPhase(jobID, phase string) {
	if c.Store == nil {
		return
	}
	job, ok := c.Store.jobs[jobID]
	if !ok || job == nil {
		return
	}
	job.StartPhase = phase
}

func (c *Coordinator) casStep(injector *FailureInjector, before, after Failpoint, op func() error) error {
	if err := c.inject(injector, before); err != nil {
		return err
	}
	if err := op(); err != nil {
		return c.checkBoundary(err)
	}
	if err := c.inject(injector, after); err != nil {
		return err
	}
	return c.checkBoundary(nil)
}

func (c *Coordinator) StoreOp(after Failpoint, injector *FailureInjector, op func() error) error {
	if err := op(); err != nil {
		return c.checkBoundary(err)
	}
	if err := c.inject(injector, after); err != nil {
		return err
	}
	return c.checkBoundary(nil)
}

func (c *Coordinator) sideEffectStep(injector *FailureInjector, before, after Failpoint, op func() error) error {
	if err := c.inject(injector, before); err != nil {
		return err
	}
	c.Store.noteModeledSideEffect()
	if err := op(); err != nil {
		return c.checkBoundary(err)
	}
	if err := c.inject(injector, after); err != nil {
		return err
	}
	return c.checkBoundary(nil)
}

func (c *Coordinator) injectPreRunnable(injector *FailureInjector, point Failpoint) error {
	if injector == nil {
		return nil
	}
	c.allowPreRunnable = true
	defer func() {
		c.allowPreRunnable = false
	}()
	if err := injector.Fail(point); err != nil {
		return c.checkBoundary(err)
	}
	return c.checkBoundary(nil)
}

func (c *Coordinator) inject(injector *FailureInjector, point Failpoint) error {
	if injector == nil {
		return nil
	}
	if err := injector.Fail(point); err != nil {
		return c.checkBoundary(err)
	}
	return nil
}

func (c *Coordinator) checkBoundary(err error) error {
	if invErr := c.Check(); invErr != nil {
		if err != nil {
			return fmt.Errorf("%w; invariant boundary: %v", err, invErr)
		}
		return invErr
	}
	return err
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
