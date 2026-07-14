package execution

import (
	"fmt"
	"strings"
)

type InvariantView struct {
	Store                           *MemoryAdmissionStore
	Obligations                     map[string]CoordinatorObligation
	FailStopping                    bool
	CurrentBootID                   string
	LifecycleState                  CoordinatorLifecycleState
	AllowPreRunnable                bool
	AllowCurrentBootOrphanReconcile bool
}

func CheckInvariants(view InvariantView) error {
	if view.Store == nil {
		return nil
	}
	lifecycleState := view.LifecycleState
	if lifecycleState == "" {
		lifecycleState = CoordinatorLifecycleRunning
	}
	switch lifecycleState {
	case CoordinatorLifecycleNotReady, CoordinatorLifecycleReconciling, CoordinatorLifecycleRunning, CoordinatorLifecycleFailStopped:
	default:
		return fmt.Errorf("coordinator has invalid lifecycle state %q", lifecycleState)
	}
	if lifecycleState == CoordinatorLifecycleNotReady && !view.FailStopping && len(view.Obligations) != 0 {
		return fmt.Errorf("not-ready coordinator has service obligations")
	}
	store := view.Store
	if store.replaySideEffects != 0 {
		return fmt.Errorf("replay has execution side effects: %d", store.replaySideEffects)
	}
	if store.silentRecreated && !view.FailStopping && lifecycleState != CoordinatorLifecycleFailStopped {
		return fmt.Errorf("initialized AdmissionStore was silently recreated")
	}

	for key, binding := range store.bindings {
		if binding.WorkspaceKey == "" || binding.RequestID == "" || binding.JobID == "" {
			return fmt.Errorf("binding %q is incomplete", key)
		}
		if _, ok := store.tombstones[key]; ok {
			return fmt.Errorf("binding %q also has tombstone", key)
		}
		if accepted := store.acceptedKeys[key]; accepted != "" && accepted != binding.JobID {
			return fmt.Errorf("request key %q reaccepted as %s after %s", key, binding.JobID, accepted)
		}
	}
	for key, tombstone := range store.tombstones {
		if tombstone.WorkspaceKey == "" || tombstone.RequestID == "" || tombstone.JobID == "" {
			return fmt.Errorf("tombstone %q is incomplete", key)
		}
		if accepted := store.acceptedKeys[key]; accepted != "" && accepted != tombstone.JobID {
			return fmt.Errorf("request key %q tombstone changed job from %s to %s", key, accepted, tombstone.JobID)
		}
	}
	for jobID, obligation := range view.Obligations {
		if !obligation.committed() {
			continue
		}
		job, ok := store.jobs[jobID]
		if !ok {
			return fmt.Errorf("live obligation %s references missing aggregate", jobID)
		}
		if job.Terminal() {
			return fmt.Errorf("live obligation %s references terminal aggregate", jobID)
		}
		for key, tombstone := range store.tombstones {
			if tombstone.JobID == jobID {
				return fmt.Errorf("tombstone %q has live obligation %s", key, jobID)
			}
		}
	}

	for _, job := range store.jobs {
		job.ensureMaps()
		if err := validateAggregateEnums(job); err != nil {
			return err
		}
		if PublicProjection(job.Decision, job.Dispatch, job.Outcome) == "" {
			return fmt.Errorf("job %s has no public projection", job.JobID)
		}
		if job.Mode != ModeLegacyUnfenced {
			if err := validateLaunchSpec(job.LaunchSpec, job.Mode); err != nil {
				return fmt.Errorf("job %s has invalid immutable launch spec: %w", job.JobID, err)
			}
			if job.LaunchSpec.WorkspaceKey != job.WorkspaceKey || job.LaunchSpec.RequestID != job.RequestID || !job.LaunchSpec.Fingerprint.Equal(job.Fingerprint) {
				return fmt.Errorf("job %s aggregate launch spec does not match aggregate identity", job.JobID)
			}
		}
		if job.Decision == DecisionAwaitingAck && (job.PermitState == PermitGranted || job.PermitMaybeSent || job.Dispatch == DispatchPermitGranted || job.Dispatch == DispatchActive) {
			return fmt.Errorf("job %s awaiting acknowledgement has permit state", job.JobID)
		}
		if job.Decision == DecisionAwaitingAck && (!job.Supervisor.Valid() || job.Dispatch == DispatchNone) {
			return fmt.Errorf("job %s awaiting acknowledgement without prepared supervisor", job.JobID)
		}
		if view.CurrentBootID != "" && job.BootID == view.CurrentBootID && !job.Terminal() && job.Mode != ModeLegacyUnfenced && !view.FailStopping {
			obligation, ok := view.Obligations[job.JobID]
			if !ok || !obligation.committed() {
				if view.AllowCurrentBootOrphanReconcile {
					continue
				}
				return fmt.Errorf("current-boot nonterminal job %s has no committed obligation", job.JobID)
			}
			if !validObligationState(obligation.state()) {
				return fmt.Errorf("job %s obligation has invalid state %q", job.JobID, obligation.state())
			}
			if obligation.Mode != job.Mode {
				return fmt.Errorf("job %s obligation mode %s does not match aggregate mode %s", job.JobID, obligation.Mode, job.Mode)
			}
			if obligation.LaunchSpec != job.LaunchSpec {
				return fmt.Errorf("job %s obligation launch spec does not match aggregate launch spec", job.JobID)
			}
			if !obligation.runnable() && !view.FailStopping && !view.AllowPreRunnable {
				return fmt.Errorf("current-boot nonterminal job %s has no runnable obligation", job.JobID)
			}
		}
		if (job.PermitState == PermitGranted || job.PermitState == PermitMaybeSent || job.PermitState == PermitConsumed || job.PermitMaybeSent || job.Dispatch == DispatchPermitGranted || job.Dispatch == DispatchActive) && !job.Supervisor.Valid() {
			return fmt.Errorf("job %s has permit without durable supervisor identity", job.JobID)
		}
		if job.Dispatch == DispatchScheduled && job.Supervisor.Valid() {
			return fmt.Errorf("job %s has scheduled dispatch with durable supervisor identity", job.JobID)
		}
		if err := validateParkedExecIdentity(job); err != nil {
			return err
		}
		if job.PermitMaybeSent && job.ContainmentRequired && job.Terminal() && job.TerminalProof != ProofContained {
			return fmt.Errorf("job %s terminalized execution-uncertain state without containment", job.JobID)
		}
		if job.PermitMaybeSent && !job.LaunchQuiescent[job.LaunchOrdinal] && !job.Contained && !job.ContainmentRequired && !job.Terminal() {
			return fmt.Errorf("job %s permit-maybe-sent is not marked containment-required", job.JobID)
		}
		for ordinal, quiescent := range job.LaunchQuiescent {
			if !quiescent {
				continue
			}
			evidence := job.LaunchEvidence[ordinal]
			if !evidence.ChildExited.Present() || !evidence.GroupEmpty.Present() {
				return fmt.Errorf("job %s launch ordinal %d quiescent without child-exit and group-empty evidence", job.JobID, ordinal)
			}
		}
		if err := validateLaunchNonceHistory(job); err != nil {
			return err
		}
		for ordinal, count := range job.LiveOrdinals {
			if count < 0 {
				return fmt.Errorf("job %s launch ordinal %d has negative live authority count", job.JobID, ordinal)
			}
			if count > 1 {
				return fmt.Errorf("job %s launch ordinal %d has %d live authorities", job.JobID, ordinal, count)
			}
			if count > 0 && job.LaunchQuiescent[ordinal] {
				return fmt.Errorf("job %s launch ordinal %d has live authority after quiescence", job.JobID, ordinal)
			}
		}
		if liveOrdinalCount(job.LiveOrdinals) > 1 {
			return fmt.Errorf("job %s has more than one live execution authority", job.JobID)
		}
		if job.ActiveOrdinal != 0 && job.LiveOrdinals[job.ActiveOrdinal] != 1 {
			return fmt.Errorf("job %s active ordinal %d is not represented in live authority set", job.JobID, job.ActiveOrdinal)
		}
		if job.ActiveOrdinal != 0 && job.LaunchQuiescent[job.ActiveOrdinal] {
			return fmt.Errorf("job %s active ordinal %d was already quiescent", job.JobID, job.ActiveOrdinal)
		}
		if job.Terminal() && !validTerminalProof(job) {
			return fmt.Errorf("job %s has invalid terminal proof %q", job.JobID, job.TerminalProof)
		}
		if job.Terminal() {
			if !validTerminalOutcomeProofReason(job) {
				return fmt.Errorf("job %s has invalid terminal outcome/proof/reason combination", job.JobID)
			}
			if job.PermitState == PermitGranted || job.PermitState == PermitMaybeSent || job.PermitState == PermitConsumed {
				return fmt.Errorf("job %s is terminal with live permit state %s", job.JobID, job.PermitState)
			}
			if job.ActiveOrdinal != 0 || liveOrdinalCount(job.LiveOrdinals) != 0 {
				return fmt.Errorf("job %s is terminal with live ordinal authority", job.JobID)
			}
			if job.Child.Valid() || job.PendingChild.Valid() {
				return fmt.Errorf("job %s is terminal with child identity still live", job.JobID)
			}
			if !job.Retired {
				return fmt.Errorf("job %s is terminal with unretired supervisor", job.JobID)
			}
			if (job.Outcome == OutcomeCompleted || job.Outcome == OutcomeCompletedNoncompliant) && !store.resultDurable(job.Result) {
				return fmt.Errorf("job %s completed terminal references unverified result digest/bytes", job.JobID)
			}
		}
		if job.Retired && !job.Contained && !job.RetirementEvidence.Present() {
			return fmt.Errorf("job %s is retired without retirement evidence", job.JobID)
		}
		if job.Contained && (!job.ContainmentSignaled || !job.ContainmentVerified || !job.Containment.Present()) {
			return fmt.Errorf("job %s is contained without containment signal/verification evidence", job.JobID)
		}
		if job.LossObserved {
			if !job.Terminal() && !view.FailStopping {
				reconciling := job.Dispatch == DispatchReconciling && job.ContainmentRequired
				containedPendingTerminal := job.Dispatch == DispatchContained && job.Contained && job.Retired
				if !reconciling && !containedPendingTerminal {
					return fmt.Errorf("job %s observed loss without live containment obligation", job.JobID)
				}
			}
			if job.Terminal() && job.TerminalProof != ProofContained {
				return fmt.Errorf("job %s observed loss without containment proof", job.JobID)
			}
		}
		if job.LaunchOrdinal == 2 && !job.LaunchQuiescent[1] {
			return fmt.Errorf("job %s launch ordinal 2 without ordinal 1 quiescent", job.JobID)
		}
	}
	return nil
}

func validateParkedExecIdentity(job *Aggregate) error {
	if job.Supervisor.Valid() {
		for _, child := range job.Supervisor.KnownChildRefs {
			if !child.Valid() {
				return fmt.Errorf("job %s supervisor has invalid known child identity", job.JobID)
			}
			if sameChildAsSupervisorLeader(child, job.Supervisor) {
				return fmt.Errorf("job %s tracks parked exec leader as a separate child", job.JobID)
			}
		}
	}
	for _, child := range []ChildRef{job.PendingChild, job.Child} {
		if !child.Valid() {
			continue
		}
		if !job.Supervisor.Valid() {
			return fmt.Errorf("job %s backend child has no durable supervisor identity", job.JobID)
		}
		if !sameChildAsSupervisorLeader(child, job.Supervisor) {
			return fmt.Errorf("job %s backend child identity diverges from parked exec supervisor leader", job.JobID)
		}
	}
	return nil
}

func sameChildAsSupervisorLeader(child ChildRef, supervisor GroupRef) bool {
	return child.PID == supervisor.LeaderPID && child.HighResStartToken == supervisor.HighResStartToken
}

func validObligationState(state ObligationState) bool {
	switch state {
	case ObligationPending, ObligationCommitted, ObligationRunnable:
		return true
	default:
		return false
	}
}

func validateLaunchNonceHistory(job *Aggregate) error {
	if len(job.LaunchNonceHistory) > 2 {
		return fmt.Errorf("job %s has more than two launch nonce history entries", job.JobID)
	}
	seenNonces := map[string]int{}
	for ordinal, nonce := range job.LaunchNonceHistory {
		if ordinal != 1 && ordinal != 2 {
			return fmt.Errorf("job %s has invalid launch nonce history ordinal %d", job.JobID, ordinal)
		}
		if previous, ok := seenNonces[nonce]; ok {
			return fmt.Errorf("job %s launch nonce reused by ordinals %d and %d", job.JobID, previous, ordinal)
		}
		seenNonces[nonce] = ordinal
	}
	_, used1 := job.LaunchNonceHistory[1]
	_, used2 := job.LaunchNonceHistory[2]
	if used2 {
		if !used1 {
			return fmt.Errorf("job %s launch ordinal 2 history without ordinal 1 history", job.JobID)
		}
		if !job.LaunchQuiescent[1] {
			return fmt.Errorf("job %s launch ordinal 2 history without ordinal 1 quiescent", job.JobID)
		}
	}
	for ordinal, quiescent := range job.LaunchQuiescent {
		if !quiescent {
			continue
		}
		if ordinal != 1 && ordinal != 2 {
			return fmt.Errorf("job %s has invalid quiescent launch ordinal %d", job.JobID, ordinal)
		}
		if _, used := job.LaunchNonceHistory[ordinal]; !used {
			return fmt.Errorf("job %s launch ordinal %d quiescent without nonce history", job.JobID, ordinal)
		}
	}
	if job.LaunchOrdinal != 0 {
		if job.LaunchOrdinal != 1 && job.LaunchOrdinal != 2 {
			return fmt.Errorf("job %s has invalid current launch ordinal %d", job.JobID, job.LaunchOrdinal)
		}
		if _, used := job.LaunchNonceHistory[job.LaunchOrdinal]; !used {
			return fmt.Errorf("job %s current launch ordinal %d has no nonce history", job.JobID, job.LaunchOrdinal)
		}
	}
	if job.PermitState == PermitGranted || job.PermitState == PermitMaybeSent || job.PermitState == PermitConsumed {
		nonce, used := job.LaunchNonceHistory[job.LaunchOrdinal]
		if !used || nonce != job.PermitNonce {
			return fmt.Errorf("job %s live launch permit nonce does not match durable history", job.JobID)
		}
		if job.LaunchQuiescent[job.LaunchOrdinal] {
			return fmt.Errorf("job %s live launch ordinal %d was already quiescent", job.JobID, job.LaunchOrdinal)
		}
	}
	return nil
}

func validTerminalProof(job *Aggregate) bool {
	switch job.TerminalProof {
	case ProofNeverPermittedAndRetired:
		return !hasLiveAuthority(job) && !job.PermitMaybeSent && job.Retired && job.RetirementEvidence.Present() && terminalOutcome(job.Outcome)
	case ProofCleanQuiescentOutcomeAndRetired:
		if !job.Retired || job.Contained || !terminalOutcome(job.Outcome) || hasLiveAuthority(job) {
			return false
		}
		if job.LaunchOrdinal == 0 || !job.LaunchQuiescent[job.LaunchOrdinal] {
			return false
		}
		evidence := job.LaunchEvidence[job.LaunchOrdinal]
		if !evidence.ChildExited.Present() || !evidence.GroupEmpty.Present() {
			return false
		}
		return job.RetirementEvidence.Present()
	case ProofContained:
		return job.Contained && job.Retired && job.ContainmentSignaled && job.ContainmentVerified && job.Containment.Present() && terminalOutcome(job.Outcome)
	default:
		return false
	}
}

func validTerminalOutcomeProofReason(job *Aggregate) bool {
	if job.TerminalReason == "" {
		return false
	}
	switch job.TerminalProof {
	case ProofNeverPermittedAndRetired:
		switch job.Outcome {
		case OutcomeCanceled:
			return job.TerminalReason == "response_undeliverable" || job.TerminalReason == "canceled_before_permit"
		case OutcomeFailed:
			return strings.HasSuffix(job.TerminalReason, "_before_launch")
		case OutcomeQuarantined:
			return true
		default:
			return false
		}
	case ProofCleanQuiescentOutcomeAndRetired:
		return terminalOutcome(job.Outcome) && job.TerminalReason == string(job.Outcome)
	case ProofContained:
		switch job.Outcome {
		case OutcomeCanceled:
			return job.TerminalReason == "canceled_after_permit"
		case OutcomeReaped:
			return job.TerminalReason == "daemon_restarted" || strings.HasSuffix(job.TerminalReason, "_contained")
		case OutcomeQuarantined:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func (s *MemoryAdmissionStore) resultDurable(result ResultRef) bool {
	if result.Path == "" {
		return false
	}
	artifact, ok := s.resultArtifacts[result.Path]
	return ok &&
		artifact.DirSynced &&
		artifact.Digest == result.Digest &&
		artifact.Bytes == result.Bytes
}

func ResultCleanupEligible(job Aggregate, obligations map[string]CoordinatorObligation, tempName string) bool {
	if job.Result.Path == tempName {
		return false
	}
	if _, ok := obligations[job.JobID]; ok {
		return false
	}
	if job.Dispatch == DispatchResultPublishing {
		return false
	}
	if !job.Terminal() {
		return false
	}
	return job.Result.Path == ""
}
