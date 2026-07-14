package execution

import "fmt"

type InvariantView struct {
	Store         *MemoryAdmissionStore
	Obligations   map[string]CoordinatorObligation
	FailStopping  bool
	CurrentBootID string
}

func CheckInvariants(view InvariantView) error {
	if view.Store == nil {
		return nil
	}
	store := view.Store
	if store.replaySideEffects != 0 {
		return fmt.Errorf("replay has execution side effects: %d", store.replaySideEffects)
	}
	if store.silentRecreated {
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

	activeOrdinals := map[string]int{}
	for _, job := range store.jobs {
		if PublicProjection(job.Decision, job.Dispatch, job.Outcome) == "" {
			return fmt.Errorf("job %s has no public projection", job.JobID)
		}
		if job.Decision == DecisionAwaitingAck && (job.PermitState == PermitGranted || job.PermitMaybeSent || job.Dispatch == DispatchPermitGranted || job.Dispatch == DispatchActive) {
			return fmt.Errorf("job %s awaiting acknowledgement has permit state", job.JobID)
		}
		if view.CurrentBootID != "" && job.BootID == view.CurrentBootID && !job.Terminal() && job.Mode != ModeLegacyUnfenced && !view.FailStopping {
			obligation, ok := view.Obligations[job.JobID]
			if !ok || !obligation.Committed {
				return fmt.Errorf("current-boot nonterminal job %s has no committed obligation", job.JobID)
			}
		}
		if (job.PermitState == PermitGranted || job.PermitMaybeSent || job.Dispatch == DispatchPermitGranted || job.Dispatch == DispatchActive) && !job.Supervisor.Valid() {
			return fmt.Errorf("job %s has permit without durable supervisor identity", job.JobID)
		}
		if job.PermitMaybeSent && job.ContainmentRequired && job.Terminal() && job.TerminalProof != ProofContained {
			return fmt.Errorf("job %s terminalized execution-uncertain state without containment", job.JobID)
		}
		if job.PermitMaybeSent && !job.LaunchQuiescent[job.LaunchOrdinal] && !job.Contained && !job.ContainmentRequired && !job.Terminal() {
			return fmt.Errorf("job %s permit-maybe-sent is not marked containment-required", job.JobID)
		}
		if job.Terminal() && !validTerminalProof(job) {
			return fmt.Errorf("job %s has invalid terminal proof %q", job.JobID, job.TerminalProof)
		}
		if job.LossObserved {
			if !job.Terminal() && !view.FailStopping {
				return fmt.Errorf("job %s observed owner/supervisor loss but is not terminal", job.JobID)
			}
			if job.Terminal() && job.TerminalProof != ProofContained {
				return fmt.Errorf("job %s observed loss without containment proof", job.JobID)
			}
		}
		if job.LaunchOrdinal == 2 && !job.LaunchQuiescent[1] {
			return fmt.Errorf("job %s launch ordinal 2 without ordinal 1 quiescent", job.JobID)
		}
		if job.ActiveOrdinal != 0 {
			if prev := activeOrdinals[job.JobID]; prev != 0 {
				return fmt.Errorf("job %s has multiple active ordinals %d and %d", job.JobID, prev, job.ActiveOrdinal)
			}
			activeOrdinals[job.JobID] = job.ActiveOrdinal
		}
	}
	return nil
}

func validTerminalProof(job *Aggregate) bool {
	switch job.TerminalProof {
	case ProofNeverPermittedAndRetired:
		return !job.PermitMaybeSent && job.Retired && terminalOutcome(job.Outcome)
	case ProofCleanQuiescentOutcomeAndRetired:
		return job.Retired && !job.Contained && terminalOutcome(job.Outcome)
	case ProofContained:
		return job.Contained && job.Retired && terminalOutcome(job.Outcome)
	default:
		return false
	}
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
