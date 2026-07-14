package execution

import "testing"

func TestNegativeOracleRejectsForbiddenStates(t *testing.T) {
	tests := []struct {
		name  string
		build func(*testing.T) InvariantView
	}{
		{name: "terminal with live permit", build: forbiddenTerminalWithLivePermit},
		{name: "two launches same ordinal", build: forbiddenTwoLaunchesSameOrdinal},
		{name: "ordinal 2 before quiescence", build: forbiddenOrdinal2BeforeQuiescence},
		{name: "terminal without containment when permit maybe sent", build: forbiddenTerminalWithoutContainment},
		{name: "mark corrupt over live permit", build: forbiddenCorruptLivePermit},
		{name: "obligation launch spec mismatch", build: forbiddenObligationLaunchSpecMismatch},
		{name: "accepted invalid empty launch spec", build: forbiddenAcceptedInvalidLaunchSpec},
		{name: "completed terminal with unverified result digest", build: forbiddenCompletedUnverifiedResult},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if err := CheckInvariants(tt.build(t)); err == nil {
				t.Fatalf("CheckInvariants returned nil for forbidden state %q", tt.name)
			}
		})
	}
}

func forbiddenTerminalWithLivePermit(t *testing.T) InvariantView {
	t.Helper()
	c, jobID := acceptedPrepared(t, "terminal-live")
	job := c.Store.jobs[jobID]
	job.Decision = DecisionTerminal
	job.Dispatch = DispatchDone
	job.Outcome = OutcomeFailed
	job.PermitState = PermitGranted
	job.ActiveOrdinal = 1
	job.LiveOrdinals[1] = 1
	job.TerminalProof = ProofNeverPermittedAndRetired
	job.Retired = true
	job.RetirementEvidence = Evidence{Kind: "group_empty", Detail: "forbidden"}
	delete(c.obligations, jobID)
	return invariantView(c)
}

func forbiddenTwoLaunchesSameOrdinal(t *testing.T) InvariantView {
	t.Helper()
	c, jobID := acceptedPrepared(t, "double-launch")
	job := c.Store.jobs[jobID]
	job.PermitState = PermitConsumed
	job.PermitMaybeSent = true
	job.ContainmentRequired = true
	job.LaunchOrdinal = 1
	job.ActiveOrdinal = 1
	job.LiveOrdinals[1] = 2
	return invariantView(c)
}

func forbiddenOrdinal2BeforeQuiescence(t *testing.T) InvariantView {
	t.Helper()
	c, jobID := acceptedPrepared(t, "ordinal-two")
	job := c.Store.jobs[jobID]
	job.PermitState = PermitGranted
	job.PermitMaybeSent = true
	job.ContainmentRequired = true
	job.LaunchOrdinal = 2
	job.ActiveOrdinal = 2
	job.LiveOrdinals[2] = 1
	return invariantView(c)
}

func forbiddenTerminalWithoutContainment(t *testing.T) InvariantView {
	t.Helper()
	c, jobID := acceptedPrepared(t, "terminal-no-containment")
	job := c.Store.jobs[jobID]
	job.Decision = DecisionTerminal
	job.Dispatch = DispatchDone
	job.Outcome = OutcomeReaped
	job.PermitState = PermitNone
	job.PermitMaybeSent = true
	job.ContainmentRequired = true
	job.TerminalProof = ProofCleanQuiescentOutcomeAndRetired
	job.Retired = true
	job.RetirementEvidence = Evidence{Kind: "group_empty", Detail: "forbidden"}
	delete(c.obligations, jobID)
	return invariantView(c)
}

func forbiddenCorruptLivePermit(t *testing.T) InvariantView {
	t.Helper()
	c, jobID := acceptedPrepared(t, "corrupt-live")
	job := c.Store.jobs[jobID]
	job.Decision = DecisionTerminal
	job.Dispatch = DispatchDone
	job.Outcome = OutcomeQuarantined
	job.Corrupt = true
	job.PermitState = PermitGranted
	job.PermitMaybeSent = true
	job.ActiveOrdinal = 1
	job.LiveOrdinals[1] = 1
	job.TerminalProof = ProofContained
	job.Contained = true
	job.ContainmentSignaled = true
	job.ContainmentVerified = true
	job.Containment = Evidence{Kind: "verified_absent", Detail: "forbidden"}
	job.Retired = true
	job.RetirementEvidence = Evidence{Kind: "verified_absent", Detail: "forbidden"}
	delete(c.obligations, jobID)
	return invariantView(c)
}

func forbiddenObligationLaunchSpecMismatch(t *testing.T) InvariantView {
	t.Helper()
	c, jobID := acceptedPrepared(t, "obligation-mismatch")
	obligation := c.obligations[jobID]
	obligation.LaunchSpec.Task = "different task"
	c.obligations[jobID] = obligation
	return invariantView(c)
}

func forbiddenAcceptedInvalidLaunchSpec(t *testing.T) InvariantView {
	t.Helper()
	c, jobID := acceptedPrepared(t, "invalid-launch")
	job := c.Store.jobs[jobID]
	job.LaunchSpec.Task = ""
	obligation := c.obligations[jobID]
	obligation.LaunchSpec = job.LaunchSpec
	c.obligations[jobID] = obligation
	return invariantView(c)
}

func forbiddenCompletedUnverifiedResult(t *testing.T) InvariantView {
	t.Helper()
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-negative-result", "owner")
	res := submitPreparedPermitted(t, c, "ws-negative-result", "req-negative-result", "fp")
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(res.JobID, OutcomeCompleted); err != nil {
		t.Fatal(err)
	}
	job := c.Store.jobs[res.JobID]
	delete(c.Store.resultArtifacts, job.Result.Path)
	return invariantView(c)
}

func acceptedPrepared(t *testing.T, suffix string) (*Coordinator, string) {
	t.Helper()
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-negative-"+suffix, "owner")
	res, err := c.Submit(modelRequest("ws-negative-"+suffix, "req-negative-"+suffix, "fp"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	return c, res.JobID
}

func invariantView(c *Coordinator) InvariantView {
	return InvariantView{
		Store:         c.Store,
		Obligations:   c.Obligations(),
		FailStopping:  c.FailStopping,
		CurrentBootID: c.BootID,
	}
}
