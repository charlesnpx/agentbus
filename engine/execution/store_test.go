package execution

import "testing"

func TestResolveReplayTombstoneAndFingerprintOrdering(t *testing.T) {
	store := NewMemoryAdmissionStore()
	req := modelRequest("ws-replay", "req-1", "fp-1")
	accepted, err := store.ResolveOrAccept(req)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Status != ResolveAcceptedNew {
		t.Fatalf("status = %s, want accepted_new", accepted.Status)
	}
	replay, err := store.ResolveOrAccept(req)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != ResolveExisting || replay.JobID != accepted.JobID {
		t.Fatalf("replay = (%s,%s), want existing %s", replay.Status, replay.JobID, accepted.JobID)
	}
	if replay.Job.ExecutionSideEffects != 0 {
		t.Fatalf("replay execution side effects = %d", replay.Job.ExecutionSideEffects)
	}

	conflict := req
	conflict.Fingerprint = CurrentFingerprint("different")
	if _, err := store.ResolveOrAccept(conflict); !IsCode(err, CodeRequestConflict) {
		t.Fatalf("conflict err = %v, want request_conflict", err)
	}

	expiredID, err := store.Expire(req.WorkspaceKey, req.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if expiredID != accepted.JobID {
		t.Fatalf("expired jobID = %s, want %s", expiredID, accepted.JobID)
	}
	if result, err := store.ResolveOrAccept(req); !IsCode(err, CodeRequestExpired) || result.JobID != accepted.JobID {
		t.Fatalf("expired replay = (%v,%v), want request_expired for %s", result, err, accepted.JobID)
	}
	if _, err := store.ResolveOrAccept(conflict); !IsCode(err, CodeRequestConflict) {
		t.Fatalf("tombstone conflict err = %v, want request_conflict", err)
	}
}

func TestRecordedUnknownFingerprintFailsClosed(t *testing.T) {
	store := NewMemoryAdmissionStore()
	req := modelRequest("ws-fp", "req-unknown", "fp-1")
	accepted, err := store.ResolveOrAccept(req)
	if err != nil {
		t.Fatal(err)
	}
	key := bindingKey(req.WorkspaceKey, req.RequestID)
	binding := store.bindings[key]
	binding.Fingerprint.Algorithm = "future"
	store.bindings[key] = binding

	if _, err := store.ResolveOrAccept(req); !IsCode(err, CodeRequestFingerprintUnsupported) {
		t.Fatalf("err = %v, want request_fingerprint_unsupported", err)
	}
	if len(store.jobs) != 1 {
		t.Fatalf("jobs = %d, want original only", len(store.jobs))
	}
	if got, ok := store.GetJob(accepted.JobID); !ok || got.JobID != accepted.JobID {
		t.Fatalf("original job missing after unsupported replay")
	}
}

func TestLegacyFencedAcknowledgementGatesPermit(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-ack", "owner")
	res, err := c.SubmitLegacyFenced(modelRequest("ws-legacy", "", "fp-legacy"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.GrantPermit(res.JobID, 1, "nonce", nil); !IsCode(err, CodePreconditionFailed) {
		t.Fatalf("grant before ack err = %v, want precondition failure", err)
	}
	if err := c.Acknowledge(res.JobID); err != nil {
		t.Fatal(err)
	}
	if err := c.GrantPermit(res.JobID, 1, "nonce", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyUnfencedNeverEntersAdmissionStore(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-unfenced", "owner")
	req := modelRequest("ws-unfenced", "", "fp-unfenced")
	req.Mode = ModeLegacyUnfenced
	if _, err := c.Submit(req, nil); !IsCode(err, CodeLegacyUnfenced) {
		t.Fatalf("err = %v, want legacy_unfenced", err)
	}
	if len(c.Store.jobs) != 0 || len(c.Store.bindings) != 0 {
		t.Fatalf("legacy unfenced entered store: jobs %d bindings %d", len(c.Store.jobs), len(c.Store.bindings))
	}
}

func TestCorruptAggregateHandlingPreservesBinding(t *testing.T) {
	store := NewMemoryAdmissionStore()
	req := modelRequest("ws-corrupt", "req-corrupt", "fp-corrupt")
	res, err := store.ResolveOrAccept(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkCorrupt(res.JobID, false, true, "checksum mismatch"); err != nil {
		t.Fatal(err)
	}
	if err := CheckInvariants(InvariantView{Store: store}); err != nil {
		t.Fatal(err)
	}
	key := bindingKey(req.WorkspaceKey, req.RequestID)
	if binding, ok := store.bindings[key]; !ok || binding.JobID != res.JobID {
		t.Fatalf("binding = (%+v,%v), want preserved for %s", binding, ok, res.JobID)
	}
	replay, err := store.ResolveOrAccept(req)
	if err != nil {
		t.Fatal(err)
	}
	if replay.JobID != res.JobID || replay.Job.Outcome != OutcomeQuarantined {
		t.Fatalf("replay = %+v, want quarantined original %s", replay, res.JobID)
	}
	conflict := req
	conflict.Fingerprint = CurrentFingerprint("different")
	if _, err := store.ResolveOrAccept(conflict); !IsCode(err, CodeRequestConflict) {
		t.Fatalf("err = %v, want request_conflict", err)
	}

	fatalStore := NewMemoryAdmissionStore()
	fatalReq := modelRequest("ws-corrupt", "req-corrupt-fatal", "fp-corrupt-fatal")
	fatalRes, err := fatalStore.ResolveOrAccept(fatalReq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fatalStore.MarkCorrupt(fatalRes.JobID, true, false, "identity missing"); !IsCode(err, CodeCorruptFatal) {
		t.Fatalf("err = %v, want corrupt_fatal", err)
	}
	if !fatalStore.fatal {
		t.Fatalf("fatal store flag was not set")
	}
}

func TestAnchorStartupDecisionTable(t *testing.T) {
	tests := []struct {
		name string
		in   AnchorInput
		want StartupAction
	}{
		{name: "first initialization", in: AnchorInput{}, want: StartupInitializeFirst},
		{name: "recover interrupted init", in: AnchorInput{DBPresent: true, DBValid: true}, want: StartupRecoverAnchor},
		{name: "missing db after init", in: AnchorInput{AnchorPresent: true, AnchorValid: true}, want: StartupFatal},
		{name: "uuid mismatch", in: AnchorInput{DBPresent: true, AnchorPresent: true, DBValid: true, AnchorValid: true, DBUUID: "a", AnchorDBUUID: "b"}, want: StartupFatal},
		{name: "db rollback", in: AnchorInput{DBPresent: true, AnchorPresent: true, DBValid: true, AnchorValid: true, DBUUID: "a", AnchorDBUUID: "a", DBGeneration: 1, HighWaterGeneration: 2}, want: StartupFatal},
		{name: "advance lagging anchor", in: AnchorInput{DBPresent: true, AnchorPresent: true, DBValid: true, AnchorValid: true, DBUUID: "a", AnchorDBUUID: "a", DBGeneration: 3, HighWaterGeneration: 2}, want: StartupAdvanceAnchor},
		{name: "continue", in: AnchorInput{DBPresent: true, AnchorPresent: true, DBValid: true, AnchorValid: true, DBUUID: "a", AnchorDBUUID: "a", DBGeneration: 2, HighWaterGeneration: 2}, want: StartupContinue},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := DecideStartupAnchor(tt.in)
			if got.Action != tt.want {
				t.Fatalf("action = %s (%s), want %s", got.Action, got.Reason, tt.want)
			}
		})
	}
}

func modelRequest(workspaceKey, requestID, fp string) SubmitRequest {
	return SubmitRequest{
		Mode:         ModeIdentifiedFenced,
		WorkspaceKey: workspaceKey,
		RequestID:    requestID,
		Fingerprint:  CurrentFingerprint(fp),
		LaunchSpec: LaunchSpec{
			WorkspaceKey: workspaceKey,
			RequestID:    requestID,
			Backend:      "codex",
			Task:         "model task",
		},
		SessionID: "session-" + requestID,
	}
}
