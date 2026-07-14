package execution

import "fmt"

type StatusQuery struct {
	PublicStates []Public
	IncludeAll   bool
}

type SubmitRequest struct {
	Mode               Mode
	WorkspaceKey       string
	RequestID          string
	LaunchSpec         LaunchSpec
	Fingerprint        Fingerprint
	JobID              string
	BootID             string
	OwnerID            string
	AttemptID          string
	Epoch              int64
	PreparedSupervisor *GroupRef
	SessionID          string
}

type ResolveStatus string

const (
	ResolveAcceptedNew      ResolveStatus = "accepted_new"
	ResolveExisting         ResolveStatus = "existing"
	ResolveExpiredTombstone ResolveStatus = "expired_tombstone"
)

type ResolveResult struct {
	Status    ResolveStatus
	Job       Aggregate
	JobID     string
	Tombstone Tombstone
}

type AdmissionStore interface {
	ResolveOrAccept(SubmitRequest) (ResolveResult, error)
	GetJob(jobID string) (Aggregate, bool)
	ListJobs(StatusQuery) ([]Aggregate, error)
	FindSession(sessionID string) (Aggregate, bool)
	ListPriorBootNonterminal(currentBootID string) ([]Aggregate, error)
	Acknowledge(jobID, attemptID string, epoch int64) (Aggregate, error)
	RejectUnacknowledged(jobID, attemptID string, epoch int64) (Aggregate, error)
	RecordSupervisor(jobID, attemptID string, epoch int64, groupRef GroupRef) (Aggregate, error)
	GrantPermit(jobID, attemptID string, epoch int64, launchOrdinal int, nonce string) (Aggregate, error)
	RequestCancel(jobID string) (Aggregate, error)
	RecordStarted(jobID, attemptID string, epoch int64, launchOrdinal int, childRef ChildRef) (Aggregate, error)
	RecordLaunchQuiescent(jobID, attemptID string, epoch int64, launchOrdinal int) (Aggregate, error)
	BeginReconciliation(jobID, attemptID string, epoch int64) (Aggregate, error)
	RecordContained(jobID, attemptID string, epoch int64, evidence Evidence) (Aggregate, error)
	RecordOutcome(jobID, attemptID string, epoch int64, outcome Outcome) (Aggregate, error)
	BeginResultPublication(jobID, path, digest string, bytes int64) (Aggregate, error)
	PublishTerminal(jobID, attemptID string, epoch int64, terminal Outcome, proof TerminalProof) (Aggregate, error)
	Expire(workspaceKey, requestID string) (string, error)
}

type MemoryAdmissionStore struct {
	step              int64
	nextJob           int
	jobs              map[string]*Aggregate
	bindings          map[string]Binding
	tombstones        map[string]Tombstone
	acceptedKeys      map[string]string
	replaySideEffects int
	silentRecreated   bool
	fatal             bool
	mutating          bool
	sideEffectInCAS   bool
}

func NewMemoryAdmissionStore() *MemoryAdmissionStore {
	return &MemoryAdmissionStore{
		nextJob:      1,
		jobs:         map[string]*Aggregate{},
		bindings:     map[string]Binding{},
		tombstones:   map[string]Tombstone{},
		acceptedKeys: map[string]string{},
	}
}

func (s *MemoryAdmissionStore) AllocateJobID() string {
	id := fmt.Sprintf("job-%04d", s.nextJob)
	s.nextJob++
	return id
}

func (s *MemoryAdmissionStore) ResolveExisting(req SubmitRequest) (ResolveResult, bool, error) {
	if req.Mode == "" {
		req.Mode = ModeIdentifiedFenced
	}
	req.Fingerprint = req.Fingerprint.normalized()
	if err := validateWorkspaceRequest(req.WorkspaceKey, req.RequestID, req.Mode == ModeIdentifiedFenced); err != nil {
		return ResolveResult{}, false, err
	}
	if req.RequestID == "" {
		return ResolveResult{}, false, nil
	}
	key := bindingKey(req.WorkspaceKey, req.RequestID)
	if binding, ok := s.bindings[key]; ok {
		result, err := s.resolveBinding(binding, req)
		return result, true, err
	}
	if tombstone, ok := s.tombstones[key]; ok {
		result, err := s.resolveTombstone(tombstone, req)
		return result, true, err
	}
	return ResolveResult{}, false, nil
}

func (s *MemoryAdmissionStore) ResolveOrAccept(req SubmitRequest) (ResolveResult, error) {
	if req.Mode == ModeLegacyUnfenced {
		return ResolveResult{}, protocolError(CodeLegacyUnfenced, "", "legacy unfenced never enters AdmissionStore")
	}
	if req.Mode == "" {
		req.Mode = ModeIdentifiedFenced
	}
	req.Fingerprint = req.Fingerprint.normalized()
	req.LaunchSpec.Fingerprint = req.Fingerprint
	req.LaunchSpec.WorkspaceKey = req.WorkspaceKey
	req.LaunchSpec.RequestID = req.RequestID

	if err := validateWorkspaceRequest(req.WorkspaceKey, req.RequestID, req.Mode == ModeIdentifiedFenced); err != nil {
		return ResolveResult{}, err
	}
	if req.RequestID != "" {
		key := bindingKey(req.WorkspaceKey, req.RequestID)
		if binding, ok := s.bindings[key]; ok {
			return s.resolveBinding(binding, req)
		}
		if tombstone, ok := s.tombstones[key]; ok {
			return s.resolveTombstone(tombstone, req)
		}
	}
	if !req.Fingerprint.supported() {
		return ResolveResult{}, protocolError(CodeRequestFingerprintUnsupported, "", "new request fingerprint is unsupported")
	}
	if req.JobID == "" {
		req.JobID = s.AllocateJobID()
	}
	if req.BootID == "" {
		req.BootID = "boot-1"
	}
	if req.OwnerID == "" {
		req.OwnerID = "owner-1"
	}
	if req.AttemptID == "" {
		req.AttemptID = "attempt-1"
	}
	if req.Epoch == 0 {
		req.Epoch = 1
	}

	s.mutating = true
	defer func() { s.mutating = false }()
	s.step++
	decision := DecisionAccepted
	dispatch := DispatchScheduled
	acknowledged := true
	var supervisor GroupRef
	if req.Mode == ModeLegacyFenced {
		decision = DecisionAwaitingAck
		dispatch = DispatchNone
		acknowledged = false
		if req.PreparedSupervisor != nil {
			supervisor = *req.PreparedSupervisor
			dispatch = DispatchSupervisorPrepared
		}
	}
	aggregate := &Aggregate{
		JobID:           req.JobID,
		WorkspaceKey:    req.WorkspaceKey,
		RequestID:       req.RequestID,
		Fingerprint:     req.Fingerprint,
		Mode:            req.Mode,
		LaunchSpec:      req.LaunchSpec,
		BootID:          req.BootID,
		OwnerID:         req.OwnerID,
		AttemptID:       req.AttemptID,
		Epoch:           req.Epoch,
		Decision:        decision,
		Dispatch:        dispatch,
		Outcome:         OutcomeNone,
		Acknowledged:    acknowledged,
		PermitState:     PermitNone,
		Supervisor:      supervisor,
		SessionID:       req.SessionID,
		CreatedStep:     s.step,
		UpdatedStep:     s.step,
		LaunchQuiescent: map[int]bool{},
	}
	s.jobs[aggregate.JobID] = aggregate
	if req.RequestID != "" {
		key := bindingKey(req.WorkspaceKey, req.RequestID)
		binding := Binding{
			WorkspaceKey: req.WorkspaceKey,
			RequestID:    req.RequestID,
			JobID:        aggregate.JobID,
			Fingerprint:  req.Fingerprint,
		}
		s.bindings[key] = binding
		if existing, ok := s.acceptedKeys[key]; ok && existing != aggregate.JobID {
			return ResolveResult{}, protocolError(CodePreconditionFailed, aggregate.JobID, "request key was already accepted")
		}
		s.acceptedKeys[key] = aggregate.JobID
	}
	return ResolveResult{Status: ResolveAcceptedNew, Job: aggregate.copy(), JobID: aggregate.JobID}, nil
}

func (s *MemoryAdmissionStore) resolveBinding(binding Binding, req SubmitRequest) (ResolveResult, error) {
	if !binding.Fingerprint.supported() {
		return ResolveResult{}, protocolError(CodeRequestFingerprintUnsupported, binding.JobID, "recorded fingerprint is unsupported")
	}
	if !binding.Fingerprint.Equal(req.Fingerprint) {
		return ResolveResult{}, protocolError(CodeRequestConflict, binding.JobID, "fingerprint mismatch")
	}
	job, ok := s.jobs[binding.JobID]
	if !ok {
		return ResolveResult{}, protocolError(CodePreconditionFailed, binding.JobID, "binding references missing job")
	}
	return ResolveResult{Status: ResolveExisting, Job: job.copy(), JobID: binding.JobID}, nil
}

func (s *MemoryAdmissionStore) resolveTombstone(tombstone Tombstone, req SubmitRequest) (ResolveResult, error) {
	if !tombstone.Fingerprint.supported() {
		return ResolveResult{}, protocolError(CodeRequestFingerprintUnsupported, tombstone.JobID, "recorded tombstone fingerprint is unsupported")
	}
	if !tombstone.Fingerprint.Equal(req.Fingerprint) {
		return ResolveResult{}, protocolError(CodeRequestConflict, tombstone.JobID, "fingerprint mismatch")
	}
	return ResolveResult{Status: ResolveExpiredTombstone, JobID: tombstone.JobID, Tombstone: tombstone}, protocolError(CodeRequestExpired, tombstone.JobID, "job was expired")
}

func (s *MemoryAdmissionStore) GetJob(jobID string) (Aggregate, bool) {
	job, ok := s.jobs[jobID]
	if !ok {
		return Aggregate{}, false
	}
	return job.copy(), true
}

func (s *MemoryAdmissionStore) ListJobs(query StatusQuery) ([]Aggregate, error) {
	wanted := map[Public]bool{}
	for _, state := range query.PublicStates {
		wanted[state] = true
	}
	var out []Aggregate
	for _, job := range s.jobs {
		if len(wanted) != 0 && !wanted[job.Public()] {
			continue
		}
		if !query.IncludeAll && job.Terminal() {
			continue
		}
		out = append(out, job.copy())
	}
	return out, nil
}

func (s *MemoryAdmissionStore) FindSession(sessionID string) (Aggregate, bool) {
	for _, job := range s.jobs {
		if job.SessionID == sessionID && sessionID != "" {
			return job.copy(), true
		}
	}
	return Aggregate{}, false
}

func (s *MemoryAdmissionStore) ListPriorBootNonterminal(currentBootID string) ([]Aggregate, error) {
	var out []Aggregate
	for _, job := range s.jobs {
		if job.BootID != currentBootID && !job.Terminal() {
			out = append(out, job.copy())
		}
	}
	return out, nil
}

func (s *MemoryAdmissionStore) Acknowledge(jobID, attemptID string, epoch int64) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.Decision != DecisionAwaitingAck {
			return protocolError(CodePreconditionFailed, job.JobID, "job is not awaiting acknowledgement")
		}
		if job.PermitState != PermitNone || job.PermitMaybeSent {
			return protocolError(CodePreconditionFailed, job.JobID, "permit exists before acknowledgement")
		}
		job.Decision = DecisionAccepted
		job.Acknowledged = true
		if job.Supervisor.Valid() {
			job.Dispatch = DispatchSupervisorPrepared
		} else {
			job.Dispatch = DispatchScheduled
		}
		return nil
	})
}

func (s *MemoryAdmissionStore) RejectUnacknowledged(jobID, attemptID string, epoch int64) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.Decision != DecisionAwaitingAck {
			return protocolError(CodePreconditionFailed, job.JobID, "job is not awaiting acknowledgement")
		}
		if job.PermitState != PermitNone || job.PermitMaybeSent {
			return protocolError(CodePreconditionFailed, job.JobID, "permit exists before rejection")
		}
		job.Decision = DecisionTerminal
		job.Dispatch = DispatchDone
		job.Outcome = OutcomeCanceled
		job.TerminalReason = "response_undeliverable"
		job.TerminalProof = ProofNeverPermittedAndRetired
		job.Retired = true
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordSupervisor(jobID, attemptID string, epoch int64, groupRef GroupRef) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !groupRef.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "invalid group identity")
		}
		if job.Decision == DecisionAwaitingAck {
			return protocolError(CodePreconditionFailed, job.JobID, "awaiting acknowledgement")
		}
		job.Supervisor = groupRef
		job.Dispatch = DispatchSupervisorPrepared
		job.Retired = false
		return nil
	})
}

func (s *MemoryAdmissionStore) GrantPermit(jobID, attemptID string, epoch int64, launchOrdinal int, nonce string) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		job.ensureMaps()
		if job.Decision == DecisionAwaitingAck {
			return protocolError(CodePreconditionFailed, job.JobID, "awaiting acknowledgement")
		}
		if job.Decision == DecisionCancelRequested {
			return protocolError(CodePreconditionFailed, job.JobID, "cancel won before permit")
		}
		if !job.Supervisor.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "supervisor identity is not durable")
		}
		if launchOrdinal != 1 && launchOrdinal != 2 {
			return protocolError(CodePreconditionFailed, job.JobID, "invalid launch ordinal")
		}
		if launchOrdinal == 2 && !job.LaunchQuiescent[1] {
			return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal 1 is not quiescent")
		}
		if job.ActiveOrdinal != 0 || job.PermitState != PermitNone {
			return protocolError(CodePreconditionFailed, job.JobID, "an ordinal is already active")
		}
		job.PermitState = PermitGranted
		job.PermitNonce = nonce
		job.PermitMaybeSent = true
		job.ContainmentRequired = true
		job.LaunchOrdinal = launchOrdinal
		job.ActiveOrdinal = launchOrdinal
		job.Retired = false
		job.Dispatch = DispatchPermitGranted
		return nil
	})
}

func (s *MemoryAdmissionStore) RequestCancel(jobID string) (Aggregate, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return Aggregate{}, protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if job.Terminal() {
		return job.copy(), nil
	}
	s.mutating = true
	defer func() { s.mutating = false }()
	s.step++
	job.UpdatedStep = s.step
	if job.Outcome != OutcomeNone {
		return job.copy(), nil
	}
	job.Decision = DecisionCancelRequested
	if job.PermitState == PermitNone && !job.PermitMaybeSent {
		job.PermitState = PermitCanceled
	}
	if job.PermitMaybeSent {
		job.ContainmentRequired = true
	}
	return job.copy(), nil
}

func (s *MemoryAdmissionStore) RecordStarted(jobID, attemptID string, epoch int64, launchOrdinal int, childRef ChildRef) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.PermitState != PermitGranted || job.LaunchOrdinal != launchOrdinal {
			return protocolError(CodePreconditionFailed, job.JobID, "matching permit is required")
		}
		if !childRef.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "invalid child identity")
		}
		job.Child = childRef
		job.Dispatch = DispatchActive
		job.ExecutionSideEffects++
		job.ActiveOrdinal = launchOrdinal
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordLaunchQuiescent(jobID, attemptID string, epoch int64, launchOrdinal int) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		job.ensureMaps()
		if launchOrdinal != job.LaunchOrdinal {
			return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal mismatch")
		}
		job.LaunchQuiescent[launchOrdinal] = true
		if job.ActiveOrdinal == launchOrdinal {
			job.ActiveOrdinal = 0
		}
		job.PermitState = PermitNone
		job.ContainmentRequired = false
		job.Retired = true
		if job.Outcome == OutcomeNone {
			job.Dispatch = DispatchSupervisorPrepared
		}
		return nil
	})
}

func (s *MemoryAdmissionStore) BeginReconciliation(jobID, attemptID string, epoch int64) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		job.Dispatch = DispatchReconciling
		job.LossObserved = true
		job.ContainmentRequired = true
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordContained(jobID, attemptID string, epoch int64, evidence Evidence) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		job.Dispatch = DispatchContained
		job.Contained = true
		job.Containment = evidence
		job.ContainmentRequired = false
		job.Retired = true
		job.ActiveOrdinal = 0
		job.PermitState = PermitNone
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordOutcome(jobID, attemptID string, epoch int64, outcome Outcome) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !terminalOutcome(outcome) {
			return protocolError(CodePreconditionFailed, job.JobID, "terminal outcome required")
		}
		if job.Outcome != OutcomeNone && job.Outcome != outcome {
			return protocolError(CodePreconditionFailed, job.JobID, "outcome already recorded")
		}
		job.Outcome = outcome
		if !job.PermitMaybeSent {
			job.Retired = true
		}
		return nil
	})
}

func (s *MemoryAdmissionStore) BeginResultPublication(jobID, path, digest string, bytes int64) (Aggregate, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return Aggregate{}, protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if job.Terminal() {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "terminal job")
	}
	if job.Outcome != OutcomeCompleted && job.Outcome != OutcomeCompletedNoncompliant {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "completed outcome required")
	}
	s.mutating = true
	defer func() { s.mutating = false }()
	s.step++
	job.UpdatedStep = s.step
	job.Dispatch = DispatchResultPublishing
	job.Result = ResultRef{Path: path, Digest: digest, Bytes: bytes}
	return job.copy(), nil
}

func (s *MemoryAdmissionStore) PublishTerminal(jobID, attemptID string, epoch int64, terminal Outcome, proof TerminalProof) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !terminalOutcome(terminal) {
			return protocolError(CodePreconditionFailed, job.JobID, "terminal outcome required")
		}
		if job.Outcome == OutcomeNone {
			job.Outcome = terminal
		}
		if job.Outcome != terminal {
			return protocolError(CodePreconditionFailed, job.JobID, "terminal outcome mismatch")
		}
		if proof == ProofNeverPermittedAndRetired && job.PermitMaybeSent {
			return protocolError(CodePreconditionFailed, job.JobID, "permit may have been observed")
		}
		if proof == ProofContained && !job.Contained {
			return protocolError(CodePreconditionFailed, job.JobID, "containment proof missing")
		}
		if proof == ProofCleanQuiescentOutcomeAndRetired && !job.Retired {
			return protocolError(CodePreconditionFailed, job.JobID, "supervisor is not retired")
		}
		if proof == ProofNone {
			return protocolError(CodePreconditionFailed, job.JobID, "terminal proof required")
		}
		if proof == ProofNeverPermittedAndRetired {
			job.Retired = true
		}
		job.TerminalProof = proof
		job.Decision = DecisionTerminal
		job.Dispatch = DispatchDone
		job.ContainmentRequired = false
		job.ActiveOrdinal = 0
		return nil
	})
}

func (s *MemoryAdmissionStore) Expire(workspaceKey, requestID string) (string, error) {
	key := bindingKey(workspaceKey, requestID)
	binding, ok := s.bindings[key]
	if !ok {
		return "", protocolError(CodeUnknownJob, "", "binding not found")
	}
	s.mutating = true
	defer func() { s.mutating = false }()
	s.step++
	delete(s.jobs, binding.JobID)
	delete(s.bindings, key)
	tombstone := Tombstone{
		WorkspaceKey: binding.WorkspaceKey,
		RequestID:    binding.RequestID,
		JobID:        binding.JobID,
		Fingerprint:  binding.Fingerprint,
		ExpiredStep:  s.step,
	}
	s.tombstones[key] = tombstone
	s.acceptedKeys[key] = binding.JobID
	return binding.JobID, nil
}

func (s *MemoryAdmissionStore) MarkCorrupt(jobID string, permitMaybe, identityTrustworthy bool, diagnostic string) (Aggregate, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return Aggregate{}, protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if permitMaybe && !identityTrustworthy {
		s.fatal = true
		return Aggregate{}, protocolError(CodeCorruptFatal, jobID, "containment identity is untrustworthy")
	}
	s.mutating = true
	defer func() { s.mutating = false }()
	s.step++
	job.Corrupt = true
	job.QuarantineDiagnostic = diagnostic
	job.Outcome = OutcomeQuarantined
	if permitMaybe {
		job.PermitMaybeSent = true
		job.Contained = true
		job.Retired = true
		job.TerminalProof = ProofContained
	} else {
		job.PermitMaybeSent = false
		job.Retired = true
		job.TerminalProof = ProofNeverPermittedAndRetired
	}
	job.Decision = DecisionTerminal
	job.Dispatch = DispatchDone
	job.UpdatedStep = s.step
	return job.copy(), nil
}

func (s *MemoryAdmissionStore) mutate(jobID, attemptID string, epoch int64, fn func(*Aggregate) error) (Aggregate, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return Aggregate{}, protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if job.Terminal() {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "terminal job")
	}
	if job.AttemptID != attemptID || job.Epoch != epoch {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "attempt precondition failed")
	}
	s.mutating = true
	defer func() { s.mutating = false }()
	if err := fn(job); err != nil {
		return Aggregate{}, err
	}
	s.step++
	job.UpdatedStep = s.step
	return job.copy(), nil
}

func (s *MemoryAdmissionStore) noteModeledSideEffect() {
	if s.mutating {
		s.sideEffectInCAS = true
	}
}
