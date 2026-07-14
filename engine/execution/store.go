package execution

import (
	"fmt"
	"reflect"
)

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
	RecordOutcome(jobID, attemptID string, epoch int64, outcome Outcome) (Aggregate, error)
	BeginResultPublication(jobID, path, digest string, bytes int64) (Aggregate, error)
	PublishTerminal(jobID, attemptID string, epoch int64, terminal Outcome, proof TerminalProof, reason string) (Aggregate, error)
	Expire(workspaceKey, requestID string) (string, error)
}

type MemoryAdmissionStore struct {
	step              int64
	nextJob           int
	allocatedJobIDs   map[string]bool
	jobs              map[string]*Aggregate
	bindings          map[string]Binding
	tombstones        map[string]Tombstone
	resultArtifacts   map[string]ResultArtifact
	acceptedKeys      map[string]string
	attempts          map[string]*AttemptAuthority
	sideEffectLedger  int
	replaySideEffects int
	replayEvents      []string
	silentRecreated   bool
	fatal             bool
	mutating          bool
	sideEffectInCAS   bool

	startupAnchorObserved             bool
	startupAnchorInput                AnchorInput
	startupAnchorDecision             StartupDecision
	startupAnchorDispositionPersisted bool
	startupAnchorCompleted            bool
	anchorState                       AnchorState
	anchorInitState                   AnchorInitState
	directBoundaryView                InvariantView
	directBoundaryViewSet             bool
}

func NewMemoryAdmissionStore() *MemoryAdmissionStore {
	return &MemoryAdmissionStore{
		nextJob:         1,
		allocatedJobIDs: map[string]bool{},
		jobs:            map[string]*Aggregate{},
		bindings:        map[string]Binding{},
		tombstones:      map[string]Tombstone{},
		resultArtifacts: map[string]ResultArtifact{},
		acceptedKeys:    map[string]string{},
		attempts:        map[string]*AttemptAuthority{},
	}
}

func (s *MemoryAdmissionStore) AllocateJobID() string {
	if s.allocatedJobIDs == nil {
		s.allocatedJobIDs = map[string]bool{}
	}
	id := fmt.Sprintf("job-%04d", s.nextJob)
	s.nextJob++
	s.allocatedJobIDs[id] = true
	return id
}

func (s *MemoryAdmissionStore) ResolveExisting(req SubmitRequest) (ResolveResult, bool, error) {
	if req.Mode == "" {
		req.Mode = ModeIdentifiedFenced
	}
	if !validMode(req.Mode) {
		return ResolveResult{}, false, protocolError(CodeRejected, "", "unknown execution mode")
	}
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
	if !validMode(req.Mode) {
		return ResolveResult{}, protocolError(CodeRejected, "", "unknown execution mode")
	}

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
	fingerprint, err := CurrentRequestFingerprint(req)
	if err != nil {
		return ResolveResult{}, err
	}
	req.Fingerprint = fingerprint
	spec, err := materializeLaunchSpec(req, req.Fingerprint)
	if err != nil {
		return ResolveResult{}, err
	}
	req.LaunchSpec = spec
	if req.JobID == "" {
		req.JobID = s.AllocateJobID()
	} else if !s.allocatedJobIDs[req.JobID] {
		return ResolveResult{}, protocolError(CodePreconditionFailed, req.JobID, "job id is allocated by admission authority")
	} else if _, exists := s.jobs[req.JobID]; exists {
		return ResolveResult{}, protocolError(CodePreconditionFailed, req.JobID, "job id collision")
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
		if req.PreparedSupervisor == nil || !req.PreparedSupervisor.Valid() {
			return ResolveResult{}, protocolError(CodePreconditionFailed, req.JobID, "legacy fenced admission requires prepared supervisor")
		}
		decision = DecisionAwaitingAck
		dispatch = DispatchSupervisorPrepared
		acknowledged = false
		supervisor = *req.PreparedSupervisor
	}
	aggregate := &Aggregate{
		JobID:              req.JobID,
		WorkspaceKey:       req.WorkspaceKey,
		RequestID:          req.RequestID,
		Fingerprint:        req.Fingerprint,
		Mode:               req.Mode,
		LaunchSpec:         req.LaunchSpec,
		BootID:             req.BootID,
		OwnerID:            req.OwnerID,
		AttemptID:          req.AttemptID,
		Epoch:              req.Epoch,
		Decision:           decision,
		Dispatch:           dispatch,
		Outcome:            OutcomeNone,
		Acknowledged:       acknowledged,
		PermitState:        PermitNone,
		Supervisor:         supervisor,
		SessionID:          req.SessionID,
		CreatedStep:        s.step,
		UpdatedStep:        s.step,
		LaunchQuiescent:    map[int]bool{},
		LaunchEvidence:     map[int]LaunchQuiescenceEvidence{},
		LaunchNonceHistory: map[int]string{},
		LiveOrdinals:       map[int]int{},
	}
	if aggregate.SessionID == "" {
		aggregate.SessionID = aggregate.LaunchSpec.SessionID
	}
	s.jobs[aggregate.JobID] = aggregate
	authority := &AttemptAuthority{Supervisor: supervisor, GrantNonceHistory: map[int]string{}}
	s.attempts[aggregate.JobID] = authority
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
	if err := s.checkDirectBoundary(nil); err != nil {
		return ResolveResult{}, err
	}
	return ResolveResult{Status: ResolveAcceptedNew, Job: aggregate.copy(), JobID: aggregate.JobID}, nil
}

func (s *MemoryAdmissionStore) resolveBinding(binding Binding, req SubmitRequest) (result ResolveResult, err error) {
	before := s.executionAuthoritySnapshot()
	defer func() {
		err = s.finishReplayObservation(before, "binding:"+binding.JobID, err)
	}()
	if !binding.Fingerprint.supported() {
		return ResolveResult{}, protocolError(CodeRequestFingerprintUnsupported, binding.JobID, "recorded fingerprint is unsupported")
	}
	replayFingerprint, err := FingerprintRequest(binding.Fingerprint.Algorithm, binding.Fingerprint.Version, req)
	if err != nil {
		return ResolveResult{}, protocolError(CodeRequestFingerprintUnsupported, binding.JobID, "recorded fingerprint is unsupported")
	}
	if !binding.Fingerprint.Equal(replayFingerprint) {
		return ResolveResult{}, protocolError(CodeRequestConflict, binding.JobID, "fingerprint mismatch")
	}
	job, ok := s.jobs[binding.JobID]
	if !ok {
		return ResolveResult{}, protocolError(CodePreconditionFailed, binding.JobID, "binding references missing job")
	}
	if err := validateBindingAggregate(binding, job); err != nil {
		return ResolveResult{}, err
	}
	return ResolveResult{Status: ResolveExisting, Job: job.copy(), JobID: binding.JobID}, nil
}

func (s *MemoryAdmissionStore) resolveTombstone(tombstone Tombstone, req SubmitRequest) (result ResolveResult, err error) {
	before := s.executionAuthoritySnapshot()
	defer func() {
		err = s.finishReplayObservation(before, "tombstone:"+tombstone.JobID, err)
	}()
	if !tombstone.Fingerprint.supported() {
		return ResolveResult{}, protocolError(CodeRequestFingerprintUnsupported, tombstone.JobID, "recorded tombstone fingerprint is unsupported")
	}
	replayFingerprint, err := FingerprintRequest(tombstone.Fingerprint.Algorithm, tombstone.Fingerprint.Version, req)
	if err != nil {
		return ResolveResult{}, protocolError(CodeRequestFingerprintUnsupported, tombstone.JobID, "recorded tombstone fingerprint is unsupported")
	}
	if !tombstone.Fingerprint.Equal(replayFingerprint) {
		return ResolveResult{}, protocolError(CodeRequestConflict, tombstone.JobID, "fingerprint mismatch")
	}
	return ResolveResult{Status: ResolveExpiredTombstone, JobID: tombstone.JobID, Tombstone: tombstone}, protocolError(CodeRequestExpired, tombstone.JobID, "job was expired")
}

func validateBindingAggregate(binding Binding, job *Aggregate) error {
	if job == nil {
		return protocolError(CodeCorruptFatal, binding.JobID, "binding references missing aggregate")
	}
	if job.JobID != binding.JobID ||
		job.WorkspaceKey != binding.WorkspaceKey ||
		job.RequestID != binding.RequestID ||
		!job.Fingerprint.Equal(binding.Fingerprint) {
		return protocolError(CodeCorruptFatal, binding.JobID, "binding does not match aggregate identity")
	}
	return nil
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
		if job.TerminalizationStarted || job.RetirementStarted || job.Retired {
			return protocolError(CodePreconditionFailed, job.JobID, "job is already terminalizing")
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

func (s *MemoryAdmissionStore) BeginRejectUnacknowledged(jobID, attemptID string, epoch int64) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.TerminalizationStarted && job.Decision == DecisionCancelRequested {
			return nil
		}
		if job.Decision != DecisionAwaitingAck {
			return protocolError(CodePreconditionFailed, job.JobID, "job is not awaiting acknowledgement")
		}
		if job.PermitState != PermitNone || job.PermitMaybeSent || hasAnyGrantEvidence(job, s.attemptAuthority(job.JobID)) {
			return protocolError(CodePreconditionFailed, job.JobID, "permit exists before rejection")
		}
		if !job.Supervisor.Valid() || job.Retired || job.RetirementStarted {
			return protocolError(CodePreconditionFailed, job.JobID, "prepared supervisor is not live")
		}
		job.Decision = DecisionCancelRequested
		job.PermitState = PermitCanceled
		job.TerminalizationStarted = true
		return nil
	})
}

func (s *MemoryAdmissionStore) RejectUnacknowledged(jobID, attemptID string, epoch int64) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.Decision != DecisionCancelRequested || !job.TerminalizationStarted {
			return protocolError(CodePreconditionFailed, job.JobID, "job rejection was not durably started")
		}
		if job.PermitState != PermitCanceled || job.PermitMaybeSent || hasAnyGrantEvidence(job, s.attemptAuthority(job.JobID)) {
			return protocolError(CodePreconditionFailed, job.JobID, "permit exists before rejection")
		}
		if job.Supervisor.Valid() && !job.Retired {
			return protocolError(CodePreconditionFailed, job.JobID, "prepared supervisor must be synchronously retired before rejection")
		}
		job.Decision = DecisionTerminal
		job.Dispatch = DispatchDone
		job.Outcome = OutcomeCanceled
		job.TerminalReason = "response_undeliverable"
		job.TerminalProof = ProofNeverPermittedAndRetired
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordSupervisor(jobID, attemptID string, epoch int64, groupRef GroupRef) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !groupRef.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "invalid group identity")
		}
		if job.Supervisor.Valid() {
			if sameGroupRef(job.Supervisor, groupRef) {
				return nil
			}
			return protocolError(CodePreconditionFailed, job.JobID, "supervisor identity is already durable")
		}
		if job.Decision != DecisionAccepted {
			return protocolError(CodePreconditionFailed, job.JobID, "supervisor can only be recorded for an accepted job")
		}
		if job.Dispatch != DispatchScheduled {
			return protocolError(CodePreconditionFailed, job.JobID, "supervisor can only be recorded from scheduled dispatch")
		}
		if job.PermitState != PermitNone || job.PermitMaybeSent || hasLiveAuthority(job) {
			return protocolError(CodePreconditionFailed, job.JobID, "supervisor can only be recorded before permit authority exists")
		}
		job.Supervisor = groupRef
		job.Dispatch = DispatchSupervisorPrepared
		job.Retired = false
		authority := s.mutableAttemptAuthority(job.JobID)
		authority.Supervisor = groupRef
		return nil
	})
}

func (s *MemoryAdmissionStore) GrantPermit(jobID, attemptID string, epoch int64, launchOrdinal int, nonce string) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		job.ensureMaps()
		if job.Decision != DecisionAccepted {
			return protocolError(CodePreconditionFailed, job.JobID, "permit requires accepted decision")
		}
		if job.Outcome != OutcomeNone {
			return protocolError(CodePreconditionFailed, job.JobID, "permit requires no terminal outcome")
		}
		if job.Dispatch != DispatchSupervisorPrepared {
			return protocolError(CodePreconditionFailed, job.JobID, "permit requires prepared supervisor dispatch")
		}
		if !job.Supervisor.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "supervisor identity is not durable")
		}
		if job.Retired || job.RetirementStarted || job.TerminalizationStarted {
			return protocolError(CodePreconditionFailed, job.JobID, "supervisor is retiring or retired")
		}
		if launchOrdinal != 1 && launchOrdinal != 2 {
			return protocolError(CodePreconditionFailed, job.JobID, "invalid launch ordinal")
		}
		if launchOrdinal == 2 && !job.LaunchQuiescent[1] {
			return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal 1 is not quiescent")
		}
		if job.LaunchQuiescent[launchOrdinal] {
			return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal was already quiescent")
		}
		if _, used := job.LaunchNonceHistory[launchOrdinal]; used {
			return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal was already used")
		}
		if launchOrdinal == 1 && len(job.LaunchNonceHistory) != 0 {
			return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal 1 must be first")
		}
		if launchOrdinal == 2 {
			if len(job.LaunchNonceHistory) != 1 {
				return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal 2 requires exactly ordinal 1 history")
			}
			if _, used := job.LaunchNonceHistory[1]; !used {
				return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal 1 history is missing")
			}
		}
		for _, usedNonce := range job.LaunchNonceHistory {
			if usedNonce == nonce {
				return protocolError(CodePreconditionFailed, job.JobID, "launch nonce was already used")
			}
		}
		if job.ActiveOrdinal != 0 || liveOrdinalCount(job.LiveOrdinals) != 0 || job.PermitState != PermitNone {
			return protocolError(CodePreconditionFailed, job.JobID, "an ordinal is already active")
		}
		job.PermitState = PermitGranted
		job.PermitNonce = nonce
		job.PermitMaybeSent = true
		job.ContainmentRequired = true
		job.LaunchOrdinal = launchOrdinal
		job.ActiveOrdinal = launchOrdinal
		job.LaunchNonceHistory[launchOrdinal] = nonce
		job.LiveOrdinals[launchOrdinal] = 1
		job.Retired = false
		job.Dispatch = DispatchPermitGranted
		authority := s.mutableAttemptAuthority(job.JobID)
		if !authority.Supervisor.Valid() {
			authority.Supervisor = job.Supervisor
		}
		authority.GrantNonceHistory[launchOrdinal] = nonce
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordPermitMaybeSent(jobID, attemptID string, epoch int64, launchOrdinal int) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.PermitState != PermitGranted || job.LaunchOrdinal != launchOrdinal {
			return protocolError(CodePreconditionFailed, job.JobID, "granted matching permit is required before send")
		}
		job.PermitState = PermitMaybeSent
		job.Dispatch = DispatchPermitGranted
		s.mutableAttemptAuthority(job.JobID).PermitMaybeSent = true
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordExecForked(jobID, attemptID string, epoch int64) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.PermitState != PermitMaybeSent {
			return protocolError(CodePreconditionFailed, job.JobID, "maybe-sent permit required before fork")
		}
		job.StartPhase = "forked"
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordExeced(jobID, attemptID string, epoch int64) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.StartPhase != "forked" {
			return protocolError(CodePreconditionFailed, job.JobID, "fork must be recorded before exec")
		}
		job.StartPhase = "execed"
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordBackendStarted(jobID, attemptID string, epoch int64, childRef ChildRef) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.StartPhase != "execed" {
			return protocolError(CodePreconditionFailed, job.JobID, "exec must be recorded before backend started")
		}
		if !childRef.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "invalid pending child identity")
		}
		job.PendingChild = childRef
		job.StartPhase = "backend_started"
		return nil
	})
}

func (s *MemoryAdmissionStore) RequestCancel(jobID string) (Aggregate, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return Aggregate{}, protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if err := validateAggregateEnums(job); err != nil {
		return Aggregate{}, err
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
	if err := s.checkDirectBoundary(nil); err != nil {
		return Aggregate{}, err
	}
	if err := validateAggregateEnums(job); err != nil {
		return Aggregate{}, err
	}
	return job.copy(), nil
}

func (s *MemoryAdmissionStore) RecordStarted(jobID, attemptID string, epoch int64, launchOrdinal int, childRef ChildRef) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if job.PermitState != PermitMaybeSent || job.LaunchOrdinal != launchOrdinal {
			return protocolError(CodePreconditionFailed, job.JobID, "matching one-use maybe-sent permit is required")
		}
		if job.Child.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "launch already has a child identity")
		}
		if !childRef.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "invalid child identity")
		}
		if !job.PendingChild.Valid() || job.PendingChild != childRef {
			return protocolError(CodePreconditionFailed, job.JobID, "backend-started child evidence is required")
		}
		job.Child = childRef
		job.PendingChild = ChildRef{}
		job.PermitState = PermitConsumed
		job.Dispatch = DispatchActive
		job.ExecutionSideEffects++
		authority := s.mutableAttemptAuthority(job.JobID)
		authority.PermitConsumed = true
		authority.ExecutionSideCount++
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
		if job.PermitState != PermitConsumed {
			return protocolError(CodePreconditionFailed, job.JobID, "launch permit was not consumed")
		}
		evidence := job.LaunchEvidence[launchOrdinal]
		if !evidence.ChildExited.Present() || !evidence.GroupEmpty.Present() {
			return protocolError(CodePreconditionFailed, job.JobID, "durable child-exit and group-empty evidence required")
		}
		job.LaunchQuiescent[launchOrdinal] = true
		if job.ActiveOrdinal == launchOrdinal {
			job.ActiveOrdinal = 0
		}
		delete(job.LiveOrdinals, launchOrdinal)
		job.PermitState = PermitNone
		job.ContainmentRequired = false
		job.Child = ChildRef{}
		job.PendingChild = ChildRef{}
		if job.Outcome == OutcomeNone {
			job.Dispatch = DispatchSupervisorPrepared
		}
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordLaunchExitEvidence(jobID, attemptID string, epoch int64, launchOrdinal int, childExited, groupEmpty Evidence) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		job.ensureMaps()
		if launchOrdinal != job.LaunchOrdinal {
			return protocolError(CodePreconditionFailed, job.JobID, "launch ordinal mismatch")
		}
		if job.PermitState != PermitConsumed || !job.Child.Valid() {
			return protocolError(CodePreconditionFailed, job.JobID, "active consumed launch required")
		}
		if !childExited.Present() || !groupEmpty.Present() {
			return protocolError(CodePreconditionFailed, job.JobID, "complete launch-exit evidence required")
		}
		job.LaunchEvidence[launchOrdinal] = LaunchQuiescenceEvidence{ChildExited: childExited, GroupEmpty: groupEmpty}
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

func (s *MemoryAdmissionStore) RecordContainmentSignaled(jobID, attemptID string, epoch int64, evidence Evidence) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !evidence.Present() {
			return protocolError(CodePreconditionFailed, job.JobID, "containment signal evidence required")
		}
		job.ContainmentSignaled = true
		job.RetirementStarted = true
		job.RetirementControlClosed = true
		job.ContainmentRequired = true
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordContainmentVerified(jobID, attemptID string, epoch int64, evidence Evidence) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !job.ContainmentSignaled {
			return protocolError(CodePreconditionFailed, job.JobID, "containment must be signaled before verification")
		}
		if !evidence.Present() {
			return protocolError(CodePreconditionFailed, job.JobID, "containment verification evidence required")
		}
		job.ContainmentVerified = true
		job.RetirementWorkerExited = true
		job.RetirementGroupEmpty = true
		job.RetirementEvidence = evidence
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordContained(jobID, attemptID string, epoch int64, evidence Evidence) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !job.ContainmentSignaled || !job.ContainmentVerified {
			return protocolError(CodePreconditionFailed, job.JobID, "containment signal and verification required")
		}
		if !evidence.Present() {
			return protocolError(CodePreconditionFailed, job.JobID, "containment evidence required")
		}
		job.Dispatch = DispatchContained
		job.Contained = true
		job.Containment = evidence
		job.ContainmentRequired = false
		job.Retired = true
		job.RetirementStarted = true
		job.RetirementControlClosed = true
		job.RetirementWorkerExited = true
		job.RetirementGroupEmpty = true
		job.RetirementEvidence = evidence
		job.ActiveOrdinal = 0
		job.LiveOrdinals = map[int]int{}
		job.PermitState = PermitNone
		job.Child = ChildRef{}
		job.PendingChild = ChildRef{}
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordRetirementStarted(jobID, attemptID string, epoch int64, evidence Evidence) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !evidence.Present() {
			return protocolError(CodePreconditionFailed, job.JobID, "retirement start evidence required")
		}
		job.RetirementStarted = true
		job.RetirementControlClosed = true
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordRetirementWorkerExited(jobID, attemptID string, epoch int64, evidence Evidence) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !job.RetirementControlClosed {
			return protocolError(CodePreconditionFailed, job.JobID, "control channel must be closed before worker exit")
		}
		if !evidence.Present() {
			return protocolError(CodePreconditionFailed, job.JobID, "worker-exit evidence required")
		}
		job.RetirementWorkerExited = true
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordRetirementGroupEmpty(jobID, attemptID string, epoch int64, evidence Evidence) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !job.RetirementWorkerExited {
			return protocolError(CodePreconditionFailed, job.JobID, "worker exit must be observed before group-empty verification")
		}
		if !evidence.Present() {
			return protocolError(CodePreconditionFailed, job.JobID, "group-empty evidence required")
		}
		job.RetirementGroupEmpty = true
		job.RetirementEvidence = evidence
		job.Retired = true
		return nil
	})
}

func (s *MemoryAdmissionStore) RecordOutcome(jobID, attemptID string, epoch int64, outcome Outcome) (Aggregate, error) {
	return s.mutate(jobID, attemptID, epoch, func(job *Aggregate) error {
		if !terminalOutcome(outcome) {
			return protocolError(CodePreconditionFailed, job.JobID, "terminal outcome required")
		}
		if completionOutcome(outcome) && (job.PermitState != PermitConsumed || job.LaunchOrdinal == 0 || !job.Child.Valid()) {
			return protocolError(CodePreconditionFailed, job.JobID, "completed outcome requires a consumed launch with durable child identity")
		}
		if job.Outcome != OutcomeNone && job.Outcome != outcome {
			return protocolError(CodePreconditionFailed, job.JobID, "outcome already recorded")
		}
		job.Outcome = outcome
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
	if err := validateAggregateEnums(job); err != nil {
		return Aggregate{}, err
	}
	if job.Outcome != OutcomeCompleted && job.Outcome != OutcomeCompletedNoncompliant {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "completed outcome required")
	}
	if path == "" || digest == "" || bytes < 0 {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "complete result metadata required")
	}
	s.mutating = true
	defer func() { s.mutating = false }()
	s.step++
	job.UpdatedStep = s.step
	job.Dispatch = DispatchResultPublishing
	job.Result = ResultRef{Path: path, Digest: digest, Bytes: bytes}
	if err := s.checkDirectBoundary(nil); err != nil {
		return Aggregate{}, err
	}
	if err := validateAggregateEnums(job); err != nil {
		return Aggregate{}, err
	}
	return job.copy(), nil
}

func (s *MemoryAdmissionStore) RecordResultTempWritten(path, digest string, bytes int64) error {
	if path == "" || digest == "" || bytes < 0 {
		return protocolError(CodePreconditionFailed, "", "complete temp result metadata required")
	}
	artifact := s.resultArtifacts[path]
	artifact.Path = path
	artifact.Digest = digest
	artifact.Bytes = bytes
	artifact.TempWritten = true
	s.resultArtifacts[path] = artifact
	return s.checkDirectBoundary(nil)
}

func (s *MemoryAdmissionStore) RecordResultTempSynced(path string) error {
	artifact, ok := s.resultArtifacts[path]
	if !ok || !artifact.TempWritten {
		return protocolError(CodePreconditionFailed, "", "temp result was not written")
	}
	artifact.TempSynced = true
	s.resultArtifacts[path] = artifact
	return s.checkDirectBoundary(nil)
}

func (s *MemoryAdmissionStore) RecordResultClosed(path string) error {
	artifact, ok := s.resultArtifacts[path]
	if !ok || !artifact.TempSynced {
		return protocolError(CodePreconditionFailed, "", "temp result was not fsynced")
	}
	artifact.Closed = true
	s.resultArtifacts[path] = artifact
	return s.checkDirectBoundary(nil)
}

func (s *MemoryAdmissionStore) RecordResultRenamed(path string) error {
	artifact, ok := s.resultArtifacts[path]
	if !ok || !artifact.Closed {
		return protocolError(CodePreconditionFailed, "", "temp result was not closed")
	}
	artifact.Renamed = true
	s.resultArtifacts[path] = artifact
	return s.checkDirectBoundary(nil)
}

func (s *MemoryAdmissionStore) RecordResultDirSynced(path string) error {
	artifact, ok := s.resultArtifacts[path]
	if !ok || !artifact.Renamed {
		return protocolError(CodePreconditionFailed, "", "result was not renamed")
	}
	artifact.DirSynced = true
	s.resultArtifacts[path] = artifact
	return s.checkDirectBoundary(nil)
}

func (s *MemoryAdmissionStore) PublishTerminal(jobID, attemptID string, epoch int64, terminal Outcome, proof TerminalProof, reason string) (Aggregate, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return Aggregate{}, protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if reason == "" {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "terminal reason required")
	}
	if terminal == OutcomeCompleted || terminal == OutcomeCompletedNoncompliant {
		artifact, ok := s.resultArtifacts[job.Result.Path]
		if !ok || !artifact.DirSynced || artifact.Digest != job.Result.Digest || artifact.Bytes != job.Result.Bytes {
			return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "durable result digest/bytes proof missing")
		}
	}
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
		if proof == ProofNone {
			return protocolError(CodePreconditionFailed, job.JobID, "terminal proof required")
		}
		if proof == ProofNeverPermittedAndRetired && (!job.Retired || executionUncertain(job)) {
			return protocolError(CodePreconditionFailed, job.JobID, "never-permitted retired proof is not established")
		}
		if proof == ProofCleanQuiescentOutcomeAndRetired && (!job.Retired || job.Contained || job.ActiveOrdinal != 0 || liveOrdinalCount(job.LiveOrdinals) != 0 || job.Child.Valid()) {
			return protocolError(CodePreconditionFailed, job.JobID, "clean quiescent retired proof is not established")
		}
		if proof == ProofCleanQuiescentOutcomeAndRetired {
			evidence := job.LaunchEvidence[job.LaunchOrdinal]
			if job.LaunchOrdinal == 0 || !job.LaunchQuiescent[job.LaunchOrdinal] || !evidence.ChildExited.Present() || !evidence.GroupEmpty.Present() {
				return protocolError(CodePreconditionFailed, job.JobID, "clean quiescent launch evidence is not established")
			}
		}
		if proof == ProofContained && (!job.Retired || !job.Contained) {
			return protocolError(CodePreconditionFailed, job.JobID, "contained proof is not established")
		}
		job.TerminalProof = proof
		job.TerminalReason = reason
		if !validTerminalProof(job) {
			return protocolError(CodePreconditionFailed, job.JobID, "terminal proof state is invalid")
		}
		if !validTerminalOutcomeProofReason(job) {
			return protocolError(CodePreconditionFailed, job.JobID, "terminal outcome/proof/reason combination is invalid")
		}
		job.Decision = DecisionTerminal
		job.Dispatch = DispatchDone
		return nil
	})
}

func (s *MemoryAdmissionStore) Expire(workspaceKey, requestID string) (string, error) {
	key := bindingKey(workspaceKey, requestID)
	binding, ok := s.bindings[key]
	if !ok {
		return "", protocolError(CodeUnknownJob, "", "binding not found")
	}
	job, ok := s.jobs[binding.JobID]
	if !ok {
		return "", protocolError(CodeUnknownJob, binding.JobID, "bound job not found")
	}
	if !job.Terminal() {
		return "", protocolError(CodePreconditionFailed, binding.JobID, "expire requires terminal job")
	}
	if err := validateAggregateEnums(job); err != nil {
		return "", err
	}
	if !job.Retired {
		return "", protocolError(CodePreconditionFailed, binding.JobID, "expire requires retired supervisor")
	}
	if hasLiveAuthority(job) {
		return "", protocolError(CodePreconditionFailed, binding.JobID, "expire requires no live authority")
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
	if err := s.checkDirectBoundary(nil); err != nil {
		return "", err
	}
	return binding.JobID, nil
}

func (s *MemoryAdmissionStore) MarkCorrupt(jobID string, diagnostic string) (Aggregate, error) {
	job, ok := s.jobs[jobID]
	if !ok {
		return Aggregate{}, protocolError(CodeUnknownJob, jobID, "unknown job")
	}
	if job.Terminal() {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "terminal job")
	}
	if err := validateAggregateEnums(job); err != nil {
		return Aggregate{}, err
	}
	authority := s.attemptAuthority(jobID)
	permitMaybe := hasAnyGrantEvidence(job, authority)
	identityTrustworthy := authority.identityTrustworthy()
	if !identityTrustworthy {
		s.fatal = true
		return Aggregate{}, protocolError(CodeCorruptFatal, jobID, "containment identity is untrustworthy")
	}
	if permitMaybe && !job.Contained {
		return Aggregate{}, protocolError(CodePreconditionFailed, jobID, "verified containment required before corrupt quarantine")
	}
	s.mutating = true
	defer func() { s.mutating = false }()
	s.step++
	job.Corrupt = true
	job.QuarantineDiagnostic = diagnostic
	job.Outcome = OutcomeQuarantined
	if !permitMaybe && job.Retired {
		job.PermitState = PermitNone
		job.PermitMaybeSent = false
		job.ContainmentRequired = false
		job.ActiveOrdinal = 0
		job.LiveOrdinals = map[int]int{}
		job.Child = ChildRef{}
		job.PendingChild = ChildRef{}
	}
	job.UpdatedStep = s.step
	if err := s.checkDirectBoundary(nil); err != nil {
		return Aggregate{}, err
	}
	if err := validateAggregateEnums(job); err != nil {
		return Aggregate{}, err
	}
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
	if err := validateAggregateEnums(job); err != nil {
		return Aggregate{}, err
	}
	job.ensureMaps()
	s.mutating = true
	defer func() { s.mutating = false }()
	if err := fn(job); err != nil {
		return Aggregate{}, err
	}
	s.step++
	job.UpdatedStep = s.step
	if err := s.checkDirectBoundary(nil); err != nil {
		return Aggregate{}, err
	}
	if err := validateAggregateEnums(job); err != nil {
		return Aggregate{}, err
	}
	return job.copy(), nil
}

func (s *MemoryAdmissionStore) syntheticStartupAnchorInput() AnchorInput {
	dbPresent := s.step != 0 || len(s.jobs) != 0 || len(s.bindings) != 0 || len(s.tombstones) != 0
	if !dbPresent {
		return AnchorInput{}
	}
	return AnchorInput{
		DBPresent:           true,
		AnchorPresent:       true,
		DBValid:             true,
		AnchorValid:         true,
		DBUUID:              "memory",
		AnchorDBUUID:        "memory",
		DBSchemaMajor:       1,
		AnchorSchemaMajor:   1,
		EverInitialized:     true,
		DBGeneration:        s.step,
		HighWaterGeneration: s.step,
	}
}

func (s *MemoryAdmissionStore) hasInitializedStorage() bool {
	return s.step != 0 ||
		len(s.jobs) != 0 ||
		len(s.bindings) != 0 ||
		len(s.tombstones) != 0 ||
		s.anchorState.EverInitialized ||
		s.anchorInitState.EverInitialized
}

func (s *MemoryAdmissionStore) completeStartupAnchorDisposition(injector *FailureInjector) error {
	if !s.startupAnchorDispositionPersisted {
		return protocolError(CodePreconditionFailed, "", "startup anchor disposition was not persisted")
	}
	if s.startupAnchorCompleted {
		return nil
	}
	decision := s.startupAnchorDecision
	switch decision.Action {
	case StartupInitializeFirst:
		return s.persistStartupAnchor(1, s.startupAnchorInput.DBGeneration, injector)
	case StartupRecoverAnchor, StartupAdvanceAnchor:
		schemaMajor := s.startupAnchorInput.DBSchemaMajor
		if schemaMajor == 0 {
			schemaMajor = s.startupAnchorInput.AnchorSchemaMajor
		}
		if schemaMajor == 0 {
			schemaMajor = 1
		}
		generation := s.startupAnchorInput.DBGeneration
		if decision.Action == StartupAdvanceAnchor && generation < s.startupAnchorInput.HighWaterGeneration {
			generation = s.startupAnchorInput.HighWaterGeneration
		}
		return s.publishStartupAnchorOnly(schemaMajor, generation, injector)
	case StartupContinue:
		s.anchorState = AnchorState{
			DBUUID:              anchorStateUUID(s.startupAnchorInput),
			SchemaMajor:         anchorStateSchemaMajor(s.startupAnchorInput),
			EverInitialized:     true,
			HighWaterGeneration: s.startupAnchorInput.HighWaterGeneration,
		}
		s.startupAnchorCompleted = true
		return nil
	case StartupFatal:
		s.fatal = true
		return protocolError(CodeCorruptFatal, "", "startup anchor fatal: "+decision.Reason)
	default:
		s.fatal = true
		return protocolError(CodeCorruptFatal, "", "startup anchor decision is invalid")
	}
}

func (s *MemoryAdmissionStore) publishStartupAnchorOnly(schemaMajor int, generation int64, injector *FailureInjector) error {
	if generation < s.step {
		generation = s.step
	}
	state, err := RunAnchorPublishWithObserver(schemaMajor, generation, injector, func(state AnchorInitState) error {
		s.anchorInitState = state
		if state.AnchorPublished {
			s.anchorState = AnchorState{
				DBUUID:              anchorStateUUID(s.startupAnchorInput),
				SchemaMajor:         state.SchemaMajor,
				EverInitialized:     true,
				HighWaterGeneration: state.HighWaterGeneration,
			}
		}
		return s.checkDirectBoundary(nil)
	})
	s.anchorInitState = state
	if err != nil {
		return err
	}
	s.anchorState = AnchorState{
		DBUUID:              anchorStateUUID(s.startupAnchorInput),
		SchemaMajor:         state.SchemaMajor,
		EverInitialized:     true,
		HighWaterGeneration: state.HighWaterGeneration,
	}
	s.startupAnchorCompleted = true
	return s.checkDirectBoundary(nil)
}

func (s *MemoryAdmissionStore) persistStartupAnchor(schemaMajor int, generation int64, injector *FailureInjector) error {
	if generation < s.step {
		generation = s.step
	}
	state, err := RunAnchorInitializationWithObserver(schemaMajor, generation, injector, func(state AnchorInitState) error {
		s.anchorInitState = state
		if state.AnchorPublished {
			s.anchorState = AnchorState{
				DBUUID:              anchorStateUUID(s.startupAnchorInput),
				SchemaMajor:         state.SchemaMajor,
				EverInitialized:     true,
				HighWaterGeneration: state.HighWaterGeneration,
			}
		}
		return s.checkDirectBoundary(nil)
	})
	s.anchorInitState = state
	if err != nil {
		return err
	}
	s.anchorState = AnchorState{
		DBUUID:              anchorStateUUID(s.startupAnchorInput),
		SchemaMajor:         state.SchemaMajor,
		EverInitialized:     true,
		HighWaterGeneration: state.HighWaterGeneration,
	}
	s.startupAnchorCompleted = true
	return s.checkDirectBoundary(nil)
}

func anchorStateUUID(input AnchorInput) string {
	if input.DBUUID != "" {
		return input.DBUUID
	}
	if input.AnchorDBUUID != "" {
		return input.AnchorDBUUID
	}
	return "memory"
}

func anchorStateSchemaMajor(input AnchorInput) int {
	if input.DBSchemaMajor != 0 {
		return input.DBSchemaMajor
	}
	if input.AnchorSchemaMajor != 0 {
		return input.AnchorSchemaMajor
	}
	return 1
}

func (s *MemoryAdmissionStore) noteModeledSideEffect() {
	s.sideEffectLedger++
	if s.mutating {
		s.sideEffectInCAS = true
	}
}

func (s *MemoryAdmissionStore) checkDirectBoundary(err error) error {
	view := InvariantView{Store: s}
	if s.directBoundaryViewSet {
		view = s.directBoundaryView
		view.Store = s
	}
	if invErr := CheckInvariants(view); invErr != nil {
		if err != nil {
			return fmt.Errorf("%w; invariant boundary: %v", err, invErr)
		}
		return invErr
	}
	return err
}

func (s *MemoryAdmissionStore) withDirectBoundaryView(view InvariantView, fn func() (ResolveResult, error)) (ResolveResult, error) {
	previousView := s.directBoundaryView
	previousSet := s.directBoundaryViewSet
	if view.Store == nil {
		view.Store = s
	}
	s.directBoundaryView = view
	s.directBoundaryViewSet = true
	defer func() {
		s.directBoundaryView = previousView
		s.directBoundaryViewSet = previousSet
	}()
	return fn()
}

func liveOrdinalCount(live map[int]int) int {
	total := 0
	for _, count := range live {
		total += count
	}
	return total
}

func executionUncertain(job *Aggregate) bool {
	if job == nil {
		return false
	}
	return hasLiveAuthority(job) ||
		(job.PermitMaybeSent && !job.Contained && !job.LaunchQuiescent[job.LaunchOrdinal])
}

func hasLiveAuthority(job *Aggregate) bool {
	if job == nil {
		return false
	}
	return job.PermitState == PermitGranted ||
		job.PermitState == PermitMaybeSent ||
		job.PermitState == PermitConsumed ||
		job.ActiveOrdinal != 0 ||
		liveOrdinalCount(job.LiveOrdinals) != 0 ||
		job.Child.Valid() ||
		job.PendingChild.Valid()
}

func hasAnyGrantEvidence(job *Aggregate, authority AttemptAuthority) bool {
	if authority.permitEvidence() {
		return true
	}
	if job == nil {
		return false
	}
	return len(job.LaunchNonceHistory) != 0 ||
		job.PermitNonce != "" ||
		job.PermitMaybeSent ||
		job.LaunchOrdinal != 0 ||
		job.ExecutionSideEffects != 0
}

func (s *MemoryAdmissionStore) mutableAttemptAuthority(jobID string) *AttemptAuthority {
	authority, ok := s.attempts[jobID]
	if !ok || authority == nil {
		authority = &AttemptAuthority{}
		s.attempts[jobID] = authority
	}
	authority.ensureMaps()
	return authority
}

func (s *MemoryAdmissionStore) attemptAuthority(jobID string) AttemptAuthority {
	authority, ok := s.attempts[jobID]
	if !ok || authority == nil {
		return AttemptAuthority{GrantNonceHistory: map[int]string{}}
	}
	return authority.copy()
}

func (s *MemoryAdmissionStore) RecordIndependentGrantEvidence(jobID string, group GroupRef, launchOrdinal int, nonce string) error {
	if launchOrdinal != 1 && launchOrdinal != 2 {
		return protocolError(CodePreconditionFailed, jobID, "invalid launch ordinal")
	}
	if !group.Valid() || nonce == "" {
		return protocolError(CodePreconditionFailed, jobID, "complete independent grant evidence required")
	}
	authority := s.mutableAttemptAuthority(jobID)
	authority.Supervisor = group
	authority.GrantNonceHistory[launchOrdinal] = nonce
	return s.checkDirectBoundary(nil)
}

type executionAuthoritySnapshot struct {
	Jobs            map[string]executionAuthorityJobSnapshot
	Attempts        map[string]AttemptAuthority
	ResultArtifacts map[string]ResultArtifact
	SideEffects     int
}

type executionAuthorityJobSnapshot struct {
	Decision                Decision
	Dispatch                Dispatch
	Outcome                 Outcome
	Acknowledged            bool
	PermitState             PermitState
	PermitNonce             string
	PermitMaybeSent         bool
	ContainmentRequired     bool
	LaunchOrdinal           int
	ActiveOrdinal           int
	LaunchQuiescent         map[int]bool
	LaunchEvidence          map[int]LaunchQuiescenceEvidence
	LaunchNonceHistory      map[int]string
	LiveOrdinals            map[int]int
	Supervisor              GroupRef
	Child                   ChildRef
	PendingChild            ChildRef
	TerminalProof           TerminalProof
	TerminalReason          string
	TerminalizationStarted  bool
	Retired                 bool
	RetirementStarted       bool
	RetirementControlClosed bool
	RetirementWorkerExited  bool
	RetirementGroupEmpty    bool
	RetirementEvidence      Evidence
	Contained               bool
	ContainmentSignaled     bool
	ContainmentVerified     bool
	Containment             Evidence
	Result                  ResultRef
	ExecutionSideEffects    int
	LossObserved            bool
	Corrupt                 bool
	StartPhase              string
}

func (s *MemoryAdmissionStore) executionAuthoritySnapshot() executionAuthoritySnapshot {
	snapshot := executionAuthoritySnapshot{
		Jobs:            map[string]executionAuthorityJobSnapshot{},
		Attempts:        map[string]AttemptAuthority{},
		ResultArtifacts: map[string]ResultArtifact{},
		SideEffects:     s.sideEffectLedger,
	}
	for jobID, job := range s.jobs {
		if job == nil {
			continue
		}
		snapshot.Jobs[jobID] = executionAuthorityJobSnapshot{
			Decision:                job.Decision,
			Dispatch:                job.Dispatch,
			Outcome:                 job.Outcome,
			Acknowledged:            job.Acknowledged,
			PermitState:             job.PermitState,
			PermitNonce:             job.PermitNonce,
			PermitMaybeSent:         job.PermitMaybeSent,
			ContainmentRequired:     job.ContainmentRequired,
			LaunchOrdinal:           job.LaunchOrdinal,
			ActiveOrdinal:           job.ActiveOrdinal,
			LaunchQuiescent:         copyBoolMap(job.LaunchQuiescent),
			LaunchEvidence:          copyLaunchEvidenceMap(job.LaunchEvidence),
			LaunchNonceHistory:      copyStringByIntMap(job.LaunchNonceHistory),
			LiveOrdinals:            copyIntMap(job.LiveOrdinals),
			Supervisor:              job.Supervisor,
			Child:                   job.Child,
			PendingChild:            job.PendingChild,
			TerminalProof:           job.TerminalProof,
			TerminalReason:          job.TerminalReason,
			TerminalizationStarted:  job.TerminalizationStarted,
			Retired:                 job.Retired,
			RetirementStarted:       job.RetirementStarted,
			RetirementControlClosed: job.RetirementControlClosed,
			RetirementWorkerExited:  job.RetirementWorkerExited,
			RetirementGroupEmpty:    job.RetirementGroupEmpty,
			RetirementEvidence:      job.RetirementEvidence,
			Contained:               job.Contained,
			ContainmentSignaled:     job.ContainmentSignaled,
			ContainmentVerified:     job.ContainmentVerified,
			Containment:             job.Containment,
			Result:                  job.Result,
			ExecutionSideEffects:    job.ExecutionSideEffects,
			LossObserved:            job.LossObserved,
			Corrupt:                 job.Corrupt,
			StartPhase:              job.StartPhase,
		}
	}
	for jobID, authority := range s.attempts {
		if authority == nil {
			continue
		}
		snapshot.Attempts[jobID] = authority.copy()
	}
	for path, artifact := range s.resultArtifacts {
		snapshot.ResultArtifacts[path] = artifact
	}
	return snapshot
}

func (s *MemoryAdmissionStore) finishReplayObservation(before executionAuthoritySnapshot, replayID string, err error) error {
	after := s.executionAuthoritySnapshot()
	if !reflect.DeepEqual(before, after) {
		s.replaySideEffects++
		s.replayEvents = append(s.replayEvents, replayID)
		replayErr := protocolError(CodePreconditionFailed, "", "replay changed execution authority state")
		if err != nil {
			return fmt.Errorf("%w; %v", err, replayErr)
		}
		return replayErr
	}
	if invErr := s.checkDirectBoundary(nil); invErr != nil {
		if err != nil {
			return fmt.Errorf("%w; replay invariant boundary: %v", err, invErr)
		}
		return invErr
	}
	return err
}
