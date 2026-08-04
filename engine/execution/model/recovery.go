package model

type RecoveryTrigger uint8

const (
	RecoveryStartupLoss RecoveryTrigger = iota + 1
	RecoveryLiveLoss
	RecoveryCancelAfterGrant
	RecoveryCorruption
	RecoveryPostGrantFailure
	RecoveryShutdown
)

func (trigger RecoveryTrigger) Valid() bool {
	switch trigger {
	case RecoveryStartupLoss, RecoveryLiveLoss, RecoveryCancelAfterGrant, RecoveryCorruption, RecoveryPostGrantFailure, RecoveryShutdown:
		return true
	default:
		return false
	}
}

func (trigger RecoveryTrigger) Validate() error {
	if !trigger.Valid() {
		return invalid("recovery_trigger", "is unknown")
	}
	return nil
}

type RecoveryActionKind uint8

const (
	RecoveryRetireThenFinalize RecoveryActionKind = iota + 1
	RecoveryContainThenFinalize
	RecoveryFinalizeCertified
	RecoveryFatalUnprovable
	RecoveryAwaitResultCertificate
)

type RecoveryAction struct {
	Kind     RecoveryActionKind
	Finalize *Finalize
}

type RecoveryPlan struct {
	BasedOnRevision uint64
	Next            RecoveryAction
}

type RecoveryLaunch struct {
	Ordinal LaunchOrdinal
	Group   GroupRef
}

func (launch RecoveryLaunch) Validate() error {
	if err := launch.Ordinal.Validate(); err != nil {
		return err
	}
	if err := launch.Group.Validate(); err != nil {
		return err
	}
	if launch.Group.Launch.Ordinal != launch.Ordinal {
		return invalid("recovery_launch.group.ordinal", "does not match launch ordinal")
	}
	return nil
}

type RecoveryWorkItem struct {
	Token              RecoveryToken
	JobID              JobID
	BasedOnRevision    uint64
	Trigger            RecoveryTrigger
	Launches           []RecoveryLaunch
	WorkspaceLayoutKey WorkspaceKey
}

func (item RecoveryWorkItem) Validate() error {
	if err := item.Token.Validate(); err != nil {
		return err
	}
	if err := item.JobID.Validate(); err != nil {
		return err
	}
	if item.Token.JobID != item.JobID {
		return invalid("recovery_work_item.token.job_id", "does not match work item job")
	}
	if err := validatePositiveUint64("recovery_work_item.based_on_revision", item.BasedOnRevision); err != nil {
		return err
	}
	if item.Token.BasedOnRevision != item.BasedOnRevision {
		return invalid("recovery_work_item.token.based_on_revision", "does not match work item revision")
	}
	if err := item.Trigger.Validate(); err != nil {
		return err
	}
	if err := validateOptionalWorkspaceLayoutKey("recovery_work_item.workspace_layout_key", item.WorkspaceLayoutKey); err != nil {
		return err
	}
	for _, launch := range item.Launches {
		if err := launch.Validate(); err != nil {
			return err
		}
		if launch.Group.Launch.Attempt.JobID != item.JobID {
			return invalid("recovery_launch.group.job_id", "does not match work item job")
		}
	}
	return nil
}

type RecoveryToken struct {
	JobID           JobID
	BasedOnRevision uint64
	RecoveryBoot    BootRef
	Opaque          string
}

func (token RecoveryToken) Validate() error {
	if err := token.JobID.Validate(); err != nil {
		return err
	}
	if err := validatePositiveUint64("recovery_token.based_on_revision", token.BasedOnRevision); err != nil {
		return err
	}
	if err := token.RecoveryBoot.Validate(); err != nil {
		return err
	}
	return validateToken("recovery_token.opaque", token.Opaque)
}

type GroupExistenceObservation string

const (
	GroupExistenceUnknown          GroupExistenceObservation = "unknown"
	GroupAbsent                    GroupExistenceObservation = "absent"
	GroupLive                      GroupExistenceObservation = "live"
	GroupExistenceContradictory    GroupExistenceObservation = "contradictory"
	GroupExistencePermissionDenied GroupExistenceObservation = "permission_denied"
	GroupExistenceUnsupported      GroupExistenceObservation = "unsupported"
)

func (observation GroupExistenceObservation) Validate() error {
	switch observation {
	case GroupExistenceUnknown, GroupAbsent, GroupLive, GroupExistenceContradictory,
		GroupExistencePermissionDenied, GroupExistenceUnsupported:
		return nil
	default:
		return invalid("group.existence", "is unknown")
	}
}

type ProcessIdentityObservation string

const (
	ProcessIdentityUnknown  ProcessIdentityObservation = "unknown"
	ProcessIdentityMissing  ProcessIdentityObservation = "missing"
	ProcessIdentityMatching ProcessIdentityObservation = "matching"
	ProcessIdentityReused   ProcessIdentityObservation = "reused"
)

func (observation ProcessIdentityObservation) Validate() error {
	switch observation {
	case ProcessIdentityUnknown, ProcessIdentityMissing, ProcessIdentityMatching, ProcessIdentityReused:
		return nil
	default:
		return invalid("process.identity", "is unknown")
	}
}

type GroupRecoveryObservation struct {
	KernelDomainID KernelDomainID
	HostBootID     string
	Group          GroupExistenceObservation
	Leader         ProcessIdentityObservation
}

func (observation GroupRecoveryObservation) Validate() error {
	if _, err := observation.kernelDomain(); err != nil {
		return err
	}
	if err := observation.Group.Validate(); err != nil {
		return err
	}
	if err := observation.Leader.Validate(); err != nil {
		return err
	}
	return nil
}

func (observation GroupRecoveryObservation) kernelDomain() (KernelDomainID, error) {
	domain := observation.KernelDomainID
	if domain.HostBootID == "" && observation.HostBootID != "" {
		domain.HostBootID = observation.HostBootID
	}
	if observation.HostBootID != "" && domain.HostBootID != observation.HostBootID {
		return KernelDomainID{}, invalid("group.host_boot_id", "does not match kernel domain")
	}
	if err := domain.Validate(); err != nil {
		return KernelDomainID{}, err
	}
	return domain, nil
}

type GroupRecoveryDecision string

const (
	GroupRecoveryQuiescent  GroupRecoveryDecision = "quiescent"
	GroupRecoverySignal     GroupRecoveryDecision = "signal"
	GroupRecoveryUnprovable GroupRecoveryDecision = "recovery_unprovable"
)

func DecideGroupRecovery(ref GroupRef, observation GroupRecoveryObservation) (GroupRecoveryDecision, error) {
	if err := ref.Validate(); err != nil {
		return "", err
	}
	if err := observation.Validate(); err != nil {
		return "", err
	}
	observationDomain, err := observation.kernelDomain()
	if err != nil {
		return "", err
	}
	relation, err := compareKernelDomain(ref.KernelDomain(), observationDomain)
	if err != nil {
		return "", err
	}
	if relation == kernelDomainDifferent {
		return GroupRecoveryQuiescent, nil
	}
	if relation == kernelDomainUnprovable {
		return GroupRecoveryUnprovable, nil
	}
	if observation.Group == GroupAbsent {
		return GroupRecoveryQuiescent, nil
	}
	if observation.Group == GroupLive && observation.Leader == ProcessIdentityMatching {
		return GroupRecoverySignal, nil
	}
	if observation.Group == GroupLive {
		return GroupRecoveryUnprovable, nil
	}
	return GroupRecoveryUnprovable, nil
}

func PlanRecovery(record SafetyRecord, trigger RecoveryTrigger) (RecoveryPlan, error) {
	if err := trigger.Validate(); err != nil {
		return RecoveryPlan{}, err
	}
	plan := RecoveryPlan{BasedOnRevision: record.Revision}
	if err := ValidateSafetyRecord(record); err != nil {
		plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
		return plan, nil
	}
	if record.Terminal != nil {
		plan.Next = RecoveryAction{Kind: RecoveryFinalizeCertified}
		return plan, nil
	}
	if record.Mode == ModeLegacyUnfenced {
		plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
		return plan, nil
	}
	if pendingCompletionResult(record, trigger) {
		plan.Next = RecoveryAction{Kind: RecoveryAwaitResultCertificate}
		return plan, nil
	}

	absenceProven := allLaunchGroupsQuiescent(record.Attempt)
	intent := recoveryTerminalIntent(record, trigger, absenceProven)
	if trigger != RecoveryShutdown && needsContainment(record, trigger) && !allLaunchGroupsQuiescent(record.Attempt) {
		if !hasPreparedLaunch(record.Attempt) {
			plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
			return plan, nil
		}
		plan.Next = RecoveryAction{Kind: RecoveryContainThenFinalize}
		return plan, nil
	}
	if finalizable(record, intent) {
		finalize := Finalize{Ref: record.Attempt.Ref, Intent: intent}
		plan.Next = RecoveryAction{Kind: RecoveryFinalizeCertified, Finalize: &finalize}
		return plan, nil
	}
	if needsContainment(record, trigger) {
		if !hasPreparedLaunch(record.Attempt) {
			plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
			return plan, nil
		}
		if !allLaunchGroupsQuiescent(record.Attempt) {
			plan.Next = RecoveryAction{Kind: RecoveryContainThenFinalize}
			return plan, nil
		}
	}
	if !hasPreparedLaunch(record.Attempt) {
		plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
		return plan, nil
	}
	if !allLaunchGroupsQuiescent(record.Attempt) {
		plan.Next = RecoveryAction{Kind: RecoveryRetireThenFinalize}
		return plan, nil
	}
	// Fatal-unprovable remains for structurally invalid/corrupt safety records,
	// legacy-unfenced open recovery, launch evidence with no durable group,
	// missing physical quiescence proof, non-pending missing result certificates,
	// and contradictory recorded outcome/release/quiescence predicates.
	plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
	return plan, nil
}

func finalizable(record SafetyRecord, intent TerminalIntent) bool {
	_, err := DeriveTerminalCertificate(record, intent)
	return err == nil
}

func pendingCompletionResult(record SafetyRecord, trigger RecoveryTrigger) bool {
	return trigger == RecoveryCancelAfterGrant &&
		record.Cancel != nil &&
		record.Outcome != nil &&
		completionOutcome(record.Outcome.Outcome) &&
		record.Result == nil
}

func needsContainment(record SafetyRecord, _ RecoveryTrigger) bool {
	return hasAnyGrant(record.Attempt) || hasAnyRelease(record.Attempt)
}

// RecoveryTerminalIntent derives the recovery terminal intent after the caller
// has resolved whether physical absence was proven.
func RecoveryTerminalIntent(record SafetyRecord, trigger RecoveryTrigger, absenceProven bool) (TerminalIntent, error) {
	if err := trigger.Validate(); err != nil {
		return TerminalIntent{}, err
	}
	if err := ValidateSafetyRecord(record); err != nil {
		return TerminalIntent{}, err
	}
	return recoveryTerminalIntent(record, trigger, absenceProven), nil
}

func recoveryTerminalIntent(record SafetyRecord, trigger RecoveryTrigger, absenceProven bool) TerminalIntent {
	afterAuthorization := hasAnyGrant(record.Attempt) || hasAnyRelease(record.Attempt)
	intent := TerminalIntent{DerivedBy: record.AdmittedBy}
	if record.Outcome != nil {
		intent.Outcome = record.Outcome.Outcome
		intent.Cause = recordedOutcomeRecoveryCause(record.Outcome.Outcome, trigger, afterAuthorization)
		intent.Contract = cloneContractStamp(record.Outcome.Contract)
		return intent
	}
	if cause, ok := recordedReleaseFailureCause(record); ok {
		intent.Cause = cause
		if cause == CauseReleaseOutcomeUnknown {
			intent.Outcome = unrecordedAfterAuthorizationOutcome(absenceProven)
		} else {
			intent.Outcome = OutcomeFailed
		}
		return intent
	}
	// Without recorded outcome progress, recovery uses the absence proof to
	// distinguish reaped from orphaned for authorized launches.
	switch trigger {
	case RecoveryCancelAfterGrant:
		intent.Outcome = OutcomeCanceled
		if afterAuthorization {
			intent.Cause = CauseCanceledAfterAuthorization
		} else {
			intent.Cause = CauseCanceledBeforeAuthorization
		}
	case RecoveryCorruption:
		intent.Outcome = OutcomeQuarantined
		intent.Cause = CauseCorruptProjection
	case RecoveryLiveLoss:
		if afterAuthorization {
			intent.Outcome = unrecordedAfterAuthorizationOutcome(absenceProven)
			intent.Cause = CauseSupervisorLostAfterAuthorization
		} else {
			intent.Outcome = OutcomeFailed
			intent.Cause = CauseSupervisorLostBeforeAuthorization
		}
	default:
		if afterAuthorization {
			intent.Outcome = unrecordedAfterAuthorizationOutcome(absenceProven)
			intent.Cause = CauseDaemonRestartedAfterAuthorization
		} else {
			intent.Outcome = OutcomeFailed
			intent.Cause = CauseDaemonRestartedBeforeAuthorization
		}
	}
	return intent
}

func unrecordedAfterAuthorizationOutcome(absenceProven bool) Outcome {
	if absenceProven {
		return OutcomeReaped
	}
	return OutcomeOrphaned
}

func recordedReleaseFailureCause(record SafetyRecord) (TerminalCause, bool) {
	launch, ok := authoritativeLaunch(record.Attempt)
	if !ok || launch.ReleaseOutcome == nil {
		return 0, false
	}
	switch launch.ReleaseOutcome.Outcome {
	case LaunchReleaseSentUnknown:
		if terminalCauseBackedByDurableFact(record, CauseReleaseOutcomeUnknown) {
			return CauseReleaseOutcomeUnknown, true
		}
	case LaunchReleaseNotSent:
		if terminalCauseBackedByDurableFact(record, CauseReleaseDefinitelyNotSent) {
			return CauseReleaseDefinitelyNotSent, true
		}
	}
	return 0, false
}

func recordedOutcomeRecoveryCause(outcome Outcome, trigger RecoveryTrigger, afterAuthorization bool) TerminalCause {
	switch outcome {
	case OutcomeCompleted, OutcomeCompletedNoncompliant, OutcomeFailed, OutcomeTimedOut, OutcomeInterrupted:
		if afterAuthorization {
			return CauseCompletedNormally
		}
		if trigger == RecoveryLiveLoss {
			return CauseSupervisorLostBeforeAuthorization
		}
		return CauseDaemonRestartedBeforeAuthorization
	case OutcomeCanceled:
		if afterAuthorization {
			return CauseCanceledAfterAuthorization
		}
		return CauseCanceledBeforeAuthorization
	case OutcomeQuarantined:
		return CauseCorruptProjection
	case OutcomeReaped:
		if afterAuthorization && trigger == RecoveryLiveLoss {
			return CauseSupervisorLostAfterAuthorization
		}
		if afterAuthorization {
			return CauseDaemonRestartedAfterAuthorization
		}
		return CauseDaemonRestartedBeforeAuthorization
	default:
		if afterAuthorization {
			return CauseDaemonRestartedAfterAuthorization
		}
		return CauseDaemonRestartedBeforeAuthorization
	}
}
