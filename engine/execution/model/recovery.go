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
)

type RecoveryAction struct {
	Kind     RecoveryActionKind
	Finalize *Finalize
}

type RecoveryPlan struct {
	BasedOnRevision uint64
	Next            RecoveryAction
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
	if !observationDomain.Equal(ref.KernelDomain()) {
		return GroupRecoveryQuiescent, nil
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

	intent := recoveryTerminalIntent(record, trigger)
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
	plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
	return plan, nil
}

func finalizable(record SafetyRecord, intent TerminalIntent) bool {
	_, err := DeriveTerminalCertificate(record, intent)
	return err == nil
}

func needsContainment(record SafetyRecord, _ RecoveryTrigger) bool {
	return hasAnyGrant(record.Attempt) || hasAnyRelease(record.Attempt)
}

func recoveryTerminalIntent(record SafetyRecord, trigger RecoveryTrigger) TerminalIntent {
	afterAuthorization := hasAnyGrant(record.Attempt) || hasAnyRelease(record.Attempt)
	intent := TerminalIntent{DerivedBy: record.AdmittedBy}
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
			intent.Outcome = OutcomeReaped
			intent.Cause = CauseSupervisorLostAfterAuthorization
		} else {
			intent.Outcome = OutcomeFailed
			intent.Cause = CauseSupervisorLostBeforeAuthorization
		}
	default:
		if afterAuthorization {
			intent.Outcome = OutcomeReaped
			intent.Cause = CauseDaemonRestartedAfterAuthorization
		} else {
			intent.Outcome = OutcomeFailed
			intent.Cause = CauseDaemonRestartedBeforeAuthorization
		}
	}
	return intent
}
