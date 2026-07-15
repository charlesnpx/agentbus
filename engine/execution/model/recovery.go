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
	GroupExistenceUnknown GroupExistenceObservation = "unknown"
	GroupAbsent           GroupExistenceObservation = "absent"
	GroupLive             GroupExistenceObservation = "live"
)

func (observation GroupExistenceObservation) Validate() error {
	switch observation {
	case GroupExistenceUnknown, GroupAbsent, GroupLive:
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

type DescendantObservation string

const (
	DescendantsUnknown DescendantObservation = "unknown"
	DescendantsAbsent  DescendantObservation = "absent"
	DescendantsPresent DescendantObservation = "present"
)

func (observation DescendantObservation) Validate() error {
	switch observation {
	case DescendantsUnknown, DescendantsAbsent, DescendantsPresent:
		return nil
	default:
		return invalid("descendants", "is unknown")
	}
}

type GroupRecoveryObservation struct {
	HostBootID  string
	Group       GroupExistenceObservation
	Leader      ProcessIdentityObservation
	Monitor     ProcessIdentityObservation
	Descendants DescendantObservation
}

func (observation GroupRecoveryObservation) Validate() error {
	if err := validateToken("group.host_boot_id", observation.HostBootID); err != nil {
		return err
	}
	if err := observation.Group.Validate(); err != nil {
		return err
	}
	if err := observation.Leader.Validate(); err != nil {
		return err
	}
	if err := observation.Monitor.Validate(); err != nil {
		return err
	}
	return observation.Descendants.Validate()
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
	if observation.HostBootID != ref.HostBootID {
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
	if observation.Leader == ProcessIdentityMissing &&
		observation.Monitor == ProcessIdentityMissing &&
		observation.Descendants == DescendantsUnknown {
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
