package model

type ContainmentDecision string

const (
	SignalDirectly               ContainmentDecision = "signal_directly"
	WaitBoundedForTrustedMonitor ContainmentDecision = "wait_bounded_for_trusted_monitor"
	AlreadyAbsent                ContainmentDecision = "already_absent"
	Unprovable                   ContainmentDecision = "unprovable"
)

type ContainmentSession struct {
	// ContinuouslyObservedLive is a caller-supplied capability result. The model
	// consumes it but does not infer continuity from discrete observations.
	BeganFromMatchingLeader  bool
	ContinuouslyObservedLive bool
}

type ContainmentMonitorObservation struct {
	Observed          bool
	KernelDomainID    KernelDomainID
	Alive             bool
	Identity          ProcessIdentityObservation
	BoundToExactGroup bool
}

func (observation ContainmentMonitorObservation) Validate() error {
	if !observation.Observed {
		return nil
	}
	if err := observation.KernelDomainID.Validate(); err != nil {
		return err
	}
	return observation.Identity.Validate()
}

// TrustedFor is true only when the monitor is in the same kernel domain, alive,
// has its own identity verified, and is bound to this exact durable group.
func (observation ContainmentMonitorObservation) TrustedFor(ref GroupRef) (bool, error) {
	if err := ref.Validate(); err != nil {
		return false, err
	}
	if err := observation.Validate(); err != nil {
		return false, err
	}
	if !observation.Observed || !observation.Alive || observation.Identity != ProcessIdentityMatching || !observation.BoundToExactGroup {
		return false, nil
	}
	relation, err := compareKernelDomain(ref.KernelDomain(), observation.KernelDomainID)
	if err != nil {
		return false, err
	}
	return relation == kernelDomainSame, nil
}

type ContainmentObservation struct {
	KernelDomainID KernelDomainID
	Group          GroupExistenceObservation
	Leader         ProcessIdentityObservation
	Monitor        ContainmentMonitorObservation
}

func (observation ContainmentObservation) Validate() error {
	if err := observation.KernelDomainID.Validate(); err != nil {
		return err
	}
	if err := observation.Group.Validate(); err != nil {
		return err
	}
	if err := observation.Leader.Validate(); err != nil {
		return err
	}
	return observation.Monitor.Validate()
}

type ContainmentAuthorization struct {
	Group           GroupRef
	Observation     ContainmentObservation
	Session         ContainmentSession
	DeadlineExpired bool
}

// DecideContainmentAuthorization is pure decision logic; callers supply all
// observations, session continuity, and deadline state.
func DecideContainmentAuthorization(input ContainmentAuthorization) (ContainmentDecision, error) {
	if err := input.Group.Validate(); err != nil {
		return "", err
	}
	if err := input.Observation.Validate(); err != nil {
		return "", err
	}
	if !input.Observation.coherent() {
		return Unprovable, nil
	}
	relation, err := compareKernelDomain(input.Group.KernelDomain(), input.Observation.KernelDomainID)
	if err != nil {
		return "", err
	}
	if relation == kernelDomainDifferent {
		return AlreadyAbsent, nil
	}
	if relation == kernelDomainUnprovable {
		return Unprovable, nil
	}
	switch input.Observation.Group {
	case GroupAbsent:
		return AlreadyAbsent, nil
	case GroupLive:
		return decideLiveContainment(input)
	default:
		return Unprovable, nil
	}
}

func decideLiveContainment(input ContainmentAuthorization) (ContainmentDecision, error) {
	switch input.Observation.Leader {
	case ProcessIdentityMatching:
		return SignalDirectly, nil
	case ProcessIdentityReused:
		return Unprovable, nil
	case ProcessIdentityMissing:
		if input.Session.BeganFromMatchingLeader && input.Session.ContinuouslyObservedLive {
			return SignalDirectly, nil
		}
	}
	trusted, err := input.Observation.Monitor.TrustedFor(input.Group)
	if err != nil {
		return "", err
	}
	if trusted && !input.DeadlineExpired {
		return WaitBoundedForTrustedMonitor, nil
	}
	return Unprovable, nil
}

func (observation ContainmentObservation) coherent() bool {
	if observation.Group == GroupAbsent && observation.leaderObservedLive() {
		return false
	}
	return observation.Monitor.coherent()
}

func (observation ContainmentObservation) leaderObservedLive() bool {
	switch observation.Leader {
	case ProcessIdentityMatching, ProcessIdentityReused:
		return true
	default:
		return false
	}
}

func (observation ContainmentMonitorObservation) coherent() bool {
	if !observation.Observed {
		return !observation.Alive && observation.Identity == "" && !observation.BoundToExactGroup
	}
	switch observation.Identity {
	case ProcessIdentityMatching, ProcessIdentityReused:
		if !observation.Alive {
			return false
		}
	case ProcessIdentityMissing:
		if observation.Alive {
			return false
		}
	}
	if observation.BoundToExactGroup && (!observation.Alive || observation.Identity != ProcessIdentityMatching) {
		return false
	}
	return true
}
