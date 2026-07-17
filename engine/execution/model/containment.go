package model

type ContainmentDecision string

const (
	SignalDirectly               ContainmentDecision = "signal_directly"
	WaitBoundedForTrustedMonitor ContainmentDecision = "wait_bounded_for_trusted_monitor"
	AlreadyAbsent                ContainmentDecision = "already_absent"
	Unprovable                   ContainmentDecision = "unprovable"
)

type ContainmentAuthorizationBasis string

const (
	ContainmentBasisNone           ContainmentAuthorizationBasis = ""
	ContainmentBasisLeader         ContainmentAuthorizationBasis = "leader"
	ContainmentBasisRetainedObject ContainmentAuthorizationBasis = "retained_object"
)

type ContainmentSession struct {
	// ContinuouslyObservedLive is set only after the engine validates continuity
	// evidence; the model does not infer continuity from discrete observations.
	BeganFromMatchingLeader  bool
	ContinuouslyObservedLive bool
}

type RetainedObjectProof string

const (
	RetainedObjectProofNone           RetainedObjectProof = ""
	RetainedObjectProofMembersPresent RetainedObjectProof = "members_present"
	RetainedObjectProofEmpty          RetainedObjectProof = "empty"
	RetainedObjectProofUnknown        RetainedObjectProof = "unknown"
)

func (proof RetainedObjectProof) Validate() error {
	switch proof {
	case RetainedObjectProofNone, RetainedObjectProofMembersPresent, RetainedObjectProofEmpty, RetainedObjectProofUnknown:
		return nil
	default:
		return invalid("containment.retained_object_proof", "is unknown")
	}
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
	RetainedObject  RetainedObjectProof
	DeadlineExpired bool
}

type ContainmentAuthorizationResult struct {
	Decision ContainmentDecision
	Basis    ContainmentAuthorizationBasis
}

// DecideContainmentAuthorization is pure decision logic; callers supply all
// observations, session continuity, and deadline state.
func DecideContainmentAuthorization(input ContainmentAuthorization) (ContainmentDecision, error) {
	result, err := DecideContainmentAuthorizationWithBasis(input)
	if err != nil {
		return "", err
	}
	return result.Decision, nil
}

// DecideContainmentAuthorizationWithBasis is pure decision logic with the
// authority basis preserved for mutating callers.
func DecideContainmentAuthorizationWithBasis(input ContainmentAuthorization) (ContainmentAuthorizationResult, error) {
	if err := input.Group.Validate(); err != nil {
		return ContainmentAuthorizationResult{}, err
	}
	if err := input.Observation.Validate(); err != nil {
		return ContainmentAuthorizationResult{}, err
	}
	if err := input.RetainedObject.Validate(); err != nil {
		return ContainmentAuthorizationResult{}, err
	}
	if !input.Observation.coherent() {
		return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
	}
	relation, err := compareKernelDomain(input.Group.KernelDomain(), input.Observation.KernelDomainID)
	if err != nil {
		return ContainmentAuthorizationResult{}, err
	}
	if input.RetainedObject != RetainedObjectProofNone {
		return decideRetainedContainment(input, relation)
	}
	if relation == kernelDomainDifferent {
		return containmentAuthorizationResult(AlreadyAbsent, ContainmentBasisNone), nil
	}
	if relation == kernelDomainUnprovable {
		return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
	}
	return decideObservedContainment(input)
}

func decideRetainedContainment(input ContainmentAuthorization, relation kernelDomainRelation) (ContainmentAuthorizationResult, error) {
	switch input.RetainedObject {
	case RetainedObjectProofEmpty:
		if input.Observation.Leader == ProcessIdentityMatching {
			return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
		}
		return containmentAuthorizationResult(AlreadyAbsent, ContainmentBasisRetainedObject), nil
	case RetainedObjectProofMembersPresent:
		if relation == kernelDomainDifferent || input.Observation.Group == GroupAbsent {
			return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
		}
		if relation == kernelDomainUnprovable {
			return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
		}
		return containmentAuthorizationResult(SignalDirectly, ContainmentBasisRetainedObject), nil
	case RetainedObjectProofUnknown:
		// Unknown retained-object state never proves absence or signal authority.
	}
	return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
}

func decideObservedContainment(input ContainmentAuthorization) (ContainmentAuthorizationResult, error) {
	switch input.Observation.Group {
	case GroupAbsent:
		return containmentAuthorizationResult(AlreadyAbsent, ContainmentBasisNone), nil
	case GroupLive:
		return decideLiveContainment(input)
	default:
		return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
	}
}

func containmentAuthorizationResult(decision ContainmentDecision, basis ContainmentAuthorizationBasis) ContainmentAuthorizationResult {
	return ContainmentAuthorizationResult{Decision: decision, Basis: basis}
}

func decideLiveContainment(input ContainmentAuthorization) (ContainmentAuthorizationResult, error) {
	switch input.Observation.Leader {
	case ProcessIdentityMatching:
		return containmentAuthorizationResult(SignalDirectly, ContainmentBasisLeader), nil
	case ProcessIdentityReused:
		return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
	case ProcessIdentityMissing:
		if input.Session.BeganFromMatchingLeader && input.Session.ContinuouslyObservedLive {
			return containmentAuthorizationResult(SignalDirectly, ContainmentBasisLeader), nil
		}
	}
	trusted, err := input.Observation.Monitor.TrustedFor(input.Group)
	if err != nil {
		return ContainmentAuthorizationResult{}, err
	}
	if trusted && !input.DeadlineExpired {
		return containmentAuthorizationResult(WaitBoundedForTrustedMonitor, ContainmentBasisNone), nil
	}
	return containmentAuthorizationResult(Unprovable, ContainmentBasisNone), nil
}

func (observation ContainmentObservation) coherent() bool {
	if observation.Group == GroupAbsent && observation.Leader != ProcessIdentityMissing {
		return false
	}
	return observation.Monitor.coherent()
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
