package procgroup

import "github.com/charlesnpx/agentbus/engine/execution/model"

type kernelDomainRelation uint8

const (
	kernelDomainSame kernelDomainRelation = iota + 1
	kernelDomainDifferent
	kernelDomainUnprovable
)

func compareKernelDomain(left, right model.KernelDomainID) (kernelDomainRelation, error) {
	if err := left.Validate(); err != nil {
		return 0, err
	}
	if err := right.Validate(); err != nil {
		return 0, err
	}
	if left.HostBootID != right.HostBootID {
		return kernelDomainDifferent, nil
	}
	if left.PIDNamespaceState == model.PIDNamespaceKnown && right.PIDNamespaceState == model.PIDNamespaceKnown {
		if left.PIDNamespaceID == right.PIDNamespaceID {
			return kernelDomainSame, nil
		}
		return kernelDomainDifferent, nil
	}
	if left.PIDNamespaceState == model.PIDNamespaceNotApplicable && right.PIDNamespaceState == model.PIDNamespaceNotApplicable {
		return kernelDomainSame, nil
	}
	return kernelDomainUnprovable, nil
}
