//go:build darwin || linux

package containment

import (
	"context"
	"errors"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/procgroup"
)

type RealObserver struct{}

type groupLeaderObservation struct {
	Group  model.GroupExistenceObservation
	Leader model.ProcessIdentityObservation
}

func (RealObserver) ObserveGroup(ctx context.Context, target model.GroupRef) (model.ContainmentObservation, error) {
	if err := ctx.Err(); err != nil {
		return model.ContainmentObservation{}, err
	}
	if err := target.Validate(); err != nil {
		return model.ContainmentObservation{}, err
	}
	currentDomain, err := procgroup.CurrentKernelDomain()
	if err != nil {
		return model.ContainmentObservation{}, err
	}
	groupClaim, err := procgroup.NewGroupClaim(target.PGID, target.KernelDomain())
	if err != nil {
		return model.ContainmentObservation{}, err
	}
	leaderClaim, err := procgroup.NewProcessClaim(
		target.Leader.PID,
		target.PGID,
		procgroup.StartToken(target.Leader.HighResStartToken),
		target.KernelDomain(),
	)
	if err != nil {
		return model.ContainmentObservation{}, err
	}
	groupLeader := stableGroupLeaderObservation(func() groupLeaderObservation {
		return groupLeaderObservation{
			Group:  procgroup.ClassifyGroup(groupClaim),
			Leader: procgroup.ClassifyProcess(leaderClaim),
		}
	})
	return model.ContainmentObservation{
		KernelDomainID: currentDomain,
		Group:          groupLeader.Group,
		Leader:         groupLeader.Leader,
		Monitor:        observeMonitor(target, currentDomain),
	}, nil
}

func stableGroupLeaderObservation(read func() groupLeaderObservation) groupLeaderObservation {
	first := read()
	if groupLeaderIncoherent(first) {
		return unknownGroupLeaderObservation()
	}
	if first.Group != model.GroupAbsent {
		return first
	}
	second := read()
	if groupLeaderIncoherent(second) || first != second {
		return unknownGroupLeaderObservation()
	}
	if first.Leader != model.ProcessIdentityMissing {
		return unknownGroupLeaderObservation()
	}
	return first
}

func groupLeaderIncoherent(observation groupLeaderObservation) bool {
	return observation.Group == model.GroupAbsent &&
		(observation.Leader == model.ProcessIdentityMatching || observation.Leader == model.ProcessIdentityReused)
}

func unknownGroupLeaderObservation() groupLeaderObservation {
	return groupLeaderObservation{
		Group:  model.GroupExistenceUnknown,
		Leader: model.ProcessIdentityUnknown,
	}
}

func observeMonitor(target model.GroupRef, currentDomain model.KernelDomainID) model.ContainmentMonitorObservation {
	observation := model.ContainmentMonitorObservation{
		Observed:       true,
		KernelDomainID: currentDomain,
		Identity:       model.ProcessIdentityUnknown,
	}
	claim, err := procgroup.ReadProcessClaim(target.Monitor.PID)
	if errors.Is(err, procgroup.ErrProcessMissing) {
		observation.Identity = model.ProcessIdentityMissing
		return observation
	}
	if err != nil {
		return observation
	}
	observation.Alive = true
	if !claim.KernelDomainID.ProvablySame(target.KernelDomain()) {
		observation.Identity = model.ProcessIdentityUnknown
		return observation
	}
	if claim.PID != target.Monitor.PID {
		observation.Identity = model.ProcessIdentityUnknown
		return observation
	}
	if claim.StartToken.String() != target.Monitor.HighResStartToken {
		observation.Identity = model.ProcessIdentityReused
		return observation
	}
	observation.Identity = model.ProcessIdentityMatching
	return observation
}
