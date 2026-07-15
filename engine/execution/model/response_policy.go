package model

type ResponseAction uint8

const (
	ResponseActionInvalid ResponseAction = iota
	RunAcceptedObligation
	RetainObligationForReplay
	AcknowledgeGrantAndRelease
	RejectAndRetireNoGrant
	RunLegacyUnfenced
	RejectLegacyUnfencedBeforeRun
	CancelAccepted
)

func (action ResponseAction) String() string {
	switch action {
	case RunAcceptedObligation:
		return "run_accepted_obligation"
	case RetainObligationForReplay:
		return "retain_obligation_for_replay"
	case AcknowledgeGrantAndRelease:
		return "acknowledge_grant_and_release"
	case RejectAndRetireNoGrant:
		return "reject_and_retire_no_grant"
	case RunLegacyUnfenced:
		return "run_legacy_unfenced"
	case RejectLegacyUnfencedBeforeRun:
		return "reject_legacy_unfenced_before_run"
	case CancelAccepted:
		return "cancel_accepted"
	default:
		return ""
	}
}

// ResponsePolicy is the mode-specific hook at the RPC response boundary.
type ResponsePolicy interface {
	Delivered() ResponseAction
	DeliveryFailed() ResponseAction
}

type IdentifiedFencedResponsePolicy struct{}

func (IdentifiedFencedResponsePolicy) Delivered() ResponseAction {
	return RunAcceptedObligation
}

func (IdentifiedFencedResponsePolicy) DeliveryFailed() ResponseAction {
	return RetainObligationForReplay
}

type LegacyFencedResponsePolicy struct{}

func (LegacyFencedResponsePolicy) Delivered() ResponseAction {
	return AcknowledgeGrantAndRelease
}

func (LegacyFencedResponsePolicy) DeliveryFailed() ResponseAction {
	return RejectAndRetireNoGrant
}

type LegacyUnfencedResponsePolicy struct{}

func (LegacyUnfencedResponsePolicy) Delivered() ResponseAction {
	return RunLegacyUnfenced
}

func (LegacyUnfencedResponsePolicy) DeliveryFailed() ResponseAction {
	return RejectLegacyUnfencedBeforeRun
}

func ResponsePolicyForMode(mode Mode) (ResponsePolicy, bool) {
	switch mode {
	case ModeIdentifiedFenced:
		return IdentifiedFencedResponsePolicy{}, true
	case ModeLegacyFenced:
		return LegacyFencedResponsePolicy{}, true
	case ModeLegacyUnfenced:
		return LegacyUnfencedResponsePolicy{}, true
	default:
		return nil, false
	}
}

func OnResponseOutcome(mode Mode, delivered bool) ResponseAction {
	policy, ok := ResponsePolicyForMode(mode)
	if !ok {
		return ResponseActionInvalid
	}
	if delivered {
		return policy.Delivered()
	}
	return policy.DeliveryFailed()
}
