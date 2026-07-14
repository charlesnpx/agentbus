package execution

type AnchorInput struct {
	DBPresent           bool
	AnchorPresent       bool
	DBValid             bool
	AnchorValid         bool
	DBUUID              string
	AnchorDBUUID        string
	DBGeneration        int64
	HighWaterGeneration int64
}

type StartupAction string

const (
	StartupInitializeFirst StartupAction = "initialize_first"
	StartupRecoverAnchor   StartupAction = "recover_anchor"
	StartupContinue        StartupAction = "continue"
	StartupAdvanceAnchor   StartupAction = "advance_anchor"
	StartupFatal           StartupAction = "fatal"
)

type StartupDecision struct {
	Action StartupAction
	Reason string
}

func (d StartupDecision) Fatal() bool {
	return d.Action == StartupFatal
}

func DecideStartupAnchor(input AnchorInput) StartupDecision {
	if !input.DBPresent && !input.AnchorPresent {
		return StartupDecision{Action: StartupInitializeFirst, Reason: "db and anchor absent"}
	}
	if input.DBPresent && !input.DBValid {
		return StartupDecision{Action: StartupFatal, Reason: "db invalid"}
	}
	if input.AnchorPresent && !input.AnchorValid {
		return StartupDecision{Action: StartupFatal, Reason: "anchor invalid"}
	}
	if input.DBPresent && !input.AnchorPresent {
		return StartupDecision{Action: StartupRecoverAnchor, Reason: "db present and anchor absent"}
	}
	if !input.DBPresent && input.AnchorPresent {
		return StartupDecision{Action: StartupFatal, Reason: "anchor present and db absent"}
	}
	if input.DBUUID != input.AnchorDBUUID {
		return StartupDecision{Action: StartupFatal, Reason: "db uuid mismatch"}
	}
	if input.DBGeneration < input.HighWaterGeneration {
		return StartupDecision{Action: StartupFatal, Reason: "db generation below anchor high-water generation"}
	}
	if input.DBGeneration > input.HighWaterGeneration {
		return StartupDecision{Action: StartupAdvanceAnchor, Reason: "anchor high-water generation lags db"}
	}
	return StartupDecision{Action: StartupContinue, Reason: "db and anchor valid"}
}
