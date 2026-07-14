package execution

type AnchorInput struct {
	DBPresent           bool
	AnchorPresent       bool
	DBValid             bool
	AnchorValid         bool
	DBUUID              string
	AnchorDBUUID        string
	DBSchemaMajor       int
	AnchorSchemaMajor   int
	EverInitialized     bool
	DBGeneration        int64
	HighWaterGeneration int64
}

type AnchorState struct {
	DBUUID              string
	SchemaMajor         int
	EverInitialized     bool
	HighWaterGeneration int64
}

type AnchorInitState struct {
	TempDBCreated       bool
	DBFsynced           bool
	Renamed             bool
	DirFsynced          bool
	AnchorPublished     bool
	AnchorDirFsynced    bool
	HighWaterGeneration int64
	SchemaMajor         int
	EverInitialized     bool
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
		if input.EverInitialized {
			return StartupDecision{Action: StartupFatal, Reason: "initialized db and anchor absent"}
		}
		return StartupDecision{Action: StartupInitializeFirst, Reason: "db and anchor absent"}
	}
	if input.DBPresent && !input.DBValid {
		return StartupDecision{Action: StartupFatal, Reason: "db invalid"}
	}
	if input.AnchorPresent && !input.AnchorValid {
		return StartupDecision{Action: StartupFatal, Reason: "anchor invalid"}
	}
	if input.AnchorPresent && !input.EverInitialized {
		return StartupDecision{Action: StartupFatal, Reason: "anchor is present but everInitialized is false"}
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
	if input.DBSchemaMajor != 0 && input.AnchorSchemaMajor != 0 && input.DBSchemaMajor != input.AnchorSchemaMajor {
		return StartupDecision{Action: StartupFatal, Reason: "schema major mismatch"}
	}
	if input.DBGeneration < input.HighWaterGeneration {
		return StartupDecision{Action: StartupFatal, Reason: "db generation below anchor high-water generation"}
	}
	if input.DBGeneration > input.HighWaterGeneration {
		return StartupDecision{Action: StartupAdvanceAnchor, Reason: "anchor high-water generation lags db"}
	}
	return StartupDecision{Action: StartupContinue, Reason: "db and anchor valid"}
}

func (s *MemoryAdmissionStore) ObserveStartupAnchor(input AnchorInput) StartupDecision {
	decision := DecideStartupAnchor(input)
	s.startupAnchorObserved = true
	s.startupAnchorInput = input
	s.startupAnchorDecision = decision
	s.startupAnchorDispositionPersisted = true
	s.startupAnchorCompleted = false
	s.silentRecreated = input.EverInitialized && !input.DBPresent && !input.AnchorPresent
	if decision.Fatal() {
		s.fatal = true
	}
	return decision
}

func AdvanceAnchorHighWater(anchor AnchorState, dbGeneration int64) AnchorState {
	if dbGeneration > anchor.HighWaterGeneration {
		anchor.HighWaterGeneration = dbGeneration
	}
	anchor.EverInitialized = true
	return anchor
}

func RunAnchorInitialization(schemaMajor int, generation int64, injector *FailureInjector) (AnchorInitState, error) {
	state := AnchorInitState{SchemaMajor: schemaMajor, HighWaterGeneration: generation}
	if err := anchorStep(injector, FailAnchorTempDBBefore, FailAnchorTempDBAfter, func() {
		state.TempDBCreated = true
	}); err != nil {
		return state, err
	}
	if err := anchorStep(injector, FailAnchorDBFsyncBefore, FailAnchorDBFsyncAfter, func() {
		state.DBFsynced = true
	}); err != nil {
		return state, err
	}
	if err := anchorStep(injector, FailAnchorRenameBefore, FailAnchorRenameAfter, func() {
		state.Renamed = true
	}); err != nil {
		return state, err
	}
	if err := anchorStep(injector, FailAnchorDirFsyncBefore, FailAnchorDirFsyncAfter, func() {
		state.DirFsynced = true
	}); err != nil {
		return state, err
	}
	if err := anchorStep(injector, FailAnchorPublishBefore, FailAnchorPublishAfter, func() {
		state.AnchorPublished = true
		state.EverInitialized = true
	}); err != nil {
		return state, err
	}
	if err := anchorStep(injector, FailAnchorPublishDirFsyncBefore, FailAnchorPublishDirFsyncAfter, func() {
		state.AnchorDirFsynced = true
	}); err != nil {
		return state, err
	}
	return state, nil
}

func anchorStep(injector *FailureInjector, before, after Failpoint, op func()) error {
	if injector != nil {
		if err := injector.Fail(before); err != nil {
			return err
		}
	}
	op()
	if injector != nil {
		if err := injector.Fail(after); err != nil {
			return err
		}
	}
	return nil
}
