package authority

import (
	"fmt"
	"reflect"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type startupProjectionRepair struct {
	Record      model.SafetyRecord
	SessionID   string
	Diagnostic  string
	Quarantine  bool
	KnownBroken bool
}

func startupMatrixTx(tx repository.ReadTx) ([]startupProjectionRepair, error) {
	images, err := tx.ListJobs(repository.JobFilter{})
	if err != nil {
		return nil, err
	}
	repairs := make([]startupProjectionRepair, 0)
	for _, image := range images {
		repair, ok, err := startupMatrixJobTx(tx, image)
		if err != nil {
			return nil, err
		}
		if ok {
			repairs = append(repairs, repair)
		}
	}
	return repairs, nil
}

func startupMatrixJobTx(tx repository.ReadTx, image repository.JobImage) (startupProjectionRepair, bool, error) {
	switch image.Safety.State {
	case repository.RecordValid:
		record := image.Safety.Value
		if err := model.ValidateSafetyRecord(record); err != nil {
			return startupProjectionRepair{}, false, fatalStartup("safety %s is unsupported: %v", record.JobID, err)
		}
		if err := requireStartupBindingTx(tx, record); err != nil {
			return startupProjectionRepair{}, false, err
		}
		repair, ok, err := projectionRepair(record, image.Projection)
		if err != nil {
			return startupProjectionRepair{}, false, err
		}
		return repair, ok, nil
	case repository.RecordCorrupt:
		jobID := image.Binding.Value.JobID
		if jobID == "" && image.Projection.State == repository.RecordValid {
			jobID = image.Projection.Value.JobID
		}
		return startupProjectionRepair{}, false, fatalCorruptStartup("safety %s: %s", jobID, diagnosticOrDefault(image.Safety.Diagnostic))
	case repository.RecordMissing:
		if image.Binding.State == repository.RecordValid {
			return startupProjectionRepair{}, false, fatalStartup("binding %s references missing safety %s", image.Binding.Value.RequestKey, image.Binding.Value.JobID)
		}
		if image.Projection.State == repository.RecordValid {
			if err := requireNoTombstoneWithProjectionTx(tx, image.Projection.Value); err != nil {
				return startupProjectionRepair{}, false, err
			}
			return startupProjectionRepair{}, false, fatalStartup("projection %s has no safety record", image.Projection.Value.JobID)
		}
		if image.Projection.State == repository.RecordCorrupt {
			return startupProjectionRepair{}, false, fatalCorruptStartup("projection without safety: %s", diagnosticOrDefault(image.Projection.Diagnostic))
		}
		return startupProjectionRepair{}, false, nil
	default:
		return startupProjectionRepair{}, false, fatalStartup("safety has unknown state")
	}
}

func requireStartupBindingTx(tx repository.ReadTx, record model.SafetyRecord) error {
	request := tx.LookupRequest(record.RequestKey)
	switch request.Tombstone.State {
	case repository.RecordValid:
		return fatalStartup("tombstone %s coexists with live safety %s", record.RequestKey, record.JobID)
	case repository.RecordCorrupt:
		return fatalCorruptStartup("tombstone %s: %s", record.RequestKey, diagnosticOrDefault(request.Tombstone.Diagnostic))
	}
	switch request.Binding.State {
	case repository.RecordValid:
		if err := request.Binding.Value.Matches(record); err != nil {
			return fatalStartup("binding %s does not match safety %s: %v", record.RequestKey, record.JobID, err)
		}
		return nil
	case repository.RecordCorrupt:
		return fatalCorruptStartup("binding %s: %s", record.RequestKey, diagnosticOrDefault(request.Binding.Diagnostic))
	case repository.RecordMissing:
		return fatalStartup("safety %s has missing binding %s", record.JobID, record.RequestKey)
	default:
		return fatalStartup("binding %s has unknown state", record.RequestKey)
	}
}

func requireNoTombstoneWithProjectionTx(tx repository.ReadTx, projection model.JobProjection) error {
	request := tx.LookupRequest(projection.RequestKey)
	switch request.Tombstone.State {
	case repository.RecordValid:
		return fatalStartup("tombstone %s coexists with live projection %s", projection.RequestKey, projection.JobID)
	case repository.RecordCorrupt:
		return fatalCorruptStartup("tombstone %s: %s", projection.RequestKey, diagnosticOrDefault(request.Tombstone.Diagnostic))
	default:
		return nil
	}
}

func projectionRepair(record model.SafetyRecord, projection repository.Record[model.JobProjection]) (startupProjectionRepair, bool, error) {
	switch projection.State {
	case repository.RecordMissing:
		return startupProjectionRepair{Record: record, Diagnostic: "projection missing", KnownBroken: true}, true, nil
	case repository.RecordCorrupt:
		return startupProjectionRepair{Record: record, Diagnostic: diagnosticOrDefault(projection.Diagnostic), Quarantine: true, KnownBroken: true}, true, nil
	case repository.RecordValid:
		sessionID := projection.Value.SessionID
		expected, err := model.Project(record, model.ProjectionMetadata{SessionID: sessionID})
		if err != nil {
			return startupProjectionRepair{Record: record, Diagnostic: fmt.Sprintf("projection metadata: %v", err), Quarantine: true, KnownBroken: true}, true, nil
		}
		if !reflect.DeepEqual(projection.Value, expected) {
			return startupProjectionRepair{Record: record, SessionID: sessionID, Diagnostic: "projection does not match safety", Quarantine: true, KnownBroken: true}, true, nil
		}
		return startupProjectionRepair{Record: record, SessionID: sessionID}, false, nil
	default:
		return startupProjectionRepair{}, false, fatalStartup("projection %s has unknown state", record.JobID)
	}
}

func repairStartupProjectionsTx(tx repository.WriteTx, repairs []startupProjectionRepair, generation uint64) error {
	for _, repair := range repairs {
		if !repair.KnownBroken {
			continue
		}
		if err := putProjectionFromSafetyTx(tx, repair.Record, repair.SessionID, repair.Diagnostic, repair.Quarantine, generation); err != nil {
			return err
		}
	}
	return nil
}

func putProjectionFromSafetyTx(tx repository.WriteTx, record model.SafetyRecord, sessionID string, diagnostic string, quarantine bool, generation uint64) error {
	projection, err := model.Project(record, model.ProjectionMetadata{SessionID: sessionID})
	if err != nil && sessionID != "" {
		sessionID = ""
		projection, err = model.Project(record, model.ProjectionMetadata{})
	}
	if err != nil {
		return err
	}
	if quarantine {
		if diagnostic == "" {
			diagnostic = "projection corrupt"
		}
		if err := tx.PutQuarantine(repository.QuarantineRecord{
			JobID:      record.JobID,
			Diagnostic: diagnostic,
			Generation: generation,
		}); err != nil {
			return err
		}
	}
	return tx.PutProjection(projection)
}

func runtimeRegistryForBootTx(tx repository.ReadTx, boot model.BootRef) (*runtimeRegistry, error) {
	images, err := tx.ListJobs(repository.JobFilter{})
	if err != nil {
		return nil, err
	}
	registry := newRuntimeRegistry()
	for _, image := range images {
		if image.Safety.State != repository.RecordValid {
			continue
		}
		record := image.Safety.Value
		if record.AdmittedBy != boot || record.Terminal != nil {
			continue
		}
		if err := registry.registerPending(record.Attempt.Ref); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func applyRecoveryCommandTx(tx repository.WriteTx, jobID model.JobID, command model.Command, nextGeneration uint64) (ApplyResult, error) {
	image := tx.LoadJob(jobID)
	if err := requireRecord("safety", image.Safety.State, image.Safety.Diagnostic); err != nil {
		return ApplyResult{}, err
	}
	record := image.Safety.Value
	if err := model.ValidateSafetyRecord(record); err != nil {
		return ApplyResult{}, fatalStartup("safety %s is unsupported: %v", record.JobID, err)
	}
	repair, needsRepair, err := projectionRepair(record, image.Projection)
	if err != nil {
		return ApplyResult{}, err
	}
	sessionID := repair.SessionID
	applied, err := applyLogicalCommand(record, command)
	if err != nil {
		return ApplyResult{}, err
	}
	if !applied.Changed {
		if !needsRepair {
			projection := image.Projection.Value
			return ApplyResult{Record: record, Projection: projection}, nil
		}
		if err := putProjectionFromSafetyTx(tx, record, sessionID, repair.Diagnostic, repair.Quarantine, nextGeneration); err != nil {
			return ApplyResult{}, err
		}
		projection, err := model.Project(record, model.ProjectionMetadata{SessionID: sessionID})
		if err != nil {
			projection, err = model.Project(record, model.ProjectionMetadata{})
			if err != nil {
				return ApplyResult{}, err
			}
		}
		return ApplyResult{Record: record, Projection: projection}, nil
	}
	nextProjection, err := model.Project(applied.Record, model.ProjectionMetadata{SessionID: sessionID})
	if err != nil {
		nextProjection, err = model.Project(applied.Record, model.ProjectionMetadata{})
		if err != nil {
			return ApplyResult{}, err
		}
	}
	if err := tx.PutSafety(applied.Record, record.Revision); err != nil {
		return ApplyResult{}, err
	}
	if repair.Quarantine {
		if err := tx.PutQuarantine(repository.QuarantineRecord{
			JobID:      record.JobID,
			Diagnostic: repair.Diagnostic,
			Generation: nextGeneration,
		}); err != nil {
			return ApplyResult{}, err
		}
	}
	if err := tx.PutProjection(nextProjection); err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{Record: applied.Record, Projection: nextProjection, Changed: true}, nil
}

func applyRecoveryQuiescenceTx(tx repository.WriteTx, jobID model.JobID, ordinal model.LaunchOrdinal, verifier custodian.AttestationVerifier, verified custodian.VerifiedQuiescence, boot model.BootRef, nextGeneration uint64) (ApplyResult, error) {
	image := tx.LoadJob(jobID)
	if err := requireRecord("safety", image.Safety.State, image.Safety.Diagnostic); err != nil {
		return ApplyResult{}, err
	}
	record := image.Safety.Value
	if err := model.ValidateSafetyRecord(record); err != nil {
		return ApplyResult{}, fatalStartup("safety %s is unsupported: %v", record.JobID, err)
	}
	repair, needsRepair, err := projectionRepair(record, image.Projection)
	if err != nil {
		return ApplyResult{}, err
	}
	sessionID := repair.SessionID
	certificate, err := verifyQuiescenceForRecord(record, ordinal, verifier, verified, boot)
	if err != nil {
		return ApplyResult{}, err
	}
	applied, err := applyVerifiedQuiescence(record, certificate)
	if err != nil {
		return ApplyResult{}, err
	}
	if !applied.Changed {
		if !needsRepair {
			projection := image.Projection.Value
			return ApplyResult{Record: record, Projection: projection}, nil
		}
		if err := putProjectionFromSafetyTx(tx, record, sessionID, repair.Diagnostic, repair.Quarantine, nextGeneration); err != nil {
			return ApplyResult{}, err
		}
		projection, err := model.Project(record, model.ProjectionMetadata{SessionID: sessionID})
		if err != nil {
			projection, err = model.Project(record, model.ProjectionMetadata{})
			if err != nil {
				return ApplyResult{}, err
			}
		}
		return ApplyResult{Record: record, Projection: projection}, nil
	}
	nextProjection, err := model.Project(applied.Record, model.ProjectionMetadata{SessionID: sessionID})
	if err != nil {
		nextProjection, err = model.Project(applied.Record, model.ProjectionMetadata{})
		if err != nil {
			return ApplyResult{}, err
		}
	}
	if err := tx.PutSafety(applied.Record, record.Revision); err != nil {
		return ApplyResult{}, err
	}
	if repair.Quarantine {
		if err := tx.PutQuarantine(repository.QuarantineRecord{
			JobID:      record.JobID,
			Diagnostic: repair.Diagnostic,
			Generation: nextGeneration,
		}); err != nil {
			return ApplyResult{}, err
		}
	}
	if err := tx.PutProjection(nextProjection); err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{Record: applied.Record, Projection: nextProjection, Changed: true}, nil
}

func fatalStartup(format string, args ...any) error {
	return fmt.Errorf("%w: startup corruption: %s", repository.ErrInvalidRecord, fmt.Sprintf(format, args...))
}

func fatalCorruptStartup(format string, args ...any) error {
	return fmt.Errorf("%w: startup corruption: %s", repository.ErrCorruptRecord, fmt.Sprintf(format, args...))
}

func diagnosticOrDefault(diagnostic string) string {
	if diagnostic == "" {
		return "corrupt"
	}
	return diagnostic
}
