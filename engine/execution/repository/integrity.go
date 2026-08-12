package repository

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type IntegrityFinding struct {
	Kind string
	Key  string
	Err  error
}

func (finding IntegrityFinding) Error() string {
	if finding.Kind == "" && finding.Key == "" {
		return finding.Err.Error()
	}
	if finding.Key == "" {
		return fmt.Sprintf("%s: %v", finding.Kind, finding.Err)
	}
	return fmt.Sprintf("%s %s: %v", finding.Kind, finding.Key, finding.Err)
}

func (finding IntegrityFinding) Unwrap() error {
	return finding.Err
}

type IntegrityError struct {
	Findings []error
}

func (err IntegrityError) Error() string {
	switch len(err.Findings) {
	case 0:
		return "repository integrity error"
	case 1:
		return fmt.Sprintf("repository integrity error: %v", err.Findings[0])
	default:
		return fmt.Sprintf("repository integrity error: %d findings", len(err.Findings))
	}
}

func (err IntegrityError) Unwrap() []error {
	return append([]error(nil), err.Findings...)
}

func NewIntegrityError(findings []error) error {
	filtered := make([]error, 0, len(findings))
	for _, finding := range findings {
		if finding != nil {
			filtered = append(filtered, finding)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return IntegrityError{Findings: filtered}
}

func NewIntegrityFinding(kind, key string, err error) error {
	if err == nil {
		return nil
	}
	return IntegrityFinding{Kind: kind, Key: key, Err: err}
}

func IntegrityFindingKinds(err error) []string {
	var kinds []string
	collectIntegrityFindingKinds(err, &kinds)
	return kinds
}

func collectIntegrityFindingKinds(err error, kinds *[]string) (bool, bool) {
	if err == nil {
		return false, false
	}
	switch typed := err.(type) {
	case IntegrityError:
		return collectIntegrityErrorFindingKinds(typed.Findings, kinds)
	case IntegrityFinding:
		classified, unclassified := collectIntegrityFindingKinds(typed.Err, kinds)
		if !classified || unclassified {
			*kinds = append(*kinds, integrityFindingKind(typed.Kind, typed.Err))
		}
		return true, false
	}
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		return collectIntegrityErrorFindingKinds(multi.Unwrap(), kinds)
	}
	if single, ok := err.(interface{ Unwrap() error }); ok {
		classified, unclassified := collectIntegrityFindingKinds(single.Unwrap(), kinds)
		if classified || unclassified {
			return classified, unclassified
		}
	}
	if kind := integrityFindingKind("", err); kind != "" {
		*kinds = append(*kinds, kind)
		return true, false
	}
	if errors.Is(err, ErrCorruptRecord) || errors.Is(err, ErrInvalidRecord) || errors.Is(err, ErrConflict) {
		return false, true
	}
	return false, false
}

func collectIntegrityErrorFindingKinds(findings []error, kinds *[]string) (bool, bool) {
	classified := false
	unclassified := false
	for _, finding := range findings {
		findingClassified, findingUnclassified := collectIntegrityFindingKinds(finding, kinds)
		classified = classified || findingClassified
		unclassified = unclassified || findingUnclassified
	}
	return classified, unclassified
}

func integrityFindingKind(kind string, err error) string {
	if kind != "" {
		return kind
	}
	var corrupt CorruptRecordKindError
	if errors.As(err, &corrupt) {
		return corrupt.Kind
	}
	if errors.Is(err, ErrProjectionMismatch) {
		return "projection"
	}
	return kind
}

type CorruptRecordKindError struct {
	Kind       string
	Key        string
	Diagnostic string
}

func (err CorruptRecordKindError) Error() string {
	if strings.TrimSpace(err.Diagnostic) == "" {
		err.Diagnostic = "corrupt"
	}
	if err.Key == "" {
		return fmt.Sprintf("%s: %s: %s", ErrCorruptRecord, err.Kind, err.Diagnostic)
	}
	return fmt.Sprintf("%s: %s %s: %s", ErrCorruptRecord, err.Kind, err.Key, err.Diagnostic)
}

func (err CorruptRecordKindError) Is(target error) bool {
	return target == ErrCorruptRecord
}

func ValidateProjectionShape(projection model.JobProjection) error {
	if projection.SchemaVersion == 0 {
		return fmt.Errorf("%w: projection.schema_version is required", ErrInvalidRecord)
	}
	if projection.Revision == 0 {
		return fmt.Errorf("%w: projection.revision is required", ErrInvalidRecord)
	}
	if err := projection.JobID.Validate(); err != nil {
		return fmt.Errorf("%w: projection.job_id: %v", ErrInvalidRecord, err)
	}
	if err := projection.RequestKey.Validate(); err != nil {
		return fmt.Errorf("%w: projection.request_key: %v", ErrInvalidRecord, err)
	}
	if err := projection.TaskIdentity.Validate(); err != nil {
		return fmt.Errorf("%w: projection.task_identity: %v", ErrInvalidRecord, err)
	}
	if err := projection.Mode.Validate(); err != nil {
		return fmt.Errorf("%w: projection.mode: %v", ErrInvalidRecord, err)
	}
	if !projection.Decision.Valid() {
		return fmt.Errorf("%w: projection.decision is unknown", ErrInvalidRecord)
	}
	if !projection.Dispatch.Valid() {
		return fmt.Errorf("%w: projection.dispatch is unknown", ErrInvalidRecord)
	}
	if !projection.Outcome.Valid() {
		return fmt.Errorf("%w: projection.outcome is unknown", ErrInvalidRecord)
	}
	if !projection.Public.Valid() {
		return fmt.Errorf("%w: projection.public is unknown", ErrInvalidRecord)
	}
	if projection.TerminalCause != 0 && !projection.TerminalCause.Valid() {
		return fmt.Errorf("%w: projection.terminal_cause is unknown", ErrInvalidRecord)
	}
	if err := model.ValidateFinalAttemptTiming(projection.FinalAttemptStartedAt, projection.FinalAttemptEndedAt); err != nil {
		return fmt.Errorf("%w: projection final attempt timing: %v", ErrInvalidRecord, err)
	}
	if (projection.FinalAttemptStartedAt != nil || projection.FinalAttemptEndedAt != nil) && projection.Decision != model.DecisionTerminal {
		return fmt.Errorf("%w: projection final attempt timing requires terminal decision", ErrInvalidRecord)
	}
	if err := model.ValidateFailureMetadata(projection.FailureClass, projection.FailureReason); err != nil {
		return fmt.Errorf("%w: projection failure metadata: %v", ErrInvalidRecord, err)
	}
	if projection.TransportFrameDrops != nil {
		if projection.TransportFrameDrops.Count == 0 || projection.TransportFrameDrops.Bytes == 0 || projection.TransportFrameDrops.RedactedPrefix == "" {
			return fmt.Errorf("%w: projection transport frame drops are invalid", ErrInvalidRecord)
		}
	}
	return nil
}

func ValidateProjectionMatches(projection model.JobProjection, record model.SafetyRecord) error {
	if err := ValidateProjectionShape(projection); err != nil {
		return err
	}
	expected, err := model.Project(record, model.ProjectionMetadata{SessionID: projection.SessionID})
	if err != nil {
		return fmt.Errorf("%w: project safety %s: %v", ErrInvalidRecord, record.JobID, err)
	}
	if !reflect.DeepEqual(projection, expected) {
		return fmt.Errorf("%w: projection %s does not match safety revision %d", ErrProjectionMismatch, projection.JobID, record.Revision)
	}
	return nil
}

func ValidateJobClosure(jobID model.JobID, image JobImage, lookupRequest func(model.RequestKey) RequestImage) error {
	if err := jobID.Validate(); err != nil {
		return fmt.Errorf("%w: job_id: %v", ErrInvalidRecord, err)
	}
	if image.Binding.State == RecordCorrupt {
		return corruptRecordError(bindingImageCorruptionKind(image.Binding.Diagnostic), jobID.String(), image.Binding.Diagnostic)
	}
	if image.Binding.State == RecordValid && image.Binding.Value.JobID != jobID {
		return fmt.Errorf("%w: binding index for job %s points to binding for job %s", ErrCorruptRecord, jobID, image.Binding.Value.JobID)
	}
	if image.Quarantine.State == RecordCorrupt {
		return corruptRecordError("quarantine", jobID.String(), image.Quarantine.Diagnostic)
	}
	if image.Quarantine.State == RecordValid {
		if image.Quarantine.Value.JobID != jobID {
			return fmt.Errorf("%w: quarantine key mismatch for %s", ErrInvalidRecord, jobID)
		}
		if err := image.Quarantine.Value.Validate(); err != nil {
			return err
		}
	}
	switch image.Safety.State {
	case RecordCorrupt:
		return corruptRecordError("safety", jobID.String(), image.Safety.Diagnostic)
	case RecordMissing:
		if image.Projection.State == RecordCorrupt {
			return corruptRecordError("projection", jobID.String(), image.Projection.Diagnostic)
		}
		if image.Projection.State == RecordValid {
			return fmt.Errorf("%w: projection %s has no safety record", ErrInvalidRecord, jobID)
		}
		if image.Binding.State == RecordValid {
			return fmt.Errorf("%w: binding for job %s has no safety record", ErrInvalidRecord, jobID)
		}
		return nil
	case RecordValid:
	default:
		return fmt.Errorf("%w: safety %s has unknown state", ErrInvalidRecord, jobID)
	}

	record := image.Safety.Value
	if record.JobID != jobID {
		return fmt.Errorf("%w: safety key mismatch for %s", ErrInvalidRecord, jobID)
	}
	if err := model.ValidateSafetyRecord(record); err != nil {
		return fmt.Errorf("%w: safety %s: %v", ErrInvalidRecord, jobID, err)
	}
	request := lookupRequest(record.RequestKey)
	if request.Tombstone.State == RecordCorrupt {
		return corruptRecordError("tombstone", record.RequestKey.String(), request.Tombstone.Diagnostic)
	}
	if request.Tombstone.State == RecordValid {
		return fmt.Errorf("%w: tombstone %s coexists with live safety %s", ErrConflict, record.RequestKey, jobID)
	}
	if request.Binding.State == RecordCorrupt {
		return corruptRecordError("binding", record.RequestKey.String(), request.Binding.Diagnostic)
	}
	if request.Binding.State != RecordValid {
		return fmt.Errorf("%w: safety %s has missing binding %s", ErrInvalidRecord, jobID, record.RequestKey)
	}
	if request.Binding.Value.JobID != jobID {
		return fmt.Errorf("%w: safety %s request binding points to %s", ErrInvalidRecord, jobID, request.Binding.Value.JobID)
	}
	if err := request.Binding.Value.Matches(record); err != nil {
		return fmt.Errorf("%w: binding %s: %v", ErrInvalidRecord, record.RequestKey, err)
	}
	switch image.Projection.State {
	case RecordCorrupt:
		return corruptRecordError("projection", jobID.String(), image.Projection.Diagnostic)
	case RecordMissing:
		return fmt.Errorf("%w: safety %s has no projection", ErrInvalidRecord, jobID)
	case RecordValid:
		if image.Projection.Value.JobID != jobID {
			return fmt.Errorf("%w: projection key mismatch for %s", ErrInvalidRecord, jobID)
		}
		if err := ValidateProjectionMatches(image.Projection.Value, record); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("%w: projection %s has unknown state", ErrInvalidRecord, jobID)
	}
}

func bindingImageCorruptionKind(diagnostic string) string {
	diagnostic = strings.ToLower(diagnostic)
	if strings.Contains(diagnostic, "binding_index") || strings.Contains(diagnostic, "binding index") {
		return "binding_index"
	}
	return "binding"
}

func ValidateRequestClosure(key model.RequestKey, image RequestImage, loadJob func(model.JobID) JobImage) error {
	if err := key.Validate(); err != nil {
		return fmt.Errorf("%w: request_key: %v", ErrInvalidRecord, err)
	}
	if image.Binding.State == RecordCorrupt {
		return corruptRecordError("binding", key.String(), image.Binding.Diagnostic)
	}
	if image.Tombstone.State == RecordCorrupt {
		return corruptRecordError("tombstone", key.String(), image.Tombstone.Diagnostic)
	}
	if image.Binding.State == RecordValid {
		binding := image.Binding.Value
		if binding.RequestKey != key {
			return fmt.Errorf("%w: binding key mismatch for %s", ErrInvalidRecord, key)
		}
		if err := binding.Validate(); err != nil {
			return fmt.Errorf("%w: binding %s: %v", ErrInvalidRecord, key, err)
		}
		if image.Tombstone.State == RecordValid {
			return fmt.Errorf("%w: request %s has live binding and tombstone", ErrConflict, key)
		}
		job := loadJob(binding.JobID)
		if job.Safety.State == RecordCorrupt {
			return corruptRecordError("safety", binding.JobID.String(), job.Safety.Diagnostic)
		}
		if job.Safety.State != RecordValid {
			return fmt.Errorf("%w: binding %s references missing safety record", ErrInvalidRecord, key)
		}
		if err := binding.Matches(job.Safety.Value); err != nil {
			return fmt.Errorf("%w: binding %s: %v", ErrInvalidRecord, key, err)
		}
	}
	if image.Tombstone.State == RecordValid {
		tombstone := image.Tombstone.Value
		if tombstone.RequestKey != key {
			return fmt.Errorf("%w: tombstone key mismatch for %s", ErrInvalidRecord, key)
		}
		if err := tombstone.Validate(); err != nil {
			return err
		}
		job := loadJob(tombstone.JobID)
		if job.Safety.State == RecordCorrupt {
			return corruptRecordError("safety", tombstone.JobID.String(), job.Safety.Diagnostic)
		}
		if job.Safety.State == RecordValid {
			return fmt.Errorf("%w: tombstoned job %s still has live safety", ErrConflict, tombstone.JobID)
		}
		if job.Projection.State == RecordCorrupt {
			return corruptRecordError("projection", tombstone.JobID.String(), job.Projection.Diagnostic)
		}
		if job.Projection.State == RecordValid {
			return fmt.Errorf("%w: tombstoned job %s still has live projection", ErrConflict, tombstone.JobID)
		}
	}
	return nil
}

func CorruptRecordError(kind, key, diagnostic string) error {
	return corruptRecordError(kind, key, diagnostic)
}

func corruptRecordError(kind, key, diagnostic string) error {
	if strings.TrimSpace(diagnostic) == "" {
		diagnostic = "corrupt"
	}
	return CorruptRecordKindError{Kind: kind, Key: key, Diagnostic: diagnostic}
}
