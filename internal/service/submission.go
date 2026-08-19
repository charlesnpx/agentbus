//go:build darwin || linux

package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/schema"
)

const jobStoreFilename = "jobs.db"

type jobSubmitInput struct {
	key               jobstore.RequestKey
	taskSpec          jcsValue
	canonicalTaskSpec []byte
}

// handleJobSubmit keeps idempotency ahead of all present-time backend and
// filesystem checks. In particular, SubmitTx does not invoke its factory for a
// matching replay, so the code below never validates a new record or resolves
// its cwd for one.
func (s *Server) handleJobSubmit(raw json.RawMessage) requestOutcome {
	input, err := parseJobSubmitInput(raw)
	if err != nil {
		return invalidParams(err)
	}
	// This is deliberately the only identity validation before hashing. It
	// neither resolves cwd nor checks whether the requested backend exists.
	if err := input.key.Validate(); err != nil {
		return invalidParams(err)
	}

	store, err := s.ensureJobStore()
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailableV3, "open job store: "+err.Error(), protocol.ErrorData{})}
	}
	record, deduplicated, err := store.SubmitTx(input.key, input.canonicalTaskSpec, func(id string) (jobstore.Record, error) {
		// Everything in this factory is new-key-only: SubmitTx has already
		// compared the canonical TaskSpec hash with a durable binding.
		spec, err := decodeTaskSpec(input.canonicalTaskSpec)
		if err != nil {
			return jobstore.Record{}, err
		}
		if err := validateNewTaskSpec(spec, input.taskSpec); err != nil {
			return jobstore.Record{}, err
		}
		if err := s.ensureConfiguredBackend(spec.Backend); err != nil {
			return jobstore.Record{}, err
		}
		canonicalCWD, err := engine.CanonicalWorkspace(spec.CWD)
		if err != nil {
			return jobstore.Record{}, err
		}
		return jobstore.Record{
			JobID:        id,
			WorkspaceKey: input.key.WorkspaceKey,
			RequestID:    input.key.RequestID,
			Backend:      spec.Backend,
			Model:        taskSpecOptionalString(spec.Model),
			CWD:          canonicalCWD,
			Write:        spec.Write,
			Effort:       taskSpecOptionalString(spec.Effort),
		}, nil
	})
	if err != nil {
		if errors.Is(err, jobstore.ErrConflict) {
			return s.jobSubmitConflict(err)
		}
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "submit job: "+err.Error(), protocol.ErrorData{})}
	}
	timeout, err := timeoutFromStoredTaskSpec(record.TaskSpec)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "stored taskSpec timeout: "+err.Error(), protocol.ErrorData{JobID: record.JobID})}
	}
	return requestOutcome{result: jobSubmitResult(record, deduplicated, timeout)}
}

// parseJobSubmitInput performs only the JSON work needed to derive the
// immutable TaskSpec hash and validate the compound identity. In particular,
// TaskSpec semantics intentionally remain in SubmitTx's new-record factory.
func parseJobSubmitInput(raw json.RawMessage) (jobSubmitInput, error) {
	root, err := parseJCSJSON(raw)
	if err != nil {
		return jobSubmitInput{}, err
	}
	if root.kind != jcsObject {
		return jobSubmitInput{}, fmt.Errorf("params must be a JSON object")
	}

	input := jobSubmitInput{}
	hasTaskSpec := false
	for _, member := range root.object {
		switch member.name {
		case "workspaceKey":
			if member.value.kind != jcsString {
				return jobSubmitInput{}, fmt.Errorf("workspaceKey must be a string")
			}
			input.key.WorkspaceKey = member.value.string
		case "requestId":
			if member.value.kind != jcsString {
				return jobSubmitInput{}, fmt.Errorf("requestId must be a string")
			}
			input.key.RequestID = member.value.string
		case "taskSpec":
			input.taskSpec = member.value
			hasTaskSpec = true
		default:
			return jobSubmitInput{}, fmt.Errorf("json: unknown field %q", member.name)
		}
	}
	if !hasTaskSpec {
		return jobSubmitInput{}, fmt.Errorf("taskSpec is required")
	}
	if input.taskSpec.kind != jcsObject {
		return jobSubmitInput{}, fmt.Errorf("taskSpec must be an object")
	}
	for _, member := range input.taskSpec.object {
		switch member.name {
		case "backend", "cwd", "write", "prompt", "model", "effort", "outputSchema", "tags", "timeoutMs":
		default:
			return jobSubmitInput{}, fmt.Errorf("json: unknown field %q", member.name)
		}
	}
	input.canonicalTaskSpec, err = canonicalJCSJSON(input.taskSpec)
	if err != nil {
		return jobSubmitInput{}, err
	}
	return input, nil
}

func decodeTaskSpec(raw json.RawMessage) (protocol.TaskSpecV3, error) {
	var spec protocol.TaskSpecV3
	if err := decodeStrict(raw, &spec); err != nil {
		return protocol.TaskSpecV3{}, err
	}
	return spec, nil
}

func validateNewTaskSpec(spec protocol.TaskSpecV3, raw jcsValue) error {
	for _, required := range []string{"backend", "cwd", "write", "prompt"} {
		value, present := raw.objectMember(required)
		if !present || value.kind == jcsNull {
			return fmt.Errorf("taskSpec missing required field %s", required)
		}
	}
	for _, optional := range []string{"model", "effort", "outputSchema", "tags", "timeoutMs"} {
		value, present := raw.objectMember(optional)
		if present && value.kind == jcsNull {
			return fmt.Errorf("taskSpec.%s cannot be null", optional)
		}
	}
	if spec.Backend == "" || spec.CWD == "" || !filepath.IsAbs(spec.CWD) || spec.Prompt == "" {
		return fmt.Errorf("taskSpec requires backend, absolute cwd, write, and prompt")
	}
	if _, errObj := timeoutFromMillis(spec.TimeoutMS); errObj != nil {
		return errors.New(errObj.Message)
	}
	if _, supplied := raw.objectMember("outputSchema"); supplied {
		// Validate compiles Draft 2020-12 schemas, including boolean roots,
		// and rejects non-schema roots and non-2020-12 declared dialects.
		// "null" is a valid JSON instance, so only a schema compilation
		// error matters here.
		if _, err := schema.Validate("null", spec.OutputSchema); err != nil {
			return fmt.Errorf("outputSchema: %w", err)
		}
	}
	return nil
}

// taskSpecOptionalString supplies Record's summary fields. Presence remains
// authoritative in the canonical task bytes that SubmitTx persists unchanged.
func taskSpecOptionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func jobSubmitResult(record jobstore.Record, deduplicated bool, timeout *engine.TimeoutResolution) protocol.JobSubmitResultV3 {
	return protocol.JobSubmitResultV3{
		JobID:        record.JobID,
		State:        record.State,
		Deduplicated: deduplicated,
		Timeout:      engine.CloneTimeoutResolution(timeout),
	}
}

func timeoutFromStoredTaskSpec(raw json.RawMessage) (*engine.TimeoutResolution, error) {
	spec, err := decodeTaskSpec(raw)
	if err != nil {
		return nil, err
	}
	timeout, errObj := timeoutFromMillis(spec.TimeoutMS)
	if errObj != nil {
		return nil, errors.New(errObj.Message)
	}
	return timeout, nil
}

func (s *Server) jobSubmitConflict(submitErr error) requestOutcome {
	var conflict *jobstore.ConflictError
	if !errors.As(submitErr, &conflict) {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, submitErr.Error(), protocol.ErrorData{})}
	}
	data := protocol.ErrorData{JobID: conflict.ExistingJobID}
	// protocol v3 has no distinct conflict error code. Keep the typed store
	// conflict visible as an invalid-task response without violating JSON-RPC's
	// result XOR error response invariant.
	return requestOutcome{
		err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, submitErr.Error(), data),
	}
}

func (s *Server) ensureJobStore() (*jobstore.Store, error) {
	if s == nil {
		return nil, errors.New("nil service server")
	}
	s.jobStoreMu.Lock()
	defer s.jobStoreMu.Unlock()
	if s.jobStore != nil {
		return s.jobStore, nil
	}
	store, err := jobstore.Open(filepath.Join(s.stateRoot, jobStoreFilename))
	if err != nil {
		return nil, err
	}
	s.jobStore = store
	return store, nil
}

func (s *Server) closeJobStore() {
	if s == nil {
		return
	}
	s.jobStoreMu.Lock()
	store := s.jobStore
	s.jobStore = nil
	s.jobStoreMu.Unlock()
	if store != nil {
		_ = store.Close()
	}
}

// ensureConfiguredBackend deliberately does only a map lookup. Live backend
// checks belong after the job has been durably admitted.
func (s *Server) ensureConfiguredBackend(name string) error {
	if _, ok := s.backends[name]; !ok {
		return fmt.Errorf("backend %q is unavailable", name)
	}
	return nil
}

func timeoutFromMillis(ms *int64) (*engine.TimeoutResolution, *protocol.ErrorObject) {
	if ms == nil {
		return &engine.TimeoutResolution{
			Effective: protocol.DefaultTimeout.Milliseconds(),
			Source:    engine.TimeoutSourceDaemonDefault,
		}, nil
	}
	if *ms < 0 {
		return nil, protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "timeoutMs cannot be negative", protocol.ErrorData{})
	}
	if *ms == 0 {
		return &engine.TimeoutResolution{
			Requested: ms,
			Effective: 0,
			Source:    engine.TimeoutSourceClient,
		}, nil
	}
	if *ms > protocol.MaxTimeout.Milliseconds() {
		return nil, protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "timeoutMs exceeds maximum", protocol.ErrorData{})
	}
	return &engine.TimeoutResolution{
		Requested: ms,
		Effective: *ms,
		Source:    engine.TimeoutSourceClient,
	}, nil
}
