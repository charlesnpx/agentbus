//go:build darwin || linux

package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jcs"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/schema"
)

const jobStoreFilename = "jobs.db"

type jobSubmitInput struct {
	key               jobstore.RequestKey
	taskSpec          map[string]json.RawMessage
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
		return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailable, "open job store: "+err.Error(), protocol.ErrorData{})}
	}
	var executionBackend engine.Backend
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
		// Production Server instances never mutate their private configured
		// backend map after New. Capture the admitted adapter with this new job
		// so execution does not repeat a backend map lookup after queueing.
		backend, ok := s.backends[spec.Backend]
		if !ok || backend == nil {
			return jobstore.Record{}, fmt.Errorf("backend %q is unavailable", spec.Backend)
		}
		executionBackend = backend
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
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "submit job: "+err.Error(), protocol.ErrorData{})}
	}
	timeout, err := timeoutFromStoredTaskSpec(record.TaskSpec)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "stored taskSpec timeout: "+err.Error(), protocol.ErrorData{JobID: record.JobID})}
	}
	// Only a live daemon owns new execution, and only a genuinely new job is
	// enqueued: a deduplicated replay is already running or terminal, so
	// enqueueing it again would be the second launch the ordering forbids.
	if !deduplicated {
		s.enqueueQueuedJob(record, executionBackend)
	}
	return requestOutcome{result: jobSubmitResult(record, deduplicated, timeout)}
}

// parseJobSubmitInput performs only the JSON work needed to derive the
// immutable TaskSpec hash and validate the compound identity. In particular,
// TaskSpec semantics intentionally remain in SubmitTx's new-record factory.
func parseJobSubmitInput(raw json.RawMessage) (jobSubmitInput, error) {
	canonicalParams, err := jcs.Render(raw)
	if err != nil {
		return jobSubmitInput{}, err
	}
	if len(canonicalParams) == 0 || canonicalParams[0] != '{' {
		return jobSubmitInput{}, fmt.Errorf("params must be a JSON object")
	}

	var root map[string]json.RawMessage
	if err := json.Unmarshal(canonicalParams, &root); err != nil {
		return jobSubmitInput{}, err
	}

	input := jobSubmitInput{}
	hasTaskSpec := false
	var taskSpecRaw json.RawMessage
	for name, value := range root {
		switch name {
		case "workspaceKey":
			if len(value) == 0 || value[0] != '"' {
				return jobSubmitInput{}, fmt.Errorf("workspaceKey must be a string")
			}
			if err := json.Unmarshal(value, &input.key.WorkspaceKey); err != nil {
				return jobSubmitInput{}, err
			}
		case "requestId":
			if len(value) == 0 || value[0] != '"' {
				return jobSubmitInput{}, fmt.Errorf("requestId must be a string")
			}
			if err := json.Unmarshal(value, &input.key.RequestID); err != nil {
				return jobSubmitInput{}, err
			}
		case "taskSpec":
			taskSpecRaw = value
			hasTaskSpec = true
		default:
			return jobSubmitInput{}, fmt.Errorf("json: unknown field %q", name)
		}
	}
	if !hasTaskSpec {
		return jobSubmitInput{}, fmt.Errorf("taskSpec is required")
	}
	if len(taskSpecRaw) == 0 || taskSpecRaw[0] != '{' {
		return jobSubmitInput{}, fmt.Errorf("taskSpec must be an object")
	}
	if err := json.Unmarshal(taskSpecRaw, &input.taskSpec); err != nil {
		return jobSubmitInput{}, err
	}
	for name := range input.taskSpec {
		switch name {
		case "backend", "cwd", "write", "prompt", "model", "effort", "outputSchema", "tags", "timeoutMs":
		default:
			return jobSubmitInput{}, fmt.Errorf("json: unknown field %q", name)
		}
	}
	input.canonicalTaskSpec = taskSpecRaw
	return input, nil
}

func decodeTaskSpec(raw json.RawMessage) (protocol.TaskSpec, error) {
	var spec protocol.TaskSpec
	if err := decodeStrict(raw, &spec); err != nil {
		return protocol.TaskSpec{}, err
	}
	return spec, nil
}

func validateNewTaskSpec(spec protocol.TaskSpec, raw map[string]json.RawMessage) error {
	for _, required := range []string{"backend", "cwd", "write", "prompt"} {
		value, present := raw[required]
		if !present || string(value) == "null" {
			return fmt.Errorf("taskSpec missing required field %s", required)
		}
	}
	for _, optional := range []string{"model", "effort", "outputSchema", "tags", "timeoutMs"} {
		value, present := raw[optional]
		if present && string(value) == "null" {
			return fmt.Errorf("taskSpec.%s cannot be null", optional)
		}
	}
	if spec.Backend == "" || spec.CWD == "" || !filepath.IsAbs(spec.CWD) || spec.Prompt == "" {
		return fmt.Errorf("taskSpec requires backend, absolute cwd, write, and prompt")
	}
	if _, errObj := timeoutFromMillis(spec.TimeoutMS); errObj != nil {
		return errors.New(errObj.Message)
	}
	if _, supplied := raw["outputSchema"]; supplied {
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

func jobSubmitResult(record jobstore.Record, deduplicated bool, timeout *engine.TimeoutResolution) protocol.JobSubmitResult {
	return protocol.JobSubmitResult{
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
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, submitErr.Error(), protocol.ErrorData{})}
	}
	data := protocol.ErrorData{JobID: conflict.ExistingJobID}
	// protocol v3 has no distinct conflict error code. Keep the typed store
	// conflict visible as an invalid-task response without violating JSON-RPC's
	// result XOR error response invariant.
	return requestOutcome{
		err: protocol.NewError(protocol.ErrorInvalidTaskSpec, submitErr.Error(), data),
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
	s.stopExecutions()
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
		return nil, protocol.NewError(protocol.ErrorInvalidTaskSpec, "timeoutMs cannot be negative", protocol.ErrorData{})
	}
	if *ms == 0 {
		return &engine.TimeoutResolution{
			Requested: ms,
			Effective: 0,
			Source:    engine.TimeoutSourceClient,
		}, nil
	}
	if *ms > protocol.MaxTimeout.Milliseconds() {
		return nil, protocol.NewError(protocol.ErrorInvalidTaskSpec, "timeoutMs exceeds maximum", protocol.ErrorData{})
	}
	return &engine.TimeoutResolution{
		Requested: ms,
		Effective: *ms,
		Source:    engine.TimeoutSourceClient,
	}, nil
}
