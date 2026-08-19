//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/schema"
)

const jobStoreFilename = "jobs.db"

// backendSetupProber is implemented by the retained CLI adapters. It performs
// the one live probe needed to create a preflight cache entry when setup has
// not populated one yet.
type backendSetupProber interface {
	SetupProbe(context.Context) (engine.BackendSetupProbe, error)
}

type jobSubmitInput struct {
	key               jobstore.RequestKey
	taskSpec          jcsValue
	canonicalTaskSpec []byte
}

// handleJobSubmit keeps idempotency ahead of all present-time backend and
// filesystem checks. In particular, SubmitTx does not invoke its factory for a
// matching replay, so the code below never probes or resolves cwd for one.
func (s *Server) handleJobSubmit(ctx context.Context, raw json.RawMessage) requestOutcome {
	if ctx == nil {
		ctx = context.Background()
	}

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
		if err := s.ensureBackendAvailable(ctx, spec.Backend); err != nil {
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
			return s.jobSubmitConflict(store, err)
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
	if _, _, errObj := timeoutFromMillis(spec.TimeoutMS); errObj != nil {
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
	_, timeout, errObj := timeoutFromMillis(spec.TimeoutMS)
	if errObj != nil {
		return nil, errors.New(errObj.Message)
	}
	return timeout, nil
}

func (s *Server) jobSubmitConflict(store *jobstore.Store, submitErr error) requestOutcome {
	var conflict *jobstore.ConflictError
	if !errors.As(submitErr, &conflict) {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, submitErr.Error(), protocol.ErrorData{})}
	}
	data := protocol.ErrorData{JobID: conflict.ExistingJobID}
	// A conflict must not use an invalid incoming TaskSpec to resolve timeout.
	// Its stored canonical TaskSpec remains the source of that resolution, even
	// though JSON-RPC's error-only conflict response deliberately exposes no
	// result object.
	if record, err := store.Get(conflict.ExistingJobID); err == nil {
		if _, err := timeoutFromStoredTaskSpec(record.TaskSpec); err != nil {
			return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "stored taskSpec timeout: "+err.Error(), data)}
		}
	}
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

func (s *Server) ensureBackendAvailable(ctx context.Context, name string) error {
	backend, ok := s.backends[name]
	if !ok {
		return fmt.Errorf("backend %q is unavailable", name)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.backendProbeMu.Lock()
	defer s.backendProbeMu.Unlock()

	cachePath, err := engine.SetupProbeCachePath(s.stateRoot)
	if err != nil {
		return err
	}
	cache, cacheErr := engine.ReadSetupProbeCache(cachePath)
	needsProbe := cacheErr != nil || !cacheHasBackend(cache, name)
	probed := false
	if needsProbe {
		if err := s.probeAndCacheBackend(ctx, name, backend, cache, cacheErr); err != nil {
			return err
		}
		probed = true
	}
	if _, err := backend.Preflight(ctx); err != nil {
		if probed {
			return err
		}
		if err := s.probeAndCacheBackend(ctx, name, backend, cache, cacheErr); err != nil {
			return err
		}
		if _, err := backend.Preflight(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) probeAndCacheBackend(ctx context.Context, name string, backend engine.Backend, cache engine.SetupProbeCache, cacheErr error) error {
	prober, ok := backend.(backendSetupProber)
	if !ok {
		return nil
	}
	probe, err := prober.SetupProbe(ctx)
	if err != nil {
		return err
	}
	if probe.Backend == "" {
		probe.Backend = name
	}
	if probe.Backend != name {
		return fmt.Errorf("backend %q setup probe returned %q", name, probe.Backend)
	}
	if cacheErr != nil || cache.Version != engine.SetupProbeCacheVersion {
		cache = engine.SetupProbeCache{Version: engine.SetupProbeCacheVersion}
	}
	cache.Backends = replaceProbe(cache.Backends, probe)
	cachePath, err := engine.SetupProbeCachePath(s.stateRoot)
	if err != nil {
		return err
	}
	return engine.WriteSetupProbeCache(cachePath, cache)
}

func cacheHasBackend(cache engine.SetupProbeCache, name string) bool {
	if cache.Version != engine.SetupProbeCacheVersion {
		return false
	}
	for _, probe := range cache.Backends {
		if probe.Backend == name {
			return true
		}
	}
	return false
}

func replaceProbe(probes []engine.BackendSetupProbe, replacement engine.BackendSetupProbe) []engine.BackendSetupProbe {
	result := make([]engine.BackendSetupProbe, 0, len(probes)+1)
	for _, probe := range probes {
		if probe.Backend != replacement.Backend {
			result = append(result, probe)
		}
	}
	return append(result, replacement)
}

func timeoutFromMillis(ms *int64) (time.Duration, *engine.TimeoutResolution, *protocol.ErrorObject) {
	if ms == nil {
		return protocol.DefaultTimeout, &engine.TimeoutResolution{
			Effective: protocol.DefaultTimeout.Milliseconds(),
			Source:    engine.TimeoutSourceDaemonDefault,
		}, nil
	}
	if *ms < 0 {
		return 0, nil, protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "timeoutMs cannot be negative", protocol.ErrorData{})
	}
	if *ms == 0 {
		return 0, &engine.TimeoutResolution{
			Requested: ms,
			Effective: 0,
			Source:    engine.TimeoutSourceClient,
		}, nil
	}
	if *ms > protocol.MaxTimeout.Milliseconds() {
		return 0, nil, protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "timeoutMs exceeds maximum", protocol.ErrorData{})
	}
	duration := time.Duration(*ms) * time.Millisecond
	return duration, &engine.TimeoutResolution{
		Requested: ms,
		Effective: duration.Milliseconds(),
		Source:    engine.TimeoutSourceClient,
	}, nil
}
