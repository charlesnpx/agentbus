//go:build darwin || linux

package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const jobStoreFilename = "jobs.db"

// backendSetupProber is implemented by the retained CLI adapters. It performs
// the one live probe needed to create a preflight cache entry when setup has
// not populated one yet.
type backendSetupProber interface {
	SetupProbe(context.Context) (engine.BackendSetupProbe, error)
}

type jobSubmitPrecheck struct {
	rawTaskSpec json.RawMessage
}

// handleJobSubmit keeps idempotency ahead of all present-time backend and
// filesystem checks. In particular, SubmitTx does not invoke its factory for a
// matching replay, so the code below never probes or resolves cwd for one.
func (s *Server) handleJobSubmit(ctx context.Context, raw json.RawMessage) requestOutcome {
	if ctx == nil {
		ctx = context.Background()
	}

	var params protocol.JobSubmitParamsV3
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	precheck, errObj := jobSubmitRawPrecheck(raw)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	if errObj := validateTaskSpecEnvelope(raw); errObj != nil {
		return requestOutcome{err: errObj}
	}

	key := jobstore.RequestKey{
		WorkspaceKey: params.WorkspaceKey,
		RequestID:    params.RequestID,
	}
	// This is deliberately the only identity validation before hashing. It
	// neither resolves cwd nor checks whether the requested backend exists.
	if err := key.Validate(); err != nil {
		return invalidParams(err)
	}
	canonicalTaskSpec, err := model.CanonicalTaskSpecJSON(precheck.rawTaskSpec)
	if err != nil {
		return invalidParams(err)
	}
	if errObj := validateStaticTaskSpec(params.TaskSpec); errObj != nil {
		return requestOutcome{err: errObj}
	}
	_, timeout, errObj := timeoutFromMillis(params.TaskSpec.TimeoutMS)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}

	store, err := s.ensureJobStore()
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailableV3, "open job store: "+err.Error(), protocol.ErrorData{})}
	}
	record, deduplicated, err := store.SubmitTx(key, canonicalTaskSpec, func(id string) (jobstore.Record, error) {
		if err := s.ensureBackendAvailable(ctx, params.TaskSpec.Backend); err != nil {
			return jobstore.Record{}, err
		}
		canonicalCWD, err := engine.CanonicalWorkspace(params.TaskSpec.CWD)
		if err != nil {
			return jobstore.Record{}, err
		}
		return jobstore.Record{
			JobID:        id,
			WorkspaceKey: key.WorkspaceKey,
			RequestID:    key.RequestID,
			Backend:      params.TaskSpec.Backend,
			Model:        taskSpecOptionalString(params.TaskSpec.Model),
			CWD:          canonicalCWD,
			Write:        params.TaskSpec.Write,
			Effort:       taskSpecOptionalString(params.TaskSpec.Effort),
		}, nil
	})
	if err != nil {
		if errors.Is(err, jobstore.ErrConflict) {
			return s.jobSubmitConflict(store, err, timeout)
		}
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "submit job: "+err.Error(), protocol.ErrorData{})}
	}
	if deduplicated {
		return requestOutcome{result: jobSubmitResult(record, true, timeout)}
	}
	return requestOutcome{result: jobSubmitResult(record, false, timeout)}
}

func jobSubmitRawPrecheck(raw json.RawMessage) (jobSubmitPrecheck, *protocol.ErrorObject) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return jobSubmitPrecheck{}, protocol.NewError(protocol.ErrorInvalidTaskSpecV3, err.Error(), protocol.ErrorData{})
	}
	rawTaskSpec, ok := envelope["taskSpec"]
	if !ok {
		return jobSubmitPrecheck{}, protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "taskSpec is required", protocol.ErrorData{})
	}
	return jobSubmitPrecheck{rawTaskSpec: append(json.RawMessage(nil), rawTaskSpec...)}, nil
}

func validateStaticTaskSpec(spec protocol.TaskSpecV3) *protocol.ErrorObject {
	if spec.Backend == "" || spec.CWD == "" || !filepath.IsAbs(spec.CWD) || spec.Prompt == "" {
		return protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "taskSpec requires backend, absolute cwd, write, and prompt", protocol.ErrorData{Backend: spec.Backend})
	}
	_, _, errObj := timeoutFromMillis(spec.TimeoutMS)
	return errObj
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

func (s *Server) jobSubmitConflict(store *jobstore.Store, submitErr error, timeout *engine.TimeoutResolution) requestOutcome {
	var conflict *jobstore.ConflictError
	if !errors.As(submitErr, &conflict) {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, submitErr.Error(), protocol.ErrorData{})}
	}
	data := protocol.ErrorData{JobID: conflict.ExistingJobID}
	result := protocol.JobSubmitResultV3{JobID: conflict.ExistingJobID, Timeout: engine.CloneTimeoutResolution(timeout)}
	if record, err := store.Get(conflict.ExistingJobID); err == nil {
		result.State = record.State
	}
	// protocol v3 has no distinct conflict error code. Keep the typed store
	// conflict visible as an invalid-task response, while retaining a complete
	// timeout result for clients that need it on every submit path.
	return requestOutcome{
		result: result,
		err:    protocol.NewError(protocol.ErrorInvalidTaskSpecV3, submitErr.Error(), data),
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
	cache, fingerprint, cacheErr := readSetupProbeCache(cachePath)
	previousFingerprint, observed := s.backendProbeFingerprints[name]
	needsProbe := cacheErr != nil || !cacheHasBackend(cache, name) || (observed && previousFingerprint != fingerprint)
	probed := false
	if needsProbe {
		if err := s.probeAndCacheBackend(ctx, name, backend, cache, cacheErr); err != nil {
			return err
		}
		probed = true
		cache, fingerprint, cacheErr = readSetupProbeCache(cachePath)
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
		cache, fingerprint, cacheErr = readSetupProbeCache(cachePath)
	}
	if cacheErr == nil && cacheHasBackend(cache, name) {
		s.backendProbeFingerprints[name] = fingerprint
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

func readSetupProbeCache(path string) (engine.SetupProbeCache, string, error) {
	cache, err := engine.ReadSetupProbeCache(path)
	if err != nil {
		return engine.SetupProbeCache{}, "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return engine.SetupProbeCache{}, "", err
	}
	fingerprint := sha256.Sum256(raw)
	return cache, fmt.Sprintf("%x", fingerprint), nil
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

func validateTaskSpecEnvelope(raw json.RawMessage) *protocol.ErrorObject {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return protocol.NewError(protocol.ErrorInvalidTaskSpecV3, err.Error(), protocol.ErrorData{})
	}
	specRaw, ok := envelope["taskSpec"]
	if !ok {
		return protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "taskSpec is required", protocol.ErrorData{})
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(specRaw, &fields); err != nil {
		return protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "taskSpec must be an object", protocol.ErrorData{})
	}
	for _, required := range []string{"backend", "cwd", "write", "prompt"} {
		if _, ok := fields[required]; !ok {
			return protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "taskSpec missing required field "+required, protocol.ErrorData{})
		}
	}
	return nil
}
