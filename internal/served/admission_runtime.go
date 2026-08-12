package served

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type servedAdmissionRuntime struct {
	runtime          custodian.Runtime
	launchCustodian  launch.CustodianPort
	supportOverride  *custodian.Support
	supportProbe     func(context.Context) custodian.Support
	verifierOverride custodian.AttestationVerifier
	closeHook        func() error
}

func newServedAdmissionRuntime(server *Server) *servedAdmissionRuntime {
	if server != nil && server.admissionRuntimeFactory != nil {
		if runtime := server.admissionRuntimeFactory(server); runtime != nil {
			return runtime
		}
	}
	if server == nil {
		return newServedAdmissionRuntimeFromRuntime(custodian.Runtime{})
	}
	return newServedAdmissionRuntimeFromRuntime(server.admissionRuntimeConfig)
}

func newServedAdmissionRuntimeFromRuntime(runtime custodian.Runtime) *servedAdmissionRuntime {
	if runtime.Process() == nil {
		runtime = custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable)
	}
	return &servedAdmissionRuntime{runtime: runtime}
}

func (s *servedAdmissionRuntime) verifiedContainmentSupported(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("%w: admission supervisor is nil", custodian.ErrSupervisorUnavailable)
	}
	support := s.support()
	if support.VerifiedContainment {
		return nil
	}
	if support.Reason != nil {
		return support.Reason
	}
	return fmt.Errorf("%w: verified containment unsupported", custodian.ErrSupervisorUnavailable)
}

func (s *servedAdmissionRuntime) support() custodian.Support {
	if s == nil {
		return custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable).Support()
	}
	if s.supportOverride != nil {
		return *s.supportOverride
	}
	return s.runtime.Support()
}

func (s *servedAdmissionRuntime) assessSupport(ctx context.Context) custodian.Support {
	if s == nil {
		return custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable).Support()
	}
	if s.supportProbe != nil {
		return s.supportProbe(ctx)
	}
	if s.supportOverride != nil {
		return *s.supportOverride
	}
	return s.runtime.SelfTest(ctx)
}

func (s *servedAdmissionRuntime) unavailableProcess() bool {
	if s == nil {
		return true
	}
	_, ok := s.runtime.Process().(custodian.UnavailableCustodian)
	return ok
}

func (s *servedAdmissionRuntime) quiescenceVerifier() custodian.AttestationVerifier {
	if s == nil {
		return custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable).Verifier()
	}
	if s.verifierOverride != (custodian.AttestationVerifier{}) {
		return s.verifierOverride
	}
	return s.runtime.Verifier()
}

func (s *servedAdmissionRuntime) launchPort() launch.CustodianPort {
	if s == nil {
		return runtimeLaunchCustodian{runtime: custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable)}
	}
	if s.launchCustodian != nil {
		return s.launchCustodian
	}
	return runtimeLaunchCustodian{runtime: s.runtime}
}

type admissionActiveCustodyReporter interface {
	HasActiveCustodies() bool
}

type admissionUnresolvedCustodyAbandoner interface {
	AbandonUnresolvedCustody(context.Context, model.GroupRef) error
}

func (s *servedAdmissionRuntime) hasActiveCustodies() bool {
	if s == nil {
		return false
	}
	if s.runtime.ActiveCustodyCount() > 0 {
		return true
	}
	if reporter, ok := s.launchCustodian.(admissionActiveCustodyReporter); ok && reporter.HasActiveCustodies() {
		return true
	}
	return false
}

func (s *servedAdmissionRuntime) abandonUnresolvedCustody(ctx context.Context, group model.GroupRef) error {
	if s == nil {
		return nil
	}
	var err error
	if abandoner, ok := s.launchCustodian.(admissionUnresolvedCustodyAbandoner); ok {
		err = errors.Join(err, abandoner.AbandonUnresolvedCustody(ctx, group))
	}
	if process := s.runtime.Process(); process != nil {
		if abandoner, ok := process.(admissionUnresolvedCustodyAbandoner); ok {
			err = errors.Join(err, abandoner.AbandonUnresolvedCustody(ctx, group))
		}
	}
	return err
}

func (s *servedAdmissionRuntime) close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.closeHook != nil {
		err = errors.Join(err, s.closeHook())
	}
	return errors.Join(err, s.runtime.Close())
}

func (s *servedAdmissionRuntime) markConsumed() {
	if s == nil {
		return
	}
	s.runtime.MarkConsumed()
}

func (s *servedAdmissionRuntime) consumed() bool {
	return s != nil && s.runtime.Consumed()
}

type runtimeLaunchCustodian struct {
	runtime custodian.Runtime
}

func (c runtimeLaunchCustodian) Prepare(ctx context.Context, spec command.ExecSpec, key model.LaunchKey) (launch.PreparedProcess, error) {
	prepared, err := c.runtime.Process().Prepare(ctx, spec, key)
	if err != nil {
		return nil, err
	}
	return runtimePreparedProcess{prepared: prepared}, nil
}

func (c runtimeLaunchCustodian) ContainAndVerify(ctx context.Context, group model.GroupRef, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	return c.runtime.Process().ContainAndVerify(ctx, group, cause)
}

func (c runtimeLaunchCustodian) HasActiveCustodies() bool {
	return c.runtime.ActiveCustodyCount() > 0
}

type runtimePreparedProcess struct {
	prepared custodian.PreparedProcess
}

func (p runtimePreparedProcess) Ref() model.GroupRef {
	if p.prepared == nil {
		return model.GroupRef{}
	}
	return p.prepared.Ref()
}

func (p runtimePreparedProcess) Release(ctx context.Context) (launch.RunningProcess, custodian.ReleaseOutcome, error) {
	if p.prepared == nil {
		return nil, custodian.ReleaseDefinitelyNotSent, custodian.ErrSupervisorUnavailable
	}
	running, outcome, err := p.prepared.Release(ctx)
	if err != nil || outcome != custodian.ReleaseAccepted {
		return nil, outcome, err
	}
	streaming, ok := running.(interface {
		Ref() model.GroupRef
		Stdin() io.WriteCloser
		Stdout() io.ReadCloser
		Stderr() io.ReadCloser
		WaitAndVerify(context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
		ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
	})
	if !ok {
		return nil, custodian.ReleaseOutcomeUnknown, fmt.Errorf("%w: running process does not expose command streams", custodian.ErrSupervisorUnavailable)
	}
	return runtimeRunningProcess{running: streaming}, custodian.ReleaseAccepted, nil
}

func (p runtimePreparedProcess) AbortAndVerify(ctx context.Context) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	if p.prepared == nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, custodian.ErrSupervisorUnavailable
	}
	return p.prepared.AbortAndVerify(ctx)
}

type runtimeRunningProcess struct {
	running interface {
		Ref() model.GroupRef
		Stdin() io.WriteCloser
		Stdout() io.ReadCloser
		Stderr() io.ReadCloser
		WaitAndVerify(context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
		ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
	}
}

func (p runtimeRunningProcess) Ref() model.GroupRef {
	return p.running.Ref()
}

func (p runtimeRunningProcess) Stdin() io.WriteCloser {
	return p.running.Stdin()
}

func (p runtimeRunningProcess) Stdout() io.ReadCloser {
	return p.running.Stdout()
}

func (p runtimeRunningProcess) Stderr() io.ReadCloser {
	return p.running.Stderr()
}

func (p runtimeRunningProcess) WaitAndVerify(ctx context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	return p.running.WaitAndVerify(ctx)
}

func (p runtimeRunningProcess) ContainAndVerify(ctx context.Context, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	return p.running.ContainAndVerify(ctx, cause)
}

func (p runtimeRunningProcess) WaitContained() bool {
	reporter, ok := p.running.(interface {
		WaitContained() bool
	})
	return ok && reporter.WaitContained()
}

type servedResultPublisher struct {
	server *Server
}

func (p servedResultPublisher) Publish(ctx context.Context, jobID model.JobID, payload []byte) (model.ResultReceipt, error) {
	if p.server != nil {
		p.server.resultPublications.Add(1)
		defer p.server.resultPublications.Add(-1)
	}
	if err := ctx.Err(); err != nil {
		return model.ResultReceipt{}, err
	}
	record, authorityOwned, err := p.authorityRecord(ctx, jobID)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	if authorityOwned {
		return p.publishAuthorityResult(jobID, record, payload)
	}
	return p.publishLegacyResult(jobID, payload)
}

func (p servedResultPublisher) publishLegacyResult(jobID model.JobID, payload []byte) (model.ResultReceipt, error) {
	store := p.server.storeForJob(jobID.String())
	if store == nil {
		return model.ResultReceipt{}, fmt.Errorf("job store not found for %s", jobID)
	}
	info, err := store.WriteResult(jobID.String(), payload, p.server.inlineResultCap)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	return servedResultReceipt(jobID, info), nil
}

func (p servedResultPublisher) publishAuthorityResult(jobID model.JobID, record model.SafetyRecord, payload []byte) (model.ResultReceipt, error) {
	layout, err := authorityResultLayout(p.server.stateRoot, record)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	info, err := engine.WriteResultForLayout(layout, jobID.String(), payload, p.server.inlineResultCap)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	if !servedPathWithinDir(layout.Results, info.ResultPath) {
		return model.ResultReceipt{}, fmt.Errorf("authority result path %q escapes results root %q", info.ResultPath, layout.Results)
	}
	return servedResultReceipt(jobID, info), nil
}

func servedResultReceipt(jobID model.JobID, info engine.ResultInfo) model.ResultReceipt {
	return model.ResultReceipt{
		JobID: jobID,
		Result: model.ResultRef{
			Path:   info.ResultPath,
			Digest: info.SHA256,
			Bytes:  info.Bytes,
		},
		DirSynced: model.Evidence{
			Kind:   "served_result",
			Detail: "directory_synced",
		},
	}
}

func (p servedResultPublisher) Verify(ctx context.Context, result model.ResultRef) (model.ResultReceipt, error) {
	if p.server != nil {
		p.server.resultPublications.Add(1)
		defer p.server.resultPublications.Add(-1)
	}
	if err := ctx.Err(); err != nil {
		return model.ResultReceipt{}, err
	}
	jobID, err := jobIDFromResultPath(result.Path)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	record, authorityOwned, err := p.authorityRecord(ctx, jobID)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	if authorityOwned {
		if err := validateAuthorityResultPath(p.server.stateRoot, record, jobID, result.Path); err != nil {
			return model.ResultReceipt{}, err
		}
	}
	raw, err := os.ReadFile(result.Path)
	if err != nil {
		return model.ResultReceipt{}, err
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != result.Digest {
		return model.ResultReceipt{}, fmt.Errorf("result digest = %s, want %s", got, result.Digest)
	}
	if int64(len(raw)) != result.Bytes {
		return model.ResultReceipt{}, fmt.Errorf("result bytes = %d, want %d", len(raw), result.Bytes)
	}
	return model.ResultReceipt{
		JobID:  jobID,
		Result: result,
		DirSynced: model.Evidence{
			Kind:   "served_result",
			Detail: "directory_synced",
		},
	}, nil
}

func (p servedResultPublisher) authorityRecord(ctx context.Context, jobID model.JobID) (model.SafetyRecord, bool, error) {
	if p.server == nil {
		return model.SafetyRecord{}, false, nil
	}
	p.server.admissionStateMu.RLock()
	ready := p.server.admissionReady
	ok := p.server.admissionInstance != nil && ready != nil
	p.server.admissionStateMu.RUnlock()
	if !ok {
		return model.SafetyRecord{}, false, authority.ErrNotReady
	}
	image, err := ready.LoadJob(ctx, jobID)
	if err != nil {
		if admissionAuthorityNotReadyError(err) {
			return model.SafetyRecord{}, false, authority.ErrNotReady
		}
		return model.SafetyRecord{}, false, err
	}
	if err := authorityImageSafetyCorruption(image); err != nil {
		return model.SafetyRecord{}, false, p.server.failStopAdmissionRepositoryCorruption(ctx, "served result authority record", err)
	}
	if image.Safety.State == repository.RecordValid {
		return image.Safety.Value, true, nil
	}
	if authorityImageEmpty(image) {
		return model.SafetyRecord{}, false, nil
	}
	return model.SafetyRecord{}, false, fmt.Errorf("authority safety state = %s for %s", image.Safety.State, jobID)
}

func authorityResultLayout(root string, record model.SafetyRecord) (engine.WorkspaceLayout, error) {
	if record.WorkspaceLayoutKey == "" {
		return engine.WorkspaceLayout{}, fmt.Errorf("authority workspace layout key missing for %s", record.JobID)
	}
	layout, err := engine.LayoutForWorkspaceKey(root, record.WorkspaceLayoutKey.String())
	if err != nil {
		return engine.WorkspaceLayout{}, fmt.Errorf("authority workspace layout for %s: %w", record.JobID, err)
	}
	return layout, nil
}

func validateAuthorityResultPath(root string, record model.SafetyRecord, jobID model.JobID, path string) error {
	layout, err := authorityResultLayout(root, record)
	if err != nil {
		return err
	}
	if !servedPathWithinDir(layout.Results, path) {
		return fmt.Errorf("authority result path %q escapes results root %q", path, layout.Results)
	}
	expected, err := engine.ResultPathForLayout(layout, jobID.String())
	if err != nil {
		return err
	}
	if filepath.Clean(path) != filepath.Clean(expected) {
		return fmt.Errorf("authority result path = %q, want %q", path, expected)
	}
	return nil
}

func servedPathWithinDir(dir, path string) bool {
	rel, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(path))
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel)
}

func jobIDFromResultPath(path string) (model.JobID, error) {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	if ext != "" {
		base = base[:len(base)-len(ext)]
	}
	return model.NewJobID(base)
}

func (s *Server) markAdmissionJob(jobID string, instance *admissionInstance) {
	s.mu.Lock()
	s.admissionJobs[jobID] = instance
	s.mu.Unlock()
}

func (s *Server) isAdmissionJob(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admissionJobs[jobID] != nil
}

func (s *Server) completeAdmissionRun(run jobRun, state engine.JobState, text string, stamp *engine.ContractStamp) error {
	jobID, err := model.NewJobID(run.jobID)
	if err != nil {
		return err
	}
	// Snapshot-checkout: Snapshot/Complete are authority operations that must
	// not hold admissionStateMu (a stalled op would block closeServeAdmission
	// past the safety fail-stop drain deadline).
	s.admissionStateMu.RLock()
	coord := s.admissionCoordinator
	ready := s.admissionInstance != nil && coord != nil
	s.admissionStateMu.RUnlock()
	if !ready {
		// Graceful Serve exit is equivalent to a daemon crash for this
		// post-accept/pre-completion window: restart recovery owns the
		// durably accepted obligation and replay returns that terminal job.
		log.Printf("agentbus daemon: admission job %s completed while authority was not ready; startup recovery must finalize the durably accepted obligation", jobID)
		return nil
	}
	snapshot, err := coord.Snapshot(context.Background(), jobID)
	if err != nil {
		return err
	}
	if snapshot.Record.Terminal != nil {
		if err := s.cleanupAdmissionBackendLogs(run, snapshot.Record.Terminal.Outcome, snapshot.Record.WorkspaceLayoutKey.String()); err != nil {
			return err
		}
		if model.DeriveCleanupDisposition(snapshot.Record) == model.CleanupDispositionUnresolved {
			if err := s.abandonAdmissionRecordUnresolvedCustody(context.Background(), snapshot.Record); err != nil {
				log.Printf("agentbus daemon: job %s unresolved custody abandon warning: %v", jobID, err)
			}
		}
		return nil
	}
	if admissionRunHasRequestedCancel(run, state) {
		cancellation := run.active.requestedCancellation()
		count, ordinal := run.active.observedWorkspaceWriteItemCountForTerminal()
		if err := coord.CancelWithMetadataAndObservedWorkspaceWriteItemCount(context.Background(), jobID, cancellation.origin, cancellation.reason, count, ordinal, nil); err != nil {
			if err := reconcileAdmissionFinalizationContention(context.Background(), coord, jobID, err); err != nil {
				return err
			}
		}
		s.abandonAdmissionUnresolvedCustody(context.Background(), coord, jobID)
		return s.cleanupAdmissionBackendLogsForCommittedTerminal(run, coord, jobID)
	}
	outcome, ok := admissionOutcomeForState(state)
	if !ok {
		return fmt.Errorf("cannot complete admission job %s with state %s", run.jobID, state)
	}
	if (state == engine.StateCompleted || state == engine.StateCompletedNoncompliant) && text == "" && run.policy != nil && run.policy.Contract != nil && stamp == nil {
		stamp = skippedStampForRun(run, s.registry, engine.SkipNoFinalMessage)
	}
	if intent, ok, err := admissionRecordedReleaseTerminalIntent(snapshot.Record); ok {
		if err != nil {
			return err
		}
		count, ordinal := run.active.observedWorkspaceWriteItemCountForTerminal()
		if err := coord.FinalizeWithObservedWorkspaceWriteItemCount(context.Background(), jobID, intent, count, ordinal); err != nil {
			if err := reconcileAdmissionFinalizationContention(context.Background(), coord, jobID, err); err != nil {
				return err
			}
		}
		s.abandonAdmissionUnresolvedCustody(context.Background(), coord, jobID)
		return s.cleanupAdmissionBackendLogsForCommittedTerminal(run, coord, jobID)
	}
	count, ordinal := run.active.observedWorkspaceWriteItemCountForTerminal()
	if err := coord.CompleteWithObservedWorkspaceWriteItemCount(context.Background(), jobID, outcome, []byte(text), stamp, count, ordinal, nil); err != nil {
		if err := reconcileAdmissionFinalizationContention(context.Background(), coord, jobID, err); err != nil {
			return err
		}
		// A different terminal won the commit race. Its committed outcome, not
		// this attempt's state, owns the private-home cleanup decision below.
	}
	s.abandonAdmissionUnresolvedCustody(context.Background(), coord, jobID)
	return s.cleanupAdmissionBackendLogsForCommittedTerminal(run, coord, jobID)
}

func (s *Server) cleanupAdmissionBackendLogsForCommittedTerminal(run jobRun, coord *admissionCoordinator, jobID model.JobID) error {
	snapshot, err := coord.Snapshot(context.Background(), jobID)
	if err != nil {
		return err
	}
	if snapshot.Record.Terminal == nil {
		return nil
	}
	return s.cleanupAdmissionBackendLogs(run, snapshot.Record.Terminal.Outcome, snapshot.Record.WorkspaceLayoutKey.String())
}

func (s *Server) cleanupAdmissionBackendLogs(run jobRun, outcome model.Outcome, workspaceID string) error {
	if run.logPaths.Stdout == "" && run.logPaths.Stderr == "" {
		return nil
	}
	cleanup := func() error {
		if outcome == model.OutcomeCompleted {
			return discardBackendLogs(run.logPaths)
		}
		return discardEmptyBackendLogs(run.logPaths)
	}
	if drain := s.admissionLogDrain(run.jobID); drain != nil {
		go func() {
			<-drain
			if err := cleanup(); err != nil {
				log.Printf("agentbus daemon: job %s backend log cleanup after event drain failed: %v", run.jobID, err)
			}
			s.finishAdmissionLogDrain(run.jobID, drain)
			s.enforceAdmissionLogRetention(workspaceID)
		}()
		return nil
	}
	if err := cleanup(); err != nil {
		return err
	}
	return nil
}

func (s *Server) cleanupManagedCodexHomeForAdmissionRun(run jobRun) {
	if run.managedCodexHome == nil {
		return
	}
	cleanup := func() error {
		return s.withAdmissionCoordinator(func(coord *admissionCoordinator) error {
			outcome, err := admissionCommittedTerminalOutcome(context.Background(), coord, model.JobID(run.jobID))
			if err != nil {
				return err
			}
			s.cleanupManagedCodexHome(run.managedCodexHome, outcome)
			return nil
		})
	}
	var err error
	if run.admissionLaunchFailed {
		err = cleanup()
	} else {
		err = s.withAdmissionJobEffectErr(run.jobID, cleanup)
	}
	if err != nil {
		log.Printf("agentbus daemon: retain managed Codex home %s: cannot read committed terminal: %v", run.codexHome, err)
	}
}

func admissionCommittedTerminalOutcome(ctx context.Context, coord *admissionCoordinator, jobID model.JobID) (model.Outcome, error) {
	snapshot, err := coord.Snapshot(ctx, jobID)
	if err != nil {
		return model.OutcomeNone, err
	}
	if err := admissionValidTerminalRecord(snapshot.Record); err != nil {
		return model.OutcomeNone, err
	}
	return snapshot.Record.Terminal.Outcome, nil
}

func (s *Server) abandonAdmissionUnresolvedCustody(ctx context.Context, coord *admissionCoordinator, jobID model.JobID) {
	if coord == nil {
		return
	}
	snapshot, err := coord.Snapshot(ctx, jobID)
	if err != nil {
		log.Printf("agentbus daemon: job %s unresolved custody abandon skipped after terminal commit: snapshot failed: %v", jobID, err)
		return
	}
	if model.DeriveCleanupDisposition(snapshot.Record) != model.CleanupDispositionUnresolved {
		return
	}
	if err := s.abandonAdmissionRecordUnresolvedCustody(ctx, snapshot.Record); err != nil {
		log.Printf("agentbus daemon: job %s unresolved custody abandon warning: %v", jobID, err)
	}
}

func (s *Server) abandonAdmissionRecordUnresolvedCustody(ctx context.Context, record model.SafetyRecord) error {
	if record.Terminal == nil {
		return nil
	}
	s.admissionStateMu.RLock()
	runtime := s.admissionRuntime
	s.admissionStateMu.RUnlock()
	if runtime == nil {
		return nil
	}
	var err error
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launchRecord, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launchRecord.Group == nil || launchRecord.Quiescence != nil {
			continue
		}
		err = errors.Join(err, runtime.abandonUnresolvedCustody(ctx, *launchRecord.Group))
	}
	return err
}

func reconcileAdmissionFinalizationContention(ctx context.Context, coord *admissionCoordinator, jobID model.JobID, err error) error {
	if err == nil || !errors.Is(err, coordinator.ErrAlreadyFinalized) {
		return err
	}
	snapshot, snapshotErr := coord.Snapshot(ctx, jobID)
	if snapshotErr != nil {
		return errors.Join(err, snapshotErr)
	}
	if validErr := admissionValidTerminalRecord(snapshot.Record); validErr != nil {
		return errors.Join(err, validErr)
	}
	return nil
}

func admissionValidTerminalRecord(record model.SafetyRecord) error {
	if record.Terminal == nil {
		return errors.New("existing admission terminal record is missing")
	}
	if err := model.ValidateSafetyRecord(record); err != nil {
		return fmt.Errorf("existing admission terminal record is invalid: %w", err)
	}
	return nil
}

func admissionRunHasRequestedCancel(run jobRun, state engine.JobState) bool {
	return state == engine.StateCanceled &&
		run.active != nil &&
		run.active.requestedTerminal() == engine.StateCanceled
}

func admissionRecordedReleaseTerminalIntent(record model.SafetyRecord) (model.TerminalIntent, bool, error) {
	if record.Terminal != nil {
		return model.TerminalIntent{}, false, nil
	}
	ordinals := record.Attempt.Launches.FilledOrdinals()
	for i := len(ordinals) - 1; i >= 0; i-- {
		ordinal := ordinals[i]
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Grant == nil || launch.ReleaseOutcome == nil {
			continue
		}
		switch launch.ReleaseOutcome.Outcome {
		case model.LaunchReleaseSentUnknown, model.LaunchReleaseNotSent:
			intent, err := model.RecoveryTerminalIntent(record, model.RecoveryLiveLoss, admissionAllLaunchGroupsQuiescent(record))
			if err != nil {
				return model.TerminalIntent{}, true, err
			}
			return intent, true, nil
		}
	}
	return model.TerminalIntent{}, false, nil
}

func admissionOutcomeForState(state engine.JobState) (model.Outcome, bool) {
	switch state {
	case engine.StateCompleted:
		return model.OutcomeCompleted, true
	case engine.StateCompletedNoncompliant:
		return model.OutcomeCompletedNoncompliant, true
	case engine.StateFailed:
		return model.OutcomeFailed, true
	case engine.StateTimedOut:
		return model.OutcomeTimedOut, true
	case engine.StateCanceled:
		return model.OutcomeCanceled, true
	case engine.StateInterrupted:
		return model.OutcomeInterrupted, true
	case engine.StateReaped:
		return model.OutcomeReaped, true
	case engine.StateQuarantined:
		return model.OutcomeQuarantined, true
	default:
		return 0, false
	}
}

func syncFileAndParent(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncParentDir(path)
}

func syncParentDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
