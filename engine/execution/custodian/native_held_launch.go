//go:build darwin || linux

package custodian

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

const nativeReleaseSecretEntropyBytes = 32

type nativeParkPrepared interface {
	Ref() model.GroupRef
	Release(context.Context) (*parklaunch.ParkedHandle, error)
	AbortAndVerify(context.Context) error
	ContainAndVerify(context.Context) error
}

type nativeHeldPreparedLaunch struct {
	mu sync.Mutex

	prepared nativeParkPrepared
	backend  nativeContainmentBackend

	releaseDefinitelyNotSent bool
	backendClosed            bool
}

type nativeHeldLaunchEffects struct {
	custodian *NativeCustodian
	spec      NativeLaunchSpec

	mu       sync.Mutex
	prepared map[string]*nativeHeldPreparedLaunch
}

type nativeHeldPreparedRegistration struct {
	ref     model.GroupRef
	effects *nativeHeldLaunchEffects
	entry   *nativeHeldPreparedLaunch
}

var _ HeldLaunchEffects = (*nativeHeldLaunchEffects)(nil)

// prepareNativeHeldLaunch is the package-private native bridge for R3A1 tests.
// The caller-supplied NativeLaunchSpec.ReleaseSecret is ignored: this path
// generates one custodian-owned secret here, copies it into the R2A PrepareSpec
// because HeldLaunchCore still validates that field, and passes the same secret
// into the R2B parklaunch.Spec. No production composition selects this path yet.
func prepareNativeHeldLaunch(ctx context.Context, custodian *NativeCustodian, spec NativeLaunchSpec) (HeldLaunch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if custodian == nil {
		return nil, fmt.Errorf("%w: custodian is nil", ErrNativeCustodianUnavailable)
	}
	secret, err := generateNativeReleaseSecret()
	if err != nil {
		return nil, err
	}
	spec.ReleaseSecret = secret
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	effects := &nativeHeldLaunchEffects{
		custodian: custodian,
		spec:      spec,
		prepared:  make(map[string]*nativeHeldPreparedLaunch),
	}
	return PrepareHeldLaunch(ctx, PrepareSpec{
		Exec:          spec.Exec,
		LaunchKey:     spec.LaunchKey,
		ReleaseSecret: secret,
	}, effects)
}

func generateNativeReleaseSecret() (model.ReleaseSecret, error) {
	var raw [nativeReleaseSecretEntropyBytes]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("%w: generate release secret: %v", ErrNativeCustodianUnavailable, err)
	}
	defer func() {
		for i := range raw {
			raw[i] = 0
		}
	}()
	secret, err := model.NewReleaseSecret("native-release-secret-v1-" + hex.EncodeToString(raw[:]))
	if err != nil {
		return "", err
	}
	return secret, nil
}

func (effects *nativeHeldLaunchEffects) Prepare(ctx context.Context, spec PrepareSpec) (model.GroupRef, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if effects == nil || effects.custodian == nil {
		return model.GroupRef{}, fmt.Errorf("%w: native held launch effects are nil", ErrNativeCustodianUnavailable)
	}
	if spec.ReleaseSecret != effects.spec.ReleaseSecret {
		return model.GroupRef{}, fmt.Errorf("%w: native held launch release secret mismatch", ErrInvalidHeldLaunch)
	}
	nativeSpec := effects.spec
	nativeSpec.Exec = spec.Exec
	nativeSpec.LaunchKey = spec.LaunchKey
	nativeSpec.ReleaseSecret = spec.ReleaseSecret
	if err := nativeSpec.Validate(); err != nil {
		return model.GroupRef{}, err
	}

	effects.custodian.mu.Lock()
	closed := effects.custodian.closed
	effects.custodian.mu.Unlock()
	if closed {
		return model.GroupRef{}, fmt.Errorf("%w: custodian is closed", ErrNativeCustodianUnavailable)
	}

	backend, err := newNativeContainmentBackend(ctx, effects.custodian)
	if err != nil {
		return model.GroupRef{}, err
	}
	launchContainment := &nativeLaunchContainment{
		params:         effects.custodian.options.ContainmentParams,
		retainedObject: backend.retainedObject(),
	}
	syncLaunchContainment := func() {
		if witness := backend.witness(); witness != nil {
			launchContainment.setWitness(witness)
		}
		if retainedObject := backend.retainedObject(); retainedObject != nil {
			launchContainment.setRetainedObject(retainedObject)
		}
	}
	parkSpec, err := effects.custodian.parklaunchSpec(nativeSpec, launchContainment, backend, syncLaunchContainment)
	if err != nil {
		return model.GroupRef{}, errors.Join(err, backend.close(ctx))
	}
	prepared, err := parklaunch.Prepare(ctx, parkSpec)
	if err != nil {
		return model.GroupRef{}, errors.Join(err, backend.close(ctx))
	}
	if !backend.witnessAcquired() {
		err := fmt.Errorf("%w: containment continuity witness was not acquired before native held prepare returned", ErrNativeCustodianUnavailable)
		return model.GroupRef{}, errors.Join(err, prepared.ContainAndVerify(ctx), backend.close(ctx))
	}
	ref := prepared.Ref()
	if err := ref.Validate(); err != nil {
		return model.GroupRef{}, errors.Join(err, prepared.ContainAndVerify(ctx), backend.close(ctx))
	}

	entry := &nativeHeldPreparedLaunch{prepared: prepared, backend: backend}
	if err := effects.registerPrepared(ref, entry); err != nil {
		return model.GroupRef{}, errors.Join(err, prepared.ContainAndVerify(ctx), backend.close(ctx))
	}
	return ref, nil
}

func (effects *nativeHeldLaunchEffects) SendRelease(ctx context.Context, _ PrepareSpec, ref model.GroupRef) (RunningProcess, ReleaseOutcome, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entry, ok := effects.lookupPrepared(ref)
	if !ok {
		return nil, ReleaseOutcomeUnknown, fmt.Errorf("%w: native held launch prepared ref not found", ErrNativeCustodianUnavailable)
	}

	entry.mu.Lock()
	handle, err := entry.prepared.Release(ctx)
	outcome := nativeReleaseOutcomeFromParklaunch(handle, err)
	if err == nil && handle == nil {
		err = fmt.Errorf("%w: parklaunch release returned nil handle", ErrNativeCustodianUnavailable)
	}
	if outcome != ReleaseAccepted && handle != nil {
		err = errors.Join(err, cleanupLaunchedHandle(ctx, handle, effects.custodian.options.ContainmentParams))
	}
	switch outcome {
	case ReleaseAccepted:
		running, adoptErr := effects.adoptReleasedHandleLocked(ctx, ref, handle, entry)
		entry.mu.Unlock()
		if adoptErr != nil {
			return nil, ReleaseOutcomeUnknown, errors.Join(err, adoptErr)
		}
		effects.deletePrepared(ref, entry)
		return running, ReleaseAccepted, nil
	case ReleaseDefinitelyNotSent:
		entry.releaseDefinitelyNotSent = true
		closeErr := entry.closeBackendLocked(ctx)
		entry.mu.Unlock()
		return nil, ReleaseDefinitelyNotSent, errors.Join(err, closeErr)
	default:
		entry.mu.Unlock()
		return nil, ReleaseOutcomeUnknown, err
	}
}

func nativeReleaseOutcomeFromParklaunch(handle *parklaunch.ParkedHandle, err error) ReleaseOutcome {
	switch {
	case err == nil && handle != nil:
		return ReleaseAccepted
	case errors.Is(err, parklaunch.ErrChannelLostBeforeRelease):
		return ReleaseDefinitelyNotSent
	case errors.Is(err, parklaunch.ErrReleaseOutcomeUnknown):
		return ReleaseOutcomeUnknown
	default:
		return ReleaseOutcomeUnknown
	}
}

func (effects *nativeHeldLaunchEffects) AbortAndVerify(ctx context.Context, ref model.GroupRef) (VerifiedQuiescence, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entry, ok := effects.lookupPrepared(ref)
	if !ok {
		return VerifiedQuiescence{}, fmt.Errorf("%w: native held launch prepared ref not found", ErrNativeCustodianUnavailable)
	}

	return effects.abortPreparedEntryAndVerify(ctx, ref, entry, false)
}

func (effects *nativeHeldLaunchEffects) ContainAndVerify(ctx context.Context, ref model.GroupRef, cause QuiescenceCause) (VerifiedQuiescence, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entry, ok := effects.lookupPrepared(ref)
	var preparedErr error
	if ok {
		entry.mu.Lock()
		preparedErr = entry.prepared.ContainAndVerify(ctx)
		if errors.Is(preparedErr, parklaunch.ErrPreparedAlreadyConsumed) || errors.Is(preparedErr, parklaunch.ErrPreparedExecutionPossible) {
			preparedErr = nil
		}
		entry.mu.Unlock()
	}

	verified, attestErr := effects.custodian.ContainAndVerify(ctx, ref, cause)
	var closeErr error
	if ok {
		entry.mu.Lock()
		closeErr = entry.closeBackendLocked(ctx)
		entry.mu.Unlock()
	}
	if preparedErr != nil || attestErr != nil || closeErr != nil {
		return verified, errors.Join(preparedErr, attestErr, closeErr)
	}
	if ok {
		effects.deletePrepared(ref, entry)
	}
	return verified, nil
}

func (effects *nativeHeldLaunchEffects) adoptReleasedHandleLocked(ctx context.Context, ref model.GroupRef, handle *parklaunch.ParkedHandle, entry *nativeHeldPreparedLaunch) (*NativeRunningProcess, error) {
	if handle == nil {
		return nil, fmt.Errorf("%w: parklaunch handle is nil", ErrNativeCustodianUnavailable)
	}
	if !handle.GroupRef.Equal(ref) {
		return nil, fmt.Errorf("%w: released group ref mismatch", ErrNativeCustodianUnavailable)
	}
	if entry == nil || entry.backend == nil || entry.backendClosed {
		return nil, errors.Join(
			fmt.Errorf("%w: native held launch containment backend is closed", ErrNativeCustodianUnavailable),
			cleanupLaunchedHandle(ctx, handle, effects.custodian.options.ContainmentParams),
		)
	}
	if !entry.backend.witnessAcquired() {
		return nil, errors.Join(
			fmt.Errorf("%w: containment continuity witness was not acquired before release", ErrNativeCustodianUnavailable),
			cleanupLaunchedHandle(ctx, handle, effects.custodian.options.ContainmentParams),
			entry.closeBackendLocked(ctx),
		)
	}
	entry.backend.attachHandle(handle)
	running := &NativeRunningProcess{
		custodian:   effects.custodian,
		handle:      handle,
		group:       handle.GroupRef,
		leader:      entry.backend.leaderRetention(),
		containment: entry.backend,
	}

	effects.custodian.mu.Lock()
	if effects.custodian.closed {
		effects.custodian.mu.Unlock()
		return nil, errors.Join(
			fmt.Errorf("%w: custodian is closed", ErrNativeCustodianUnavailable),
			cleanupLaunchedHandle(ctx, handle, effects.custodian.options.ContainmentParams),
			entry.closeBackendLocked(ctx),
		)
	}
	key := groupKey(handle.GroupRef)
	if effects.custodian.running == nil {
		effects.custodian.running = make(map[string]*NativeRunningProcess)
	}
	if effects.custodian.finalized == nil {
		effects.custodian.finalized = make(map[string]*NativeRunningProcess)
	}
	delete(effects.custodian.finalized, key)
	effects.custodian.running[key] = running
	effects.custodian.mu.Unlock()
	return running, nil
}

func (effects *nativeHeldLaunchEffects) lookupPrepared(ref model.GroupRef) (*nativeHeldPreparedLaunch, bool) {
	if effects == nil {
		return nil, false
	}
	effects.mu.Lock()
	defer effects.mu.Unlock()
	entry := effects.prepared[groupKey(ref)]
	return entry, entry != nil
}

func (effects *nativeHeldLaunchEffects) abortPreparedEntryAndVerify(ctx context.Context, ref model.GroupRef, entry *nativeHeldPreparedLaunch, refuseRunning bool) (VerifiedQuiescence, error) {
	if effects == nil || effects.custodian == nil {
		return VerifiedQuiescence{}, fmt.Errorf("%w: native held launch effects are nil", ErrNativeCustodianUnavailable)
	}
	if entry == nil {
		return VerifiedQuiescence{}, fmt.Errorf("%w: native held launch prepared entry is nil", ErrNativeCustodianUnavailable)
	}
	entry.mu.Lock()
	if refuseRunning && effects.custodian.hasRunningPreparedRef(ref) {
		entry.mu.Unlock()
		return VerifiedQuiescence{}, ErrHeldLaunchExecutionPossible
	}
	var abortErr error
	if !entry.releaseDefinitelyNotSent {
		abortErr = entry.prepared.AbortAndVerify(ctx)
	}
	entry.mu.Unlock()

	verified, attestErr := effects.custodian.ContainAndVerify(ctx, ref, QuiescenceCauseAbort)
	entry.mu.Lock()
	closeErr := entry.closeBackendLocked(ctx)
	entry.mu.Unlock()
	if attestErr != nil || closeErr != nil {
		return verified, errors.Join(abortErr, attestErr, closeErr)
	}
	effects.deletePrepared(ref, entry)
	return verified, nil
}

func (effects *nativeHeldLaunchEffects) registerPrepared(ref model.GroupRef, entry *nativeHeldPreparedLaunch) error {
	if effects == nil || effects.custodian == nil || entry == nil {
		return fmt.Errorf("%w: native held launch prepared registry input is nil", ErrNativeCustodianUnavailable)
	}
	key := groupKey(ref)
	effects.custodian.mu.Lock()
	if effects.custodian.closed {
		effects.custodian.mu.Unlock()
		return fmt.Errorf("%w: custodian is closed", ErrNativeCustodianUnavailable)
	}
	if effects.custodian.heldPrepared == nil {
		effects.custodian.heldPrepared = make(map[string]nativeHeldPreparedRegistration)
	}
	if effects.custodian.heldPrepared[key].entry != nil {
		effects.custodian.mu.Unlock()
		return fmt.Errorf("%w: duplicate native held launch group ref", ErrNativeCustodianUnavailable)
	}
	effects.mu.Lock()
	if effects.prepared == nil {
		effects.prepared = make(map[string]*nativeHeldPreparedLaunch)
	}
	if effects.prepared[key] != nil {
		effects.mu.Unlock()
		effects.custodian.mu.Unlock()
		return fmt.Errorf("%w: duplicate native held launch group ref", ErrNativeCustodianUnavailable)
	}
	effects.prepared[key] = entry
	effects.custodian.heldPrepared[key] = nativeHeldPreparedRegistration{
		ref:     ref,
		effects: effects,
		entry:   entry,
	}
	effects.mu.Unlock()
	effects.custodian.mu.Unlock()
	return nil
}

func (effects *nativeHeldLaunchEffects) deletePrepared(ref model.GroupRef, entry *nativeHeldPreparedLaunch) {
	if effects == nil {
		return
	}
	if effects.custodian != nil {
		effects.custodian.unregisterNativeHeldPrepared(ref, entry)
	}
	effects.mu.Lock()
	defer effects.mu.Unlock()
	key := groupKey(ref)
	if current := effects.prepared[key]; current == entry || entry == nil {
		delete(effects.prepared, key)
	}
}

func (custodian *NativeCustodian) unregisterNativeHeldPrepared(ref model.GroupRef, entry *nativeHeldPreparedLaunch) {
	if custodian == nil {
		return
	}
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	key := groupKey(ref)
	if current := custodian.heldPrepared[key]; current.entry == entry || entry == nil {
		delete(custodian.heldPrepared, key)
	}
}

func (custodian *NativeCustodian) snapshotNativeHeldPreparedLocked() []nativeHeldPreparedRegistration {
	if custodian == nil || len(custodian.heldPrepared) == 0 {
		return nil
	}
	prepared := make([]nativeHeldPreparedRegistration, 0, len(custodian.heldPrepared))
	for _, registration := range custodian.heldPrepared {
		prepared = append(prepared, registration)
	}
	return prepared
}

func (custodian *NativeCustodian) closeNativeHeldPrepared(ctx context.Context, prepared []nativeHeldPreparedRegistration) error {
	for _, registration := range prepared {
		if registration.effects == nil || registration.entry == nil {
			return fmt.Errorf("%w: native held launch prepared registry is invalid", ErrHeldLaunchCloseRefused)
		}
		if !custodian.nativeHeldPreparedRegistered(registration.ref, registration.entry) {
			continue
		}
		if _, err := registration.effects.abortPreparedEntryAndVerify(ctx, registration.ref, registration.entry, true); err != nil {
			return errors.Join(
				fmt.Errorf("%w: cannot close custodian with held prepared launch %s", ErrHeldLaunchCloseRefused, groupKey(registration.ref)),
				err,
			)
		}
	}
	return nil
}

func (custodian *NativeCustodian) nativeHeldPreparedRegistered(ref model.GroupRef, entry *nativeHeldPreparedLaunch) bool {
	if custodian == nil {
		return false
	}
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	return custodian.heldPrepared[groupKey(ref)].entry == entry
}

func (custodian *NativeCustodian) hasRunningPreparedRef(ref model.GroupRef) bool {
	if custodian == nil {
		return false
	}
	custodian.mu.Lock()
	defer custodian.mu.Unlock()
	return custodian.running[groupKey(ref)] != nil
}

func (entry *nativeHeldPreparedLaunch) closeBackendLocked(ctx context.Context) error {
	if entry == nil || entry.backend == nil || entry.backendClosed {
		return nil
	}
	if err := entry.backend.close(ctx); err != nil {
		return err
	}
	entry.backendClosed = true
	return nil
}
