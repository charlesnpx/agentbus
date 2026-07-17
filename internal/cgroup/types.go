package cgroup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
)

var (
	ErrUnsupported   = errors.New("cgroup unsupported")
	ErrInvalid       = errors.New("cgroup invalid")
	ErrPopulated     = errors.New("cgroup populated")
	errLeafCollision = errors.New("cgroup leaf collision")
	errProcessGone   = errors.New("process gone")
)

type Support struct {
	Supported          bool
	RuntimeProbePassed bool
	Degraded           bool
	Platform           string
	Reason             error
}

func (support Support) Strict() bool {
	return support.Supported && support.RuntimeProbePassed && !support.Degraded && support.Reason == nil
}

type RootIdentity struct {
	HostBootID        string
	PIDNamespaceID    string
	CgroupNamespaceID string
	MountID           string
	HierarchyID       string
	Unified           bool
	ReadOnly          bool
	Delegated         bool
	Exclusive         bool
	Threaded          bool
	KillAvailable     bool
	FreezeAvailable   bool
	RootObject        ObjectIdentity
}

func (identity RootIdentity) kernelDomain() (model.KernelDomainID, error) {
	if strings.TrimSpace(identity.HostBootID) == "" {
		return model.KernelDomainID{}, fmt.Errorf("%w: host boot id is required", ErrInvalid)
	}
	if strings.TrimSpace(identity.PIDNamespaceID) == "" {
		return model.KernelDomainID{}, fmt.Errorf("%w: pid namespace id is required", ErrInvalid)
	}
	hostBootID, err := model.CanonicalHostBootID(identity.HostBootID)
	if err != nil {
		return model.KernelDomainID{}, err
	}
	pidNamespaceID, err := model.CanonicalPIDNamespaceID(identity.PIDNamespaceID)
	if err != nil {
		return model.KernelDomainID{}, err
	}
	return model.NewKernelDomainIDWithRetainedDomain(
		hostBootID,
		pidNamespaceID,
		identity.retainedDomainID(),
	)
}

func (identity RootIdentity) retainedDomainID() string {
	parts := []string{
		identity.HostBootID,
		identity.PIDNamespaceID,
		identity.CgroupNamespaceID,
		identity.MountID,
		identity.HierarchyID,
		identity.RootObject.stableToken(),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "cgdomain-" + hex.EncodeToString(sum[:16])
}

type ObjectIdentity struct {
	Device     uint64
	Inode      uint64
	ChangeSec  int64
	ChangeNsec int64
	Generation string
}

func (identity ObjectIdentity) Equal(other ObjectIdentity) bool {
	return identity.Device == other.Device &&
		identity.Inode == other.Inode &&
		identity.Generation == other.Generation
}

func (identity ObjectIdentity) valid() bool {
	return identity.Device != 0 && identity.Inode != 0
}

func (identity ObjectIdentity) durable() bool {
	return identity.valid() && identity.Generation != ""
}

func (identity ObjectIdentity) sameLiveObject(other ObjectIdentity) bool {
	return identity.valid() &&
		other.valid() &&
		identity.Device == other.Device &&
		identity.Inode == other.Inode
}

func (identity ObjectIdentity) durableEqual(other ObjectIdentity) bool {
	return identity.durable() && other.durable() && identity.Equal(other)
}

func (identity ObjectIdentity) stableToken() string {
	if !identity.valid() {
		return ""
	}
	if identity.Generation != "" {
		return fmt.Sprintf("%x-%x-%s", identity.Device, identity.Inode, hex.EncodeToString([]byte(identity.Generation)))
	}
	return fmt.Sprintf("%x-%x", identity.Device, identity.Inode)
}

func parseObjectIdentityToken(token string) (ObjectIdentity, error) {
	parts := strings.Split(token, "-")
	if len(parts) != 2 && len(parts) != 3 {
		return ObjectIdentity{}, fmt.Errorf("%w: invalid object identity token", ErrInvalid)
	}
	device, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return ObjectIdentity{}, fmt.Errorf("%w: invalid object identity device", ErrInvalid)
	}
	inode, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return ObjectIdentity{}, fmt.Errorf("%w: invalid object identity inode", ErrInvalid)
	}
	identity := ObjectIdentity{Device: device, Inode: inode}
	if len(parts) == 3 {
		generation, err := hex.DecodeString(parts[2])
		if err != nil {
			return ObjectIdentity{}, fmt.Errorf("%w: invalid object identity generation", ErrInvalid)
		}
		identity.Generation = string(generation)
	}
	if !identity.valid() {
		return ObjectIdentity{}, fmt.Errorf("%w: incomplete object identity", ErrInvalid)
	}
	return identity, nil
}

type Events struct {
	Populated bool
}

type FreezeState uint8

const (
	FreezeUnknown FreezeState = iota
	FreezeThawed
	FreezeFrozen
)

type CgroupFeatures struct {
	KillAvailable   bool
	FreezeAvailable bool
}

type cgroupObject interface {
	LeafName() string
	RootObject() ObjectIdentity
	LeafObject() ObjectIdentity
	Close() error
}

type cgroupFS interface {
	RootIdentity(context.Context) (RootIdentity, error)
	CreateChild(context.Context, string) (cgroupObject, error)
	Open(context.Context, string) (cgroupObject, error)
	// Verify validates the held root/leaf object handles only. The current leaf
	// pathname is reserved for Open and guarded tombstone cleanup decisions.
	Verify(context.Context, cgroupObject) (bool, error)
	ProbeFeatures(context.Context, cgroupObject) (CgroupFeatures, error)
	WriteProcs(context.Context, cgroupObject, int) error
	ReadProcs(context.Context, cgroupObject) ([]int, error)
	// ReadEvents reads cgroup.events from the held leaf object, not the current
	// leaf name target.
	ReadEvents(context.Context, cgroupObject) (Events, error)
	WriteKill(context.Context, cgroupObject) error
	WriteFreeze(context.Context, cgroupObject, FreezeState) error
	ReadFreeze(context.Context, cgroupObject) (FreezeState, error)
	// Remove is best-effort cleanup for a held object already proven empty by
	// Membership. Absence must not depend on name-based directory removal.
	Remove(context.Context, cgroupObject) error
}

type processTerminator interface {
	Open(int) (processHandle, error)
	SendTerm(processHandle) error
	Close(processHandle) error
}

type processHandle interface{}

type probeProcess struct {
	PID     int
	Wait    func() error
	Cleanup func()
}

type probeSpawner func(context.Context) (probeProcess, error)

type managerOptions struct {
	newLeaf    func() (string, error)
	terminator processTerminator
	spawnProbe probeSpawner
}

type Manager struct {
	mu         sync.Mutex
	fs         cgroupFS
	newLeaf    func() (string, error)
	terminator processTerminator
	spawnProbe probeSpawner
	closed     bool
}

func newManagerWithFS(fs cgroupFS, options managerOptions) *Manager {
	if options.newLeaf == nil {
		options.newLeaf = randomLeaf
	}
	if options.terminator == nil {
		options.terminator = unsupportedTerminator{}
	}
	if options.spawnProbe == nil {
		options.spawnProbe = spawnSleepProbe
	}
	return &Manager{
		fs:         fs,
		newLeaf:    options.newLeaf,
		terminator: options.terminator,
		spawnProbe: options.spawnProbe,
	}
}

func (manager *Manager) Probe(ctx context.Context) Support {
	if manager == nil || manager.fs == nil {
		return unsupportedSupport(fmt.Errorf("%w: manager is nil", ErrUnsupported))
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closed {
		return unsupportedSupport(fmt.Errorf("%w: manager is closed", ErrUnsupported))
	}
	root, err := manager.fs.RootIdentity(ctx)
	if err != nil {
		return unsupportedSupport(err)
	}
	if err := strictSupportError(root); err != nil {
		return unsupportedSupport(err)
	}

	capability, err := manager.create(ctx, root)
	if err != nil {
		return unsupportedSupport(err)
	}
	defer capability.Release()
	defer func() {
		if capability.removed {
			return
		}
		_, _ = capability.Kill(ctx)
		_ = capability.Remove(ctx)
	}()

	features, err := capability.ProbeFeatures(ctx)
	if err != nil {
		return unsupportedSupport(fmt.Errorf("%w: probe cgroup features: %v", ErrUnsupported, err))
	}
	if !features.KillAvailable {
		reason := fmt.Errorf("%w: cgroup.kill is required", ErrUnsupported)
		if features.FreezeAvailable {
			return Support{
				Supported:          false,
				RuntimeProbePassed: false,
				Degraded:           true,
				Platform:           runtime.GOOS,
				Reason:             reason,
			}
		}
		return unsupportedSupport(reason)
	}

	proc, err := manager.spawnProbe(ctx)
	if err != nil {
		return unsupportedSupport(fmt.Errorf("%w: probe process: %v", ErrUnsupported, err))
	}
	if proc.Cleanup != nil {
		defer proc.Cleanup()
	}
	if err := manager.fs.WriteProcs(ctx, capability.object, proc.PID); err != nil {
		return unsupportedSupport(fmt.Errorf("%w: place probe process: %v", ErrUnsupported, err))
	}
	if membership, err := capability.Membership(ctx); err != nil || membership != containment.RetainedMembershipPresent {
		return unsupportedSupport(fmt.Errorf("%w: observe populated: membership=%v err=%v", ErrUnsupported, membership, err))
	}
	if result, err := capability.Kill(ctx); err != nil || result != containment.SignalDelivered {
		return unsupportedSupport(fmt.Errorf("%w: kill probe process: result=%v err=%v", ErrUnsupported, result, err))
	}
	if proc.Wait != nil {
		if err := proc.Wait(); err != nil {
			return unsupportedSupport(fmt.Errorf("%w: wait probe process: %v", ErrUnsupported, err))
		}
	}
	if membership, err := capability.Membership(ctx); err != nil || membership != containment.RetainedMembershipEmpty {
		return unsupportedSupport(fmt.Errorf("%w: observe empty: membership=%v err=%v", ErrUnsupported, membership, err))
	}
	if err := capability.Remove(ctx); err != nil {
		return unsupportedSupport(fmt.Errorf("%w: remove probe cgroup: %v", ErrUnsupported, err))
	}
	return Support{
		Supported:          true,
		RuntimeProbePassed: true,
		Platform:           runtime.GOOS,
	}
}

func strictSupportError(root RootIdentity) error {
	switch {
	case !root.Unified:
		return fmt.Errorf("%w: cgroup v2 unified hierarchy is required", ErrUnsupported)
	case root.ReadOnly:
		return fmt.Errorf("%w: cgroupfs is read-only", ErrUnsupported)
	case !root.Delegated:
		return fmt.Errorf("%w: delegated writable cgroup root is required", ErrUnsupported)
	case !root.Exclusive:
		return fmt.Errorf("%w: exclusive delegated cgroup root ownership is required", ErrUnsupported)
	case root.Threaded:
		return fmt.Errorf("%w: threaded cgroups are not strict containment", ErrUnsupported)
	case !root.RootObject.durable():
		return fmt.Errorf("%w: delegated cgroup root durable identity is required", ErrUnsupported)
	default:
		return nil
	}
}

func unsupportedSupport(reason error) Support {
	if reason == nil {
		reason = ErrUnsupported
	}
	return Support{
		Supported:          false,
		RuntimeProbePassed: false,
		Platform:           runtime.GOOS,
		Reason:             reason,
	}
}

const retainedIDVersion = "cg2a"

type retainedDescriptor struct {
	leafName string
	root     ObjectIdentity
	leaf     ObjectIdentity
}

func newRetainedID(leafName string, object cgroupObject) (string, error) {
	if object == nil {
		return "", fmt.Errorf("%w: cgroup object is missing", ErrInvalid)
	}
	if err := validateRetainedID(leafName); err != nil {
		return "", err
	}
	root := object.RootObject()
	leaf := object.LeafObject()
	if !root.durable() || !leaf.durable() {
		return "", fmt.Errorf("%w: cgroup durable object identity is required", ErrUnsupported)
	}
	encodedLeaf := base64.RawURLEncoding.EncodeToString([]byte(leafName))
	retainedID := strings.Join([]string{
		retainedIDVersion,
		encodedLeaf,
		root.stableToken(),
		leaf.stableToken(),
	}, ".")
	if len(retainedID) > 256 {
		return "", fmt.Errorf("%w: retained id is too long", ErrInvalid)
	}
	return retainedID, nil
}

func parseRetainedID(retainedID string) (retainedDescriptor, error) {
	parts := strings.Split(retainedID, ".")
	if len(parts) != 4 || parts[0] != retainedIDVersion {
		return retainedDescriptor{}, fmt.Errorf("%w: retained id does not carry cgroup object identity", ErrInvalid)
	}
	leafBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return retainedDescriptor{}, fmt.Errorf("%w: retained id leaf name is invalid", ErrInvalid)
	}
	leafName := string(leafBytes)
	if err := validateRetainedID(leafName); err != nil {
		return retainedDescriptor{}, err
	}
	root, err := parseObjectIdentityToken(parts[2])
	if err != nil {
		return retainedDescriptor{}, err
	}
	leaf, err := parseObjectIdentityToken(parts[3])
	if err != nil {
		return retainedDescriptor{}, err
	}
	return retainedDescriptor{leafName: leafName, root: root, leaf: leaf}, nil
}

func (descriptor retainedDescriptor) matches(object cgroupObject) bool {
	if object == nil {
		return false
	}
	return descriptor.leafName == object.LeafName() &&
		descriptor.root.durableEqual(object.RootObject()) &&
		descriptor.leaf.durableEqual(object.LeafObject())
}

func (manager *Manager) AcquireRetainedGroup(ctx context.Context, target model.GroupRef, _ time.Time) (containment.RetainedGroupCapability, error) {
	if manager == nil || manager.fs == nil {
		return nil, fmt.Errorf("%w: manager is nil", ErrUnsupported)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closed {
		return nil, fmt.Errorf("%w: manager is closed", ErrUnsupported)
	}
	root, err := manager.fs.RootIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if err := strictSupportError(root); err != nil {
		return nil, err
	}
	if target.RetainedID == "" {
		capability, err := manager.create(ctx, root)
		if err != nil {
			return nil, err
		}
		return capability, nil
	}
	capability, err := manager.open(ctx, root, target.RetainedID)
	if err != nil {
		return nil, err
	}
	return capability, nil
}

func (manager *Manager) Close() error {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.closed {
		return nil
	}
	manager.closed = true
	closer, ok := manager.fs.(interface{ Close() error })
	if !ok || closer == nil {
		return nil
	}
	return closer.Close()
}

func (manager *Manager) create(ctx context.Context, root RootIdentity) (*Capability, error) {
	kernelDomain, err := root.kernelDomain()
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 16; attempt++ {
		retainedID, err := manager.newLeaf()
		if err != nil {
			return nil, err
		}
		if err := validateRetainedID(retainedID); err != nil {
			return nil, err
		}
		object, err := manager.fs.CreateChild(ctx, retainedID)
		if err != nil {
			if errors.Is(err, errLeafCollision) {
				continue
			}
			if errors.Is(err, ErrInvalid) {
				return nil, err
			}
			return nil, err
		}
		if !root.RootObject.durableEqual(object.RootObject()) {
			_ = object.Close()
			return nil, fmt.Errorf("%w: cgroup root changed during acquisition", ErrUnsupported)
		}
		durableID, err := newRetainedID(retainedID, object)
		if err != nil {
			_ = object.Close()
			return nil, err
		}
		return &Capability{
			fs:           manager.fs,
			terminator:   manager.terminator,
			retainedID:   durableID,
			leafName:     retainedID,
			kernelDomain: kernelDomain,
			object:       object,
		}, nil
	}
	return nil, fmt.Errorf("%w: could not allocate unique cgroup leaf", ErrUnsupported)
}

func (manager *Manager) open(ctx context.Context, root RootIdentity, retainedID string) (*Capability, error) {
	descriptor, err := parseRetainedID(retainedID)
	if err != nil {
		return nil, err
	}
	kernelDomain, err := root.kernelDomain()
	if err != nil {
		return nil, err
	}
	object, err := manager.fs.Open(ctx, descriptor.leafName)
	if err != nil {
		return nil, err
	}
	if !root.RootObject.durableEqual(object.RootObject()) {
		_ = object.Close()
		return nil, fmt.Errorf("%w: cgroup root changed during acquisition", ErrUnsupported)
	}
	if !descriptor.matches(object) {
		_ = object.Close()
		return nil, fmt.Errorf("%w: retained cgroup object identity mismatch", ErrUnsupported)
	}
	return &Capability{
		fs:           manager.fs,
		terminator:   manager.terminator,
		retainedID:   retainedID,
		leafName:     descriptor.leafName,
		kernelDomain: kernelDomain,
		object:       object,
	}, nil
}

type Capability struct {
	fs           cgroupFS
	terminator   processTerminator
	retainedID   string
	leafName     string
	kernelDomain model.KernelDomainID
	object       cgroupObject
	released     bool
	removed      bool
}

func (capability *Capability) Identity() containment.RetainedGroupIdentity {
	if capability == nil {
		return containment.RetainedGroupIdentity{}
	}
	return containment.RetainedGroupIdentity{
		RetainedID:     capability.retainedID,
		KernelDomainID: capability.kernelDomain,
	}
}

func (capability *Capability) Membership(ctx context.Context) (containment.RetainedGroupMembership, error) {
	if capability == nil || capability.released || capability.removed {
		return containment.RetainedMembershipUnknown, fmt.Errorf("%w: cgroup capability is closed", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return containment.RetainedMembershipUnknown, err
	}
	events, err := capability.fs.ReadEvents(ctx, capability.object)
	if err != nil {
		return containment.RetainedMembershipUnknown, nil
	}
	if events.Populated {
		return containment.RetainedMembershipPresent, nil
	}
	return containment.RetainedMembershipEmpty, nil
}

func (capability *Capability) PlacePID(ctx context.Context, pid int) error {
	if capability == nil || capability.released || capability.removed {
		return fmt.Errorf("%w: cgroup capability is closed", ErrInvalid)
	}
	if pid <= 0 {
		return fmt.Errorf("%w: pid must be positive", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := capability.requireHeld(ctx); err != nil {
		return err
	}
	if err := capability.fs.WriteProcs(ctx, capability.object, pid); err != nil {
		return err
	}
	pids, err := capability.fs.ReadProcs(ctx, capability.object)
	if err != nil {
		return err
	}
	if !slices.Contains(pids, pid) {
		return fmt.Errorf("%w: pid %d was not observed in retained cgroup after placement", ErrUnsupported, pid)
	}
	return nil
}

func (capability *Capability) StillHeld(ctx context.Context) (bool, error) {
	if capability == nil || capability.released || capability.removed {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return capability.fs.Verify(ctx, capability.object)
}

func (capability *Capability) BeginGroupContinuity(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, observedAt time.Time) containment.GroupContinuity {
	if capability == nil || observedAt.IsZero() || observation.Group != model.GroupLive || observation.Leader != model.ProcessIdentityMatching {
		return cgroupContinuity{}
	}
	if !capability.identityMatches(target) {
		return cgroupContinuity{}
	}
	membership, err := capability.Membership(ctx)
	if err != nil || membership != containment.RetainedMembershipPresent {
		return cgroupContinuity{}
	}
	held, err := capability.StillHeld(ctx)
	if err != nil || !held {
		return cgroupContinuity{}
	}
	return cgroupContinuity{capability: capability, begin: observedAt}
}

type cgroupContinuity struct {
	capability *Capability
	begin      time.Time
}

func (continuity cgroupContinuity) ConfirmContinuouslyLive(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, begin, end time.Time) containment.GroupContinuityEvidence {
	if continuity.capability == nil || begin.Before(continuity.begin) || end.Before(begin) {
		return containment.GroupContinuityEvidence{}
	}
	if observation.Group != model.GroupLive || !continuity.capability.identityMatches(target) {
		return containment.GroupContinuityEvidence{}
	}
	membership, err := continuity.capability.Membership(ctx)
	if err != nil || membership != containment.RetainedMembershipPresent {
		return containment.GroupContinuityEvidence{}
	}
	held, err := continuity.capability.StillHeld(ctx)
	if err != nil || !held {
		return containment.GroupContinuityEvidence{}
	}
	evidence, err := containment.NewGroupContinuityEvidence(target, begin, end)
	if err != nil {
		return containment.GroupContinuityEvidence{}
	}
	return evidence
}

func (capability *Capability) identityMatches(target model.GroupRef) bool {
	if capability == nil {
		return false
	}
	if err := target.Validate(); err != nil {
		return false
	}
	identity := capability.Identity()
	if err := identity.KernelDomainID.Validate(); err != nil {
		return false
	}
	return identity.RetainedID == target.RetainedID && identity.KernelDomainID.ProvablySame(target.KernelDomain())
}

func (capability *Capability) ProbeFeatures(ctx context.Context) (CgroupFeatures, error) {
	if capability == nil || capability.released || capability.removed {
		return CgroupFeatures{}, fmt.Errorf("%w: cgroup capability is closed", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return CgroupFeatures{}, err
	}
	return capability.fs.ProbeFeatures(ctx, capability.object)
}

func (capability *Capability) SignalTerm(ctx context.Context) (containment.SignalResult, error) {
	if capability == nil || capability.released || capability.removed {
		return containment.SignalUnprovable, fmt.Errorf("%w: cgroup capability is closed", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return containment.SignalUnprovable, err
	}
	if err := capability.requireHeld(ctx); err != nil {
		return containment.SignalUnprovable, err
	}
	pids, err := capability.fs.ReadProcs(ctx, capability.object)
	if err != nil {
		return containment.SignalUnprovable, err
	}
	pids = uniquePIDs(pids)
	if len(pids) == 0 {
		events, err := capability.fs.ReadEvents(ctx, capability.object)
		if err != nil {
			return containment.SignalUnprovable, err
		}
		if events.Populated {
			return containment.SignalDelivered, nil
		}
		return containment.SignalTargetAbsent, nil
	}
	for _, pid := range pids {
		handle, err := capability.terminator.Open(pid)
		if err != nil {
			if errors.Is(err, errProcessGone) {
				continue
			}
			return containment.SignalUnprovable, err
		}
		current, verifyErr := capability.fs.ReadProcs(ctx, capability.object)
		if verifyErr != nil {
			_ = capability.terminator.Close(handle)
			return containment.SignalUnprovable, verifyErr
		}
		if !slices.Contains(current, pid) {
			_ = capability.terminator.Close(handle)
			continue
		}
		if err := capability.terminator.SendTerm(handle); err != nil {
			_ = capability.terminator.Close(handle)
			return containment.SignalUnprovable, err
		}
		if err := capability.terminator.Close(handle); err != nil {
			return containment.SignalUnprovable, err
		}
	}
	return containment.SignalDelivered, nil
}

func (capability *Capability) Kill(ctx context.Context) (containment.SignalResult, error) {
	if capability == nil || capability.released || capability.removed {
		return containment.SignalUnprovable, fmt.Errorf("%w: cgroup capability is closed", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return containment.SignalUnprovable, err
	}
	if err := capability.requireHeld(ctx); err != nil {
		return containment.SignalUnprovable, err
	}
	if err := capability.fs.WriteKill(ctx, capability.object); err != nil {
		return containment.SignalUnprovable, err
	}
	return containment.SignalDelivered, nil
}

func (capability *Capability) Remove(ctx context.Context) error {
	if capability == nil || capability.released {
		return fmt.Errorf("%w: cgroup capability is closed", ErrInvalid)
	}
	if capability.removed {
		return nil
	}
	membership, err := capability.Membership(ctx)
	if err != nil {
		return err
	}
	if membership != containment.RetainedMembershipEmpty {
		return ErrPopulated
	}
	_ = capability.fs.Remove(ctx, capability.object)
	capability.removed = true
	return nil
}

func (capability *Capability) Release() error {
	if capability == nil {
		return nil
	}
	if capability.released {
		return nil
	}
	capability.released = true
	if capability.object == nil {
		return nil
	}
	return capability.object.Close()
}

func (capability *Capability) requireHeld(ctx context.Context) error {
	held, err := capability.StillHeld(ctx)
	if err != nil {
		return err
	}
	if !held {
		return fmt.Errorf("%w: cgroup object is no longer held", ErrUnsupported)
	}
	return nil
}

func validateRetainedID(retainedID string) error {
	if retainedID == "" {
		return fmt.Errorf("%w: retained id is required", ErrInvalid)
	}
	if strings.ContainsAny(retainedID, `/\`) || retainedID == "." || retainedID == ".." {
		return fmt.Errorf("%w: retained id must be an opaque leaf", ErrInvalid)
	}
	if !tokenSafe(retainedID) {
		return fmt.Errorf("%w: retained id must be token-safe", ErrInvalid)
	}
	return nil
}

type cgroupMount struct {
	ID         string
	MountPoint string
	FSType     string
}

func classifyUnifiedCgroupRoot(procSelfCgroup string, mounts []cgroupMount, root string) (cgroupMount, error) {
	if err := requireUnifiedSelfCgroup(procSelfCgroup); err != nil {
		return cgroupMount{}, err
	}
	mount, ok := bestMountForRoot(mounts, root)
	if !ok {
		return cgroupMount{}, fmt.Errorf("%w: cgroup mount for root is unknown", ErrUnsupported)
	}
	if mount.FSType != "cgroup2" {
		return cgroupMount{}, fmt.Errorf("%w: cgroup root is not a unified cgroup2 mount", ErrUnsupported)
	}
	if strings.TrimSpace(mount.ID) == "" {
		return cgroupMount{}, fmt.Errorf("%w: cgroup mount id is unknown", ErrUnsupported)
	}
	return mount, nil
}

func requireUnifiedSelfCgroup(data string) error {
	lines := strings.Split(data, "\n")
	unified := 0
	legacy := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			return fmt.Errorf("%w: malformed /proc/self/cgroup", ErrUnsupported)
		}
		if parts[0] == "0" && parts[1] == "" {
			unified++
			continue
		}
		legacy++
	}
	if unified != 1 || legacy != 0 {
		return fmt.Errorf("%w: cgroup hierarchy is legacy, hybrid, or ambiguous", ErrUnsupported)
	}
	return nil
}

func bestMountForRoot(mounts []cgroupMount, root string) (cgroupMount, bool) {
	root = filepath.Clean(root)
	var best cgroupMount
	bestLen := -1
	for _, mount := range mounts {
		mountPoint := filepath.Clean(mount.MountPoint)
		if root != mountPoint && !strings.HasPrefix(root, mountPoint+"/") {
			continue
		}
		if len(mountPoint) > bestLen {
			best = mount
			bestLen = len(mountPoint)
		}
	}
	return best, bestLen >= 0
}

func parseMountInfo(data string) ([]cgroupMount, error) {
	lines := strings.Split(data, "\n")
	mounts := make([]cgroupMount, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return nil, fmt.Errorf("%w: malformed mountinfo", ErrUnsupported)
		}
		separator := slices.Index(fields, "-")
		if separator < 0 || separator+1 >= len(fields) || separator < 5 {
			return nil, fmt.Errorf("%w: malformed mountinfo", ErrUnsupported)
		}
		mounts = append(mounts, cgroupMount{
			ID:         fields[0],
			MountPoint: unescapeMountInfo(fields[4]),
			FSType:     fields[separator+1],
		})
	}
	return mounts, nil
}

func unescapeMountInfo(value string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(value)
}

func tokenSafe(value string) bool {
	if len(value) > 256 {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	return true
}

func randomLeaf() (string, error) {
	return randomLeafFromReader(rand.Reader)
}

func randomLeafFromReader(reader io.Reader) (string, error) {
	var raw [16]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return "", fmt.Errorf("%w: crypto-random cgroup leaf unavailable: %v", ErrUnsupported, err)
	}
	return "cg-" + hex.EncodeToString(raw[:]), nil
}

func isManagedLeafName(name string) bool {
	if len(name) != len("cg-")+32 || !strings.HasPrefix(name, "cg-") {
		return false
	}
	for _, r := range name[len("cg-"):] {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

func uniquePIDs(pids []int) []int {
	seen := make(map[int]struct{}, len(pids))
	out := make([]int, 0, len(pids))
	for _, pid := range pids {
		if pid <= 1 {
			continue
		}
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		out = append(out, pid)
	}
	slices.Sort(out)
	return out
}

type unsupportedTerminator struct{}

func (unsupportedTerminator) Open(int) (processHandle, error) {
	return nil, fmt.Errorf("%w: pidfd terminator is unavailable", ErrUnsupported)
}

func (unsupportedTerminator) SendTerm(processHandle) error {
	return fmt.Errorf("%w: pidfd terminator is unavailable", ErrUnsupported)
}

func (unsupportedTerminator) Close(processHandle) error {
	return nil
}

func spawnSleepProbe(ctx context.Context) (probeProcess, error) {
	cmd := exec.CommandContext(ctx, "sleep", "60")
	if err := cmd.Start(); err != nil {
		return probeProcess{}, err
	}
	return probeProcess{
		PID: cmd.Process.Pid,
		Wait: func() error {
			err := cmd.Wait()
			if err == nil {
				return nil
			}
			if exitErr, ok := err.(*exec.ExitError); ok && !exitErr.Success() {
				return nil
			}
			return err
		},
		Cleanup: func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			_ = cmd.Wait()
		},
	}, nil
}
