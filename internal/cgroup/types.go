package cgroup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
)

var (
	ErrUnsupported = errors.New("cgroup unsupported")
	ErrInvalid     = errors.New("cgroup invalid")
	ErrPopulated   = errors.New("cgroup populated")
	errProcessGone = errors.New("process gone")
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
	Threaded          bool
	KillAvailable     bool
	FreezeAvailable   bool
}

func (identity RootIdentity) kernelDomain() (model.KernelDomainID, error) {
	if strings.TrimSpace(identity.HostBootID) == "" {
		return model.KernelDomainID{}, fmt.Errorf("%w: host boot id is required", ErrInvalid)
	}
	if strings.TrimSpace(identity.PIDNamespaceID) == "" {
		return model.KernelDomainID{}, fmt.Errorf("%w: pid namespace id is required", ErrInvalid)
	}
	return model.NewKernelDomainIDWithRetainedDomain(
		tokenOrDigest("boot", identity.HostBootID),
		tokenOrDigest("pidns", identity.PIDNamespaceID),
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
		identity.ChangeSec == other.ChangeSec &&
		identity.ChangeNsec == other.ChangeNsec &&
		identity.Generation == other.Generation
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

type cgroupFS interface {
	RootIdentity(context.Context) (RootIdentity, error)
	CreateChild(context.Context, string) (ObjectIdentity, error)
	Open(context.Context, string) (ObjectIdentity, error)
	WriteProcs(context.Context, string, int) error
	ReadProcs(context.Context, string) ([]int, error)
	ReadEvents(context.Context, string) (Events, error)
	WriteKill(context.Context, string) error
	WriteFreeze(context.Context, string, FreezeState) error
	ReadFreeze(context.Context, string) (FreezeState, error)
	Remove(context.Context, string) error
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
	newLeaf    func() string
	terminator processTerminator
	spawnProbe probeSpawner
}

type Manager struct {
	fs         cgroupFS
	newLeaf    func() string
	terminator processTerminator
	spawnProbe probeSpawner
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
	root, err := manager.fs.RootIdentity(ctx)
	if err != nil {
		return unsupportedSupport(err)
	}
	if err := strictSupportError(root); err != nil {
		if !root.KillAvailable && root.FreezeAvailable {
			return Support{
				Supported:          false,
				RuntimeProbePassed: false,
				Degraded:           true,
				Platform:           runtime.GOOS,
				Reason:             err,
			}
		}
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

	proc, err := manager.spawnProbe(ctx)
	if err != nil {
		return unsupportedSupport(fmt.Errorf("%w: probe process: %v", ErrUnsupported, err))
	}
	if proc.Cleanup != nil {
		defer proc.Cleanup()
	}
	if err := manager.fs.WriteProcs(ctx, capability.retainedID, proc.PID); err != nil {
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
	case root.Threaded:
		return fmt.Errorf("%w: threaded cgroups are not strict containment", ErrUnsupported)
	case !root.KillAvailable:
		return fmt.Errorf("%w: cgroup.kill is required", ErrUnsupported)
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

func (manager *Manager) AcquireRetainedGroup(ctx context.Context, target model.GroupRef, _ time.Time) (containment.RetainedGroupCapability, error) {
	if manager == nil || manager.fs == nil {
		return nil, fmt.Errorf("%w: manager is nil", ErrUnsupported)
	}
	root, err := manager.fs.RootIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if err := strictSupportError(root); err != nil {
		return nil, err
	}
	if target.RetainedID == "" {
		return manager.create(ctx, root)
	}
	return manager.open(ctx, root, target.RetainedID)
}

func (manager *Manager) create(ctx context.Context, root RootIdentity) (*Capability, error) {
	kernelDomain, err := root.kernelDomain()
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 16; attempt++ {
		retainedID := manager.newLeaf()
		if err := validateRetainedID(retainedID); err != nil {
			return nil, err
		}
		object, err := manager.fs.CreateChild(ctx, retainedID)
		if err != nil {
			if errors.Is(err, ErrInvalid) {
				return nil, err
			}
			continue
		}
		return &Capability{
			fs:           manager.fs,
			terminator:   manager.terminator,
			retainedID:   retainedID,
			kernelDomain: kernelDomain,
			object:       object,
		}, nil
	}
	return nil, fmt.Errorf("%w: could not allocate unique cgroup leaf", ErrUnsupported)
}

func (manager *Manager) open(ctx context.Context, root RootIdentity, retainedID string) (*Capability, error) {
	if err := validateRetainedID(retainedID); err != nil {
		return nil, err
	}
	kernelDomain, err := root.kernelDomain()
	if err != nil {
		return nil, err
	}
	object, err := manager.fs.Open(ctx, retainedID)
	if err != nil {
		return nil, err
	}
	return &Capability{
		fs:           manager.fs,
		terminator:   manager.terminator,
		retainedID:   retainedID,
		kernelDomain: kernelDomain,
		object:       object,
	}, nil
}

type Capability struct {
	fs           cgroupFS
	terminator   processTerminator
	retainedID   string
	kernelDomain model.KernelDomainID
	object       ObjectIdentity
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
	events, err := capability.fs.ReadEvents(ctx, capability.retainedID)
	if err != nil {
		return containment.RetainedMembershipUnknown, nil
	}
	if events.Populated {
		return containment.RetainedMembershipPresent, nil
	}
	return containment.RetainedMembershipEmpty, nil
}

func (capability *Capability) StillHeld(ctx context.Context) (bool, error) {
	if capability == nil || capability.released || capability.removed {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	current, err := capability.fs.Open(ctx, capability.retainedID)
	if err != nil {
		return false, nil
	}
	return capability.object.Equal(current), nil
}

func (capability *Capability) SignalTerm(ctx context.Context) (containment.SignalResult, error) {
	if capability == nil || capability.released || capability.removed {
		return containment.SignalUnprovable, fmt.Errorf("%w: cgroup capability is closed", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return containment.SignalUnprovable, err
	}
	pids, err := capability.fs.ReadProcs(ctx, capability.retainedID)
	if err != nil {
		return containment.SignalUnprovable, err
	}
	pids = uniquePIDs(pids)
	if len(pids) == 0 {
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
		current, verifyErr := capability.fs.ReadProcs(ctx, capability.retainedID)
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
	if err := capability.fs.WriteKill(ctx, capability.retainedID); err != nil {
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
	if err := capability.fs.Remove(ctx, capability.retainedID); err != nil {
		return err
	}
	capability.removed = true
	return nil
}

func (capability *Capability) Release() error {
	if capability == nil {
		return nil
	}
	capability.released = true
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

func tokenOrDigest(prefix, value string) string {
	value = strings.TrimSpace(value)
	if tokenSafe(value) {
		return value
	}
	return tokenize(prefix, value)
}

func randomLeaf() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		now := time.Now().UnixNano()
		return "cg-" + strconv.FormatInt(now, 36)
	}
	return "cg-" + hex.EncodeToString(raw[:])
}

func tokenize(prefix, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return prefix + "-unknown"
	}
	var builder strings.Builder
	builder.WriteString(prefix)
	builder.WriteByte('-')
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	out := builder.String()
	if len(out) <= 256 {
		return out
	}
	sum := sha256.Sum256([]byte(value))
	return prefix + "-" + hex.EncodeToString(sum[:16])
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
