package cgroup

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
)

func TestAcquireCreatesUniqueLeaf(t *testing.T) {
	fs := newFakeCgroupFS()
	fs.mustCreate("cg-collision")
	manager := newFakeManager(fs, leafSequence("cg-collision", "cg-next"))

	capability, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if err != nil {
		t.Fatalf("AcquireRetainedGroup() error = %v", err)
	}

	identity := capability.Identity()
	descriptor, err := parseRetainedID(identity.RetainedID)
	if err != nil {
		t.Fatalf("retained id %q did not parse: %v", identity.RetainedID, err)
	}
	if descriptor.leafName != "cg-next" {
		t.Fatalf("retained leaf = %q, want cg-next", descriptor.leafName)
	}
	if !fs.exists("cg-next") {
		t.Fatalf("created leaf was not present in fake fs")
	}
	if identity.KernelDomainID.RetainedDomainState != model.RetainedDomainKnown {
		t.Fatalf("retained domain state = %v, want known", identity.KernelDomainID.RetainedDomainState)
	}
}

func TestMembershipMapsPopulatedEmptyAndUnknown(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-membership"))
	capability := acquireCapability(t, manager)

	fs.setProcs(capability.retainedID, 100)
	if got := membership(t, capability); got != containment.RetainedMembershipPresent {
		t.Fatalf("membership populated = %v, want present", got)
	}

	fs.setProcs(capability.retainedID)
	if got := membership(t, capability); got != containment.RetainedMembershipEmpty {
		t.Fatalf("membership empty = %v, want empty", got)
	}

	fs.setEventsErr(capability.retainedID, errors.New("events unreadable"))
	got, err := capability.Membership(context.Background())
	if err != nil {
		t.Fatalf("Membership() read error surfaced err = %v, want unknown without err", err)
	}
	if got != containment.RetainedMembershipUnknown {
		t.Fatalf("membership on events read error = %v, want unknown", got)
	}
}

func TestKillEmptiesThenRemoveFences(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-kill"))
	capability := acquireCapability(t, manager)
	fs.setProcs(capability.retainedID, 100, 101)

	result, err := capability.Kill(context.Background())
	if err != nil || result != containment.SignalDelivered {
		t.Fatalf("Kill() = %v, %v; want delivered nil", result, err)
	}
	if got := membership(t, capability); got != containment.RetainedMembershipEmpty {
		t.Fatalf("membership after kill = %v, want empty", got)
	}
	if err := capability.Remove(context.Background()); err != nil {
		t.Fatalf("Remove() after empty error = %v", err)
	}
	if fs.exists(capability.retainedID) {
		t.Fatalf("leaf still exists after remove")
	}
}

func TestRemoveBeforeEmptyRefused(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-populated"))
	capability := acquireCapability(t, manager)
	fs.setProcs(capability.retainedID, 100)

	err := capability.Remove(context.Background())
	if !errors.Is(err, ErrPopulated) {
		t.Fatalf("Remove() error = %v, want ErrPopulated", err)
	}
	if !fs.exists(capability.retainedID) {
		t.Fatalf("populated leaf was removed")
	}
}

func TestRecreatedLeafInvalidatesStillHeld(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-recreated"))
	capability := acquireCapability(t, manager)

	held, err := capability.StillHeld(context.Background())
	if err != nil || !held {
		t.Fatalf("StillHeld() before recreate = %v, %v; want true nil", held, err)
	}
	fs.recreate(capability.retainedID)
	held, err = capability.StillHeld(context.Background())
	if err != nil {
		t.Fatalf("StillHeld() after recreate error = %v", err)
	}
	if held {
		t.Fatalf("StillHeld() after recreate = true, want false")
	}
}

func TestOpenRejectsRecreatedLeafDurableIdentityMismatch(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-open-recreated"))
	capability := acquireCapability(t, manager)
	retainedID := capability.retainedID
	fs.recreate(retainedID)

	reopened, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{RetainedID: retainedID}, time.Now())
	if reopened != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("AcquireRetainedGroup() after recreate = %T, %v; want nil ErrUnsupported", reopened, err)
	}
}

func TestSignalTermUsesPidfdVerificationAndDoesNotChasePostEnumerationFork(t *testing.T) {
	fs := newFakeCgroupFS()
	terminator := &recordingTerminator{}
	manager := newManagerWithFS(fs, managerOptions{
		newLeaf:    leafSequence("cg-term"),
		terminator: terminator,
		spawnProbe: fakeProbeSpawner,
	})
	capability := acquireCapability(t, manager)
	fs.setProcs(capability.retainedID, 100)
	added := false
	fs.onReadProcs = func(id string, _ []int) {
		if id == capability.leafName && !added {
			added = true
			fs.addProcs(id, 101)
		}
	}

	result, err := capability.SignalTerm(context.Background())
	if err != nil || result != containment.SignalDelivered {
		t.Fatalf("SignalTerm() = %v, %v; want delivered nil", result, err)
	}
	if !slices.Equal(terminator.signaled, []int{100}) {
		t.Fatalf("signaled pids = %v, want [100]", terminator.signaled)
	}
	if got := membership(t, capability); got != containment.RetainedMembershipPresent {
		t.Fatalf("membership after fork race = %v, want present", got)
	}
}

func TestSignalTermDirectEmptyButDescendantPopulatedIsNotAbsent(t *testing.T) {
	fs := newFakeCgroupFS()
	terminator := &recordingTerminator{}
	manager := newManagerWithFS(fs, managerOptions{
		newLeaf:    leafSequence("cg-nested"),
		terminator: terminator,
		spawnProbe: fakeProbeSpawner,
	})
	capability := acquireCapability(t, manager)
	fs.addChildProcs(capability.retainedID, "child", 200)

	result, err := capability.SignalTerm(context.Background())
	if err != nil || result != containment.SignalDelivered {
		t.Fatalf("SignalTerm() = %v, %v; want delivered nil", result, err)
	}
	if len(terminator.signaled) != 0 {
		t.Fatalf("signaled direct pids = %v, want none", terminator.signaled)
	}
	if got := membership(t, capability); got != containment.RetainedMembershipPresent {
		t.Fatalf("membership with populated child = %v, want present", got)
	}

	result, err = capability.Kill(context.Background())
	if err != nil || result != containment.SignalDelivered {
		t.Fatalf("Kill() = %v, %v; want delivered nil", result, err)
	}
	result, err = capability.SignalTerm(context.Background())
	if err != nil || result != containment.SignalTargetAbsent {
		t.Fatalf("SignalTerm() after recursive empty = %v, %v; want absent nil", result, err)
	}
}

func TestSignalTermFailsClosedWhenLeafRecreatedBeforeRead(t *testing.T) {
	fs := newFakeCgroupFS()
	terminator := &recordingTerminator{}
	manager := newManagerWithFS(fs, managerOptions{
		newLeaf:    leafSequence("cg-term-recreate"),
		terminator: terminator,
		spawnProbe: fakeProbeSpawner,
	})
	capability := acquireCapability(t, manager)
	fs.setProcs(capability.retainedID, 100)
	recreated := false
	fs.onBeforeReadProcs = func(id string) {
		if id != capability.leafName || recreated {
			return
		}
		recreated = true
		fs.recreate(capability.retainedID)
		fs.setProcs(capability.retainedID, 999)
	}

	result, err := capability.SignalTerm(context.Background())
	if err == nil || result != containment.SignalUnprovable {
		t.Fatalf("SignalTerm() = %v, %v; want unprovable error", result, err)
	}
	if len(terminator.signaled) != 0 {
		t.Fatalf("signaled pids = %v, want none", terminator.signaled)
	}
	leaf, err := fs.leaf(capability.retainedID)
	if err != nil {
		t.Fatalf("replacement leaf missing: %v", err)
	}
	if _, ok := leaf.procs[999]; !ok {
		t.Fatalf("replacement leaf procs = %v, want untouched pid 999", leaf.procs)
	}
}

func TestKillFailsClosedWhenLeafRecreatedBeforeWrite(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-kill-recreate"))
	capability := acquireCapability(t, manager)
	fs.setProcs(capability.retainedID, 100)
	recreated := false
	fs.onBeforeWriteKill = func(id string) {
		if id != capability.leafName || recreated {
			return
		}
		recreated = true
		fs.recreate(capability.retainedID)
		fs.setProcs(capability.retainedID, 999)
	}

	result, err := capability.Kill(context.Background())
	if err == nil || result != containment.SignalUnprovable {
		t.Fatalf("Kill() = %v, %v; want unprovable error", result, err)
	}
	if fs.killWrites != 0 {
		t.Fatalf("kill writes = %d, want 0", fs.killWrites)
	}
	leaf, err := fs.leaf(capability.retainedID)
	if err != nil {
		t.Fatalf("replacement leaf missing: %v", err)
	}
	if _, ok := leaf.procs[999]; !ok {
		t.Fatalf("replacement leaf procs = %v, want untouched pid 999", leaf.procs)
	}
}

func TestProbeClassifiesStrictSupportAndUnsupportedConditions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RootIdentity, *fakeCgroupFS)
	}{
		{
			name: "cgroupfs_absent",
			mutate: func(_ *RootIdentity, fs *fakeCgroupFS) {
				fs.rootErr = fmt.Errorf("%w: absent", ErrUnsupported)
			},
		},
		{
			name: "cgroup_v1_or_hybrid",
			mutate: func(root *RootIdentity, _ *fakeCgroupFS) {
				root.Unified = false
			},
		},
		{
			name: "read_only",
			mutate: func(root *RootIdentity, _ *fakeCgroupFS) {
				root.ReadOnly = true
			},
		},
		{
			name: "no_delegation",
			mutate: func(root *RootIdentity, _ *fakeCgroupFS) {
				root.Delegated = false
			},
		},
		{
			name: "container_without_delegation",
			mutate: func(root *RootIdentity, _ *fakeCgroupFS) {
				root.Delegated = false
			},
		},
		{
			name: "threaded",
			mutate: func(root *RootIdentity, _ *fakeCgroupFS) {
				root.Threaded = true
			},
		},
		{
			name: "missing_kill_without_freeze_fallback",
			mutate: func(root *RootIdentity, _ *fakeCgroupFS) {
				root.KillAvailable = false
				root.FreezeAvailable = false
			},
		},
	}

	t.Run("supported", func(t *testing.T) {
		fs := newFakeCgroupFS()
		manager := newFakeManager(fs, leafSequence("cg-probe"))
		support := manager.Probe(context.Background())
		if !support.Strict() {
			t.Fatalf("Probe() support = %#v, want strict supported", support)
		}
		if fs.killWrites != 2 || fs.removeCalls != 1 {
			t.Fatalf("probe kill/remove calls = %d/%d, want 2/1", fs.killWrites, fs.removeCalls)
		}
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFakeCgroupFS()
			root := fs.root
			tt.mutate(&root, fs)
			fs.root = root
			manager := newFakeManager(fs, leafSequence("cg-probe"))
			support := manager.Probe(context.Background())
			if support.Supported || support.RuntimeProbePassed || support.Reason == nil || !errors.Is(support.Reason, ErrUnsupported) {
				t.Fatalf("Probe() support = %#v, want unsupported reason", support)
			}
		})
	}
}

func TestProbeFlagsFreezeFallbackAsDegraded(t *testing.T) {
	fs := newFakeCgroupFS()
	fs.root.KillAvailable = false
	fs.root.FreezeAvailable = true
	manager := newFakeManager(fs, leafSequence("cg-freeze"))

	support := manager.Probe(context.Background())
	if support.Supported || support.RuntimeProbePassed || !support.Degraded || !errors.Is(support.Reason, ErrUnsupported) {
		t.Fatalf("Probe() support = %#v, want degraded unsupported", support)
	}
}

func TestClassifyUnifiedCgroupRootRejectsHybridAndAmbiguous(t *testing.T) {
	validMounts := []cgroupMount{{ID: "42", MountPoint: "/sys/fs/cgroup", FSType: "cgroup2"}}
	mount, err := classifyUnifiedCgroupRoot("0::/\n", validMounts, "/sys/fs/cgroup")
	if err != nil {
		t.Fatalf("classify valid unified root error = %v", err)
	}
	if mount.ID != "42" {
		t.Fatalf("mount id = %q, want 42", mount.ID)
	}

	tests := []struct {
		name   string
		cgroup string
		mounts []cgroupMount
		root   string
	}{
		{
			name:   "hybrid",
			cgroup: "0::/\n2:cpu,cpuacct:/\n",
			mounts: validMounts,
			root:   "/sys/fs/cgroup",
		},
		{
			name:   "legacy_only",
			cgroup: "2:cpu,cpuacct:/\n",
			mounts: validMounts,
			root:   "/sys/fs/cgroup",
		},
		{
			name:   "malformed",
			cgroup: "not-a-cgroup-record\n",
			mounts: validMounts,
			root:   "/sys/fs/cgroup",
		},
		{
			name:   "non_cgroup2_mount",
			cgroup: "0::/\n",
			mounts: []cgroupMount{{ID: "10", MountPoint: "/sys/fs/cgroup", FSType: "cgroup"}},
			root:   "/sys/fs/cgroup",
		},
		{
			name:   "missing_mount",
			cgroup: "0::/\n",
			mounts: validMounts,
			root:   "/not/sys/fs/cgroup",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := classifyUnifiedCgroupRoot(tt.cgroup, tt.mounts, tt.root)
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("classify error = %v, want ErrUnsupported", err)
			}
		})
	}
}

func acquireCapability(t *testing.T, manager *Manager) *Capability {
	t.Helper()
	capability, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if err != nil {
		t.Fatalf("AcquireRetainedGroup() error = %v", err)
	}
	typed, ok := capability.(*Capability)
	if !ok {
		t.Fatalf("capability = %T, want *Capability", capability)
	}
	return typed
}

func membership(t *testing.T, capability *Capability) containment.RetainedGroupMembership {
	t.Helper()
	membership, err := capability.Membership(context.Background())
	if err != nil {
		t.Fatalf("Membership() error = %v", err)
	}
	return membership
}

func newFakeManager(fs *fakeCgroupFS, leaves func() string) *Manager {
	return newManagerWithFS(fs, managerOptions{
		newLeaf:    leaves,
		terminator: &recordingTerminator{},
		spawnProbe: fakeProbeSpawner,
	})
}

func leafSequence(values ...string) func() string {
	index := 0
	return func() string {
		if index >= len(values) {
			return values[len(values)-1]
		}
		value := values[index]
		index++
		return value
	}
}

func fakeProbeSpawner(context.Context) (probeProcess, error) {
	return probeProcess{PID: 4242, Wait: func() error { return nil }}, nil
}

type recordingTerminator struct {
	signaled []int
	closed   []int
}

func (terminator *recordingTerminator) Open(pid int) (processHandle, error) {
	return pid, nil
}

func (terminator *recordingTerminator) SendTerm(handle processHandle) error {
	terminator.signaled = append(terminator.signaled, handle.(int))
	return nil
}

func (terminator *recordingTerminator) Close(handle processHandle) error {
	terminator.closed = append(terminator.closed, handle.(int))
	return nil
}

type fakeCgroupFS struct {
	root              RootIdentity
	rootErr           error
	leaves            map[string]*fakeLeaf
	nextInode         uint64
	onBeforeReadProcs func(string)
	onReadProcs       func(string, []int)
	onBeforeWriteKill func(string)
	killWrites        int
	removeCalls       int
}

type fakeLeaf struct {
	object    ObjectIdentity
	procs     map[int]struct{}
	children  map[string]map[int]struct{}
	eventsErr error
	freeze    FreezeState
	removed   bool
}

type fakeCgroupObject struct {
	name    string
	root    ObjectIdentity
	leaf    ObjectIdentity
	leafRef *fakeLeaf
	closed  bool
}

func (object *fakeCgroupObject) LeafName() string {
	if object == nil {
		return ""
	}
	return object.name
}

func (object *fakeCgroupObject) RootObject() ObjectIdentity {
	if object == nil {
		return ObjectIdentity{}
	}
	return object.root
}

func (object *fakeCgroupObject) LeafObject() ObjectIdentity {
	if object == nil {
		return ObjectIdentity{}
	}
	return object.leaf
}

func (object *fakeCgroupObject) Close() error {
	if object != nil {
		object.closed = true
	}
	return nil
}

func newFakeCgroupFS() *fakeCgroupFS {
	return &fakeCgroupFS{
		root: RootIdentity{
			HostBootID:        "host-boot-1",
			PIDNamespaceID:    "pidns-1",
			CgroupNamespaceID: "cgns-1",
			MountID:           "mount-1",
			HierarchyID:       "hierarchy-1",
			Unified:           true,
			Delegated:         true,
			KillAvailable:     true,
			FreezeAvailable:   true,
			RootObject:        ObjectIdentity{Device: 1, Inode: 2, Generation: "root-1"},
		},
		leaves:    map[string]*fakeLeaf{},
		nextInode: 10,
	}
}

func (fs *fakeCgroupFS) RootIdentity(context.Context) (RootIdentity, error) {
	if fs.rootErr != nil {
		return RootIdentity{}, fs.rootErr
	}
	return fs.root, nil
}

func (fs *fakeCgroupFS) CreateChild(_ context.Context, id string) (cgroupObject, error) {
	if err := validateRetainedID(id); err != nil {
		return nil, err
	}
	if leaf, ok := fs.leaves[id]; ok && !leaf.removed {
		return nil, fmt.Errorf("%w: exists", ErrUnsupported)
	}
	leaf := fs.newLeaf()
	fs.leaves[id] = leaf
	return fs.objectFor(id, leaf), nil
}

func (fs *fakeCgroupFS) Open(_ context.Context, id string) (cgroupObject, error) {
	leaf, ok := fs.leaves[id]
	if !ok || leaf.removed {
		return nil, fmt.Errorf("%w: missing leaf", ErrUnsupported)
	}
	return fs.objectFor(id, leaf), nil
}

func (fs *fakeCgroupFS) Verify(_ context.Context, object cgroupObject) (bool, error) {
	_, err := fs.leafForObject(object)
	if err != nil {
		if errors.Is(err, ErrUnsupported) || errors.Is(err, ErrInvalid) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (fs *fakeCgroupFS) ProbeFeatures(ctx context.Context, object cgroupObject) (CgroupFeatures, error) {
	features := CgroupFeatures{}
	if fs.root.KillAvailable {
		if err := fs.WriteKill(ctx, object); err != nil {
			return features, err
		}
		features.KillAvailable = true
	}
	if fs.root.FreezeAvailable {
		if err := fs.WriteFreeze(ctx, object, FreezeFrozen); err != nil {
			return features, err
		}
		state, err := fs.ReadFreeze(ctx, object)
		if err != nil {
			return features, err
		}
		if err := fs.WriteFreeze(ctx, object, FreezeThawed); err != nil {
			return features, err
		}
		thawed, err := fs.ReadFreeze(ctx, object)
		if err != nil {
			return features, err
		}
		features.FreezeAvailable = state == FreezeFrozen && thawed == FreezeThawed
	}
	return features, nil
}

func (fs *fakeCgroupFS) WriteProcs(_ context.Context, object cgroupObject, pid int) error {
	leaf, err := fs.leafForObject(object)
	if err != nil {
		return err
	}
	leaf.procs[pid] = struct{}{}
	return nil
}

func (fs *fakeCgroupFS) ReadProcs(_ context.Context, object cgroupObject) ([]int, error) {
	if fs.onBeforeReadProcs != nil {
		fs.onBeforeReadProcs(object.LeafName())
	}
	leaf, err := fs.leafForObject(object)
	if err != nil {
		return nil, err
	}
	pids := make([]int, 0, len(leaf.procs))
	for pid := range leaf.procs {
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	if fs.onReadProcs != nil {
		fs.onReadProcs(object.LeafName(), slices.Clone(pids))
	}
	return pids, nil
}

func (fs *fakeCgroupFS) ReadEvents(_ context.Context, object cgroupObject) (Events, error) {
	leaf, err := fs.leafForObject(object)
	if err != nil {
		return Events{}, err
	}
	if leaf.eventsErr != nil {
		return Events{}, leaf.eventsErr
	}
	return Events{Populated: leaf.populated()}, nil
}

func (fs *fakeCgroupFS) WriteKill(_ context.Context, object cgroupObject) error {
	if !fs.root.KillAvailable {
		return fmt.Errorf("%w: cgroup.kill missing", ErrUnsupported)
	}
	if fs.onBeforeWriteKill != nil {
		fs.onBeforeWriteKill(object.LeafName())
	}
	leaf, err := fs.leafForObject(object)
	if err != nil {
		return err
	}
	fs.killWrites++
	leaf.procs = map[int]struct{}{}
	leaf.children = map[string]map[int]struct{}{}
	return nil
}

func (fs *fakeCgroupFS) WriteFreeze(_ context.Context, object cgroupObject, state FreezeState) error {
	if !fs.root.FreezeAvailable {
		return fmt.Errorf("%w: cgroup.freeze missing", ErrUnsupported)
	}
	leaf, err := fs.leafForObject(object)
	if err != nil {
		return err
	}
	leaf.freeze = state
	return nil
}

func (fs *fakeCgroupFS) ReadFreeze(_ context.Context, object cgroupObject) (FreezeState, error) {
	if !fs.root.FreezeAvailable {
		return FreezeUnknown, fmt.Errorf("%w: cgroup.freeze missing", ErrUnsupported)
	}
	leaf, err := fs.leafForObject(object)
	if err != nil {
		return FreezeUnknown, err
	}
	return leaf.freeze, nil
}

func (fs *fakeCgroupFS) Remove(_ context.Context, object cgroupObject) error {
	leaf, err := fs.leafForObject(object)
	if err != nil {
		return err
	}
	if leaf.populated() {
		return ErrPopulated
	}
	fs.removeCalls++
	leaf.removed = true
	return nil
}

func (fs *fakeCgroupFS) mustCreate(id string) {
	if _, err := fs.CreateChild(context.Background(), id); err != nil {
		panic(err)
	}
}

func (fs *fakeCgroupFS) exists(id string) bool {
	leaf, ok := fs.leaves[fs.leafName(id)]
	return ok && !leaf.removed
}

func (fs *fakeCgroupFS) setProcs(id string, pids ...int) {
	leaf, err := fs.leaf(id)
	if err != nil {
		panic(err)
	}
	leaf.procs = map[int]struct{}{}
	for _, pid := range pids {
		leaf.procs[pid] = struct{}{}
	}
}

func (fs *fakeCgroupFS) addProcs(id string, pids ...int) {
	leaf, err := fs.leaf(id)
	if err != nil {
		panic(err)
	}
	for _, pid := range pids {
		leaf.procs[pid] = struct{}{}
	}
}

func (fs *fakeCgroupFS) addChildProcs(id, child string, pids ...int) {
	leaf, err := fs.leaf(id)
	if err != nil {
		panic(err)
	}
	if leaf.children == nil {
		leaf.children = map[string]map[int]struct{}{}
	}
	if leaf.children[child] == nil {
		leaf.children[child] = map[int]struct{}{}
	}
	for _, pid := range pids {
		leaf.children[child][pid] = struct{}{}
	}
}

func (fs *fakeCgroupFS) setEventsErr(id string, err error) {
	leaf, leafErr := fs.leaf(id)
	if leafErr != nil {
		panic(leafErr)
	}
	leaf.eventsErr = err
}

func (fs *fakeCgroupFS) recreate(id string) {
	leaf, err := fs.leaf(id)
	if err != nil {
		panic(err)
	}
	leaf.removed = true
	fs.leaves[fs.leafName(id)] = fs.newLeaf()
}

func (fs *fakeCgroupFS) leaf(id string) (*fakeLeaf, error) {
	name := fs.leafName(id)
	leaf, ok := fs.leaves[name]
	if !ok || leaf.removed {
		return nil, fmt.Errorf("%w: missing leaf", ErrUnsupported)
	}
	return leaf, nil
}

func (fs *fakeCgroupFS) leafName(id string) string {
	if _, ok := fs.leaves[id]; ok {
		return id
	}
	descriptor, err := parseRetainedID(id)
	if err == nil {
		return descriptor.leafName
	}
	return id
}

func (fs *fakeCgroupFS) newLeaf() *fakeLeaf {
	fs.nextInode++
	return &fakeLeaf{
		object:   ObjectIdentity{Device: 1, Inode: fs.nextInode, Generation: fmt.Sprintf("gen-%d", fs.nextInode)},
		procs:    map[int]struct{}{},
		children: map[string]map[int]struct{}{},
		freeze:   FreezeThawed,
	}
}

func (fs *fakeCgroupFS) objectFor(name string, leaf *fakeLeaf) *fakeCgroupObject {
	return &fakeCgroupObject{
		name:    name,
		root:    fs.root.RootObject,
		leaf:    leaf.object,
		leafRef: leaf,
	}
}

func (fs *fakeCgroupFS) leafForObject(object cgroupObject) (*fakeLeaf, error) {
	fakeObject, ok := object.(*fakeCgroupObject)
	if !ok || fakeObject == nil || fakeObject.closed {
		return nil, fmt.Errorf("%w: invalid cgroup object", ErrInvalid)
	}
	current, ok := fs.leaves[fakeObject.name]
	if !ok || current.removed || fakeObject.leafRef.removed || current != fakeObject.leafRef {
		return nil, fmt.Errorf("%w: stale cgroup object", ErrUnsupported)
	}
	if !fakeObject.root.Equal(fs.root.RootObject) || !fakeObject.leaf.Equal(current.object) {
		return nil, fmt.Errorf("%w: stale cgroup object", ErrUnsupported)
	}
	return current, nil
}

func (leaf *fakeLeaf) populated() bool {
	if len(leaf.procs) > 0 {
		return true
	}
	for _, procs := range leaf.children {
		if len(procs) > 0 {
			return true
		}
	}
	return false
}
