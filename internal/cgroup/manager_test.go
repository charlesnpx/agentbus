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
	if identity.RetainedID != "cg-next" {
		t.Fatalf("retained id = %q, want cg-next", identity.RetainedID)
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
		if id == capability.retainedID && !added {
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
			name: "missing_kill",
			mutate: func(root *RootIdentity, _ *fakeCgroupFS) {
				root.KillAvailable = false
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
		if fs.killWrites != 1 || fs.removeCalls != 1 {
			t.Fatalf("probe kill/remove calls = %d/%d, want 1/1", fs.killWrites, fs.removeCalls)
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
	root        RootIdentity
	rootErr     error
	leaves      map[string]*fakeLeaf
	nextInode   uint64
	onReadProcs func(string, []int)
	killWrites  int
	removeCalls int
}

type fakeLeaf struct {
	object    ObjectIdentity
	procs     map[int]struct{}
	eventsErr error
	freeze    FreezeState
	removed   bool
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

func (fs *fakeCgroupFS) CreateChild(_ context.Context, id string) (ObjectIdentity, error) {
	if err := validateRetainedID(id); err != nil {
		return ObjectIdentity{}, err
	}
	if leaf, ok := fs.leaves[id]; ok && !leaf.removed {
		return ObjectIdentity{}, fmt.Errorf("%w: exists", ErrUnsupported)
	}
	fs.nextInode++
	leaf := &fakeLeaf{
		object: ObjectIdentity{Device: 1, Inode: fs.nextInode, Generation: fmt.Sprintf("gen-%d", fs.nextInode)},
		procs:  map[int]struct{}{},
		freeze: FreezeThawed,
	}
	fs.leaves[id] = leaf
	return leaf.object, nil
}

func (fs *fakeCgroupFS) Open(_ context.Context, id string) (ObjectIdentity, error) {
	leaf, ok := fs.leaves[id]
	if !ok || leaf.removed {
		return ObjectIdentity{}, fmt.Errorf("%w: missing leaf", ErrUnsupported)
	}
	return leaf.object, nil
}

func (fs *fakeCgroupFS) WriteProcs(_ context.Context, id string, pid int) error {
	leaf, err := fs.leaf(id)
	if err != nil {
		return err
	}
	leaf.procs[pid] = struct{}{}
	return nil
}

func (fs *fakeCgroupFS) ReadProcs(_ context.Context, id string) ([]int, error) {
	leaf, err := fs.leaf(id)
	if err != nil {
		return nil, err
	}
	pids := make([]int, 0, len(leaf.procs))
	for pid := range leaf.procs {
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	if fs.onReadProcs != nil {
		fs.onReadProcs(id, slices.Clone(pids))
	}
	return pids, nil
}

func (fs *fakeCgroupFS) ReadEvents(_ context.Context, id string) (Events, error) {
	leaf, err := fs.leaf(id)
	if err != nil {
		return Events{}, err
	}
	if leaf.eventsErr != nil {
		return Events{}, leaf.eventsErr
	}
	return Events{Populated: len(leaf.procs) > 0}, nil
}

func (fs *fakeCgroupFS) WriteKill(_ context.Context, id string) error {
	if !fs.root.KillAvailable {
		return fmt.Errorf("%w: cgroup.kill missing", ErrUnsupported)
	}
	leaf, err := fs.leaf(id)
	if err != nil {
		return err
	}
	fs.killWrites++
	leaf.procs = map[int]struct{}{}
	return nil
}

func (fs *fakeCgroupFS) WriteFreeze(_ context.Context, id string, state FreezeState) error {
	if !fs.root.FreezeAvailable {
		return fmt.Errorf("%w: cgroup.freeze missing", ErrUnsupported)
	}
	leaf, err := fs.leaf(id)
	if err != nil {
		return err
	}
	leaf.freeze = state
	return nil
}

func (fs *fakeCgroupFS) ReadFreeze(_ context.Context, id string) (FreezeState, error) {
	if !fs.root.FreezeAvailable {
		return FreezeUnknown, fmt.Errorf("%w: cgroup.freeze missing", ErrUnsupported)
	}
	leaf, err := fs.leaf(id)
	if err != nil {
		return FreezeUnknown, err
	}
	return leaf.freeze, nil
}

func (fs *fakeCgroupFS) Remove(_ context.Context, id string) error {
	leaf, err := fs.leaf(id)
	if err != nil {
		return err
	}
	if len(leaf.procs) > 0 {
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
	leaf, ok := fs.leaves[id]
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
	fs.nextInode++
	leaf.object = ObjectIdentity{Device: 1, Inode: fs.nextInode, Generation: fmt.Sprintf("gen-%d", fs.nextInode)}
}

func (fs *fakeCgroupFS) leaf(id string) (*fakeLeaf, error) {
	leaf, ok := fs.leaves[id]
	if !ok || leaf.removed {
		return nil, fmt.Errorf("%w: missing leaf", ErrUnsupported)
	}
	return leaf, nil
}
