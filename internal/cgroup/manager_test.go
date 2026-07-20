package cgroup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
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

func TestAcquireFailsClosedWhenCryptoRandomLeafUnavailable(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, func() (string, error) {
		return randomLeafFromReader(errReader{})
	})

	capability, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if capability != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("AcquireRetainedGroup() with unavailable crypto rand = %T, %v; want nil ErrUnsupported", capability, err)
	}
}

func TestRandomLeafUsesCryptoEntropyForManagedNames(t *testing.T) {
	first, err := randomLeafFromReader(bytes.NewReader(bytes.Repeat([]byte{0x11}, 16)))
	if err != nil {
		t.Fatalf("randomLeafFromReader() first error = %v", err)
	}
	second, err := randomLeafFromReader(bytes.NewReader(bytes.Repeat([]byte{0x22}, 16)))
	if err != nil {
		t.Fatalf("randomLeafFromReader() second error = %v", err)
	}
	if first == second {
		t.Fatalf("random leaves reused name %q for distinct entropy", first)
	}
	if !isManagedLeafName(first) || !isManagedLeafName(second) {
		t.Fatalf("random leaves = %q/%q, want managed names", first, second)
	}
}

func TestAcquireFailsClosedWhenLeafRecreatedBetweenMkdirAndOpen(t *testing.T) {
	fs := newFakeCgroupFS()
	recreated := false
	fs.onOpen = func(name string) {
		if recreated {
			return
		}
		recreated = true
		fs.recreate(name)
	}
	manager := newFakeManager(fs, leafSequence("cg-acquire-race"))

	capability, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if capability != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("AcquireRetainedGroup() after recreate = %T, %v; want nil ErrUnsupported", capability, err)
	}
	if !fs.exists("cg-acquire-race") {
		t.Fatalf("replacement leaf was removed during failed acquisition")
	}
}

func TestAcquireFailsClosedWhenLeafRecreatedBetweenPathStatAndHandle(t *testing.T) {
	fs := newFakeCgroupFS()
	recreated := false
	fs.onPathHandle = func(name string) {
		if recreated {
			return
		}
		recreated = true
		fs.recreate(name)
	}
	manager := newFakeManager(fs, leafSequence("cg-path-handle-race"))

	capability, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if capability != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("AcquireRetainedGroup() after path handle race = %T, %v; want nil ErrUnsupported", capability, err)
	}
	if !fs.exists("cg-path-handle-race") {
		t.Fatalf("replacement leaf was removed during failed acquisition")
	}
}

func TestAcquireFailsClosedWhenRootRetargetsBetweenProbeAndCreate(t *testing.T) {
	fs := newFakeCgroupFS()
	retargeted := false
	fs.onAfterRootIdentity = func() {
		if retargeted {
			return
		}
		retargeted = true
		fs.root.RootObject = ObjectIdentity{Device: 9, Inode: 9, Generation: "root-retargeted"}
	}
	manager := newFakeManager(fs, leafSequence("cg-root-retarget"))

	capability, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if capability != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("AcquireRetainedGroup() after root retarget = %T, %v; want nil ErrUnsupported", capability, err)
	}
}

func TestExclusiveLeaseAllowsOnlyOneManager(t *testing.T) {
	lease := &fakeRootLease{}
	firstFS := newFakeCgroupFS()
	firstFS.lease = lease
	secondFS := newFakeCgroupFS()
	secondFS.lease = lease
	first := newFakeManager(firstFS, leafSequence("cg-first"))
	second := newFakeManager(secondFS, leafSequence("cg-second"))

	if capability, err := first.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now()); err != nil {
		t.Fatalf("first AcquireRetainedGroup() error = %v", err)
	} else if capability == nil {
		t.Fatalf("first AcquireRetainedGroup() capability is nil")
	}
	capability, err := second.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if capability != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("second AcquireRetainedGroup() with held lease = %T, %v; want nil ErrUnsupported", capability, err)
	}
}

func TestManagerCloseReleasesExclusiveRootLease(t *testing.T) {
	lease := &fakeRootLease{}
	firstFS := newFakeCgroupFS()
	firstFS.lease = lease
	secondFS := newFakeCgroupFS()
	secondFS.lease = lease
	first := newFakeManager(firstFS, leafSequence("cg-first"))
	second := newFakeManager(secondFS, leafSequence("cg-second"))

	capability := acquireCapability(t, first)
	if _, err := second.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now()); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("second AcquireRetainedGroup() before close error = %v, want ErrUnsupported", err)
	}
	if err := capability.Release(); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if firstFS.closeCalls != 1 {
		t.Fatalf("first fs close calls = %d, want 1", firstFS.closeCalls)
	}
	if lease.holder != nil {
		t.Fatalf("lease holder after Close() = %p, want nil", lease.holder)
	}
	secondCapability, err := second.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if err != nil {
		t.Fatalf("second AcquireRetainedGroup() after close error = %v", err)
	}
	if secondCapability == nil {
		t.Fatalf("second AcquireRetainedGroup() after close capability is nil")
	}
}

func TestAcquireWithoutRootLeaseKeepsGroupCapabilityAndAllowsRootReacquire(t *testing.T) {
	ctx := context.Background()
	lease := &fakeRootLease{}
	daemonFS := newFakeCgroupFS()
	daemonFS.lease = lease
	daemon := newFakeManager(daemonFS, leafSequence("cg-daemon-held"))
	daemonCapability := acquireCapability(t, daemon)
	if err := daemonCapability.PlacePID(ctx, 100); err != nil {
		t.Fatalf("daemon PlacePID() error = %v", err)
	}
	if err := daemonCapability.ReleaseRootLease(); err != nil {
		t.Fatalf("daemon ReleaseRootLease() error = %v", err)
	}
	if lease.holder != nil {
		t.Fatalf("lease holder after daemon ReleaseRootLease() = %p, want nil", lease.holder)
	}
	if object := daemonCapability.object.(*fakeCgroupObject); object.closed {
		t.Fatalf("daemon retained object closed after root lease release")
	}

	monitorFS := newFakeCgroupFS()
	monitorFS.lease = lease
	monitorFS.root = daemonFS.root
	monitorFS.leaves = daemonFS.leaves
	monitor := newFakeManager(monitorFS, leafSequence("cg-monitor-unused"))
	monitorRaw, err := monitor.AcquireRetainedGroupWithoutRootLease(ctx, model.GroupRef{RetainedID: daemonCapability.retainedID}, time.Now())
	if err != nil {
		t.Fatalf("monitor AcquireRetainedGroupWithoutRootLease() error = %v", err)
	}
	monitorCapability := monitorRaw.(*Capability)
	if lease.holder != nil {
		t.Fatalf("lease holder after monitor acquisition = %p, want nil", lease.holder)
	}
	if got := membership(t, monitorCapability); got != containment.RetainedMembershipPresent {
		t.Fatalf("monitor membership after root lease release = %v, want present", got)
	}

	contenderFS := newFakeCgroupFS()
	contenderFS.lease = lease
	contenderFS.root = daemonFS.root
	contenderFS.leaves = daemonFS.leaves
	contender := newFakeManager(contenderFS, leafSequence("cg-contender"))
	contenderCapability, err := contender.AcquireRetainedGroup(ctx, model.GroupRef{}, time.Now())
	if err != nil {
		t.Fatalf("contender AcquireRetainedGroup() error = %v", err)
	}
	defer contenderCapability.Release()
	if lease.holder != contenderFS {
		t.Fatalf("lease holder after contender acquisition = %p, want contender fs %p", lease.holder, contenderFS)
	}

	result, err := monitorCapability.Kill(ctx)
	if err != nil || result != containment.SignalDelivered {
		t.Fatalf("monitor Kill() after root reacquire = %v, %v; want delivered nil", result, err)
	}
	if got := membership(t, monitorCapability); got != containment.RetainedMembershipEmpty {
		t.Fatalf("monitor membership after Kill() = %v, want empty", got)
	}
	if object := monitorCapability.object.(*fakeCgroupObject); object.closed {
		t.Fatalf("monitor retained object closed before Release()")
	}
	if err := monitorCapability.Release(); err != nil {
		t.Fatalf("monitor Release() error = %v", err)
	}
	if object := monitorCapability.object.(*fakeCgroupObject); !object.closed {
		t.Fatalf("monitor retained object open after Release()")
	}
}

func TestReleaseRootLeaseWaitsForConcurrentAcquireCreateChild(t *testing.T) {
	ctx := context.Background()
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-held", "cg-concurrent"))
	capability := acquireCapability(t, manager)

	createEntered := make(chan struct{})
	allowCreate := make(chan struct{})
	releaseEntered := make(chan struct{}, 1)
	fs.onCreateChild = func(name string) {
		if name != "cg-concurrent" {
			return
		}
		close(createEntered)
		<-allowCreate
	}
	fs.onReleaseRootLease = func() {
		releaseEntered <- struct{}{}
	}

	acquireDone := make(chan error, 1)
	go func() {
		acquired, err := manager.AcquireRetainedGroup(ctx, model.GroupRef{}, time.Now())
		if acquired != nil {
			_ = acquired.Release()
		}
		acquireDone <- err
	}()
	<-createEntered

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- capability.ReleaseRootLease()
	}()
	select {
	case <-releaseEntered:
		t.Fatalf("ReleaseRootLease entered fs.releaseRootLease while CreateChild was still using the shared root")
	case err := <-releaseDone:
		t.Fatalf("ReleaseRootLease completed while CreateChild was still using the shared root: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(allowCreate)
	if err := <-acquireDone; err != nil {
		t.Fatalf("concurrent AcquireRetainedGroup() error = %v", err)
	}
	if !fs.exists("cg-concurrent") {
		t.Fatalf("concurrent AcquireRetainedGroup() did not leave created leaf")
	}
	if err := <-releaseDone; err != nil {
		t.Fatalf("ReleaseRootLease() error = %v", err)
	}
	select {
	case <-releaseEntered:
	default:
		t.Fatalf("releaseRootLease hook was not reached after CreateChild completed")
	}
}

func TestReleaseRootLeaseWaitsForConcurrentRemove(t *testing.T) {
	ctx := context.Background()
	fs := newFakeCgroupFS()
	fs.nameCleanupAllowed = true
	fs.failRemoveAfterRootLeaseRelease = true
	manager := newFakeManager(fs, leafSequence("cg-remove", "cg-releaser"))
	removeCapability := acquireCapability(t, manager)
	defer removeCapability.Release()
	releaserCapability := acquireCapability(t, manager)
	defer releaserCapability.Release()

	removeEntered := make(chan struct{})
	allowRemove := make(chan struct{})
	releaseEntered := make(chan struct{}, 1)
	fs.onAfterFinalVerify = func(name string) {
		if name != "cg-remove" {
			return
		}
		close(removeEntered)
		<-allowRemove
	}
	fs.onReleaseRootLease = func() {
		releaseEntered <- struct{}{}
	}

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- removeCapability.Remove(ctx)
	}()
	select {
	case <-removeEntered:
	case err := <-removeDone:
		t.Fatalf("Remove() completed before the fake remove hook was reached: %v", err)
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("Remove() did not reach the fake remove hook")
	}

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- releaserCapability.ReleaseRootLease()
	}()
	select {
	case <-releaseEntered:
		t.Fatalf("ReleaseRootLease entered fs.releaseRootLease while Remove was still using the shared root")
	case err := <-releaseDone:
		t.Fatalf("ReleaseRootLease completed while Remove was still using the shared root: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(allowRemove)
	if err := <-removeDone; err != nil {
		if errors.Is(err, unix.EBADF) {
			t.Fatalf("Remove() used the shared root after ReleaseRootLease closed it: %v", err)
		}
		t.Fatalf("Remove() error = %v", err)
	}
	if fs.exists("cg-remove") {
		t.Fatalf("empty leaf still exists after Remove()")
	}
	if fs.removeCalls != 1 {
		t.Fatalf("remove calls = %d, want 1", fs.removeCalls)
	}
	if err := <-releaseDone; err != nil {
		t.Fatalf("ReleaseRootLease() error = %v", err)
	}
	select {
	case <-releaseEntered:
	default:
		t.Fatalf("releaseRootLease hook was not reached after Remove completed")
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

func TestPlacePIDWritesHeldObjectBeforeRelease(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-place"))
	capability := acquireCapability(t, manager)

	if err := capability.PlacePID(context.Background(), 1234); err != nil {
		t.Fatalf("PlacePID() error = %v", err)
	}

	pids, err := capability.fs.ReadProcs(context.Background(), capability.object)
	if err != nil {
		t.Fatalf("ReadProcs() error = %v", err)
	}
	if !slices.Contains(pids, 1234) {
		t.Fatalf("placed pids = %v, want 1234", pids)
	}
	if got := membership(t, capability); got != containment.RetainedMembershipPresent {
		t.Fatalf("membership after PlacePID() = %v, want present", got)
	}
}

func TestPlacePIDWritesHeldObjectAfterNameReplacement(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-place-replaced"))
	capability := acquireCapability(t, manager)
	heldLeaf := capability.object.(*fakeCgroupObject).leafRef
	fs.replaceNameTarget(capability.retainedID)

	if err := capability.PlacePID(context.Background(), 4321); err != nil {
		t.Fatalf("PlacePID() error = %v", err)
	}

	if _, ok := heldLeaf.procs[4321]; !ok {
		t.Fatalf("held leaf procs = %v, want placed pid", heldLeaf.procs)
	}
	replacement, err := fs.leaf(capability.retainedID)
	if err != nil {
		t.Fatalf("replacement leaf missing: %v", err)
	}
	if _, ok := replacement.procs[4321]; ok {
		t.Fatalf("replacement leaf received placed pid")
	}
}

func TestPlaceProcessRejectsReusedPIDAfterCgroupWrite(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-place-reused-pid"))
	capability := acquireCapability(t, manager)
	domain, err := model.NewKernelDomainID("host-boot-place", "pidns-place")
	if err != nil {
		t.Fatal(err)
	}
	expected, err := procgroup.NewProcessClaim(1234, 1234, "start-original", domain)
	if err != nil {
		t.Fatal(err)
	}
	reused, err := procgroup.NewProcessClaim(1234, 1234, "start-reused", domain)
	if err != nil {
		t.Fatal(err)
	}
	originalReadProcessClaim := readProcessClaimForPlacement
	t.Cleanup(func() {
		readProcessClaimForPlacement = originalReadProcessClaim
	})
	calls := 0
	readProcessClaimForPlacement = func(pid int) (procgroup.ProcessClaim, error) {
		if pid != expected.PID {
			t.Fatalf("read process pid = %d, want %d", pid, expected.PID)
		}
		calls++
		if calls == 1 {
			return expected, nil
		}
		return reused, nil
	}

	err = capability.PlaceProcess(context.Background(), expected)
	if err == nil {
		t.Fatal("PlaceProcess() error = nil, want PID reuse fence failure")
	}
	if !strings.Contains(err.Error(), "process identity changed during cgroup placement") {
		t.Fatalf("PlaceProcess() error = %v, want identity changed", err)
	}
	if calls != 2 {
		t.Fatalf("placement identity reads = %d, want 2", calls)
	}
}

func TestNameReplacementDoesNotVetoHeldObjectEmptyProof(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-name-replacement"))
	capability := acquireCapability(t, manager)
	heldObject := capability.object.(*fakeCgroupObject)
	heldLeaf := heldObject.leafRef
	heldIdentity := heldObject.LeafObject()

	if got := membership(t, capability); got != containment.RetainedMembershipEmpty {
		t.Fatalf("membership before name replacement = %v, want empty", got)
	}
	fs.replaceNameTarget(capability.retainedID)
	replacement, err := fs.leaf(capability.retainedID)
	if err != nil {
		t.Fatalf("replacement leaf missing: %v", err)
	}
	if replacement == heldLeaf {
		t.Fatalf("replacement reused held leaf")
	}
	if heldIdentity == replacement.object {
		t.Fatalf("replacement identity = held identity %#v", heldIdentity)
	}
	fs.setProcs(capability.retainedID, 999)

	if got := membership(t, capability); got != containment.RetainedMembershipEmpty {
		t.Fatalf("membership after name replacement = %v, want held-object empty", got)
	}
	held, err := capability.StillHeld(context.Background())
	if err != nil || !held {
		t.Fatalf("StillHeld() after name replacement = %v, %v; want true nil", held, err)
	}
	if heldLeaf.populated() {
		t.Fatalf("held leaf became populated through replacement name target")
	}
}

func TestDestroyedHeldObjectReportsUnknownMembershipAndUnheld(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-destroyed"))
	capability := acquireCapability(t, manager)

	fs.recreate(capability.retainedID)
	membership, err := capability.Membership(context.Background())
	if err != nil {
		t.Fatalf("Membership() after destroy error = %v", err)
	}
	if membership != containment.RetainedMembershipUnknown {
		t.Fatalf("Membership() after destroy = %v, want unknown", membership)
	}
	held, err := capability.StillHeld(context.Background())
	if err != nil {
		t.Fatalf("StillHeld() after destroy error = %v", err)
	}
	if held {
		t.Fatalf("StillHeld() after destroy = true, want false")
	}
}

func TestKillEmptiesThenRemoveTombstonesAfterEmptyProof(t *testing.T) {
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
	if !fs.exists(capability.retainedID) {
		t.Fatalf("empty leaf was unlinked; want tombstone left behind")
	}
	if !fs.tombstoned(capability.retainedID) {
		t.Fatalf("empty leaf was not recorded as a tombstone")
	}
	if fs.removeCalls != 0 {
		t.Fatalf("remove calls = %d, want 0", fs.removeCalls)
	}
}

func TestRemoveRetiresEmptyLeafWhenCurrentNameStillMatches(t *testing.T) {
	fs := newFakeCgroupFS()
	fs.nameCleanupAllowed = true
	manager := newFakeManager(fs, leafSequence("cg-retire"))
	capability := acquireCapability(t, manager)

	if got := membership(t, capability); got != containment.RetainedMembershipEmpty {
		t.Fatalf("membership before Remove() = %v, want empty", got)
	}
	if err := capability.Remove(context.Background()); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if fs.exists(capability.retainedID) {
		t.Fatalf("empty leaf still exists after Remove()")
	}
	if fs.removeCalls != 1 {
		t.Fatalf("remove calls = %d, want 1", fs.removeCalls)
	}
}

func TestRemoveSurfacesCleanupError(t *testing.T) {
	fs := newFakeCgroupFS()
	fs.nameCleanupAllowed = true
	fs.removeErr = unix.EBADF
	manager := newFakeManager(fs, leafSequence("cg-remove-error"))
	capability := acquireCapability(t, manager)

	err := capability.Remove(context.Background())
	if !errors.Is(err, unix.EBADF) {
		t.Fatalf("Remove() error = %v, want EBADF", err)
	}
	if capability.removed {
		t.Fatalf("capability marked removed after failed cleanup")
	}
	if !fs.exists(capability.retainedID) {
		t.Fatalf("leaf was removed after failed cleanup")
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

func TestRemoveTombstonesWhenEmptyReplacementAppearsAfterFinalVerify(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-remove-race"))
	capability := acquireCapability(t, manager)
	heldIdentity := capability.object.LeafObject()
	recreated := false
	fs.onAfterFinalVerify = func(name string) {
		if recreated {
			return
		}
		recreated = true
		fs.recreate(name)
	}

	err := capability.Remove(context.Background())
	if err != nil {
		t.Fatalf("Remove() after empty replacement error = %v", err)
	}
	if fs.removeCalls != 0 {
		t.Fatalf("remove calls = %d, want 0", fs.removeCalls)
	}
	name := fs.leafName(capability.retainedID)
	tombstone, ok := fs.tombstones[name]
	if !ok {
		t.Fatalf("held empty leaf was not recorded as a tombstone")
	}
	if tombstone != heldIdentity {
		t.Fatalf("tombstone identity = %#v, want held identity %#v", tombstone, heldIdentity)
	}
	leaf, err := fs.leaf(capability.retainedID)
	if err != nil {
		t.Fatalf("replacement leaf missing: %v", err)
	}
	if leaf.object == heldIdentity {
		t.Fatalf("replacement identity = held identity %#v", heldIdentity)
	}
	if tombstone == leaf.object {
		t.Fatalf("tombstone identity = replacement identity %#v", tombstone)
	}
	if leaf.populated() {
		t.Fatalf("replacement leaf populated = true, want empty")
	}
}

func TestFakeUnlinkRemovesCurrentNameTarget(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-unlink-name"))
	capability := acquireCapability(t, manager)
	name := capability.leafName
	heldLeaf := capability.object.(*fakeCgroupObject).leafRef

	fs.recreate(name)
	replacement, err := fs.leaf(name)
	if err != nil {
		t.Fatalf("replacement leaf missing: %v", err)
	}
	if replacement == heldLeaf {
		t.Fatalf("replacement reused held leaf")
	}
	if err := fs.unlinkName(name); err != nil {
		t.Fatalf("unlinkName() error = %v", err)
	}
	if fs.exists(name) {
		t.Fatalf("current name target still exists after unlink")
	}
	if !heldLeaf.removed {
		t.Fatalf("held leaf should have been detached by recreate")
	}
	if !replacement.removed {
		t.Fatalf("replacement was not removed by name-based unlink")
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

func TestOpenRejectsSameNameInodeReuseDurableIdentityMismatch(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-open-inode-reuse"))
	capability := acquireCapability(t, manager)
	retainedID := capability.retainedID
	descriptor, err := parseRetainedID(retainedID)
	if err != nil {
		t.Fatalf("parse retained id error = %v", err)
	}
	fs.recreateReusingInode(retainedID)
	replacement, err := fs.leaf(retainedID)
	if err != nil {
		t.Fatalf("replacement leaf missing: %v", err)
	}
	if replacement.object.Inode != descriptor.leaf.Inode || replacement.object.Generation == descriptor.leaf.Generation {
		t.Fatalf("replacement identity = %#v, want same inode and different generation from %#v", replacement.object, descriptor.leaf)
	}

	reopened, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{RetainedID: retainedID}, time.Now())
	if reopened != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("AcquireRetainedGroup() after inode reuse = %T, %v; want nil ErrUnsupported", reopened, err)
	}
}

func TestOpenRejectsGenerationUnavailable(t *testing.T) {
	fs := newFakeCgroupFS()
	manager := newFakeManager(fs, leafSequence("cg-open-no-generation"))
	capability := acquireCapability(t, manager)
	retainedID := capability.retainedID
	fs.generationAvailable = false

	reopened, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{RetainedID: retainedID}, time.Now())
	if reopened != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("AcquireRetainedGroup() without generation = %T, %v; want nil ErrUnsupported", reopened, err)
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
			name: "exclusive_delegation_not_established",
			mutate: func(root *RootIdentity, _ *fakeCgroupFS) {
				root.Exclusive = false
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
		if fs.killWrites != 2 || fs.removeCalls != 0 || fs.tombstoneCalls != 1 {
			t.Fatalf("probe kill/remove/tombstone calls = %d/%d/%d, want 2/0/1", fs.killWrites, fs.removeCalls, fs.tombstoneCalls)
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

func newFakeManager(fs *fakeCgroupFS, leaves func() (string, error)) *Manager {
	return newManagerWithFS(fs, managerOptions{
		newLeaf:    leaves,
		terminator: &recordingTerminator{},
		spawnProbe: fakeProbeSpawner,
	})
}

func leafSequence(values ...string) func() (string, error) {
	index := 0
	return func() (string, error) {
		if index >= len(values) {
			return values[len(values)-1], nil
		}
		value := values[index]
		index++
		return value, nil
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
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
	root                            RootIdentity
	rootErr                         error
	lease                           *fakeRootLease
	leaves                          map[string]*fakeLeaf
	tombstones                      map[string]ObjectIdentity
	nextInode                       uint64
	nextGeneration                  uint64
	generationAvailable             bool
	nameCleanupAllowed              bool
	failRemoveAfterRootLeaseRelease bool
	removeErr                       error
	onAfterRootIdentity             func()
	onPathStat                      func(string)
	onPathHandle                    func(string)
	onOpen                          func(string)
	onCreateChild                   func(string)
	onReleaseRootLease              func()
	onFinalVerify                   func(string)
	onAfterFinalVerify              func(string)
	onUnlink                        func(string)
	onBeforeReadProcs               func(string)
	onReadProcs                     func(string, []int)
	onBeforeWriteKill               func(string)
	killWrites                      int
	removeCalls                     int
	tombstoneCalls                  int
	rootLeaseReleases               int
	closeCalls                      int
}

type fakeRootLease struct {
	holder *fakeCgroupFS
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
	fs := &fakeCgroupFS{
		root: RootIdentity{
			HostBootID:        "host-boot-1",
			PIDNamespaceID:    "pidns-1",
			CgroupNamespaceID: "cgns-1",
			MountID:           "mount-1",
			HierarchyID:       "hierarchy-1",
			Unified:           true,
			Delegated:         true,
			Exclusive:         true,
			KillAvailable:     true,
			FreezeAvailable:   true,
			RootObject:        ObjectIdentity{Device: 1, Inode: 2, Generation: "root-1"},
		},
		leaves:              map[string]*fakeLeaf{},
		tombstones:          map[string]ObjectIdentity{},
		nextInode:           10,
		nextGeneration:      10,
		generationAvailable: true,
	}
	fs.lease = &fakeRootLease{}
	return fs
}

func (fs *fakeCgroupFS) RootIdentity(context.Context) (RootIdentity, error) {
	if fs.rootErr != nil {
		return RootIdentity{}, fs.rootErr
	}
	if fs.lease != nil {
		if fs.lease.holder == nil {
			fs.lease.holder = fs
		} else if fs.lease.holder != fs {
			return RootIdentity{}, fmt.Errorf("%w: exclusive delegation already leased", ErrUnsupported)
		}
	}
	root := fs.root
	if !fs.generationAvailable {
		root.RootObject.Generation = ""
	}
	if fs.onAfterRootIdentity != nil {
		fs.onAfterRootIdentity()
	}
	return root, nil
}

func (fs *fakeCgroupFS) Close() error {
	fs.closeCalls++
	return fs.releaseRootLease()
}

func (fs *fakeCgroupFS) releaseRootLease() error {
	if fs.onReleaseRootLease != nil {
		fs.onReleaseRootLease()
	}
	fs.rootLeaseReleases++
	if fs.lease != nil && fs.lease.holder == fs {
		fs.lease.holder = nil
	}
	return nil
}

func (fs *fakeCgroupFS) CreateChild(_ context.Context, id string) (cgroupObject, error) {
	if err := validateRetainedID(id); err != nil {
		return nil, err
	}
	if fs.onCreateChild != nil {
		fs.onCreateChild(id)
	}
	if leaf, ok := fs.leaves[id]; ok && !leaf.removed {
		return nil, fmt.Errorf("%w: %w", ErrUnsupported, errLeafCollision)
	}
	leaf := fs.newLeaf()
	fs.leaves[id] = leaf
	createdLeaf, err := fs.identityAt(id)
	if err != nil {
		return nil, err
	}
	object, err := fs.openObject(id)
	if err != nil {
		fs.recordTombstoneIfNameMatches(id, createdLeaf)
		return nil, err
	}
	if !createdLeaf.durableEqual(object.leaf) || !leaf.object.durableEqual(object.leaf) {
		fs.recordTombstoneIfNameMatches(id, createdLeaf)
		return nil, fmt.Errorf("%w: created cgroup leaf was replaced before open", ErrUnsupported)
	}
	return object, nil
}

func (fs *fakeCgroupFS) Open(_ context.Context, id string) (cgroupObject, error) {
	return fs.openObject(id)
}

func (fs *fakeCgroupFS) Verify(_ context.Context, object cgroupObject) (bool, error) {
	_, err := fs.heldLeafForObject(object)
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
	leaf, err := fs.heldLeafForObject(object)
	if err != nil {
		return err
	}
	if leaf.populated() {
		return ErrPopulated
	}
	if fs.onFinalVerify != nil {
		fs.onFinalVerify(object.LeafName())
	}
	currentIdentity, err := fs.identityAt(object.LeafName())
	if err != nil || !object.LeafObject().durableEqual(currentIdentity) {
		fs.recordTombstone(object.LeafName(), object.LeafObject())
		return nil
	}
	if fs.onAfterFinalVerify != nil {
		fs.onAfterFinalVerify(object.LeafName())
	}
	if fs.failRemoveAfterRootLeaseRelease && fs.lease != nil && fs.lease.holder != fs {
		return unix.EBADF
	}
	if !fs.nameCleanupAllowed {
		fs.recordTombstone(object.LeafName(), object.LeafObject())
		return nil
	}
	if fs.onUnlink != nil {
		fs.onUnlink(object.LeafName())
	}
	if fs.removeErr != nil {
		return fs.removeErr
	}
	return fs.unlinkName(object.LeafName())
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

func (fs *fakeCgroupFS) tombstoned(id string) bool {
	_, ok := fs.tombstones[fs.leafName(id)]
	return ok
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
	name := fs.leafName(id)
	if leaf, ok := fs.leaves[name]; ok {
		leaf.removed = true
	}
	fs.leaves[name] = fs.newLeaf()
}

func (fs *fakeCgroupFS) replaceNameTarget(id string) {
	name := fs.leafName(id)
	fs.leaves[name] = fs.newLeaf()
}

func (fs *fakeCgroupFS) recreateReusingInode(id string) {
	name := fs.leafName(id)
	leaf, err := fs.leaf(name)
	if err != nil {
		panic(err)
	}
	inode := leaf.object.Inode
	leaf.removed = true
	fs.leaves[name] = fs.newLeafWithInode(inode)
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
	return fs.newLeafWithInode(fs.nextInode)
}

func (fs *fakeCgroupFS) newLeafWithInode(inode uint64) *fakeLeaf {
	fs.nextGeneration++
	return &fakeLeaf{
		object:   ObjectIdentity{Device: 1, Inode: inode, Generation: fmt.Sprintf("gen-%d", fs.nextGeneration)},
		procs:    map[int]struct{}{},
		children: map[string]map[int]struct{}{},
		freeze:   FreezeThawed,
	}
}

func (fs *fakeCgroupFS) objectFor(name string, leaf *fakeLeaf) *fakeCgroupObject {
	root := fs.root.RootObject
	object := leaf.object
	if !fs.generationAvailable {
		root.Generation = ""
		object.Generation = ""
	}
	return &fakeCgroupObject{
		name:    name,
		root:    root,
		leaf:    object,
		leafRef: leaf,
	}
}

func (fs *fakeCgroupFS) identityAt(name string) (ObjectIdentity, error) {
	if fs.onPathStat != nil {
		fs.onPathStat(name)
	}
	leaf, err := fs.leaf(name)
	if err != nil {
		return ObjectIdentity{}, err
	}
	identity := leaf.object
	identity.Generation = ""
	if fs.onPathHandle != nil {
		fs.onPathHandle(name)
	}
	leaf, err = fs.leaf(name)
	if err != nil {
		return ObjectIdentity{}, err
	}
	if fs.generationAvailable {
		identity.Generation = leaf.object.Generation
	}
	return identity, nil
}

func (fs *fakeCgroupFS) openObject(name string) (*fakeCgroupObject, error) {
	if fs.onOpen != nil {
		fs.onOpen(name)
	}
	leaf, err := fs.leaf(name)
	if err != nil {
		return nil, err
	}
	return fs.objectFor(name, leaf), nil
}

func (fs *fakeCgroupFS) leafForObject(object cgroupObject) (*fakeLeaf, error) {
	return fs.heldLeafForObject(object)
}

func (fs *fakeCgroupFS) heldLeafForObject(object cgroupObject) (*fakeLeaf, error) {
	fakeObject, ok := object.(*fakeCgroupObject)
	if !ok || fakeObject == nil || fakeObject.closed || fakeObject.leafRef == nil || fakeObject.leafRef.removed {
		return nil, fmt.Errorf("%w: invalid cgroup object", ErrInvalid)
	}
	root := fs.root.RootObject
	leaf := fakeObject.leafRef.object
	if !fs.generationAvailable {
		root.Generation = ""
		leaf.Generation = ""
	}
	if !fakeObject.root.durableEqual(root) || !fakeObject.leaf.durableEqual(leaf) {
		return nil, fmt.Errorf("%w: stale cgroup object", ErrUnsupported)
	}
	return fakeObject.leafRef, nil
}

func (fs *fakeCgroupFS) recordTombstoneIfNameMatches(name string, expected ObjectIdentity) {
	current, ok := fs.leaves[name]
	if !ok || current.removed || !expected.durableEqual(current.object) {
		return
	}
	fs.recordTombstone(name, expected)
}

func (fs *fakeCgroupFS) recordTombstone(name string, identity ObjectIdentity) {
	fs.tombstoneCalls++
	fs.tombstones[name] = identity
}

func (fs *fakeCgroupFS) unlinkName(name string) error {
	current, ok := fs.leaves[name]
	if !ok || current.removed {
		return nil
	}
	fs.removeCalls++
	current.removed = true
	delete(fs.leaves, name)
	return nil
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
