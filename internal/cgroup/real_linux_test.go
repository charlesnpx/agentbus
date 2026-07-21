//go:build linux

package cgroup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRealFSAcquireHeldRootRejectsUnleasedRoot(t *testing.T) {
	root := ObjectIdentity{Device: 1, Inode: 2, Generation: "root"}
	fs := &realFS{
		held: &realRoot{
			fd:       -1,
			identity: RootIdentity{RootObject: root},
			leased:   false,
		},
	}

	held, release, ok := fs.acquireHeldRoot(root)
	if ok || held != nil || release != nil {
		t.Fatalf("acquireHeldRoot() with leased=false = held:%v release-nil:%t ok:%t, want nil true false", held, release == nil, ok)
	}
}

func TestRealFSRemoveWithoutRootLeaseFailsClosed(t *testing.T) {
	rootPath := t.TempDir()
	leaffd, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open leaf fd: %v", err)
	}
	defer unix.Close(leaffd)

	root := ObjectIdentity{Device: 1, Inode: 2, Generation: "root"}
	leaf := ObjectIdentity{Device: 1, Inode: 3, Generation: "leaf"}
	fs := &realFS{root: rootPath}
	object := &realObject{
		rootPath: rootPath,
		leafName: "cg-no-root-lease",
		leaffd:   leaffd,
		root:     root,
		leaf:     leaf,
	}

	err = fs.Remove(context.Background(), object)
	if !errors.Is(err, ErrRootLeaseUnavailable) {
		t.Fatalf("Remove() without root lease error = %v, want ErrRootLeaseUnavailable", err)
	}
}

func TestRealFSRootLeaseFlockContentionIsTypedUnavailable(t *testing.T) {
	rootPath := t.TempDir()
	first, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatalf("open first root fd: %v", err)
	}
	defer unix.Close(first)
	second, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatalf("open second root fd: %v", err)
	}
	defer unix.Close(second)

	fs := &realFS{root: rootPath}
	exclusive, err := fs.establishExclusiveDelegation(first)
	if err != nil {
		t.Fatalf("first establishExclusiveDelegation() error = %v", err)
	}
	if !exclusive {
		t.Fatal("first establishExclusiveDelegation() exclusive = false, want true")
	}
	defer unix.Flock(first, unix.LOCK_UN)

	exclusive, err = fs.establishExclusiveDelegation(second)
	if exclusive || !errors.Is(err, ErrRootLeaseUnavailable) || errors.Is(err, ErrUnsupported) {
		t.Fatalf("second establishExclusiveDelegation() = %v, %v; want false ErrRootLeaseUnavailable only", exclusive, err)
	}
}

func TestRealFSCgroupRootOpenAndLeaseErrorsStayTyped(t *testing.T) {
	openTests := []struct {
		err         error
		retryable   bool
		unsupported bool
	}{
		{err: unix.EMFILE, retryable: true},
		{err: unix.ENFILE, retryable: true},
		{err: unix.ENOMEM, retryable: true},
		{err: unix.EINTR, retryable: true},
		{err: unix.ENOLCK, unsupported: true},
		{err: unix.ENOENT, unsupported: true},
		{err: unix.EACCES, unsupported: true},
		{err: unix.EPERM, unsupported: true},
	}
	for _, tt := range openTests {
		openErr := cgroupRootOpenError(tt.err)
		if got := errors.Is(openErr, ErrRootLeaseUnavailable); got != tt.retryable {
			t.Fatalf("cgroupRootOpenError(%v) retryable = %t error=%v, want %t", tt.err, got, openErr, tt.retryable)
		}
		if got := errors.Is(openErr, ErrUnsupported); got != tt.unsupported {
			t.Fatalf("cgroupRootOpenError(%v) unsupported = %t error=%v, want %t", tt.err, got, openErr, tt.unsupported)
		}
	}

	leaseTests := []struct {
		err         error
		retryable   bool
		unsupported bool
	}{
		{err: unix.EAGAIN, retryable: true},
		{err: unix.EWOULDBLOCK, retryable: true},
		{err: unix.EINTR, retryable: true},
		{err: unix.ENOLCK, unsupported: true},
		{err: unix.EACCES, unsupported: true},
		{err: unix.EPERM, unsupported: true},
	}
	for _, tt := range leaseTests {
		leaseErr := cgroupRootLeaseAcquireError(tt.err)
		if got := errors.Is(leaseErr, ErrRootLeaseUnavailable); got != tt.retryable {
			t.Fatalf("cgroupRootLeaseAcquireError(%v) retryable = %t error=%v, want %t", tt.err, got, leaseErr, tt.retryable)
		}
		if got := errors.Is(leaseErr, ErrUnsupported); got != tt.unsupported {
			t.Fatalf("cgroupRootLeaseAcquireError(%v) unsupported = %t error=%v, want %t", tt.err, got, leaseErr, tt.unsupported)
		}
	}
}

func TestRealFSRemoveTombstonesMissingLeafWithoutReentrantLock(t *testing.T) {
	rootPath := t.TempDir()
	rootfd, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open root fd: %v", err)
	}
	leaffd, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(rootfd)
		t.Fatalf("open leaf fd: %v", err)
	}
	defer unix.Close(leaffd)

	root := ObjectIdentity{Device: 1, Inode: 2, Generation: "root"}
	leaf := ObjectIdentity{Device: 1, Inode: 3, Generation: "leaf"}
	fs := &realFS{
		root: rootPath,
		held: &realRoot{
			fd: rootfd,
			identity: RootIdentity{
				RootObject: root,
			},
			leased: true,
		},
	}
	defer fs.releaseRootLease()

	object := &realObject{
		rootPath: rootPath,
		leafName: "cg-missing",
		rootfd:   rootfd,
		leaffd:   leaffd,
		root:     root,
		leaf:     leaf,
	}

	done := make(chan error, 1)
	go func() {
		done <- fs.Remove(context.Background(), object)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Remove() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Remove() did not complete; possible reentrant fs.mu tombstone deadlock")
	}

	fs.mu.Lock()
	tombstone, ok := fs.tombstones[object.leafName]
	fs.mu.Unlock()
	if !ok {
		t.Fatalf("Remove() did not record tombstone for missing leaf")
	}
	if !leaf.durableEqual(tombstone) {
		t.Fatalf("tombstone = %+v, want %+v", tombstone, leaf)
	}
}

func TestRealFSUnleasedInheritedCleanupRemovesMatchingLeaf(t *testing.T) {
	fs, object := newUnleasedInheritedCleanupFixture(t, "cg-inherited-cleanup")
	defer object.Close()

	if err := fs.removeUnleasedInheritedLeaf(context.Background(), object); err != nil {
		t.Fatalf("removeUnleasedInheritedLeaf() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(fs.root, object.leafName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retained leaf stat after unleased cleanup = %v, want not-exist", err)
	}
	fs.mu.Lock()
	tombstone, ok := fs.tombstones[object.leafName]
	fs.mu.Unlock()
	if !ok {
		t.Fatalf("unleased cleanup did not tombstone removed leaf")
	}
	if !object.leaf.durableEqual(tombstone) {
		t.Fatalf("unleased cleanup tombstone = %+v, want %+v", tombstone, object.leaf)
	}
}

func TestRealFSUnleasedInheritedCleanupSkipsWhenRootLeaseHeld(t *testing.T) {
	fs, object := newUnleasedInheritedCleanupFixture(t, "cg-inherited-contender")
	defer object.Close()
	path := filepath.Join(fs.root, object.leafName)
	contenderRoot, err := unix.Open(fs.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open contender root: %v", err)
	}
	defer unix.Close(contenderRoot)
	if err := unix.Flock(contenderRoot, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("contender flock root: %v", err)
	}
	defer unix.Flock(contenderRoot, unix.LOCK_UN)

	err = fs.removeUnleasedInheritedLeaf(context.Background(), object)
	if !errors.Is(err, ErrRootLeaseUnavailable) || errors.Is(err, ErrUnsupported) {
		t.Fatalf("removeUnleasedInheritedLeaf() under contender flock error = %v, want ErrRootLeaseUnavailable only", err)
	}
	if !strings.Contains(err.Error(), "unleased cleanup skipped: root lease held by another owner") {
		t.Fatalf("removeUnleasedInheritedLeaf() under contender flock error = %q, want cleanup skip reason", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("retained leaf stat after skipped cleanup = %v, want present", err)
	}
	fs.mu.Lock()
	_, tombstoned := fs.tombstones[object.leafName]
	fs.mu.Unlock()
	if tombstoned {
		t.Fatalf("unleased cleanup tombstoned leaf after skipped cleanup")
	}
}

func TestRealFSUnleasedInheritedCleanupTombstonesStaleNameAtEntry(t *testing.T) {
	fs, object := newUnleasedInheritedCleanupFixture(t, "cg-inherited-stale")
	defer object.Close()
	path := filepath.Join(fs.root, object.leafName)
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove original leaf: %v", err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("recreate contender leaf: %v", err)
	}
	marker := filepath.Join(path, "contender-marker")
	if err := os.WriteFile(marker, []byte("owned\n"), 0o600); err != nil {
		t.Fatalf("write contender marker: %v", err)
	}

	err := fs.removeUnleasedInheritedLeaf(context.Background(), object)
	if err != nil {
		t.Fatalf("removeUnleasedInheritedLeaf() after identity drift error = %v, want nil", err)
	}
	requireUnleasedCleanupReplacementUntouched(t, marker)
	requireUnleasedCleanupTombstone(t, fs, object)
}

func TestRealFSUnleasedInheritedCleanupRechecksNameAfterInheritedVerify(t *testing.T) {
	fs, object := newUnleasedInheritedCleanupFixture(t, "cg-inherited-race")
	defer object.Close()
	path := filepath.Join(fs.root, object.leafName)
	marker := filepath.Join(path, "contender-marker")

	err := fs.removeUnleasedInheritedLeafAfterVerify(context.Background(), object, func() {
		if err := os.Remove(path); err != nil {
			t.Fatalf("remove original leaf: %v", err)
		}
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatalf("recreate contender leaf: %v", err)
		}
		if err := os.WriteFile(marker, []byte("owned\n"), 0o600); err != nil {
			t.Fatalf("write contender marker: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("removeUnleasedInheritedLeaf() after verified identity drift error = %v, want nil", err)
	}
	requireUnleasedCleanupReplacementUntouched(t, marker)
	requireUnleasedCleanupTombstone(t, fs, object)
}

func requireUnleasedCleanupReplacementUntouched(t *testing.T, marker string) {
	t.Helper()
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("contender marker stat after cleanup attempt = %v, want present", err)
	}
}

func requireUnleasedCleanupTombstone(t *testing.T, fs *realFS, object *realObject) {
	t.Helper()
	fs.mu.Lock()
	tombstone, ok := fs.tombstones[object.leafName]
	fs.mu.Unlock()
	if !ok {
		t.Fatalf("unleased cleanup did not tombstone replaced leaf")
	}
	if !object.leaf.durableEqual(tombstone) {
		t.Fatalf("unleased cleanup tombstone = %+v, want %+v", tombstone, object.leaf)
	}
}

func newUnleasedInheritedCleanupFixture(t *testing.T, leafName string) (*realFS, *realObject) {
	t.Helper()
	rootPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootPath, leafName), 0o755); err != nil {
		t.Fatalf("mkdir leaf: %v", err)
	}
	rootfd, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open root fd: %v", err)
	}
	leaffd, err := unix.Openat(rootfd, leafName, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(rootfd)
		t.Fatalf("open leaf fd: %v", err)
	}
	root, err := identityFromFD(rootfd)
	if err != nil {
		_ = unix.Close(rootfd)
		_ = unix.Close(leaffd)
		t.Skipf("filesystem does not provide durable root handles: %v", err)
	}
	leaf, err := identityFromFD(leaffd)
	if err != nil {
		_ = unix.Close(rootfd)
		_ = unix.Close(leaffd)
		t.Skipf("filesystem does not provide durable leaf handles: %v", err)
	}
	fs := &realFS{root: rootPath}
	object := &realObject{
		rootPath:    rootPath,
		leafName:    leafName,
		rootfd:      rootfd,
		leaffd:      leaffd,
		root:        root,
		leaf:        leaf,
		closeRootFD: true,
	}
	return fs, object
}
