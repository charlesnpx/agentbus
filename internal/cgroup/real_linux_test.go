//go:build linux

package cgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

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
	openErr := cgroupRootOpenError(unix.ENOENT)
	if !errors.Is(openErr, ErrUnsupported) {
		t.Fatalf("cgroupRootOpenError(ENOENT) = %v, want ErrUnsupported", openErr)
	}
	openErr = cgroupRootOpenError(unix.EPERM)
	if !errors.Is(openErr, ErrUnsupported) {
		t.Fatalf("cgroupRootOpenError(EPERM) = %v, want ErrUnsupported", openErr)
	}
	leaseErr := cgroupRootLeaseAcquireError(unix.EPERM)
	if !errors.Is(leaseErr, ErrUnsupported) || errors.Is(leaseErr, ErrRootLeaseUnavailable) {
		t.Fatalf("cgroupRootLeaseAcquireError(EPERM) = %v, want ErrUnsupported only", leaseErr)
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
