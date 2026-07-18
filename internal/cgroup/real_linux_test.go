//go:build linux

package cgroup

import (
	"context"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

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
