package client

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAutostartLockPathUsesExistingRootIdentityOnDarwin(t *testing.T) {
	parent := shortClientTempDir(t)
	root := filepath.Join(parent, "StateRoot")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	variant := filepath.Join(parent, "stateroot")
	rootInfo, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	variantInfo, err := os.Stat(variant)
	if err != nil || !os.SameFile(rootInfo, variantInfo) {
		t.Skip("filesystem is case-sensitive; case-variant spelling does not resolve to the same root")
	}
	originalUserCacheDir := autostartUserCacheDir
	t.Cleanup(func() { autostartUserCacheDir = originalUserCacheDir })
	cacheDir := filepath.Join(t.TempDir(), "cache")
	autostartUserCacheDir = func() (string, error) {
		return cacheDir, nil
	}

	firstKey, err := stateRootAutostartLockKey(root)
	if err != nil {
		t.Fatal(err)
	}
	secondKey, err := stateRootAutostartLockKey(variant)
	if err != nil {
		t.Fatal(err)
	}
	if firstKey != secondKey {
		t.Fatalf("lock keys differ for same root inode: %q != %q", firstKey, secondKey)
	}
	firstPath, err := autostartLockPath(firstKey)
	if err != nil {
		t.Fatal(err)
	}
	secondPath, err := autostartLockPath(secondKey)
	if err != nil {
		t.Fatal(err)
	}
	if firstPath != secondPath {
		t.Fatalf("lock paths differ for same root inode: %q != %q", firstPath, secondPath)
	}
}
