package engine

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestAtomicWriteFileSurvivesPreRenameCrash(t *testing.T) {
	if os.Getenv("AGENTBUS_ATOMIC_CRASH_CHILD") == "1" {
		atomicWriteFileCrashHook = func(stage, _ string) {
			if stage == "after-temp-sync" {
				os.Exit(23)
			}
		}
		if err := atomicWriteFile(os.Getenv("AGENTBUS_ATOMIC_TARGET"), []byte(os.Getenv("AGENTBUS_ATOMIC_NEW_DATA")), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}

	target := filepath.Join(t.TempDir(), "state", "result.txt")
	before := []byte("before")
	if err := atomicWriteFile(target, before, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAtomicWriteFileSurvivesPreRenameCrash$")
	cmd.Env = append(os.Environ(),
		"AGENTBUS_ATOMIC_CRASH_CHILD=1",
		"AGENTBUS_ATOMIC_TARGET="+target,
		"AGENTBUS_ATOMIC_NEW_DATA=after",
	)
	err := cmd.Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 23 {
		t.Fatalf("crash child err = %v, want exit 23", err)
	}
	after, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("target changed across pre-rename crash:\nbefore=%s\nafter=%s", before, after)
	}
}

func TestAtomicWriteFileRetainsRequestedModeAfterRename(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not report all Unix permission bits")
	}

	target := filepath.Join(t.TempDir(), "state", "result.txt")
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("before"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := atomicWriteFile(target, []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode after rename = %#o, want %#o", got, os.FileMode(0o600))
	}
}
