package engine

import (
	"os"
	"os/exec"
	"path/filepath"
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
