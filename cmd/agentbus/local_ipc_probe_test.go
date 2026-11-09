package main

import (
	"bytes"
	"os"
	"testing"
)

func TestRunLocalIPCProbeBindsConnectsAndCleansSocket(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "ipc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)

	var stderr bytes.Buffer
	if code := runLocalIPCProbe([]string{directory}, &stderr); code != 0 {
		t.Fatalf("local IPC probe exit=%d stderr=%q, want success", code, stderr.String())
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("local IPC probe left socket artifacts: %#v", entries)
	}
}
