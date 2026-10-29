//go:build darwin || linux

package service

import (
	"reflect"
	"testing"

	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestExecutionPostureByBackend(t *testing.T) {
	writeCaches := map[string]string{"GOCACHE": "job-cache/go-build"}
	localIPCCaches := map[string]string{"GOCACHE": "job-cache/go-build", "TMPDIR": "private-ipc"}
	tests := []struct {
		name      string
		backend   string
		write     bool
		verify    bool
		localIPC  bool
		sandboxed bool
		want      protocol.ExecutionPosture
	}{
		{
			name:      "codex inspect",
			backend:   "codex",
			sandboxed: true,
			want: protocol.ExecutionPosture{
				Intent: "inspect", Shell: "sandboxed", Network: "denied", LocalIPC: "denied", GitMetadata: "read-only", Enforcement: "os-sandbox",
			},
		},
		{
			name:      "codex verify",
			backend:   "codex",
			verify:    true,
			sandboxed: true,
			want: protocol.ExecutionPosture{
				Intent: "verify", Scratch: "job-cache", Caches: writeCaches,
				Shell: "sandboxed", Network: "denied", LocalIPC: "denied", GitMetadata: "none", Enforcement: "os-sandbox",
			},
		},
		{
			name:      "codex edit",
			backend:   "codex",
			write:     true,
			sandboxed: true,
			want: protocol.ExecutionPosture{
				Intent: "edit", SourceWritable: true, Scratch: "job-cache", Caches: writeCaches,
				Shell: "sandboxed", Network: "denied", LocalIPC: "denied", GitMetadata: "read-only", Enforcement: "os-sandbox",
			},
		},
		{
			name:      "codex edit with local IPC",
			backend:   "codex",
			write:     true,
			localIPC:  true,
			sandboxed: true,
			want: protocol.ExecutionPosture{
				Intent: "edit", SourceWritable: true, Scratch: "job-cache", Caches: localIPCCaches,
				Shell: "sandboxed", Network: "denied", LocalIPC: "private-dir", GitMetadata: "read-only", Enforcement: "os-sandbox",
			},
		},
		{
			name:    "trusted Codex edit",
			backend: "codex",
			write:   true,
			want: protocol.ExecutionPosture{
				Intent: "edit", SourceWritable: true, Scratch: "job-cache", Caches: writeCaches,
				Shell: "unenforced", Network: "unenforced", LocalIPC: "unenforced", GitMetadata: "writable", Enforcement: "none",
			},
		},
		{
			name:    "claude inspect",
			backend: "claude",
			want: protocol.ExecutionPosture{
				Intent: "inspect", Shell: "restricted", Network: "unenforced", LocalIPC: "unenforced", GitMetadata: "unenforced", Enforcement: "none",
			},
		},
		{
			name:    "claude edit",
			backend: "claude",
			write:   true,
			want: protocol.ExecutionPosture{
				Intent: "edit", SourceWritable: true, Scratch: "job-cache", Caches: writeCaches,
				Shell: "unenforced", Network: "unenforced", LocalIPC: "unenforced", GitMetadata: "writable", Enforcement: "none",
			},
		},
		{
			name:    "cursor inspect",
			backend: "cursor",
			want: protocol.ExecutionPosture{
				Intent: "inspect", Shell: "unenforced", Network: "unenforced", LocalIPC: "unenforced", GitMetadata: "read-only", Enforcement: "agent-permissions",
			},
		},
		{
			name:    "cursor edit",
			backend: "cursor",
			write:   true,
			want: protocol.ExecutionPosture{
				Intent: "edit", SourceWritable: true, Scratch: "job-cache", Caches: writeCaches,
				Shell: "unenforced", Network: "unenforced", LocalIPC: "unenforced", GitMetadata: "writable", Enforcement: "agent-permissions",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var scratch string
			var caches map[string]string
			if test.write || test.verify {
				scratch = "job-cache"
				caches = writeCaches
				if test.localIPC {
					caches = localIPCCaches
				}
			}
			got := executionPosture(test.backend, test.write, test.verify, test.localIPC, test.sandboxed, scratch, caches)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("executionPosture(%q, %t) = %#v, want %#v", test.backend, test.write, got, test.want)
			}
		})
	}
}
