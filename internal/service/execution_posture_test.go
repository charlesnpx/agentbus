//go:build darwin || linux

package service

import (
	"reflect"
	"testing"

	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestExecutionPostureByBackend(t *testing.T) {
	writeCaches := map[string]string{"GOCACHE": "job-cache/go-build"}
	tests := []struct {
		name    string
		backend string
		write   bool
		want    protocol.ExecutionPosture
	}{
		{
			name:    "codex inspect",
			backend: "codex",
			want: protocol.ExecutionPosture{
				Intent: "inspect", Network: "denied", GitMetadata: "read-only", Enforcement: "os-sandbox",
			},
		},
		{
			name:    "codex edit",
			backend: "codex",
			write:   true,
			want: protocol.ExecutionPosture{
				Intent: "edit", SourceWritable: true, Scratch: "job-cache", Caches: writeCaches,
				Network: "denied", GitMetadata: "read-only", Enforcement: "os-sandbox",
			},
		},
		{
			name:    "claude inspect",
			backend: "claude",
			want: protocol.ExecutionPosture{
				Intent: "inspect", Network: "unenforced", GitMetadata: "unenforced", Enforcement: "none",
			},
		},
		{
			name:    "claude edit",
			backend: "claude",
			write:   true,
			want: protocol.ExecutionPosture{
				Intent: "edit", SourceWritable: true, Scratch: "job-cache", Caches: writeCaches,
				Network: "unenforced", GitMetadata: "writable", Enforcement: "none",
			},
		},
		{
			name:    "cursor inspect",
			backend: "cursor",
			want: protocol.ExecutionPosture{
				Intent: "inspect", Network: "unenforced", GitMetadata: "read-only", Enforcement: "agent-permissions",
			},
		},
		{
			name:    "cursor edit",
			backend: "cursor",
			write:   true,
			want: protocol.ExecutionPosture{
				Intent: "edit", SourceWritable: true, Scratch: "job-cache", Caches: writeCaches,
				Network: "unenforced", GitMetadata: "writable", Enforcement: "agent-permissions",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var scratch string
			var caches map[string]string
			if test.write {
				scratch = "job-cache"
				caches = writeCaches
			}
			got := executionPosture(test.backend, test.write, scratch, caches)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("executionPosture(%q, %t) = %#v, want %#v", test.backend, test.write, got, test.want)
			}
		})
	}
}
