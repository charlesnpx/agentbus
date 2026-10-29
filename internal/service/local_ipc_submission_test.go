//go:build darwin || linux

package service

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestJobSubmitLocalIPCValidationAndReplayIdentity(t *testing.T) {
	originalGOOS := localIPCGOOS
	defer func() { localIPCGOOS = originalGOOS }()

	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{
		helloBackend{name: "codex"}, helloBackend{name: "claude"}, helloBackend{name: "cursor"},
	}})
	localIPCGOOS = "darwin"
	missingWrite := json.RawMessage(`{"workspaceKey":"local-ipc-missing-write","requestId":"request","taskSpec":{"backend":"codex","cwd":"/workspace","prompt":"task","localIPC":true}}`)
	if outcome := server.handleJobSubmit(missingWrite); outcome.err == nil || !strings.Contains(outcome.err.Message, "requires write: true") {
		t.Fatalf("localIPC without write outcome = %#v, want explicit write-mode rejection", outcome.err)
	}
	for _, test := range []struct {
		name    string
		key     string
		backend string
		write   bool
		want    string
	}{
		{name: "write required", key: "no-write", backend: "codex", write: false, want: "requires write: true"},
		{name: "claude unsupported", key: "claude", backend: "claude", write: true, want: `backend "claude"`},
		{name: "cursor unsupported", key: "cursor", backend: "cursor", write: true, want: `backend "cursor"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := submissionParams("local-ipc-"+test.key, "request", test.backend, t.TempDir(), "task")
			params.TaskSpec.Write = test.write
			params.TaskSpec.LocalIPC = true
			outcome := submitForTest(t, server, params)
			if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec || !strings.Contains(outcome.err.Message, test.want) {
				t.Fatalf("localIPC rejection = %#v, want invalid task spec containing %q", outcome.err, test.want)
			}
		})
	}

	localIPCGOOS = "linux"
	linux := submissionParams("local-ipc-linux", "request", "codex", t.TempDir(), "task")
	linux.TaskSpec.Write = true
	linux.TaskSpec.LocalIPC = true
	if outcome := submitForTest(t, server, linux); outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec || !strings.Contains(outcome.err.Message, "Unix sockets") {
		t.Fatalf("Linux localIPC rejection = %#v, want the Unix socket sandbox limitation", outcome.err)
	}

	localIPCGOOS = "darwin"
	accepted := submissionParams("local-ipc-darwin", "request", "codex", t.TempDir(), "task")
	accepted.TaskSpec.Write = true
	accepted.TaskSpec.LocalIPC = true
	first := submitResultForTest(t, submitForTest(t, server, accepted))
	replay := submitResultForTest(t, submitForTest(t, server, accepted))
	if replay.JobID != first.JobID || !replay.Deduplicated {
		t.Fatalf("localIPC replay = %#v, want deduplicated job %q", replay, first.JobID)
	}
	encoded, err := json.Marshal(accepted.TaskSpec)
	if err != nil {
		t.Fatal(err)
	}
	input, err := parseJobSubmitInput(json.RawMessage(`{"workspaceKey":"w","requestId":"r","taskSpec":` + string(encoded) + `}`))
	if err != nil {
		t.Fatal(err)
	}
	if input.taskSpec["localIPC"] == nil {
		t.Fatalf("localIPC canonical task spec = %s, want explicit replay identity", input.canonicalTaskSpec)
	}
	withoutIPC := accepted.TaskSpec
	withoutIPC.LocalIPC = false
	withoutIPCBytes, err := json.Marshal(withoutIPC)
	if err != nil {
		t.Fatal(err)
	}
	withoutInput, err := parseJobSubmitInput(json.RawMessage(`{"workspaceKey":"w","requestId":"r","taskSpec":` + string(withoutIPCBytes) + `}`))
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(input.canonicalTaskSpec) == sha256.Sum256(withoutInput.canonicalTaskSpec) {
		t.Fatal("explicit localIPC has the same TaskSpec hash as omission")
	}

	null := json.RawMessage(`{"workspaceKey":"local-ipc-null","requestId":"request","taskSpec":{"backend":"codex","cwd":"/workspace","write":true,"prompt":"task","localIPC":null}}`)
	if outcome := server.handleJobSubmit(null); outcome.err == nil || !strings.Contains(outcome.err.Message, "localIPC cannot be null") {
		t.Fatalf("null localIPC outcome = %#v, want explicit null rejection", outcome.err)
	}
	if _, err := taskSpecFromRecord(jobstore.Record{
		Backend: "codex", TaskSpec: json.RawMessage(`{"backend":"codex","cwd":"/workspace","prompt":"task","write":false,"localIPC":true}`),
	}); err == nil || !strings.Contains(err.Error(), "requires write: true") {
		t.Fatalf("durable localIPC validation error = %v, want write-mode rejection", err)
	}
}
