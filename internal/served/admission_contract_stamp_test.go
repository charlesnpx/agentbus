package served

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestAdmissionContractStampRoundTripsToAuthorityResult(t *testing.T) {
	cases := []struct {
		name        string
		text        string
		wantState   engine.JobState
		wantStatus  engine.ContractStatus
		wantMissing []string
	}{
		{
			name:       "compliant",
			text:       "PASS\n",
			wantState:  engine.StateCompleted,
			wantStatus: engine.ContractCompliant,
		},
		{
			name:        "noncompliant-missing",
			text:        "FAIL\n",
			wantState:   engine.StateCompletedNoncompliant,
			wantStatus:  engine.ContractNoncompliant,
			wantMissing: []string{"firstLineEnum"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend("fake")
			backend.events = func(string, bool) []engine.Event {
				return []engine.Event{{Type: engine.EventAgentText, Text: tt.text}}
			}
			server, _, cwd := newUnstartedTestServer(t, backend)
			enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))

			submitted := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-contract-stamp-" + tt.name,
				RequestID:    "request-contract-stamp-" + tt.name,
				TaskSpec: protocol.TaskSpec{
					Backend: "fake",
					CWD:     cwd,
					Write:   false,
					Prompt:  "contract stamp",
					Policy: &engine.TurnPolicy{
						Contract: &engine.ContractSpec{Shape: &engine.ShapeSpec{FirstLineEnum: []string{"PASS"}}},
					},
				},
			})

			record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
			if record.Terminal == nil {
				t.Fatal("authority terminal record missing")
			}
			assertContractStampForTest(t, record.Terminal.Contract, tt.wantStatus, tt.wantMissing)

			result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
			if result.State != tt.wantState {
				t.Fatalf("authority result state = %s, want %s", result.State, tt.wantState)
			}
			assertContractStampForTest(t, result.Contract, tt.wantStatus, tt.wantMissing)
		})
	}
}

func TestAuthorityResultInfoSignalsTextElision(t *testing.T) {
	server := &Server{inlineResultCap: engine.DefaultInlineResultCap}
	dir := t.TempDir()

	smallRaw := []byte("small result")
	small := server.authorityResultInfo(writeAuthorityResultInfoFixture(t, dir, "small.txt", smallRaw))
	if small == nil || small.Text != string(smallRaw) || small.TextElided {
		t.Fatalf("small result = %+v, want inline text without elision", small)
	}

	largeRaw := bytes.Repeat([]byte("x"), engine.DefaultInlineResultCap)
	large := server.authorityResultInfo(writeAuthorityResultInfoFixture(t, dir, "large.txt", largeRaw))
	if large == nil || large.Text != "" || !large.TextElided {
		t.Fatalf("large result = %+v, want elided text signal", large)
	}
}

func assertContractStampForTest(t *testing.T, stamp *engine.ContractStamp, wantStatus engine.ContractStatus, wantMissing []string) {
	t.Helper()
	if stamp == nil {
		t.Fatalf("contract stamp is nil, want status %s", wantStatus)
	}
	if stamp.Status != wantStatus {
		t.Fatalf("contract status = %s, want %s; stamp=%+v", stamp.Status, wantStatus, stamp)
	}
	if len(stamp.Missing) != len(wantMissing) {
		t.Fatalf("contract missing = %#v, want %#v", stamp.Missing, wantMissing)
	}
	for i := range wantMissing {
		if stamp.Missing[i] != wantMissing[i] {
			t.Fatalf("contract missing = %#v, want %#v", stamp.Missing, wantMissing)
		}
	}
	if stamp.Attempts != 1 {
		t.Fatalf("contract attempts = %d, want 1", stamp.Attempts)
	}
	if stamp.ContractSHA256 == "" {
		t.Fatalf("contract sha256 missing from stamp: %+v", stamp)
	}
}

func writeAuthorityResultInfoFixture(t *testing.T, dir, name string, raw []byte) model.ResultRef {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return model.ResultRef{
		Path:   path,
		Digest: hex.EncodeToString(sum[:]),
		Bytes:  int64(len(raw)),
	}
}
