package duplex

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

type diagnosticDriver struct {
	FixtureDriver
	ready <-chan struct{}
	mode  string
}

func (d diagnosticDriver) RunTurn(ctx context.Context, _ *Conn, id string, _ engine.SessionOpts, _ engine.TurnInput, emit EmitFunc) (string, error) {
	select {
	case <-d.ready:
	case <-ctx.Done():
		return id, ctx.Err()
	}
	switch d.mode {
	case "success":
		emit(engine.Event{Type: engine.EventResultMessage, Text: "ok"})
		return id, nil
	case "structured":
		return id, &engine.ProviderFailureError{Evidence: engine.ProviderFailureEvidence{Class: "usage_limit", Code: "rate_limit", Message: "provider limit"}, Cause: errors.New("provider limit")}
	default:
		return id, ErrBackendExitedBeforeTerminal
	}
}

func TestLargeStderrCaptureAndDiagnosticPrecedence(t *testing.T) {
	for _, mode := range []string{"failure", "success", "structured"} {
		t.Run(mode, func(t *testing.T) {
			proc := newFakeCommand()
			ready := make(chan struct{})
			driver := diagnosticDriver{FixtureDriver: FixtureDriver{Spec: command.ExecSpec{Argv: []string{"fixture"}}}, ready: ready, mode: mode}
			session := newSessionWithDriver(t, &fakeRunner{running: proc}, driver, 0, "")
			path := filepath.Join(t.TempDir(), "stderr.log")
			done := make(chan error, 1)
			go func() {
				chunk := bytes.Repeat([]byte("stderr\xff-"), 8192)
				var err error
				for written := 0; written < 65<<20; written += len(chunk) {
					if _, err = proc.stderrW.Write(chunk); err != nil {
						break
					}
				}
				if err == nil {
					_, err = io.WriteString(proc.stderrW, "FINAL-STDERR-SUFFIX")
				}
				_ = proc.stderrW.Close()
				_ = proc.stdoutW.Close()
				if mode == "success" {
					proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
				} else {
					proc.finish(command.ExitObservation{Exited: true, Code: 9}, errors.New("exit status 9"))
				}
				close(ready)
				done <- err
			}()
			events, err := session.Turn(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, LogPaths: engine.LogPaths{Stderr: path}})
			if err != nil {
				t.Fatal(err)
			}
			got := collectEventsWithTimeout(t, events, 30*time.Second)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Size() != engine.DefaultCappedLogCapBytes {
				t.Fatalf("raw stderr stat = %v, %v", info, err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			marker := engine.TruncationMarker()
			tail := make([]byte, len(marker))
			if _, err = f.ReadAt(tail, info.Size()-int64(len(marker))); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
			if string(tail) != marker {
				t.Fatalf("raw stderr marker = %q", tail)
			}
			var terminal []engine.Event
			for _, ev := range got {
				if ev.Type == engine.EventTerminalError {
					terminal = append(terminal, ev)
				}
				if mode == "success" && ev.Type == engine.EventWarning {
					t.Fatalf("noisy success warning: %#v", ev)
				}
			}
			switch mode {
			case "success":
				if len(terminal) != 0 {
					t.Fatalf("noisy success failed: %#v", terminal)
				}
			case "structured":
				if len(terminal) != 1 || terminal[0].Text != "provider limit" {
					t.Fatalf("structured error lost precedence: %#v", terminal)
				}
				var structured *engine.ProviderFailureError
				if !errors.As(terminal[0].Err, &structured) || structured.Evidence.Code != "rate_limit" {
					t.Fatalf("lost structured evidence: %#v", terminal[0])
				}
			default:
				if len(terminal) != 1 || len(terminal[0].Text) > stderrDiagnosticLimit || !utf8.ValidString(terminal[0].Text) || !strings.HasPrefix(terminal[0].Text, stderrDiagnosticMarker) || !strings.HasSuffix(terminal[0].Text, "FINAL-STDERR-SUFFIX") {
					t.Fatalf("invalid stderr diagnostic: %#v", terminal)
				}
			}
		})
	}
}

func TestDiagnosticTailBoundsExpandedInvalidUTF8(t *testing.T) {
	var tail diagnosticTail
	_, _ = tail.Write(bytes.Repeat([]byte{0xff, 'a'}, stderrDiagnosticLimit))
	_, _ = tail.Write([]byte("recognizable end"))
	got := tail.String()
	if len(got) > stderrDiagnosticLimit || !utf8.ValidString(got) || !strings.HasSuffix(got, "recognizable end") {
		t.Fatalf("invalid bounded tail len=%d", len(got))
	}
}
