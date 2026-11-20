package duplex

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/command"
)

func TestLargeJSONLHelper(t *testing.T) {
	if os.Getenv("AGENTBUS_JSONL_FIXTURE") != "1" {
		return
	}
	payload := strings.Repeat("x\"\\\n", 3<<20)
	frame, err := json.Marshal(map[string]string{"text": payload})
	if err != nil {
		os.Exit(2)
	}
	frame = append(frame, '\n')
	_, _ = io.WriteString(os.Stderr, "ready\n")
	if _, err := os.Stdout.Write(frame); err != nil {
		os.Exit(3)
	}
	_, _ = io.WriteString(os.Stderr, "done\n")
	os.Exit(0)
}

type gatedSmallReader struct {
	io.ReadCloser
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (r *gatedSmallReader) Read(p []byte) (int, error) {
	if len(p) > 4096 {
		p = p[:4096]
	}
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		r.once.Do(func() { close(r.started); <-r.release })
	}
	return n, err
}

func TestJSONLDecodesLargeEscapedFrameUnderBackpressure(t *testing.T) {
	payload := strings.Repeat("x\"\\\n", 3<<20)
	frame, err := json.Marshal(map[string]string{"text": payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) <= 16<<20 || len(frame) >= MaxJSONLineBytes {
		t.Fatalf("frame size %d", len(frame))
	}
	running, err := (command.DirectCommandRunner{}).Start(context.Background(), command.ExecSpec{
		Argv: []string{os.Args[0], "-test.run=^TestLargeJSONLHelper$"},
		Env:  append(os.Environ(), "AGENTBUS_JSONL_FIXTURE=1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer running.Stdin().Close()
	defer running.Stderr().Close()
	defer running.Stdout().Close()
	progress := make(chan string, 2)
	go func() {
		scanner := bufio.NewScanner(running.Stderr())
		for scanner.Scan() {
			progress <- scanner.Text()
		}
		close(progress)
	}()
	if got := <-progress; got != "ready" {
		t.Fatalf("helper status %q", got)
	}
	src := &gatedSmallReader{ReadCloser: running.Stdout(), started: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		select {
		case <-src.release:
		default:
			close(src.release)
		}
	}()
	frames, done := make(chan Frame, 1), make(chan struct{})
	go readJSONL(src, nil, frames, done, nil)
	select {
	case <-src.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first read did not start")
	}
	select {
	case got := <-progress:
		t.Fatalf("helper finished before release: %q", got)
	case <-time.After(75 * time.Millisecond):
	}
	close(src.release)
	select {
	case got := <-frames:
		if got.Err != nil || string(got.Raw) != string(frame) || got.Object["text"] != payload {
			t.Fatalf("decoded frame mismatch: raw bytes %d, error %v", len(got.Raw), got.Err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("JSONL decode timed out")
	}
	<-done
	if frame, ok := <-frames; ok {
		t.Fatalf("unexpected trailing frame: %#v", frame)
	}
	if got := <-progress; got != "done" {
		t.Fatalf("helper status %q", got)
	}
	if exit, err := running.Wait(context.Background()); err != nil || exit.Code != 0 {
		t.Fatalf("exit %+v: %v", exit, err)
	}
}
