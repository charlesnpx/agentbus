package command

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLargePipeHelper(t *testing.T) {
	if os.Getenv("AGENTBUS_PIPE_FIXTURE") != "1" {
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

func TestOwnedPipePreservesLargeEscapedFrameUnderBackpressure(t *testing.T) {
	payload := strings.Repeat("x\"\\\n", 3<<20)
	frame, err := json.Marshal(map[string]string{"text": payload})
	if err != nil {
		t.Fatal(err)
	}
	frame = append(frame, '\n')
	if len(frame) <= 16<<20 || len(frame) >= 32<<20 {
		t.Fatalf("unexpected encoded frame size %d", len(frame))
	}
	running, err := (DirectCommandRunner{}).Start(context.Background(), ExecSpec{
		Argv: []string{os.Args[0], "-test.run=^TestLargePipeHelper$"},
		Env:  append(os.Environ(), "AGENTBUS_PIPE_FIXTURE=1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer running.Stdin().Close()
	defer running.Stdout().Close()
	defer running.Stderr().Close()
	progress := make(chan string, 2)
	go func() {
		scanner := bufio.NewScanner(running.Stderr())
		for scanner.Scan() {
			progress <- scanner.Text()
		}
		close(progress)
	}()
	if got := <-progress; got != "ready" {
		t.Fatalf("helper status = %q", got)
	}
	firstRead := make(chan struct{})
	continueRead := make(chan struct{})
	defer func() {
		select {
		case <-continueRead:
		default:
			close(continueRead)
		}
	}()
	type readResult struct {
		data []byte
		err  error
	}
	readDone := make(chan readResult, 1)
	go func() {
		var out bytes.Buffer
		buf := make([]byte, 4096)
		first := true
		for {
			n, err := running.Stdout().Read(buf)
			if n > 0 {
				_, _ = out.Write(buf[:n])
				if first {
					first = false
					close(firstRead)
					<-continueRead
				}
			}
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				readDone <- readResult{out.Bytes(), err}
				return
			}
		}
	}()
	select {
	case <-firstRead:
	case <-time.After(5 * time.Second):
		t.Fatal("first read did not start")
	}
	select {
	case got := <-progress:
		t.Fatalf("helper finished before consumer released pipe: %q", got)
	case <-time.After(75 * time.Millisecond):
	}
	close(continueRead)
	var got readResult
	select {
	case got = <-readDone:
	case <-time.After(30 * time.Second):
		t.Fatal("reading frame timed out")
	}
	if got.err != nil || !bytes.Equal(got.data, frame) {
		t.Fatalf("frame mismatch: got %d bytes, err %v", len(got.data), got.err)
	}
	if status := <-progress; status != "done" {
		t.Fatalf("helper status = %q", status)
	}
	if exit, err := running.Wait(context.Background()); err != nil || exit.Code != 0 {
		t.Fatalf("exit %+v: %v", exit, err)
	}
}
