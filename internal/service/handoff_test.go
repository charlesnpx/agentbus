//go:build darwin || linux

package service

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

// The in-memory connection exercises the same admission gate and response
// framing as the Unix listener without depending on host socket permissions.
func TestDrainingRetainsEndpointUntilStoreUsersRetire(t *testing.T) {
	s := newTestServer(t, t.TempDir(), Config{})
	store, err := s.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	s.beginExecutions(context.Background())
	listener := &drainTestListener{closed: make(chan struct{})}
	life := &serveLifecycle{listener: listener, cancel: func() {}, pruneCancel: func() {}, acceptSettled: make(chan struct{}), serveDone: make(chan struct{}), shutdownDone: make(chan struct{}), connections: make(map[net.Conn]struct{})}
	if err := s.registerLifecycle(life); err != nil {
		t.Fatal(err)
	}
	defer s.clearLifecycle(life)
	go func() { _, _ = listener.Accept(); life.settleAccept() }()
	go func() { <-life.shutdownDone; close(life.serveDone) }()
	if !s.beginRequest() {
		t.Fatal("initial handler was refused")
	}
	held := true
	defer func() {
		if held {
			s.endRequest()
		}
	}()
	if !s.startShutdown(life, false) {
		t.Fatal("failed to enter drain")
	}
	s.executionMu.Lock()
	executionCtx := s.executionCtx
	s.executionMu.Unlock()
	if executionCtx == nil || executionCtx.Err() != nil {
		t.Fatal("drain canceled executions before admitted handlers retired")
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	go (&connection{server: s, conn: serverConn}).serve()
	_ = clientConn.SetDeadline(time.Now().Add(2 * time.Second))
	writer, reader := json.NewEncoder(clientConn), bufio.NewReader(clientConn)
	if err := writer.Encode(protocol.Request{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: protocol.MethodHello, Params: mustJSON(t, protocol.HelloParams{Token: s.token, ClientProtocolVersion: protocol.Version})}); err != nil {
		t.Fatal(err)
	}
	var hello protocol.Response
	line, err := reader.ReadBytes('\n')
	if err != nil || json.Unmarshal(line, &hello) != nil || hello.Error != nil {
		t.Fatalf("hello during drain = %s, %v", line, err)
	}
	if err := writer.Encode(protocol.Request{JSONRPC: "2.0", ID: json.RawMessage(`2`), Method: protocol.MethodJobGet, Params: mustJSON(t, protocol.JobGetParams{JobID: "job-1"})}); err != nil {
		t.Fatal(err)
	}
	line, err = reader.ReadBytes('\n')
	var response protocol.Response
	if err != nil || json.Unmarshal(line, &response) != nil || response.Error == nil || response.Error.Data.Code != protocol.ErrorDaemonDraining {
		t.Fatalf("draining response = %s, %v", line, err)
	}
	select {
	case <-life.shutdownDone:
		t.Fatal("shutdown completed while admitted handler held the store")
	default:
	}
	if current, err := s.ensureJobStore(); err != nil || current != store {
		t.Fatalf("store closed before handler retired: %v", err)
	}
	select {
	case <-listener.closed:
		t.Fatal("listener closed before store users retired")
	default:
	}
	deadlineCtx, cancelDeadline := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = s.Shutdown(deadlineCtx)
	cancelDeadline()
	if !errors.Is(err, ErrShutdownDeadlineExceeded) {
		t.Fatalf("shutdown with an admitted handler = %v, want deadline failure", err)
	}
	if current, err := s.ensureJobStore(); err != nil || current != store {
		t.Fatalf("deadline failure lost the still-owned store: %v", err)
	}

	s.endRequest()
	held = false
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ensureJobStore(); err == nil {
		t.Fatal("shutdown server reopened its store")
	}
	replacement, err := jobstore.Open(filepath.Join(s.stateRoot, jobStoreFilename))
	if err != nil {
		t.Fatalf("replacement could not acquire released store: %v", err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}
