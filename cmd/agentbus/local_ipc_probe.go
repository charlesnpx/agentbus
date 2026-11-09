package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
)

func runLocalIPCProbe(args []string, stderr io.Writer) int {
	if len(args) != 1 {
		return localIPCProbeFailure(stderr, errors.New("expected one absolute IPC directory"))
	}
	directory := args[0]
	if !filepath.IsAbs(directory) {
		return localIPCProbeFailure(stderr, errors.New("IPC directory must be absolute"))
	}
	info, err := os.Stat(directory)
	if err != nil {
		return localIPCProbeFailure(stderr, fmt.Errorf("inspect IPC directory: %w", err))
	}
	if !info.IsDir() {
		return localIPCProbeFailure(stderr, errors.New("IPC path is not a directory"))
	}

	socketPath := filepath.Join(directory, "agentbus-probe.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return localIPCProbeFailure(stderr, fmt.Errorf("listen on local Unix socket: %w", err))
	}
	client, dialErr := net.Dial("unix", socketPath)
	if dialErr != nil {
		return localIPCProbeFailure(stderr, cleanupLocalIPCProbe(listener, nil, socketPath, fmt.Errorf("connect to local Unix socket: %w", dialErr)))
	}
	server, acceptErr := listener.Accept()
	if acceptErr != nil {
		return localIPCProbeFailure(stderr, cleanupLocalIPCProbe(listener, []net.Conn{client}, socketPath, fmt.Errorf("accept local Unix socket connection: %w", acceptErr)))
	}
	if err := cleanupLocalIPCProbe(listener, []net.Conn{client, server}, socketPath, nil); err != nil {
		return localIPCProbeFailure(stderr, err)
	}
	return 0
}

func cleanupLocalIPCProbe(listener net.Listener, connections []net.Conn, socketPath string, cause error) error {
	for _, connection := range connections {
		if err := connection.Close(); err != nil {
			cause = errors.Join(cause, fmt.Errorf("close local Unix socket connection: %w", err))
		}
	}
	if err := listener.Close(); err != nil {
		cause = errors.Join(cause, fmt.Errorf("close local Unix socket listener: %w", err))
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		cause = errors.Join(cause, fmt.Errorf("remove local Unix socket: %w", err))
	}
	return cause
}

func localIPCProbeFailure(stderr io.Writer, err error) int {
	_, _ = fmt.Fprintf(stderr, "agentbus local IPC probe: %v\n", err)
	return 1
}
