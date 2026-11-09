//go:build darwin || linux

package service

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestBuildLocalIPCDenyPathsResolveAndScopeCandidates(t *testing.T) {
	root := t.TempDir()
	stateRoot := filepath.Join(root, "state-real")
	if err := os.Mkdir(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	stateSocket := filepath.Join(stateRoot, protocol.SocketName)
	if err := os.WriteFile(stateSocket, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	stateAlias := filepath.Join(root, "state-alias")
	if err := os.Symlink(stateRoot, stateAlias); err != nil {
		t.Fatal(err)
	}
	workspaceParent := filepath.Join(root, "workspace-parent")
	workspace := filepath.Join(workspaceParent, "repo")
	scratch := filepath.Join(root, "scratch")
	ipcRoot := filepath.Join(root, "ipc")
	for _, path := range []string{workspace, scratch, ipcRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	home := filepath.Join(root, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	homeSocket := filepath.Join(home, "ssh-agent.sock")
	if err := os.WriteFile(homeSocket, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	resolvedHomeSocket, err := filepath.EvalSymlinks(homeSocket)
	if err != nil {
		t.Fatal(err)
	}
	ancestorSocket := filepath.Join(workspaceParent, "ssh-agent.sock")
	if err := os.WriteFile(ancestorSocket, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	resolvedAncestorSocket, err := filepath.EvalSymlinks(ancestorSocket)
	if err != nil {
		t.Fatal(err)
	}
	resolvedWorkspaceParent, err := filepath.EvalSymlinks(workspaceParent)
	if err != nil {
		t.Fatal(err)
	}

	gotHome := buildLocalIPCDenyPaths(stateAlias, workspace, scratch, ipcRoot, homeSocket, home, 987654)
	resolvedStateSocket, err := filepath.EvalSymlinks(stateSocket)
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(gotHome, resolvedStateSocket) || containsString(gotHome, filepath.Join(stateAlias, protocol.SocketName)) {
		t.Fatalf("deny paths = %#v, want resolved state socket %q and no alias", gotHome, resolvedStateSocket)
	}
	if !containsString(gotHome, resolvedHomeSocket) {
		t.Fatalf("deny paths = %#v, want SSH socket file when its directory is home", gotHome)
	}
	if containsString(gotHome, filepath.Join(stateRoot, protocol.TokenFileName)) {
		t.Fatalf("deny paths = %#v, missing token path should be dropped", gotHome)
	}
	if duplicates(gotHome) {
		t.Fatalf("deny paths contain duplicates after resolution: %#v", gotHome)
	}

	gotAncestor := buildLocalIPCDenyPaths(stateAlias, workspace, scratch, ipcRoot, ancestorSocket, home, 987654)
	if !containsString(gotAncestor, resolvedAncestorSocket) || containsString(gotAncestor, resolvedWorkspaceParent) {
		t.Fatalf("deny paths = %#v, want SSH socket file when its directory is a workspace ancestor", gotAncestor)
	}

	missingSocket := filepath.Join(root, "missing", "ssh-agent.sock")
	gotMissing := buildLocalIPCDenyPaths(stateAlias, workspace, scratch, ipcRoot, missingSocket, home, 987654)
	if containsString(gotMissing, filepath.Dir(missingSocket)) || containsString(gotMissing, missingSocket) {
		t.Fatalf("deny paths = %#v, want unresolved SSH path dropped", gotMissing)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func duplicates(values []string) bool {
	copy := append([]string(nil), values...)
	for i, value := range copy {
		for _, other := range copy[i+1:] {
			if value == other {
				return true
			}
		}
	}
	return false
}

func TestConcurrentLocalIPCRootsCleanIndependently(t *testing.T) {
	originalTempRoot := localIPCTempRoot
	tempRoot, err := os.MkdirTemp("/tmp", "ipct-")
	if err != nil {
		t.Fatal(err)
	}
	localIPCTempRoot = tempRoot
	t.Cleanup(func() {
		localIPCTempRoot = originalTempRoot
		_ = os.RemoveAll(tempRoot)
	})

	start := make(chan struct{})
	results := make(chan struct {
		root string
		err  error
	}, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			root, err := createLocalIPCRoot()
			results <- struct {
				root string
				err  error
			}{root: root, err: err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("create concurrent IPC roots: first=%q (%v), second=%q (%v)", first.root, first.err, second.root, second.err)
	}
	if first.root == second.root {
		t.Fatalf("concurrent jobs received the same IPC root %q", first.root)
	}
	if err := removeLocalIPCRoot(first.root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(second.root); err != nil {
		t.Fatalf("cleaning one job removed the other IPC root: %v", err)
	}
	if err := removeLocalIPCRoot(second.root); err != nil {
		t.Fatal(err)
	}
}

func TestRecordLocalIPCRootAcceptsPlatformPolicies(t *testing.T) {
	for _, policy := range []string{"private-dir", "all-unix-sockets"} {
		path := filepath.Join(t.TempDir(), "ipc")
		record := jobstore.Record{Execution: &protocol.ExecutionPosture{
			LocalIPC: policy,
			Caches:   map[string]string{"TMPDIR": path},
		}}
		if got := recordLocalIPCRoot(record); got != path {
			t.Fatalf("recordLocalIPCRoot(%q) = %q, want %q", policy, got, path)
		}
	}
}
