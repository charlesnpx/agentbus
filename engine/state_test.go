package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCanonicalWorkspaceCanonicalizesSymlinkAliases(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(filepath.Dir(workspace), "workspace-alias")
	if err := os.Symlink(workspace, alias); err != nil {
		t.Skipf("create workspace symlink: %v", err)
	}

	directCanonical, err := CanonicalWorkspace(workspace)
	if err != nil {
		t.Fatal(err)
	}
	aliasCanonical, err := CanonicalWorkspace(alias)
	if err != nil {
		t.Fatal(err)
	}
	if directCanonical != aliasCanonical || WorkspaceKey(directCanonical) != WorkspaceKey(aliasCanonical) {
		t.Fatalf("workspace aliases resolved differently: direct=%q alias=%q", directCanonical, aliasCanonical)
	}
}
