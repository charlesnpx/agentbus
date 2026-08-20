package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceLayoutCanonicalizesSymlinkAliases(t *testing.T) {
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
	directLayout, err := LayoutForWorkspace(t.TempDir(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	aliasLayout, err := LayoutForWorkspace(directLayout.Root, alias)
	if err != nil {
		t.Fatal(err)
	}

	if WorkspaceKey(directCanonical) != WorkspaceKey(aliasCanonical) || directLayout != aliasLayout {
		t.Fatalf("workspace aliases produced different keys or layouts: direct=%+v alias=%+v", directLayout, aliasLayout)
	}
}
