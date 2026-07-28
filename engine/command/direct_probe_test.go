package command

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDirectProbeRunnerRunsVersionProbe(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "version-marker")
	binary := filepath.Join(dir, "probe-cli")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then
  printf probed > "$PROBE_MARKER"
  printf 'probe-cli 1.2.3\n'
  exit 0
fi
exit 9
`
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	runner := DirectProbeRunner{}
	path, err := runner.LookPath(binary)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), ProbeSpec{
		Argv: []string{path, "--version"},
		Env:  append(os.Environ(), "PROBE_MARKER="+marker),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(result.Stdout), "1.2.3") {
		t.Fatalf("stdout = %q, want version", string(result.Stdout))
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("version marker stat err = %v", err)
	}
}
