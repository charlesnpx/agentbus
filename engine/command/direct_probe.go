package command

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strings"
)

// DirectProbeRunner preserves legacy executable lookup and short probe
// commands while keeping probing behind an explicit interface.
type DirectProbeRunner struct{}

func (DirectProbeRunner) LookPath(file string) (string, error) {
	if strings.TrimSpace(file) == "" {
		return "", errors.New("probe binary is required")
	}
	return exec.LookPath(file)
}

func (DirectProbeRunner) Run(ctx context.Context, spec ProbeSpec) (ProbeResult, error) {
	if len(spec.Argv) == 0 || strings.TrimSpace(spec.Argv[0]) == "" {
		return ProbeResult{}, errors.New("probe argv is required")
	}
	cmd := exec.CommandContext(ctx, spec.Argv[0], spec.Argv[1:]...)
	if spec.Dir != "" {
		cmd.Dir = spec.Dir
	}
	if spec.Env != nil {
		cmd.Env = append([]string(nil), spec.Env...)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return ProbeResult{
		Stdout: append([]byte(nil), stdout.Bytes()...),
		Stderr: append([]byte(nil), stderr.Bytes()...),
	}, err
}
