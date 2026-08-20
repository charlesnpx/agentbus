package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DefaultCancelGrace is the protocol default grace period before SIGKILL.
const DefaultCancelGrace = 10 * time.Second

var ErrProcessIdentityUnverifiable = errors.New("process identity unverifiable")

func safePathForID(dir, id, ext string) (string, error) {
	if err := validateJobID(id); err != nil {
		return "", err
	}
	path := filepath.Join(dir, id+ext)
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return "", err
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("job id %q escapes state namespace", id)
	}
	return path, nil
}

func validateJobID(jobID string) error {
	if (!strings.HasPrefix(jobID, "job_") && !strings.HasPrefix(jobID, "job-")) || len(jobID) <= len("job_") || len(jobID) > 128 {
		return fmt.Errorf("invalid job id %q", jobID)
	}
	for _, r := range jobID {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return fmt.Errorf("invalid job id %q", jobID)
	}
	return nil
}

func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	atomicWriteFileCrashHook("after-temp-sync", tmpName)
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return fsyncDir(dir)
}

var atomicWriteFileCrashHook = func(string, string) {}

func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
