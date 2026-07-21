package authority

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type FileAnchorOption func(*fileAnchor)

func WithFileAnchorFailStopHook(hook func(string)) FileAnchorOption {
	return func(anchor *fileAnchor) {
		anchor.failStopHook = hook
	}
}

func NewFileAnchor(path, dbUUID string, schemaMajor uint16, options ...FileAnchorOption) Anchor {
	anchor := &fileAnchor{
		path:        path,
		dbUUID:      dbUUID,
		schemaMajor: schemaMajor,
	}
	for _, option := range options {
		if option != nil {
			option(anchor)
		}
	}
	return anchor
}

func LoadFileAnchorSnapshot(path string) (AnchorSnapshot, error) {
	return loadFileAnchorSnapshot(path)
}

type fileAnchor struct {
	path         string
	dbUUID       string
	schemaMajor  uint16
	failStopHook func(string)
}

func (a *fileAnchor) Begin(ctx context.Context, boot model.BootRef, generation uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := boot.Validate(); err != nil {
		return "", err
	}
	snapshot, err := loadFileAnchorSnapshot(a.path)
	if err != nil {
		return "", err
	}
	if err := a.ensureIdentity(&snapshot, generation); err != nil {
		return "", err
	}
	// A persisted fail-stop is sticky: Begin must not convert it back to
	// reconciling. Only the explicit clear-fail-stop admin verb may clear it.
	if snapshot.Phase == "fail_stopped" {
		return "", FailStoppedError{Reason: snapshot.Reason}
	}
	if snapshot.Generation < generation {
		snapshot.Generation = generation
	}
	token := fmt.Sprintf("recovery-%s-%s-%d", boot.BootID, boot.OwnerID, generation)
	snapshot.Phase = "reconciling"
	snapshot.Boot = boot
	snapshot.Token = token
	snapshot.Reason = ""
	if err := saveFileAnchorSnapshot(a.path, snapshot); err != nil {
		return "", err
	}
	return token, nil
}

func (a *fileAnchor) SealReady(ctx context.Context, boot model.BootRef, generation uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := boot.Validate(); err != nil {
		return "", err
	}
	snapshot, err := loadFileAnchorSnapshot(a.path)
	if err != nil {
		return "", err
	}
	if err := a.requireIdentity(snapshot); err != nil {
		return "", err
	}
	if snapshot.Generation != generation {
		return "", ErrStaleCapability
	}
	if snapshot.Phase == "fail_stopped" {
		return "", FailStoppedError{Reason: snapshot.Reason}
	}
	token := fmt.Sprintf("ready-%s-%s-%d", boot.BootID, boot.OwnerID, generation)
	snapshot.Phase = "ready"
	snapshot.Boot = boot
	snapshot.Token = token
	snapshot.Reason = ""
	if err := saveFileAnchorSnapshot(a.path, snapshot); err != nil {
		return "", err
	}
	return token, nil
}

func (a *fileAnchor) Advance(ctx context.Context, boot model.BootRef, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := boot.Validate(); err != nil {
		return err
	}
	snapshot, err := loadFileAnchorSnapshot(a.path)
	if err != nil {
		return err
	}
	if err := a.requireIdentity(snapshot); err != nil {
		return err
	}
	if snapshot.Phase == "fail_stopped" {
		return FailStoppedError{Reason: snapshot.Reason}
	}
	if snapshot.Generation > generation {
		return fmt.Errorf("%w: anchor generation %d is ahead of db generation %d", ErrAnchorInvariant, snapshot.Generation, generation)
	}
	snapshot.Generation = generation
	snapshot.Boot = boot
	return saveFileAnchorSnapshot(a.path, snapshot)
}

func (a *fileAnchor) FailStop(ctx context.Context, boot model.BootRef, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := boot.Validate(); err != nil {
		return err
	}
	snapshot, err := loadFileAnchorSnapshot(a.path)
	if err != nil {
		return err
	}
	if err := a.requireIdentity(snapshot); err != nil {
		return err
	}
	snapshot.Phase = "fail_stopped"
	snapshot.Boot = boot
	snapshot.Reason = reason
	if err := saveFileAnchorSnapshot(a.path, snapshot); err != nil {
		return err
	}
	if a.failStopHook != nil {
		a.failStopHook(reason)
	}
	return nil
}

func (a *fileAnchor) VerifyReady(boot model.BootRef, token string, generation uint64) error {
	return a.verify("ready", boot, token, generation)
}

func (a *fileAnchor) VerifyRecovery(boot model.BootRef, token string, generation uint64) error {
	return a.verify("reconciling", boot, token, generation)
}

func (a *fileAnchor) verify(phase string, boot model.BootRef, token string, generation uint64) error {
	snapshot, err := loadFileAnchorSnapshot(a.path)
	if err != nil {
		return err
	}
	if err := a.requireIdentity(snapshot); err != nil {
		return err
	}
	if snapshot.Phase == "fail_stopped" {
		return FailStoppedError{Reason: snapshot.Reason}
	}
	if snapshot.Phase != phase || snapshot.Boot != boot || snapshot.Token != token || snapshot.Generation != generation {
		return ErrStaleCapability
	}
	return nil
}

func loadFileAnchorSnapshot(path string) (AnchorSnapshot, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return AnchorSnapshot{}, nil
	}
	if err != nil {
		return AnchorSnapshot{}, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return AnchorSnapshot{}, fmt.Errorf("%w: anchor is empty", ErrAnchorInvariant)
	}
	var snapshot AnchorSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return AnchorSnapshot{}, fmt.Errorf("%w: anchor is corrupt: %v", ErrAnchorInvariant, err)
	}
	return snapshot, nil
}

func saveFileAnchorSnapshot(path string, snapshot AnchorSnapshot) error {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return atomicWriteDurable(path, raw, 0o600)
}

func (a *fileAnchor) ensureIdentity(snapshot *AnchorSnapshot, generation uint64) error {
	if err := a.validateIdentity(); err != nil {
		return err
	}
	if !snapshot.Initialized {
		if generation != 0 {
			return fmt.Errorf("%w: missing anchor for initialized db generation %d", ErrAnchorInvariant, generation)
		}
		*snapshot = AnchorSnapshot{
			Initialized: true,
			DBUUID:      a.dbUUID,
			SchemaMajor: a.schemaMajor,
			Generation:  generation,
		}
		return nil
	}
	if err := a.requireIdentity(*snapshot); err != nil {
		return err
	}
	if snapshot.Generation > generation {
		return fmt.Errorf("%w: anchor generation %d is ahead of db generation %d", ErrAnchorInvariant, snapshot.Generation, generation)
	}
	return nil
}

func (a *fileAnchor) requireIdentity(snapshot AnchorSnapshot) error {
	if err := a.validateIdentity(); err != nil {
		return err
	}
	if !snapshot.Initialized {
		return fmt.Errorf("%w: anchor is missing", ErrAnchorInvariant)
	}
	if snapshot.DBUUID != a.dbUUID {
		return fmt.Errorf("%w: db uuid mismatch", ErrAnchorInvariant)
	}
	if snapshot.SchemaMajor != a.schemaMajor {
		return fmt.Errorf("%w: schema major mismatch", ErrAnchorInvariant)
	}
	return nil
}

func (a *fileAnchor) validateIdentity() error {
	if a.dbUUID == "" || a.schemaMajor == 0 {
		return fmt.Errorf("%w: invalid anchor identity", ErrAnchorInvariant)
	}
	return nil
}

func atomicWriteDurable(path string, data []byte, mode os.FileMode) error {
	if err := atomicWrite(path, data, mode); err != nil {
		return err
	}
	return syncFileAndParent(path)
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}

func syncFileAndParent(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
