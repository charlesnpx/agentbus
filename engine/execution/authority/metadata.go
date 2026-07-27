package authority

import (
	"context"
	"errors"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

const CurrentAdmissionContractVersion = repository.CurrentAdmissionContractVersion

var ErrAdmissionContractMismatch = errors.New("admission contract version mismatch")

type AdmissionRootMetadata = repository.AdmissionRootMetadata

type IncompatibleAdmissionContractVersionError struct {
	RootContractVersion   uint16
	DaemonContractVersion uint16
	Activated             bool
}

func (e IncompatibleAdmissionContractVersionError) Error() string {
	state := "candidate"
	if e.Activated {
		state = "activated"
	}
	return fmt.Sprintf("%s: %s root contract version %d, daemon contract version %d", ErrAdmissionContractMismatch, state, e.RootContractVersion, e.DaemonContractVersion)
}

func (e IncompatibleAdmissionContractVersionError) Is(target error) bool {
	return target == ErrAdmissionContractMismatch
}

func ValidateAdmissionRootContract(metadata AdmissionRootMetadata) error {
	if metadata.ContractVersion == 0 && !metadata.Activated {
		return nil
	}
	if metadata.ContractVersion != CurrentAdmissionContractVersion {
		return IncompatibleAdmissionContractVersionError{
			RootContractVersion:   metadata.ContractVersion,
			DaemonContractVersion: CurrentAdmissionContractVersion,
			Activated:             metadata.Activated,
		}
	}
	return nil
}

func (b *Bootstrapper) RootMetadata(ctx context.Context) (AdmissionRootMetadata, error) {
	if b == nil || b.core == nil {
		return AdmissionRootMetadata{}, ErrNotReady
	}
	b.core.mu.Lock()
	defer b.core.mu.Unlock()

	var metadata AdmissionRootMetadata
	if err := b.core.view(ctx, "root metadata", func(tx repository.ReadTx) error {
		meta, err := b.core.requireMeta(tx)
		if err != nil {
			return err
		}
		metadata = meta.AdmissionRoot
		return nil
	}); err != nil {
		return AdmissionRootMetadata{}, err
	}
	return metadata, nil
}

func (s *RecoverySession) RootMetadata(ctx context.Context) (AdmissionRootMetadata, error) {
	if s == nil || s.core == nil {
		return AdmissionRootMetadata{}, ErrNotReady
	}
	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	var metadata AdmissionRootMetadata
	if err := s.core.view(ctx, "recovery root metadata", func(tx repository.ReadTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		metadata = meta.AdmissionRoot
		return nil
	}); err != nil {
		return AdmissionRootMetadata{}, err
	}
	return metadata, nil
}

func (s *RecoverySession) ActivateRoot(ctx context.Context) (AdmissionRootMetadata, repository.Commit, error) {
	if s == nil || s.core == nil {
		return AdmissionRootMetadata{}, repository.Commit{}, ErrNotReady
	}
	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	var metadata AdmissionRootMetadata
	changed := false
	commit, err := s.core.update(ctx, "activate root", func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		if err := ValidateAdmissionRootContract(meta.AdmissionRoot); err != nil {
			return err
		}
		if meta.AdmissionRoot.Activated {
			metadata = meta.AdmissionRoot
			return nil
		}
		meta.AdmissionRoot = AdmissionRootMetadata{
			Activated:       true,
			ContractVersion: CurrentAdmissionContractVersion,
			ActivatedAtGen:  meta.Generation + 1,
		}
		if err := tx.PutMeta(meta); err != nil {
			return err
		}
		metadata = meta.AdmissionRoot
		changed = true
		return nil
	})
	if err != nil {
		return AdmissionRootMetadata{}, commit, err
	}
	if changed {
		if err := s.core.advanceRecoveryLocked(ctx, &s.token, commit.Generation); err != nil {
			return metadata, commit, err
		}
	}
	return metadata, commit, nil
}
