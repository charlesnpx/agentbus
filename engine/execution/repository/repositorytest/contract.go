package repositorytest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type Factory struct {
	New           func(*testing.T) repository.Repository
	Snapshot      func(*testing.T, repository.Repository) []byte
	CorruptSafety func(*testing.T, repository.Repository, model.JobID, string)
	MissingMeta   func(*testing.T, repository.Repository)
}

func RunRepositoryContract(t *testing.T, factory Factory) {
	t.Helper()
	if factory.New == nil {
		t.Fatal("repositorytest.Factory.New is required")
	}
	if factory.Snapshot == nil {
		t.Fatal("repositorytest.Factory.Snapshot is required")
	}
	if factory.CorruptSafety == nil {
		t.Fatal("repositorytest.Factory.CorruptSafety is required")
	}
	if factory.MissingMeta == nil {
		t.Fatal("repositorytest.Factory.MissingMeta is required")
	}

	t.Run("atomic acceptance", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "accept")

		commit, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if _, err := tx.AllocateJobID(); err != nil {
				return err
			}
			if err := tx.PutBinding(fixture.Binding); err != nil {
				return err
			}
			if err := tx.PutSafety(fixture.Record, 0); err != nil {
				return err
			}
			return tx.PutProjection(fixture.Projection)
		})
		if err != nil {
			t.Fatalf("atomic acceptance Update error = %v", err)
		}
		if commit.Generation != 1 {
			t.Fatalf("generation = %d, want 1", commit.Generation)
		}

		assertRequestBinding(t, repo, fixture.Binding)
		assertJobImage(t, repo, fixture)
	})

	t.Run("acceptance conflict rolls back proposed records and allocation", func(t *testing.T) {
		repo := factory.New(t)
		existing := newFixture(t, "conflict")
		acceptFixture(t, repo, existing)
		before := factory.Snapshot(t, repo)

		conflict := existing
		conflict.JobID = model.JobID("job-conflict-other")
		conflict.Record.JobID = conflict.JobID
		conflict.Record.Attempt.Ref.JobID = conflict.JobID
		conflict.Binding.JobID = conflict.JobID
		conflict.Projection.JobID = conflict.JobID

		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if _, err := tx.AllocateJobID(); err != nil {
				return err
			}
			if err := tx.PutSafety(conflict.Record, 0); err != nil {
				return err
			}
			return tx.PutBinding(conflict.Binding)
		})
		if !errors.Is(err, repository.ErrConflict) {
			t.Fatalf("conflicting acceptance error = %v, want ErrConflict", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))

		next := newFixture(t, "next-after-conflict")
		commit, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			id, err := tx.AllocateJobID()
			if err != nil {
				return err
			}
			if id != "job-00000000000000000002" {
				return fmt.Errorf("allocated id after rollback = %s, want job-00000000000000000002", id)
			}
			return putAcceptance(tx, next)
		})
		if err != nil {
			t.Fatalf("post-rollback acceptance Update error = %v", err)
		}
		if commit.Generation != 2 {
			t.Fatalf("post-rollback generation = %d, want 2", commit.Generation)
		}
	})

	t.Run("cas mismatch rolls back", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "cas")
		acceptFixture(t, repo, fixture)
		before := factory.Snapshot(t, repo)

		next := fixture.Record
		next.Revision = 2
		next.Cancel = &model.CancelFact{JobID: fixture.JobID, RequestedBy: fixture.Boot}
		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			return tx.PutSafety(next, 0)
		})
		if !errors.Is(err, repository.ErrCASMismatch) {
			t.Fatalf("CAS mismatch error = %v, want ErrCASMismatch", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
	})

	t.Run("kernel validation error rolls back", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "kernel")
		acceptFixture(t, repo, fixture)
		before := factory.Snapshot(t, repo)

		invalid := fixture.Record
		invalid.Revision = 0
		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if _, err := tx.AllocateJobID(); err != nil {
				return err
			}
			return tx.PutSafety(invalid, fixture.Record.Revision)
		})
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("invalid safety error = %v, want ErrInvalidRecord", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
	})

	t.Run("projection mismatch rolls back", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "projection")
		before := factory.Snapshot(t, repo)

		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if _, err := tx.AllocateJobID(); err != nil {
				return err
			}
			if err := tx.PutBinding(fixture.Binding); err != nil {
				return err
			}
			if err := tx.PutSafety(fixture.Record, 0); err != nil {
				return err
			}
			bad := fixture.Projection
			bad.Decision = model.DecisionCancelRequested
			return tx.PutProjection(bad)
		})
		if !errors.Is(err, repository.ErrProjectionMismatch) {
			t.Fatalf("projection mismatch error = %v, want ErrProjectionMismatch", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
	})

	t.Run("terminal derivation failure rolls back", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "terminal")
		acceptFixture(t, repo, fixture)
		before := factory.Snapshot(t, repo)

		invalid := fixture.Record
		invalid.Revision = 2
		invalid.Terminal = &model.TerminalCertificate{
			JobID:               fixture.JobID,
			Attempt:             fixture.Record.Attempt.Ref,
			Outcome:             model.OutcomeCompleted,
			Proof:               model.ProofCleanQuiescentOutcomeAndRetired,
			Cause:               model.CauseCompletedNormally,
			DerivedFromRevision: fixture.Record.Revision,
			DerivedBy:           fixture.Boot,
		}
		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			return tx.PutSafety(invalid, fixture.Record.Revision)
		})
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("invalid terminal error = %v, want ErrInvalidRecord", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
	})

	t.Run("callback error and panic roll back", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "callback")
		before := factory.Snapshot(t, repo)
		sentinel := errors.New("callback rejection")

		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if err := putAcceptance(tx, fixture); err != nil {
				return err
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("callback rejection error = %v, want sentinel", err)
		}
		if !errors.Is(err, repository.ErrDefinitelyNotCommitted) {
			t.Fatalf("callback rejection error = %v, want ErrDefinitelyNotCommitted", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))

		_, err = repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if err := putAcceptance(tx, fixture); err != nil {
				return err
			}
			panic("boom")
		})
		if !errors.Is(err, repository.ErrTransactionPanic) {
			t.Fatalf("panic error = %v, want ErrTransactionPanic", err)
		}
		if !errors.Is(err, repository.ErrDefinitelyNotCommitted) {
			t.Fatalf("panic error = %v, want ErrDefinitelyNotCommitted", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
	})

	t.Run("generation advances once and read-only replay does not advance", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "generation")
		commit := acceptFixture(t, repo, fixture)
		if commit.Generation != 1 {
			t.Fatalf("accept generation = %d, want 1", commit.Generation)
		}

		noop, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			image := tx.LookupRequest(fixture.RequestKey)
			if image.Binding.State != repository.RecordValid {
				return fmt.Errorf("replay binding state = %s, want valid", image.Binding.State)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("read-only replay Update error = %v", err)
		}
		if noop.Generation != commit.Generation {
			t.Fatalf("read-only replay generation = %d, want %d", noop.Generation, commit.Generation)
		}

		duplicate, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if err := tx.PutBinding(fixture.Binding); err != nil {
				return err
			}
			if err := tx.PutSafety(fixture.Record, fixture.Record.Revision); err != nil {
				return err
			}
			return tx.PutProjection(fixture.Projection)
		})
		if err != nil {
			t.Fatalf("duplicate acceptance Update error = %v", err)
		}
		if duplicate.Generation != commit.Generation {
			t.Fatalf("duplicate acceptance generation = %d, want %d", duplicate.Generation, commit.Generation)
		}
	})

	t.Run("safety launch proof pointers are isolated", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newQuiescentFixture(t, "launch-clone")
		want := cloneSafetyRecordForTest(fixture.Record)

		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if _, err := tx.AllocateJobID(); err != nil {
				return err
			}
			if err := putAcceptance(tx, fixture); err != nil {
				return err
			}
			mutateLaunchPhysicalFactForTest(t, &fixture.Record, model.LaunchOrdinalOne)
			return nil
		})
		if err != nil {
			t.Fatalf("launch clone acceptance Update error = %v", err)
		}
		assertJobSafetyRecord(t, repo, fixture.JobID, want)

		before := factory.Snapshot(t, repo)
		loaded := loadSafetyRecord(t, repo, fixture.JobID)
		mutateLaunchPhysicalFactForTest(t, &loaded, model.LaunchOrdinalOne)
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
		assertJobSafetyRecord(t, repo, fixture.JobID, want)
	})

	t.Run("expiry atomically replaces live records with tombstone", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "expiry")
		acceptFixture(t, repo, fixture)

		commit, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if err := tx.DeleteLiveJob(fixture.JobID); err != nil {
				return err
			}
			return tx.PutTombstone(repository.Tombstone{
				RequestKey:        fixture.RequestKey,
				JobID:             fixture.JobID,
				TaskIdentity:      fixture.Identity,
				ExpiredGeneration: 2,
			})
		})
		if err != nil {
			t.Fatalf("expiry Update error = %v", err)
		}
		if commit.Generation != 2 {
			t.Fatalf("expiry generation = %d, want 2", commit.Generation)
		}

		if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
			request := tx.LookupRequest(fixture.RequestKey)
			if request.Binding.State != repository.RecordMissing {
				return fmt.Errorf("expired binding state = %s, want missing", request.Binding.State)
			}
			if request.Tombstone.State != repository.RecordValid {
				return fmt.Errorf("tombstone state = %s, want valid", request.Tombstone.State)
			}
			job := tx.LoadJob(fixture.JobID)
			if job.Safety.State != repository.RecordMissing || job.Projection.State != repository.RecordMissing {
				return fmt.Errorf("expired job safety/projection states = %s/%s, want missing/missing", job.Safety.State, job.Projection.State)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		noop, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			request := tx.LookupRequest(fixture.RequestKey)
			if request.Tombstone.State != repository.RecordValid {
				return fmt.Errorf("expired replay tombstone state = %s, want valid", request.Tombstone.State)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("expired replay Update error = %v", err)
		}
		if noop.Generation != commit.Generation {
			t.Fatalf("expired replay generation = %d, want %d", noop.Generation, commit.Generation)
		}
	})

	t.Run("corruption state is explicit and failed policy write rolls back", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "corrupt")
		acceptFixture(t, repo, fixture)
		factory.CorruptSafety(t, repo, fixture.JobID, "checksum mismatch")

		if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
			job := tx.LoadJob(fixture.JobID)
			if job.Safety.State != repository.RecordCorrupt {
				return fmt.Errorf("safety state = %s, want corrupt", job.Safety.State)
			}
			if job.Safety.Diagnostic == "" {
				return errors.New("corrupt safety diagnostic is empty")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		before := factory.Snapshot(t, repo)
		other := newFixture(t, "after-corrupt")
		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			if _, err := tx.AllocateJobID(); err != nil {
				return err
			}
			return putAcceptance(tx, other)
		})
		if !errors.Is(err, repository.ErrCorruptRecord) {
			t.Fatalf("corrupt policy error = %v, want ErrCorruptRecord", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
	})

	t.Run("list nonterminal by boot", func(t *testing.T) {
		repo := factory.New(t)
		fixture := newFixture(t, "list")
		acceptFixture(t, repo, fixture)
		if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
			jobs, err := tx.ListNonterminalByBoot(fixture.Boot.BootID)
			if err != nil {
				return err
			}
			if len(jobs) != 1 || jobs[0].Safety.State != repository.RecordValid || jobs[0].Safety.Value.JobID != fixture.JobID {
				return fmt.Errorf("ListNonterminalByBoot returned %#v, want one valid job %s", jobs, fixture.JobID)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("missing meta is allowed only on a fresh root", func(t *testing.T) {
		t.Run("populated root rejects silent reconstruction", func(t *testing.T) {
			repo := factory.New(t)
			fixture := newFixture(t, "missing-meta-populated")
			acceptFixture(t, repo, fixture)
			factory.MissingMeta(t, repo)
			before := factory.Snapshot(t, repo)

			_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
				return tx.PutMeta(repository.AuthorityMeta{
					SchemaVersion:   repository.CurrentAuthorityMetaSchemaVersion,
					Generation:      0,
					NextJobSequence: 1,
				})
			})
			if !errors.Is(err, repository.ErrCorruptRecord) {
				t.Fatalf("missing populated meta PutMeta error = %v, want ErrCorruptRecord", err)
			}
			assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
		})

		t.Run("fresh root may declare metadata", func(t *testing.T) {
			repo := factory.New(t)
			factory.MissingMeta(t, repo)
			commit, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
				return tx.PutMeta(repository.AuthorityMeta{
					SchemaVersion:   repository.CurrentAuthorityMetaSchemaVersion,
					Generation:      0,
					NextJobSequence: 1,
					AdmissionRoot: repository.AdmissionRootMetadata{
						ContractVersion: repository.CurrentAdmissionContractVersion,
					},
				})
			})
			if err != nil {
				t.Fatalf("missing fresh meta PutMeta error = %v", err)
			}
			assertMetaForTest(t, repo, repository.AuthorityMeta{
				SchemaVersion:   repository.CurrentAuthorityMetaSchemaVersion,
				Generation:      commit.Generation,
				NextJobSequence: 1,
				AdmissionRoot: repository.AdmissionRootMetadata{
					ContractVersion: repository.CurrentAdmissionContractVersion,
				},
			})
		})
	})

	t.Run("contract version is declare once", func(t *testing.T) {
		repo := factory.New(t)
		commit, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			meta := loadMetaForTest(t, tx)
			meta.AdmissionRoot.ContractVersion = repository.CurrentAdmissionContractVersion
			return tx.PutMeta(meta)
		})
		if err != nil {
			t.Fatalf("fresh contract declaration error = %v", err)
		}
		declared := repository.AuthorityMeta{
			SchemaVersion:   repository.CurrentAuthorityMetaSchemaVersion,
			Generation:      commit.Generation,
			NextJobSequence: 1,
			AdmissionRoot: repository.AdmissionRootMetadata{
				ContractVersion: repository.CurrentAdmissionContractVersion,
			},
		}
		assertMetaForTest(t, repo, declared)
		before := factory.Snapshot(t, repo)

		_, err = repo.Update(context.Background(), func(tx repository.WriteTx) error {
			meta := loadMetaForTest(t, tx)
			meta.AdmissionRoot.ContractVersion++
			return tx.PutMeta(meta)
		})
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("unactivated contract mutation error = %v, want ErrInvalidRecord", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
		assertMetaForTest(t, repo, declared)
	})

	t.Run("activation and seal metadata are one way", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(repository.AuthorityMeta) repository.AuthorityMeta
		}{
			{
				name: "clear activated",
				mutate: func(meta repository.AuthorityMeta) repository.AuthorityMeta {
					meta.AdmissionRoot.Activated = false
					meta.AdmissionRoot.ContractVersion = 0
					meta.AdmissionRoot.ActivatedAtGen = 0
					return meta
				},
			},
			{
				name: "clear sealed",
				mutate: func(meta repository.AuthorityMeta) repository.AuthorityMeta {
					meta.Sealed = false
					return meta
				},
			},
			{
				name: "forge activated at generation",
				mutate: func(meta repository.AuthorityMeta) repository.AuthorityMeta {
					meta.AdmissionRoot.ActivatedAtGen++
					return meta
				},
			},
			{
				name: "change activated contract",
				mutate: func(meta repository.AuthorityMeta) repository.AuthorityMeta {
					meta.AdmissionRoot.ContractVersion++
					return meta
				},
			},
			{
				name: "change sealed successor uuid",
				mutate: func(meta repository.AuthorityMeta) repository.AuthorityMeta {
					meta.SuccessorDomainUUID = "successor-domain-mutated"
					return meta
				},
			},
		}
		for _, tt := range cases {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				repo := factory.New(t)
				activated, sealed := activateAndSealMetaForTest(t, repo)
				before := factory.Snapshot(t, repo)

				_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
					meta := tt.mutate(sealed)
					return tx.PutMeta(meta)
				})
				if !errors.Is(err, repository.ErrInvalidRecord) {
					t.Fatalf("forbidden PutMeta error = %v, want ErrInvalidRecord", err)
				}
				assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
				assertMetaForTest(t, repo, sealed)

				commit, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
					meta := loadMetaForTest(t, tx)
					meta.NextJobSequence++
					return tx.PutMeta(meta)
				})
				if err != nil {
					t.Fatalf("later valid PutMeta error = %v", err)
				}
				after := sealed
				after.Generation = commit.Generation
				after.NextJobSequence++
				assertMetaForTest(t, repo, after)
				if after.AdmissionRoot != activated.AdmissionRoot || !after.Sealed {
					t.Fatalf("later PutMeta metadata = %+v, want activated metadata %+v sealed=true", after, activated.AdmissionRoot)
				}
			})
		}
	})

	t.Run("activation generation must be commit generation", func(t *testing.T) {
		repo := factory.New(t)
		before := factory.Snapshot(t, repo)
		_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
			meta := loadMetaForTest(t, tx)
			meta.AdmissionRoot = repository.AdmissionRootMetadata{
				Activated:       true,
				ContractVersion: repository.CurrentAdmissionContractVersion,
				ActivatedAtGen:  meta.Generation + 2,
			}
			return tx.PutMeta(meta)
		})
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("forged activation generation error = %v, want ErrInvalidRecord", err)
		}
		assertSnapshotUnchanged(t, before, factory.Snapshot(t, repo))
	})
}

type fixture struct {
	RequestKey model.RequestKey
	JobID      model.JobID
	Identity   model.TaskIdentity
	Boot       model.BootRef
	Binding    model.Binding
	Record     model.SafetyRecord
	Projection model.JobProjection
}

func newFixture(t *testing.T, name string) fixture {
	t.Helper()
	key, err := model.NewRequestKey("workspace-"+name, "request-"+name)
	if err != nil {
		t.Fatalf("NewRequestKey: %v", err)
	}
	jobID, err := model.NewJobID("job-" + name)
	if err != nil {
		t.Fatalf("NewJobID: %v", err)
	}
	boot, err := model.NewBootRef("boot-"+name, "owner-"+name)
	if err != nil {
		t.Fatalf("NewBootRef: %v", err)
	}
	attemptID, err := model.NewAttemptID("attempt-" + name)
	if err != nil {
		t.Fatalf("NewAttemptID: %v", err)
	}
	identity := model.NewSHA256TaskIdentity([]byte("task-" + name))
	record := model.SafetyRecord{
		SchemaVersion: 1,
		Revision:      1,
		JobID:         jobID,
		RequestKey:    key,
		TaskIdentity:  identity,
		Mode:          model.ModeIdentifiedFenced,
		AdmittedBy:    boot,
		Attempt: model.AttemptProof{
			Ref: model.AttemptRef{JobID: jobID, AttemptID: attemptID, Epoch: 1},
		},
	}
	if err := model.ValidateSafetyRecord(record); err != nil {
		t.Fatalf("fixture safety record is invalid: %v", err)
	}
	binding := model.Binding{
		RequestKey:   key,
		JobID:        jobID,
		TaskIdentity: identity,
		Mode:         model.ModeIdentifiedFenced,
	}
	if err := binding.Matches(record); err != nil {
		t.Fatalf("fixture binding mismatch: %v", err)
	}
	projection, err := model.Project(record, model.ProjectionMetadata{SessionID: "session-" + name})
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	return fixture{
		RequestKey: key,
		JobID:      jobID,
		Identity:   identity,
		Boot:       boot,
		Binding:    binding,
		Record:     record,
		Projection: projection,
	}
}

func newQuiescentFixture(t *testing.T, name string) fixture {
	t.Helper()
	fixture := newFixture(t, name)
	group := fixtureGroupRef(fixture, model.LaunchOrdinalOne)
	grant := model.LaunchGrant{
		Attempt:   fixture.Record.Attempt.Ref,
		Ordinal:   model.LaunchOrdinalOne,
		Nonce:     model.LaunchNonce("nonce-" + name),
		GrantedBy: fixture.Boot,
	}
	releaseOutcome := model.LaunchReleaseOutcomeFact{
		Attempt:    fixture.Record.Attempt.Ref,
		Ordinal:    model.LaunchOrdinalOne,
		Outcome:    model.LaunchReleaseNotSent,
		RecordedBy: fixture.Boot,
	}
	quiescence := model.QuiescenceCertificate{
		Attempt:     fixture.Record.Attempt.Ref,
		Ordinal:     model.LaunchOrdinalOne,
		Group:       group,
		Method:      model.QuiescenceAlreadyAbsent,
		CertifiedBy: fixture.Boot,
	}
	fixture.Record.Attempt.Launches.First = &model.LaunchProof{
		Ordinal:        model.LaunchOrdinalOne,
		Group:          &group,
		Grant:          &grant,
		ReleaseOutcome: &releaseOutcome,
		Quiescence:     &quiescence,
	}
	if err := model.ValidateSafetyRecord(fixture.Record); err != nil {
		t.Fatalf("quiescent fixture safety record is invalid: %v", err)
	}
	projection, err := model.Project(fixture.Record, model.ProjectionMetadata{SessionID: "session-" + name})
	if err != nil {
		t.Fatalf("Project quiescent fixture: %v", err)
	}
	fixture.Projection = projection
	return fixture
}

func fixtureGroupRef(fixture fixture, ordinal model.LaunchOrdinal) model.GroupRef {
	pgid := 20 + int(ordinal)
	return model.GroupRef{
		Version:   1,
		CustodyID: model.CustodyID("custody-" + fixture.JobID.String() + "-" + ordinal.String()),
		Launch: model.LaunchKey{
			Attempt: fixture.Record.Attempt.Ref,
			Ordinal: ordinal,
		},
		HostBootID:        "host-boot-" + fixture.JobID.String(),
		PIDNamespaceState: model.PIDNamespaceNotApplicable,
		PGID:              pgid,
		Leader: model.ProcessIdentity{
			PID:               pgid,
			HighResStartToken: "leader-start-" + ordinal.String(),
		},
		Monitor: model.ProcessIdentity{
			PID:               30 + int(ordinal),
			HighResStartToken: "monitor-start-" + ordinal.String(),
		},
		RetainedID: "retained-" + fixture.JobID.String() + "-" + ordinal.String(),
	}
}

func acceptFixture(t *testing.T, repo repository.Repository, fixture fixture) repository.Commit {
	t.Helper()
	commit, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
		if _, err := tx.AllocateJobID(); err != nil {
			return err
		}
		return putAcceptance(tx, fixture)
	})
	if err != nil {
		t.Fatalf("acceptance Update error = %v", err)
	}
	return commit
}

func putAcceptance(tx repository.WriteTx, fixture fixture) error {
	if err := tx.PutBinding(fixture.Binding); err != nil {
		return err
	}
	if err := tx.PutSafety(fixture.Record, 0); err != nil {
		return err
	}
	return tx.PutProjection(fixture.Projection)
}

func activateAndSealMetaForTest(t *testing.T, repo repository.Repository) (repository.AuthorityMeta, repository.AuthorityMeta) {
	t.Helper()
	var activated repository.AuthorityMeta
	commit, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
		meta := loadMetaForTest(t, tx)
		meta.AdmissionRoot = repository.AdmissionRootMetadata{
			Activated:       true,
			ContractVersion: repository.CurrentAdmissionContractVersion,
			ActivatedAtGen:  meta.Generation + 1,
		}
		activated = meta
		return tx.PutMeta(meta)
	})
	if err != nil {
		t.Fatalf("activate PutMeta error = %v", err)
	}
	activated.Generation = commit.Generation

	var sealed repository.AuthorityMeta
	commit, err = repo.Update(context.Background(), func(tx repository.WriteTx) error {
		meta := loadMetaForTest(t, tx)
		meta.Sealed = true
		meta.SuccessorDomainUUID = "successor-domain-0001"
		meta.SuccessorStateRoot = "/tmp/successor-domain-0001"
		sealed = meta
		return tx.PutMeta(meta)
	})
	if err != nil {
		t.Fatalf("seal PutMeta error = %v", err)
	}
	sealed.Generation = commit.Generation
	return activated, sealed
}

func loadMetaForTest(t *testing.T, tx repository.ReadTx) repository.AuthorityMeta {
	t.Helper()
	record := tx.Meta()
	if record.State != repository.RecordValid {
		t.Fatalf("meta state = %s, want valid", record.State)
	}
	return record.Value
}

func assertMetaForTest(t *testing.T, repo repository.Repository, want repository.AuthorityMeta) {
	t.Helper()
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		got := loadMetaForTest(t, tx)
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("meta = %#v, want %#v", got, want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertRequestBinding(t *testing.T, repo repository.Repository, binding model.Binding) {
	t.Helper()
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		request := tx.LookupRequest(binding.RequestKey)
		if request.Binding.State != repository.RecordValid {
			return fmt.Errorf("binding state = %s, want valid", request.Binding.State)
		}
		if !reflect.DeepEqual(request.Binding.Value, binding) {
			return fmt.Errorf("binding = %#v, want %#v", request.Binding.Value, binding)
		}
		if request.Tombstone.State != repository.RecordMissing {
			return fmt.Errorf("tombstone state = %s, want missing", request.Tombstone.State)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func loadSafetyRecord(t *testing.T, repo repository.Repository, jobID model.JobID) model.SafetyRecord {
	t.Helper()
	var record model.SafetyRecord
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		job := tx.LoadJob(jobID)
		if job.Safety.State != repository.RecordValid {
			return fmt.Errorf("job safety state = %s, want valid", job.Safety.State)
		}
		record = job.Safety.Value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return record
}

func assertJobSafetyRecord(t *testing.T, repo repository.Repository, jobID model.JobID, want model.SafetyRecord) {
	t.Helper()
	got := loadSafetyRecord(t, repo, jobID)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("safety = %#v, want %#v", got, want)
	}
}

func assertJobImage(t *testing.T, repo repository.Repository, fixture fixture) {
	t.Helper()
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		job := tx.LoadJob(fixture.JobID)
		if job.Binding.State != repository.RecordValid {
			return fmt.Errorf("job binding state = %s, want valid", job.Binding.State)
		}
		if job.Safety.State != repository.RecordValid {
			return fmt.Errorf("job safety state = %s, want valid", job.Safety.State)
		}
		if job.Projection.State != repository.RecordValid {
			return fmt.Errorf("job projection state = %s, want valid", job.Projection.State)
		}
		if !reflect.DeepEqual(job.Safety.Value, fixture.Record) {
			return fmt.Errorf("safety = %#v, want %#v", job.Safety.Value, fixture.Record)
		}
		if !reflect.DeepEqual(job.Projection.Value, fixture.Projection) {
			return fmt.Errorf("projection = %#v, want %#v", job.Projection.Value, fixture.Projection)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertSnapshotUnchanged(t *testing.T, before, after []byte) {
	t.Helper()
	if !bytes.Equal(before, after) {
		t.Fatalf("snapshot changed after rejected transaction\nbefore: %s\nafter:  %s", before, after)
	}
}

func mutateLaunchPhysicalFactForTest(t *testing.T, record *model.SafetyRecord, ordinal model.LaunchOrdinal) {
	t.Helper()
	launch, ok := record.Attempt.Launches.Get(ordinal)
	if !ok || launch.Group == nil || launch.Quiescence == nil {
		t.Fatalf("launch %s is missing group or quiescence", ordinal)
	}
	launch.Group.CustodyID = model.CustodyID("custody-forged-" + record.JobID.String())
	launch.Group.PGID += 1000
	launch.Group.Leader.PID += 1000
	launch.Group.Leader.HighResStartToken = "forged-leader"
	launch.Group.Monitor.PID += 1000
	launch.Group.Monitor.HighResStartToken = "forged-monitor"
	launch.Group.RetainedID = "forged-retained-" + record.JobID.String()
	launch.ReleaseOutcome.Outcome = model.LaunchReleaseSentUnknown
	launch.Quiescence.Group = *launch.Group
}

func cloneSafetyRecordForTest(record model.SafetyRecord) model.SafetyRecord {
	next := record
	next.Attempt.Launches = model.LaunchSlots[model.LaunchProof]{
		First:  cloneLaunchProofForTest(record.Attempt.Launches.First),
		Second: cloneLaunchProofForTest(record.Attempt.Launches.Second),
	}
	next.Acknowledgement = clonePtrForTest(record.Acknowledgement)
	next.Cancel = clonePtrForTest(record.Cancel)
	next.Outcome = clonePtrForTest(record.Outcome)
	next.Result = clonePtrForTest(record.Result)
	next.Terminal = clonePtrForTest(record.Terminal)
	return next
}

func cloneLaunchProofForTest(launch *model.LaunchProof) *model.LaunchProof {
	if launch == nil {
		return nil
	}
	copied := *launch
	copied.Group = clonePtrForTest(launch.Group)
	copied.Grant = clonePtrForTest(launch.Grant)
	copied.ReleaseOutcome = clonePtrForTest(launch.ReleaseOutcome)
	copied.Released = clonePtrForTest(launch.Released)
	copied.Quiescence = clonePtrForTest(launch.Quiescence)
	return &copied
}

func clonePtrForTest[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}
