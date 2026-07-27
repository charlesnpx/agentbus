package bbolt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/repository/repositorytest"
	bolt "go.etcd.io/bbolt"
)

func TestRepositoryContract(t *testing.T) {
	repositorytest.RunRepositoryContract(t, repositorytest.Factory{
		New: func(t *testing.T) repository.Repository {
			t.Helper()
			repo := newReopeningRepository(t)
			return repo
		},
		Snapshot: func(t *testing.T, repo repository.Repository) []byte {
			t.Helper()
			bboltRepo, ok := repo.(interface{ SnapshotBytes() []byte })
			if !ok {
				t.Fatalf("repo type = %T, want SnapshotBytes", repo)
			}
			return bboltRepo.SnapshotBytes()
		},
		CorruptSafety: func(t *testing.T, repo repository.Repository, jobID model.JobID, diagnostic string) {
			t.Helper()
			bboltRepo, ok := repo.(interface {
				InjectCorruptSafetyForTest(model.JobID, string)
			})
			if !ok {
				t.Fatalf("repo type = %T, want InjectCorruptSafetyForTest", repo)
			}
			bboltRepo.InjectCorruptSafetyForTest(jobID, diagnostic)
		},
		CorruptBinding: func(t *testing.T, repo repository.Repository, key model.RequestKey, diagnostic string) {
			t.Helper()
			bboltRepo, ok := repo.(interface {
				InjectCorruptBindingForTest(model.RequestKey, string)
			})
			if !ok {
				t.Fatalf("repo type = %T, want InjectCorruptBindingForTest", repo)
			}
			bboltRepo.InjectCorruptBindingForTest(key, diagnostic)
		},
		CorruptTombstone: func(t *testing.T, repo repository.Repository, key model.RequestKey, diagnostic string) {
			t.Helper()
			bboltRepo, ok := repo.(interface {
				InjectCorruptTombstoneForTest(model.RequestKey, string)
			})
			if !ok {
				t.Fatalf("repo type = %T, want InjectCorruptTombstoneForTest", repo)
			}
			bboltRepo.InjectCorruptTombstoneForTest(key, diagnostic)
		},
		MissingMeta: func(t *testing.T, repo repository.Repository) {
			t.Helper()
			bboltRepo, ok := repo.(interface {
				InjectMissingMetaForTest()
			})
			if !ok {
				t.Fatalf("repo type = %T, want InjectMissingMetaForTest", repo)
			}
			bboltRepo.InjectMissingMetaForTest()
		},
		FailCommitAfterCommit: func(t *testing.T, repo repository.Repository, err error) {
			t.Helper()
			bboltRepo, ok := repo.(interface {
				FailCommitAfterCommitForTest(error)
			})
			if !ok {
				t.Fatalf("repo type = %T, want FailCommitAfterCommitForTest", repo)
			}
			bboltRepo.FailCommitAfterCommitForTest(err)
		},
		Audit: func(t *testing.T, repo repository.Repository) error {
			t.Helper()
			auditor, ok := repo.(repository.Auditor)
			if !ok {
				t.Fatalf("repo type = %T, want repository.Auditor", repo)
			}
			return auditor.AuditIntegrity(context.Background())
		},
	})
}

func TestAdmissionRepositoryRequiredBucketsMatchInitializedRepository(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer repo.Close()

	var gotBuckets []string
	var gotMetaKeys []string
	if err := repo.db.View(func(tx *bolt.Tx) error {
		if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			gotBuckets = append(gotBuckets, string(name))
			return nil
		}); err != nil {
			return err
		}
		meta := tx.Bucket(bucketMeta)
		if meta == nil {
			return errors.New("meta bucket is missing")
		}
		return meta.ForEach(func(key, _ []byte) error {
			gotMetaKeys = append(gotMetaKeys, string(key))
			return nil
		})
	}); err != nil {
		t.Fatalf("list initialized structure: %v", err)
	}
	sort.Strings(gotBuckets)
	wantBuckets := AdmissionRepositoryRequiredBuckets()
	sort.Strings(wantBuckets)
	if !reflect.DeepEqual(gotBuckets, wantBuckets) {
		t.Fatalf("initialized buckets = %v, want %v", gotBuckets, wantBuckets)
	}
	sort.Strings(gotMetaKeys)
	wantMetaKeys := AdmissionRepositoryRequiredMetaKeys()
	sort.Strings(wantMetaKeys)
	if !reflect.DeepEqual(gotMetaKeys, wantMetaKeys) {
		t.Fatalf("initialized meta keys = %v, want %v", gotMetaKeys, wantMetaKeys)
	}
	if err := repo.VerifyInitializedStructure(); err != nil {
		t.Fatalf("VerifyInitializedStructure() error = %v", err)
	}
}

func TestAdmissionRepositoryRequiredAccessorsReturnCopies(t *testing.T) {
	buckets := AdmissionRepositoryRequiredBuckets()
	if len(buckets) == 0 {
		t.Fatal("AdmissionRepositoryRequiredBuckets returned empty list")
	}
	buckets[0] = "mutated"
	if got := AdmissionRepositoryRequiredBuckets()[0]; got == "mutated" {
		t.Fatal("AdmissionRepositoryRequiredBuckets returned mutable backing storage")
	}

	metaKeys := AdmissionRepositoryRequiredMetaKeys()
	if len(metaKeys) == 0 {
		t.Fatal("AdmissionRepositoryRequiredMetaKeys returned empty list")
	}
	metaKeys[0] = "mutated"
	if got := AdmissionRepositoryRequiredMetaKeys()[0]; got == "mutated" {
		t.Fatal("AdmissionRepositoryRequiredMetaKeys returned mutable backing storage")
	}
}

func TestOpenExistingRequiresStrictSchemaV2(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	before := createSchemaV1RepositoryForTest(t, path)

	repo, err := OpenExisting(path, &bolt.Options{Timeout: time.Second})
	if err == nil {
		repo.Close()
		t.Fatal("OpenExisting succeeded for schema v1, want incompatible schema error")
	}
	var schemaErr UnsupportedAuthorityMetaSchemaVersionError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("OpenExisting error = %T %v, want UnsupportedAuthorityMetaSchemaVersionError", err, err)
	}
	if schemaErr.SchemaVersion != 1 {
		t.Fatalf("schema version = %d, want 1", schemaErr.SchemaVersion)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("failed old-schema open mutated database bytes")
	}

	repo, err = OpenExistingReadOnly(path, &bolt.Options{Timeout: time.Second})
	if err == nil {
		repo.Close()
		t.Fatal("OpenExistingReadOnly succeeded for schema v1, want incompatible schema error")
	}
	var readOnlySchemaErr UnsupportedAuthorityMetaSchemaVersionError
	if !errors.As(err, &readOnlySchemaErr) {
		t.Fatalf("OpenExistingReadOnly error = %T %v, want UnsupportedAuthorityMetaSchemaVersionError", err, err)
	}
}

func TestCreateInitializesStrictSchemaV2AndBindingIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	schema, err := repo.AuthorityMetaSchemaVersion()
	if err != nil {
		t.Fatal(err)
	}
	if schema != repository.StrictAuthorityMetaSchemaVersion {
		t.Fatalf("schema = %d, want %d", schema, repository.StrictAuthorityMetaSchemaVersion)
	}
	if err := repo.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketBindingIndex) == nil {
			return errors.New("binding_index bucket missing")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRemovesPartialDatabaseAfterInitializeFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	initializeErr := errors.New("initialize fresh failed")
	previous := initializeFreshForTest
	initializeFreshForTest = func() error {
		return initializeErr
	}
	t.Cleanup(func() {
		initializeFreshForTest = previous
	})

	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if repo != nil {
		_ = repo.Close()
		t.Fatal("Create returned repository after injected initialize failure")
	}
	if !errors.Is(err, initializeErr) {
		t.Fatalf("Create error = %v, want initialize failure", err)
	}
	if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial database stat = %v, want missing", statErr)
	}

	initializeFreshForTest = nil
	repo, err = Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("retry Create error = %v", err)
	}
	defer repo.Close()
	if _, statErr := os.Lstat(path); statErr != nil {
		t.Fatalf("retry database stat = %v", statErr)
	}
}

func TestBindingIndexMismatchIsCorruptionAndAuditFinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	fixture := newBboltFixture(t, "index-mismatch")
	acceptBboltFixture(t, repo, fixture)
	if err := repo.db.Update(func(tx *bolt.Tx) error {
		index := tx.Bucket(bucketBindingIndex)
		if index == nil {
			return errors.New("binding_index bucket missing")
		}
		return index.Put(jobIDKey(fixture.JobID), requestKeyBytes(mustRequestKeyForTest(t, "workspace-other", "request-other")))
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		image := tx.LoadJob(fixture.JobID)
		if image.Binding.State != repository.RecordCorrupt {
			return fmt.Errorf("binding state = %s, want corrupt", image.Binding.State)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.AuditIntegrity(context.Background()); !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("AuditIntegrity error = %v, want ErrCorruptRecord", err)
	}
}

func TestBindingIndexValueCorruptionFailsProductionReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	fixture := newBboltFixture(t, "index-value-corrupt")
	acceptBboltFixture(t, repo, fixture)
	repo.InjectCorruptBindingIndexValueForTest(fixture.JobID, mustRequestKeyForTest(t, "workspace-index-value-other", "request-index-value-other"))

	err = repo.View(context.Background(), func(tx repository.ReadTx) error {
		_, err := tx.ListJobs(repository.JobFilter{})
		return err
	})
	requireCorruptKind(t, err, "binding_index")

	err = repo.View(context.Background(), func(tx repository.ReadTx) error {
		_, err := tx.RootStats()
		return err
	})
	requireCorruptKind(t, err, "binding_index")

	err = repo.View(context.Background(), func(tx repository.ReadTx) error {
		image := tx.LoadJob(fixture.JobID)
		if image.Binding.State != repository.RecordCorrupt {
			return fmt.Errorf("binding state = %s, want corrupt", image.Binding.State)
		}
		return repository.ValidateJobClosure(fixture.JobID, image, tx.LookupRequest)
	})
	requireCorruptKind(t, err, "binding_index")
}

func TestPutBindingRejectsOrphanBindingIndexEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	fixture := newBboltFixture(t, "orphan-index-put")
	repo.InjectCorruptBindingIndexValueForTest(fixture.JobID, fixture.RequestKey)

	_, err = repo.Update(context.Background(), func(tx repository.WriteTx) error {
		return tx.PutBinding(fixture.Binding)
	})
	if !errors.Is(err, repository.ErrDefinitelyNotCommitted) {
		t.Fatalf("PutBinding error = %v, want ErrDefinitelyNotCommitted", err)
	}
	requireCorruptKind(t, err, "binding_index")
}

func TestAuditIntegrityReportsBindingIndexAndSafetyFindings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	fixture := newBboltFixture(t, "audit-index-and-safety")
	acceptBboltFixture(t, repo, fixture)
	repo.InjectCorruptBindingIndexValueForTest(fixture.JobID, mustRequestKeyForTest(t, "workspace-audit-index-other", "request-audit-index-other"))
	repo.InjectCorruptSafetyForTest(fixture.JobID, "safety checksum")

	err = repo.AuditIntegrity(context.Background())
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("AuditIntegrity error = %v, want ErrCorruptRecord", err)
	}
	for _, kind := range []string{"binding_index", "safety"} {
		if !errorTreeHasCorruptKind(err, kind) {
			t.Fatalf("AuditIntegrity error = %v, want %s finding", err, kind)
		}
	}
}

func TestAuditIntegrityClassifiesProjectionOnlyFindings(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(*testing.T, *Repository, bboltFixture)
	}{
		{
			name: "missing",
			arrange: func(t *testing.T, repo *Repository, fixture bboltFixture) {
				t.Helper()
				if err := repo.db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket(bucketProjections).Delete(jobIDKey(fixture.JobID))
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "corrupt",
			arrange: func(t *testing.T, repo *Repository, fixture bboltFixture) {
				t.Helper()
				repo.InjectCorruptProjectionForTest(fixture.JobID, "projection checksum")
			},
		},
		{
			name: "mismatch",
			arrange: func(t *testing.T, repo *Repository, fixture bboltFixture) {
				t.Helper()
				mismatched := fixture.Projection
				mismatched.Public = model.PublicStarting
				if err := repo.db.Update(func(tx *bolt.Tx) error {
					return putEnvelope(tx.Bucket(bucketProjections), kindProjection, jobIDKey(fixture.JobID), mismatched, revisionProjection(mismatched))
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "admission.db")
			repo, err := Create(path, &bolt.Options{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer repo.Close()
			fixture := newBboltFixture(t, "audit-projection-"+tt.name)
			acceptBboltFixture(t, repo, fixture)
			tt.arrange(t, repo, fixture)

			err = repo.AuditIntegrity(context.Background())
			if err == nil {
				t.Fatal("AuditIntegrity error = nil, want projection finding")
			}
			kinds := repository.IntegrityFindingKinds(err)
			if len(kinds) == 0 {
				t.Fatalf("AuditIntegrity error = %v, want classified findings", err)
			}
			for _, kind := range kinds {
				if kind != "projection" {
					t.Fatalf("AuditIntegrity kinds = %v, want only projection", kinds)
				}
			}
		})
	}
}

func TestAuditIntegrityMissingBindingIndexReturnsTypedFinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	fixture := newBboltFixture(t, "missing-index-audit")
	acceptBboltFixture(t, repo, fixture)
	if err := repo.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(bucketBindingIndex)
	}); err != nil {
		t.Fatal(err)
	}
	err = repo.AuditIntegrity(context.Background())
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("AuditIntegrity error = %v, want ErrCorruptRecord", err)
	}
	var aggregate interface{ Unwrap() []error }
	if !errors.As(err, &aggregate) {
		t.Fatalf("AuditIntegrity error = %T %v, want aggregate", err, err)
	}
	found := false
	for _, finding := range aggregate.Unwrap() {
		if strings.Contains(finding.Error(), "binding_index") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("AuditIntegrity error = %v, want binding_index finding", err)
	}
}

func TestRootStatsFailsOnCorruptSafetyBindingAndTombstoneRecords(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*Repository, bboltFixture)
	}{
		{
			name: "safety",
			corrupt: func(repo *Repository, fixture bboltFixture) {
				repo.InjectCorruptSafetyForTest(fixture.JobID, "safety checksum")
			},
		},
		{
			name: "binding",
			corrupt: func(repo *Repository, fixture bboltFixture) {
				repo.InjectCorruptBindingForTest(fixture.RequestKey, "binding checksum")
			},
		},
		{
			name: "tombstone",
			corrupt: func(repo *Repository, fixture bboltFixture) {
				if _, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
					if err := tx.DeleteLiveJob(fixture.JobID); err != nil {
						return err
					}
					return tx.PutTombstone(repository.Tombstone{
						RequestKey:        fixture.RequestKey,
						JobID:             fixture.JobID,
						TaskIdentity:      fixture.Identity,
						ExpiredGeneration: 2,
					})
				}); err != nil {
					t.Fatalf("create tombstone: %v", err)
				}
				repo.InjectCorruptTombstoneForTest(fixture.RequestKey, "tombstone checksum")
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "admission.db")
			repo, err := Create(path, &bolt.Options{Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer repo.Close()
			fixture := newBboltFixture(t, "rootstats-corrupt-"+tt.name)
			acceptBboltFixture(t, repo, fixture)
			tt.corrupt(repo, fixture)
			err = repo.View(context.Background(), func(tx repository.ReadTx) error {
				_, err := tx.RootStats()
				return err
			})
			if !errors.Is(err, repository.ErrCorruptRecord) {
				t.Fatalf("RootStats error = %v, want ErrCorruptRecord", err)
			}
			if !strings.Contains(err.Error(), tt.name) {
				t.Fatalf("RootStats error = %v, want %s diagnostic", err, tt.name)
			}
		})
	}
}

func requireCorruptKind(t *testing.T, err error, kind string) {
	t.Helper()
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("error = %v, want ErrCorruptRecord", err)
	}
	var corrupt repository.CorruptRecordKindError
	if !errors.As(err, &corrupt) || corrupt.Kind != kind {
		t.Fatalf("error = %T %v, want corrupt kind %s", err, err, kind)
	}
}

func errorTreeHasCorruptKind(err error, kind string) bool {
	if err == nil {
		return false
	}
	switch typed := err.(type) {
	case repository.CorruptRecordKindError:
		return typed.Kind == kind
	case interface{ Unwrap() []error }:
		for _, child := range typed.Unwrap() {
			if errorTreeHasCorruptKind(child, kind) {
				return true
			}
		}
		return false
	case interface{ Unwrap() error }:
		return errorTreeHasCorruptKind(typed.Unwrap(), kind)
	default:
		return false
	}
}

func errorTreeContains(err error, substring string) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), substring) {
		return true
	}
	switch typed := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range typed.Unwrap() {
			if errorTreeContains(child, substring) {
				return true
			}
		}
		return false
	case interface{ Unwrap() error }:
		return errorTreeContains(typed.Unwrap(), substring)
	default:
		return false
	}
}

func TestPointLookupDoesNotTraverseHistoricalBindings(t *testing.T) {
	repo := newSeededBboltRepository(t, 200)
	target := model.JobID("job-seed-000199")
	repo.ResetOperationCountsForTest()
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		image := tx.LoadJob(target)
		if image.Safety.State != repository.RecordValid || image.Binding.State != repository.RecordValid {
			return fmt.Errorf("target image states binding=%s safety=%s", image.Binding.State, image.Safety.State)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	counts := repo.OperationCountsForTest()
	if got := counts["foreach:bindings"]; got != 0 {
		t.Fatalf("point LoadJob traversed bindings %d time(s), want 0", got)
	}
	if got := counts["get:binding_index"]; got != 1 {
		t.Fatalf("binding_index gets = %d, want 1", got)
	}
	if got := counts["get:bindings"]; got != 1 {
		t.Fatalf("binding gets = %d, want 1", got)
	}
}

func TestOneRecordUpdateTouchesBoundedKeysAndDoesNotRecreateBuckets(t *testing.T) {
	repo := newSeededBboltRepository(t, 500)
	fixture := seedFixture(t, 499)
	repo.ResetOperationCountsForTest()
	_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
		image := tx.LoadJob(fixture.JobID)
		if image.Safety.State != repository.RecordValid {
			return fmt.Errorf("safety state = %s", image.Safety.State)
		}
		next := image.Safety.Value
		next.Revision++
		next.Cancel = &model.CancelFact{JobID: fixture.JobID, RequestedBy: fixture.Boot}
		if err := tx.PutSafety(next, image.Safety.Value.Revision); err != nil {
			return err
		}
		projection, err := model.Project(next, model.ProjectionMetadata{SessionID: fixture.Projection.SessionID})
		if err != nil {
			return err
		}
		return tx.PutProjection(projection)
	})
	if err != nil {
		t.Fatal(err)
	}
	counts := repo.OperationCountsForTest()
	for _, key := range []string{"create_bucket", "delete_bucket"} {
		if counts[key] != 0 {
			t.Fatalf("%s count = %d, want 0", key, counts[key])
		}
	}
	for key, count := range counts {
		if strings.HasPrefix(key, "foreach:") {
			t.Fatalf("one-record update used %s %d time(s), want no bucket scans", key, count)
		}
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	if total > 60 {
		t.Fatalf("operation count = %d, want bounded <= 60; counts=%v", total, counts)
	}
}

func TestPutMetaTouchesBoundedKeysIndependentOfHistory(t *testing.T) {
	repo := newSeededBboltRepository(t, 1500)
	repo.ResetOperationCountsForTest()
	_, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
		meta := tx.Meta()
		if meta.State != repository.RecordValid {
			return fmt.Errorf("meta state = %s, want valid", meta.State)
		}
		next := meta.Value
		next.NextJobSequence++
		return tx.PutMeta(next)
	})
	if err != nil {
		t.Fatal(err)
	}
	counts := repo.OperationCountsForTest()
	for key, count := range counts {
		if strings.HasPrefix(key, "foreach:") {
			t.Fatalf("PutMeta used %s %d time(s), want no history scans; counts=%v", key, count, counts)
		}
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	if total > 12 {
		t.Fatalf("PutMeta operation count = %d, want bounded <= 12; counts=%v", total, counts)
	}
}

func TestDefinitelyNotCommittedUpdateLeavesDatabaseBytesUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	before := readFileBytesForTest(t, path)
	fixture := newBboltFixture(t, "bytes-rejected")
	sentinel := errors.New("reject before commit")

	_, err = repo.Update(context.Background(), func(tx repository.WriteTx) error {
		if err := tx.PutBinding(fixture.Binding); err != nil {
			return err
		}
		if err := tx.PutSafety(fixture.Record, 0); err != nil {
			return err
		}
		if err := tx.PutProjection(fixture.Projection); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, repository.ErrDefinitelyNotCommitted) || !errors.Is(err, sentinel) {
		t.Fatalf("Update error = %v, want ErrDefinitelyNotCommitted wrapping sentinel", err)
	}
	after := readFileBytesForTest(t, path)
	if !bytes.Equal(before, after) {
		t.Fatal("definitely-not-committed update changed database bytes")
	}
}

func TestAuditDoesNotMutateDatabaseBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	acceptBboltFixture(t, repo, newBboltFixture(t, "audit-bytes"))
	before := readFileBytesForTest(t, path)
	if err := repo.AuditIntegrity(context.Background()); err != nil {
		t.Fatal(err)
	}
	after := readFileBytesForTest(t, path)
	if !bytes.Equal(before, after) {
		t.Fatal("AuditIntegrity mutated database bytes")
	}
}

func TestStructuralIntegrityDetectsReachableFreedPageWithoutMutatingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	acceptBboltFixture(t, repo, newBboltFixture(t, "reachable-freed"))
	if err := repo.Close(); err != nil {
		t.Fatalf("close before corruption: %v", err)
	}

	corruptFreelistWithReachablePageForTest(t, path)
	corruptBytes := readFileBytesForTest(t, path)

	reopened, err := OpenExisting(path, &bolt.Options{Timeout: time.Second})
	if err == nil {
		_ = reopened.Close()
		t.Fatal("OpenExisting succeeded for reachable-freed corruption, want ErrCorruptRecord")
	}
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("OpenExisting error = %v, want ErrCorruptRecord", err)
	}
	if !errorTreeContains(err, "reachable freed") {
		t.Fatalf("OpenExisting error = %v, want reachable-freed diagnostic", err)
	}
	if after := readFileBytesForTest(t, path); !bytes.Equal(corruptBytes, after) {
		t.Fatal("OpenExisting structural failure mutated database bytes")
	}

	rawDB, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		t.Fatalf("raw read-only open after corruption: %v", err)
	}
	auditRepo := &Repository{db: rawDB}
	err = auditRepo.AuditIntegrity(context.Background())
	if closeErr := auditRepo.Close(); closeErr != nil {
		t.Fatalf("close raw read-only repo: %v", closeErr)
	}
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("AuditIntegrity error = %v, want ErrCorruptRecord", err)
	}
	if kinds := repository.IntegrityFindingKinds(err); !reflect.DeepEqual(kinds, []string{"structure"}) {
		t.Fatalf("AuditIntegrity finding kinds = %v, want [structure]", kinds)
	}
	if after := readFileBytesForTest(t, path); !bytes.Equal(corruptBytes, after) {
		t.Fatal("AuditIntegrity structural failure mutated database bytes")
	}
}

func TestUnrelatedCorruptRecordRawBytesPreservedAcrossSuccessfulCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	corrupt := newBboltFixture(t, "raw-corrupt")
	acceptBboltFixture(t, repo, corrupt)
	repo.InjectCorruptSafetyForTest(corrupt.JobID, "safety checksum")
	rawBefore := readRawBucketValueForTest(t, repo, bucketSafety, jobIDKey(corrupt.JobID))

	other := newBboltFixture(t, "raw-other")
	acceptBboltFixture(t, repo, other)
	rawAfter := readRawBucketValueForTest(t, repo, bucketSafety, jobIDKey(corrupt.JobID))
	if !bytes.Equal(rawBefore, rawAfter) {
		t.Fatal("unrelated corrupt safety raw bytes changed across successful unrelated commit")
	}
}

func corruptFreelistWithReachablePageForTest(t *testing.T, path string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	meta, ok, err := readBoltPreflightMeta(file, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("active bbolt meta not found")
	}
	if meta.root <= 1 || meta.root >= meta.pgid {
		t.Fatalf("active root page = %d, high water mark = %d", meta.root, meta.pgid)
	}
	if meta.freelist <= 1 || meta.freelist >= meta.pgid {
		t.Fatalf("active freelist page = %d, high water mark = %d", meta.freelist, meta.pgid)
	}
	if meta.root == meta.freelist {
		t.Fatalf("root page and freelist page are both %d", meta.root)
	}
	page, err := readBoltPreflightPage(file, meta.freelist, meta.pageSize)
	if err != nil {
		t.Fatal(err)
	}
	if page.flags != boltFreelistPageFlag {
		t.Fatalf("page %d flags = 0x%x, want freelist", meta.freelist, page.flags)
	}

	var count [2]byte
	binary.LittleEndian.PutUint16(count[:], 1)
	if _, err := file.WriteAt(count[:], int64(meta.freelist*meta.pageSize+10)); err != nil {
		t.Fatalf("write freelist count: %v", err)
	}
	var id [8]byte
	binary.LittleEndian.PutUint64(id[:], meta.root)
	if _, err := file.WriteAt(id[:], int64(meta.freelist*meta.pageSize+boltPageHeaderSize)); err != nil {
		t.Fatalf("write freelist id: %v", err)
	}
}

func BenchmarkRepositoryPointLookup(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("%d-identities", size), func(b *testing.B) {
			repo := newDirectSeededBboltRepositoryForBenchmark(b, size)
			target := model.JobID(fmt.Sprintf("job-seed-%06d", size-1))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
					image := tx.LoadJob(target)
					if image.Safety.State != repository.RecordValid || image.Binding.State != repository.RecordValid {
						return fmt.Errorf("target image states binding=%s safety=%s", image.Binding.State, image.Safety.State)
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type reopeningRepository struct {
	t    *testing.T
	path string
	repo *Repository
}

func newReopeningRepository(t *testing.T) *reopeningRepository {
	t.Helper()
	wrapper := &reopeningRepository{
		t:    t,
		path: filepath.Join(t.TempDir(), "admission.db"),
	}
	wrapper.reopen()
	t.Cleanup(func() {
		if wrapper.repo != nil {
			if err := wrapper.repo.Close(); err != nil {
				t.Fatalf("close bbolt repository: %v", err)
			}
		}
	})
	return wrapper
}

func (r *reopeningRepository) View(ctx context.Context, fn func(repository.ReadTx) error) error {
	return r.repo.View(ctx, fn)
}

func (r *reopeningRepository) Update(ctx context.Context, fn func(repository.WriteTx) error) (repository.Commit, error) {
	commit, err := r.repo.Update(ctx, fn)
	if r.repo.VerifyInitializedStructure() == nil {
		r.reopen()
	}
	return commit, err
}

func (r *reopeningRepository) SnapshotBytes() []byte {
	return r.repo.SnapshotBytes()
}

func (r *reopeningRepository) InjectCorruptSafetyForTest(jobID model.JobID, diagnostic string) {
	r.repo.InjectCorruptSafetyForTest(jobID, diagnostic)
	r.reopen()
}

func (r *reopeningRepository) InjectCorruptBindingForTest(key model.RequestKey, diagnostic string) {
	r.repo.InjectCorruptBindingForTest(key, diagnostic)
	r.reopen()
}

func (r *reopeningRepository) InjectCorruptTombstoneForTest(key model.RequestKey, diagnostic string) {
	r.repo.InjectCorruptTombstoneForTest(key, diagnostic)
	r.reopen()
}

func (r *reopeningRepository) InjectMissingMetaForTest() {
	r.repo.InjectMissingMetaForTest()
}

func (r *reopeningRepository) FailCommitAfterCommitForTest(err error) {
	r.repo.FailCommitAfterCommitForTest(err)
}

func (r *reopeningRepository) AuditIntegrity(ctx context.Context) error {
	return r.repo.AuditIntegrity(ctx)
}

func (r *reopeningRepository) reopen() {
	r.t.Helper()
	if r.repo != nil {
		if err := r.repo.Close(); err != nil {
			r.t.Fatalf("close bbolt repository before reopen: %v", err)
		}
	}
	var repo *Repository
	var err error
	if _, statErr := os.Stat(r.path); errors.Is(statErr, os.ErrNotExist) {
		repo, err = Create(r.path, &bolt.Options{Timeout: time.Second})
	} else if statErr != nil {
		r.t.Fatalf("stat bbolt repository: %v", statErr)
	} else {
		repo, err = OpenExisting(r.path, &bolt.Options{Timeout: time.Second})
	}
	if err != nil {
		r.t.Fatalf("open bbolt repository: %v", err)
	}
	r.repo = repo
}

type bboltFixture struct {
	RequestKey model.RequestKey
	JobID      model.JobID
	Identity   model.TaskIdentity
	Boot       model.BootRef
	Binding    model.Binding
	Record     model.SafetyRecord
	Projection model.JobProjection
}

func createSchemaV1RepositoryForTest(t *testing.T, path string) []byte {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketMeta, bucketBindings, bucketSafety, bucketProjections, bucketTombstones, bucketQuarantine} {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		if err := putEnvelope(tx.Bucket(bucketMeta), kindDBUUID, keyDBUUID, "bbolt-db-schema-v1", 0); err != nil {
			return err
		}
		meta := repository.AuthorityMeta{
			SchemaVersion:   1,
			Generation:      0,
			NextJobSequence: 1,
		}
		return putEnvelope(tx.Bucket(bucketMeta), kindMeta, keyMeta, meta, revisionMeta(meta))
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newSeededBboltRepository(t *testing.T, count int) *Repository {
	t.Helper()
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := repo.Close(); err != nil {
			t.Fatalf("close bbolt repository: %v", err)
		}
	})
	_, err = repo.Update(context.Background(), func(tx repository.WriteTx) error {
		for i := 0; i < count; i++ {
			fixture := seedFixture(t, i)
			if err := tx.PutBinding(fixture.Binding); err != nil {
				return err
			}
			if err := tx.PutSafety(fixture.Record, 0); err != nil {
				return err
			}
			if err := tx.PutProjection(fixture.Projection); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func newDirectSeededBboltRepositoryForBenchmark(b *testing.B, count int) *Repository {
	b.Helper()
	path := filepath.Join(b.TempDir(), "admission.db")
	repo, err := Create(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := repo.Close(); err != nil {
			b.Fatalf("close bbolt repository: %v", err)
		}
	})
	if err := repo.db.Update(func(tx *bolt.Tx) error {
		meta := (readTx{tx: tx}).Meta()
		if meta.State != repository.RecordValid {
			return fmt.Errorf("meta state = %s", meta.State)
		}
		meta.Value.Generation = 1
		meta.Value.NextJobSequence = uint64(count + 1)
		if err := putEnvelope(tx.Bucket(bucketMeta), kindMeta, keyMeta, meta.Value, revisionMeta(meta.Value)); err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			fixture := seedFixture(b, i)
			key := requestKeyBytes(fixture.RequestKey)
			if err := putEnvelope(tx.Bucket(bucketBindings), kindBinding, key, fixture.Binding, revisionBinding(fixture.Binding)); err != nil {
				return err
			}
			if err := tx.Bucket(bucketBindingIndex).Put(jobIDKey(fixture.JobID), key); err != nil {
				return err
			}
			if err := putEnvelope(tx.Bucket(bucketSafety), kindSafety, jobIDKey(fixture.JobID), fixture.Record, revisionSafety(fixture.Record)); err != nil {
				return err
			}
			if err := putEnvelope(tx.Bucket(bucketProjections), kindProjection, jobIDKey(fixture.JobID), fixture.Projection, revisionProjection(fixture.Projection)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return repo
}

func seedFixture(t testing.TB, i int) bboltFixture {
	t.Helper()
	name := fmt.Sprintf("seed-%06d", i)
	key := mustRequestKeyForTest(t, "workspace-"+name, "request-"+name)
	jobID, err := model.NewJobID(fmt.Sprintf("job-seed-%06d", i))
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-"+name, "owner-"+name)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := model.NewAttemptID("attempt-" + name)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	binding := model.Binding{
		RequestKey:   key,
		JobID:        jobID,
		TaskIdentity: identity,
		Mode:         model.ModeIdentifiedFenced,
	}
	if err := binding.Matches(record); err != nil {
		t.Fatal(err)
	}
	projection, err := model.Project(record, model.ProjectionMetadata{SessionID: "session-" + name})
	if err != nil {
		t.Fatal(err)
	}
	return bboltFixture{
		RequestKey: key,
		JobID:      jobID,
		Identity:   identity,
		Boot:       boot,
		Binding:    binding,
		Record:     record,
		Projection: projection,
	}
}

func newBboltFixture(t *testing.T, name string) bboltFixture {
	t.Helper()
	key := mustRequestKeyForTest(t, "workspace-"+name, "request-"+name)
	jobID, err := model.NewJobID("job-" + name)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-"+name, "owner-"+name)
	if err != nil {
		t.Fatal(err)
	}
	attemptID, err := model.NewAttemptID("attempt-" + name)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	binding := model.Binding{
		RequestKey:   key,
		JobID:        jobID,
		TaskIdentity: identity,
		Mode:         model.ModeIdentifiedFenced,
	}
	if err := binding.Matches(record); err != nil {
		t.Fatal(err)
	}
	projection, err := model.Project(record, model.ProjectionMetadata{SessionID: "session-" + name})
	if err != nil {
		t.Fatal(err)
	}
	return bboltFixture{
		RequestKey: key,
		JobID:      jobID,
		Identity:   identity,
		Boot:       boot,
		Binding:    binding,
		Record:     record,
		Projection: projection,
	}
}

func acceptBboltFixture(t *testing.T, repo *Repository, fixture bboltFixture) {
	t.Helper()
	if _, err := repo.Update(context.Background(), func(tx repository.WriteTx) error {
		if err := tx.PutBinding(fixture.Binding); err != nil {
			return err
		}
		if err := tx.PutSafety(fixture.Record, 0); err != nil {
			return err
		}
		return tx.PutProjection(fixture.Projection)
	}); err != nil {
		t.Fatal(err)
	}
}

func mustRequestKeyForTest(t testing.TB, workspace, request string) model.RequestKey {
	t.Helper()
	key, err := model.NewRequestKey(workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func readFileBytesForTest(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readRawBucketValueForTest(t testing.TB, repo *Repository, bucketName, key []byte) []byte {
	t.Helper()
	var out []byte
	if err := repo.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return fmt.Errorf("bucket %q is missing", string(bucketName))
		}
		raw := bucket.Get(key)
		if raw == nil {
			return fmt.Errorf("key %q is missing", string(key))
		}
		out = append([]byte(nil), raw...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
