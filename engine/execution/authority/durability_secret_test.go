package authority_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
)

var legacyReleaseCredentialNames = []string{
	"release" + "Secret",
	"Release" + "Secret",
	"Grant" + "Token",
}

func TestDurableAuthorityRecordsDoNotPersistLegacyReleaseCredentialBytes(t *testing.T) {
	assertDurableTypeGraphOmitsLegacyReleaseCredentials(t,
		reflect.TypeOf(repository.AuthorityMeta{}),
		reflect.TypeOf(model.Binding{}),
		reflect.TypeOf(model.SafetyRecord{}),
		reflect.TypeOf(model.JobProjection{}),
		reflect.TypeOf(repository.Tombstone{}),
		reflect.TypeOf(repository.QuarantineRecord{}),
		reflect.TypeOf(authority.AnchorSnapshot{}),
	)
	sentinel := []byte("release-secret-sentinel-abd-r3a2-logical-layer")

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "admission.bbolt")
	repo, err := bboltrepo.NewRepository(path)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	repoClosed := false
	t.Cleanup(func() {
		if !repoClosed {
			if err := repo.Close(); err != nil {
				t.Fatalf("close bbolt repository: %v", err)
			}
		}
	})

	issuer, verifier := custodian.NewAttestationChannel()
	anchorStore := authority.NewAnchorStore()
	bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(verifier))
	if err != nil {
		t.Fatalf("NewBootstrapper() error = %v", err)
	}
	boot, err := model.NewBootRef("boot-live-secret", "owner-live-secret")
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatalf("SealReady() error = %v", err)
	}
	accepted, err := ready.Accept(ctx, authority.AcceptRequest{
		RequestKey: model.RequestKey{
			WorkspaceKey: "workspace-live-secret",
			RequestID:    "request-live-secret",
		},
		TaskIdentity: model.NewSHA256TaskIdentity([]byte("live-secret")),
		Mode:         model.ModeIdentifiedFenced,
		SessionID:    "session-live-secret",
	})
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}

	cust := &releaseSecretRecordingCustodian{issuer: issuer}
	controller, err := launch.New(liveSecretAuthorityPort{ready: ready}, cust)
	if err != nil {
		t.Fatalf("launch.New() error = %v", err)
	}
	process, err := controller.Start(ctx, launch.LaunchRequest{
		Context: launch.LaunchContext{
			JobID:   accepted.Record.JobID,
			Attempt: accepted.Record.Attempt.Ref,
			Ordinal: model.LaunchOrdinalOne,
		},
		Exec: command.ExecSpec{Argv: []string{"/bin/echo", "ok"}, Env: []string{"A=B"}, Dir: "/tmp"},
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if _, err := process.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	result, err := process.Result(ctx)
	if err != nil {
		t.Fatalf("Result() error = %v", err)
	}
	if !result.ReleaseRecorded {
		t.Fatal("release was not durably recorded")
	}

	rawRecord, rawProjection, rawBinding := loadLiveSecretDurableJSON(t, repo, accepted)
	assertBytesOmitLegacyReleaseCredential(t, "serialized safety record", rawRecord, sentinel)
	assertBytesOmitLegacyReleaseCredential(t, "serialized projection", rawProjection, sentinel)
	assertBytesOmitLegacyReleaseCredential(t, "serialized binding", rawBinding, sentinel)
	assertBytesOmitLegacyReleaseCredential(t, "bbolt snapshot", repo.SnapshotBytes(), sentinel)
	assertBytesOmitLegacyReleaseCredential(t, "anchor snapshot", anchorStore.SnapshotBytes(), sentinel)
	if err := repo.Close(); err != nil {
		t.Fatalf("close bbolt repository before raw read: %v", err)
	}
	repoClosed = true
	rawDB, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read bbolt file: %v", err)
	}
	assertBytesOmitLegacyReleaseCredential(t, "raw bbolt file", rawDB, sentinel)
}

type liveSecretAuthorityPort struct {
	ready *authority.Ready
}

func (a liveSecretAuthorityPort) BindGroup(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, group model.GroupRef) (launch.DurabilityOutcome, error) {
	applied, err := a.ready.BindGroup(ctx, jobID, ref, ordinal, group)
	return applied.Durability, err
}

func (a liveSecretAuthorityPort) AllocateGrant(ctx context.Context, ref model.AttemptRef, ordinal model.LaunchOrdinal) (model.LaunchGrant, launch.DurabilityOutcome, error) {
	return a.ready.AllocateGrant(ctx, ref, ordinal)
}

func (a liveSecretAuthorityPort) RecordRelease(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, child model.ChildIdentity, evidence model.Evidence) (launch.DurabilityOutcome, error) {
	applied, err := a.ready.RecordRelease(ctx, jobID, ref, ordinal, child, evidence)
	return applied.Durability, err
}

func (a liveSecretAuthorityPort) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) (launch.DurabilityOutcome, error) {
	applied, err := a.ready.RecordQuiescence(ctx, jobID, ordinal, verified)
	return applied.Durability, err
}

func (a liveSecretAuthorityPort) FailStop(ctx context.Context, reason error) error {
	if reason == nil {
		reason = errors.New("launch fail-stop")
	}
	return a.ready.FailStop(ctx, reason.Error())
}

type releaseSecretRecordingCustodian struct {
	issuer custodian.AttestationIssuer
}

func (c *releaseSecretRecordingCustodian) Prepare(_ context.Context, spec command.ExecSpec, key model.LaunchKey) (launch.PreparedProcess, error) {
	if len(spec.Argv) == 0 || spec.Argv[0] == "" {
		return nil, errors.New("exec argv is required")
	}
	group := liveSecretGroupRef(key)
	return &releaseSecretPrepared{custodian: c, group: group}, nil
}

func (c *releaseSecretRecordingCustodian) ContainAndVerify(_ context.Context, group model.GroupRef, _ custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error) {
	return c.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: group, Method: model.QuiescenceTermKill})
}

type releaseSecretPrepared struct {
	custodian *releaseSecretRecordingCustodian
	group     model.GroupRef
}

func (p *releaseSecretPrepared) Ref() model.GroupRef {
	return p.group
}

func (p *releaseSecretPrepared) Release(context.Context) (launch.RunningProcess, custodian.ReleaseOutcome, error) {
	return &releaseSecretRunning{issuer: p.custodian.issuer, group: p.group}, custodian.ReleaseAccepted, nil
}

func (p *releaseSecretPrepared) AbortAndVerify(context.Context) (custodian.VerifiedQuiescence, error) {
	return p.custodian.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: p.group, Method: model.QuiescenceAlreadyAbsent})
}

type releaseSecretRunning struct {
	issuer custodian.AttestationIssuer
	group  model.GroupRef
}

func (r *releaseSecretRunning) Ref() model.GroupRef {
	return r.group
}

func (r *releaseSecretRunning) Stdin() io.WriteCloser {
	return nopWriteCloser{}
}

func (r *releaseSecretRunning) Stdout() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(nil))
}

func (r *releaseSecretRunning) Stderr() io.ReadCloser {
	return io.NopCloser(bytes.NewReader(nil))
}

func (r *releaseSecretRunning) WaitAndVerify(context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, error) {
	verified, err := r.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: r.group, Method: model.QuiescenceNaturalExit})
	return command.ExitObservation{Exited: true, Code: 0}, verified, err
}

func (r *releaseSecretRunning) ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error) {
	return r.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: r.group, Method: model.QuiescenceTermKill})
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (nopWriteCloser) Close() error {
	return nil
}

func liveSecretGroupRef(key model.LaunchKey) model.GroupRef {
	pgid := 7200 + int(key.Ordinal)
	return model.GroupRef{
		Version:           1,
		CustodyID:         model.CustodyID("custody-live-secret-" + key.Ordinal.String()),
		Launch:            key,
		HostBootID:        "host-live-secret",
		PIDNamespaceState: model.PIDNamespaceNotApplicable,
		PGID:              pgid,
		Leader:            model.ProcessIdentity{PID: pgid, HighResStartToken: "leader-live-secret"},
		Monitor:           model.ProcessIdentity{PID: pgid + 1, HighResStartToken: "monitor-live-secret"},
		RetainedID:        "retained-live-secret",
	}
}

func loadLiveSecretDurableJSON(t *testing.T, repo repository.Repository, accepted authority.AcceptResult) ([]byte, []byte, []byte) {
	t.Helper()
	var record model.SafetyRecord
	var projection model.JobProjection
	var binding model.Binding
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		image := tx.LoadJob(accepted.Record.JobID)
		if image.Safety.State != repository.RecordValid {
			t.Fatalf("safety state = %s, want valid", image.Safety.State)
		}
		if image.Projection.State != repository.RecordValid {
			t.Fatalf("projection state = %s, want valid", image.Projection.State)
		}
		request := tx.LookupRequest(accepted.Record.RequestKey)
		if request.Binding.State != repository.RecordValid {
			t.Fatalf("binding state = %s, want valid", request.Binding.State)
		}
		record = image.Safety.Value
		projection = image.Projection.Value
		binding = request.Binding.Value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rawRecord, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	rawProjection, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	rawBinding, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	return rawRecord, rawProjection, rawBinding
}

func assertBytesOmitLegacyReleaseCredential(t *testing.T, label string, raw []byte, sentinel []byte) {
	t.Helper()
	if bytes.Contains(raw, sentinel) {
		t.Fatalf("%s contains legacy release credential bytes", label)
	}
	for _, field := range legacyReleaseCredentialNames {
		if bytes.Contains(raw, []byte(field)) {
			t.Fatalf("%s contains legacy release credential field name %q", label, field)
		}
	}
}

func assertDurableTypeGraphOmitsLegacyReleaseCredentials(t *testing.T, roots ...reflect.Type) {
	t.Helper()
	seen := map[reflect.Type]bool{}
	for _, root := range roots {
		assertTypeGraphOmitsLegacyReleaseCredentials(t, root, root.String(), seen)
	}
}

func assertTypeGraphOmitsLegacyReleaseCredentials(t *testing.T, typ reflect.Type, path string, seen map[reflect.Type]bool) {
	t.Helper()
	if containsLegacyReleaseCredentialName(typ.String()) {
		t.Fatalf("durable type graph contains legacy release credential type %s at %s", typ, path)
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
		path += ".*"
		if containsLegacyReleaseCredentialName(typ.String()) {
			t.Fatalf("durable type graph contains legacy release credential type %s at %s", typ, path)
		}
	}
	if seen[typ] {
		return
	}
	seen[typ] = true
	switch typ.Kind() {
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if containsLegacyReleaseCredentialName(field.Name) {
				t.Fatalf("durable type graph contains legacy release credential field %s.%s", path, field.Name)
			}
			assertTypeGraphOmitsLegacyReleaseCredentials(t, field.Type, path+"."+field.Name, seen)
		}
	case reflect.Slice, reflect.Array:
		assertTypeGraphOmitsLegacyReleaseCredentials(t, typ.Elem(), path+"[]", seen)
	case reflect.Map:
		assertTypeGraphOmitsLegacyReleaseCredentials(t, typ.Key(), path+"{key}", seen)
		assertTypeGraphOmitsLegacyReleaseCredentials(t, typ.Elem(), path+"{value}", seen)
	}
}

func containsLegacyReleaseCredentialName(value string) bool {
	for _, name := range legacyReleaseCredentialNames {
		if strings.Contains(value, name) {
			return true
		}
	}
	return false
}
