package authority

import (
	"context"
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestAllocateGrantCommitsRandomAuthorityNonceBeforeReturn(t *testing.T) {
	ctx := context.Background()
	ready := newReady(t, memory.NewRepository(), "allocate-grant")

	accepted, err := ready.Accept(ctx, acceptRequest(t, "allocate-grant"))
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	group := groupRef(ref, model.LaunchOrdinalOne)
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}

	grant, durability, err := ready.AllocateGrant(ctx, ref, model.LaunchOrdinalOne)
	if err != nil {
		t.Fatal(err)
	}
	if durability != CommittedAndAnchored {
		t.Fatalf("durability = %v, want CommittedAndAnchored", durability)
	}
	if grant.Nonce == "" {
		t.Fatal("grant nonce is empty")
	}
	if grant.Nonce == model.LaunchNonce("permit-"+ref.JobID.String()+"-"+model.LaunchOrdinalOne.String()) {
		t.Fatalf("grant nonce = %q, want non-deterministic authority nonce", grant.Nonce)
	}

	image, err := ready.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	launch, ok := image.Safety.Value.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || launch.Grant == nil {
		t.Fatal("stored launch grant missing after AllocateGrant returned")
	}
	if *launch.Grant != grant {
		t.Fatalf("stored grant = %#v, want %#v", *launch.Grant, grant)
	}
}

func TestAllocateGrantProducesUniqueNonces(t *testing.T) {
	ctx := context.Background()
	ready := newReady(t, memory.NewRepository(), "allocate-unique")
	seen := map[model.LaunchNonce]struct{}{}

	for i := 0; i < 24; i++ {
		name := "allocate-unique-" + string(rune('a'+i))
		accepted, err := ready.Accept(ctx, acceptRequest(t, name))
		if err != nil {
			t.Fatal(err)
		}
		ref := accepted.Record.Attempt.Ref
		group := groupRef(ref, model.LaunchOrdinalOne)
		if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
			t.Fatal(err)
		}
		grant, durability, err := ready.AllocateGrant(ctx, ref, model.LaunchOrdinalOne)
		if err != nil {
			t.Fatal(err)
		}
		if durability != CommittedAndAnchored {
			t.Fatalf("durability = %v, want CommittedAndAnchored", durability)
		}
		if _, ok := seen[grant.Nonce]; ok {
			t.Fatalf("duplicate grant nonce %q", grant.Nonce)
		}
		seen[grant.Nonce] = struct{}{}
	}
}

func TestAllocateGrantReturnsUnknownDurabilityOnAnchorFailure(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchor := &advanceFailAnchor{FakeAnchor: NewFakeAnchor()}
	bootstrapper, err := NewBootstrapper(repo, WithAnchor(anchor))
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-allocate-unknown", "owner-allocate-unknown")
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ready.Accept(ctx, acceptRequest(t, "allocate-unknown"))
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	group := groupRef(ref, model.LaunchOrdinalOne)
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}

	anchor.failAdvance = true
	grant, durability, err := ready.AllocateGrant(ctx, ref, model.LaunchOrdinalOne)
	if err == nil {
		t.Fatal("AllocateGrant error = nil, want anchor failure")
	}
	if durability != CommitOutcomeUnknown {
		t.Fatalf("durability = %v, want CommitOutcomeUnknown", durability)
	}
	if grant.Nonce == "" {
		t.Fatal("grant nonce is empty")
	}
	if _, err := ready.Accept(ctx, acceptRequest(t, "allocate-after-fail-stop")); !errors.Is(err, ErrFailStopped) {
		t.Fatalf("accept after ambiguous grant error = %v, want ErrFailStopped", err)
	}
}

type advanceFailAnchor struct {
	*FakeAnchor
	failAdvance bool
}

func (a *advanceFailAnchor) Advance(ctx context.Context, boot model.BootRef, generation uint64) error {
	if a.failAdvance {
		return errors.New("advance failed")
	}
	return a.FakeAnchor.Advance(ctx, boot, generation)
}
