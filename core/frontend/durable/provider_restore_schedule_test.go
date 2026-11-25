package durable_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func awaitRestoreCondition(t *testing.T, ctx context.Context, what string, ready func() bool) {
	t.Helper()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", what, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
func TestFreshStoreRefusesWriteBeforeAssignmentThenPublishesAfterClaim(t *testing.T) {
	ctx := hangGuard(t)
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	p := layoutChunkProvider(t, t.TempDir(), fixture, view, 0)
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	want := block(0xD7)
	if _, err := s.Write(33, want); !errors.Is(err, storage.ErrLayoutFresh) {
		t.Fatalf("write before the claim: %v; want ErrLayoutFresh", err)
	}
	if h := layoutHead(t, fixture); h.Epoch != 0 || h.Holder != "" || h.Current != nil {
		t.Fatal("unexpected claimed HEAD before assignment", h)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}))
	awaitRestoreCondition(t, ctx, "deferred attachment", func() bool {
		st := p.DurableStatuses()
		return len(st) == 1 && st[0].Operational && st[0].Chunk != nil && st[0].Chunk.Attached && !st[0].Chunk.Stats.Reconciling
	})
	lsn, err := s.Write(33, want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if h, err := p.CaptureVolume(ctx, "v1"); err != nil || h != lsn {
		t.Fatal("cut", h, err)
	}
	fresh := layoutChunkProvider(t, t.TempDir(), fixture, viewAs("rB", 4), 0)
	got, err := openRecovered(t, fresh).Read(33)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("remote restore lost checkpointed bytes: got=%x err=%v", sha256.Sum256(got), err)
	}
}

