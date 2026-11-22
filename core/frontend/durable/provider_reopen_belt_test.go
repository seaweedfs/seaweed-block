package durable_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type reopenClaimRaceRemote struct {
	storage.ChunkRemote
	foreign storage.ChunkHead
	reads   int
}

func (r *reopenClaimRaceRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	h, err := r.ChunkRemote.GetHead(ctx)
	if err != nil {
		return h, err
	}
	r.reads++
	if r.reads == 1 {
		// The early local-root check sees our publication. Before the claim's
		// independent read, another holder publishes below the current epoch.
		if _, err := r.ChunkRemote.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, r.foreign); err != nil {
			return storage.ChunkHead{}, err
		}
	}
	return h, nil
}

func TestLocalReopenRejectsForeignClaimImage(t *testing.T) {
	ctx := hangGuard(t)
	r := storage.NewMemChunkRemote()
	root := t.TempDir()
	first := chunkProvider(t, root, r, viewAs("rA", 3), 0)
	s := openRecovered(t, first)
	writeSync(t, s, 0, 0x31)
	if _, err := first.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	writeSync(t, s, 1, 0x42)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	foreign := headHolder(t, r)
	foreign.Epoch, foreign.Holder = 4, "rB"
	race := &reopenClaimRaceRemote{ChunkRemote: r, foreign: foreign}
	second := chunkProvider(t, root, race, viewAs("rA", 5), 0)
	if _, err := second.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	_, err := second.RecoverVolume(ctx, "v1")
	if !errors.Is(err, durable.ErrChunkStaleLocalRoot) || !errors.Is(err, storage.ErrChunkLayerFenced) {
		t.Errorf("foreign claim image admitted over local WAL: %v", err)
	}
	if race.reads < 2 {
		t.Fatal("control did not reach the independent claim observation")
	}
	if after := headHolder(t, r); after.Epoch != 4 || after.Holder != "rB" || after.Gen != foreign.Gen+1 {
		t.Errorf("refused reopen changed the foreign HEAD: %+v", after)
	}
	be, err := second.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := be.Read(ctx, 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
		t.Errorf("foreign claim image exposed I/O: %v", err)
	}
	if got, err := second.LogicalStorage("v1").Read(1); err != nil || !bytes.Equal(got, block(0x42)) {
		t.Errorf("refusal changed acknowledged local bytes: %v", err)
	}
}

func TestLocalReopenClaimsBeforeServing(t *testing.T) {
	ctx := hangGuard(t)
	r := storage.NewMemChunkRemote()
	root := t.TempDir()
	first := chunkProvider(t, root, r, v1View(), 0)
	s := openRecovered(t, first)
	writeSync(t, s, 0, 0x31)
	if _, err := first.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	before := headHolder(t, r)
	writeSync(t, s, 1, 0x42) // locally acknowledged, not yet in HEAD
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := chunkProvider(t, root, r, v1View(), 0)
	s = openRecovered(t, second)
	after := headHolder(t, r)
	if after.Gen <= before.Gen || after.Epoch != before.Epoch || after.Holder != before.Holder || after.RemoteLSN != before.RemoteLSN {
		t.Errorf("local reopen served without replacing the old CAS guard: before=%+v after=%+v", before, after)
	}
	if got, err := s.Read(1); err != nil || !bytes.Equal(got, block(0x42)) {
		t.Fatal("claim replaced locally acknowledged bytes with HEAD", err)
	}
	if _, err := second.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(headBlock(t, r, 0), block(0x31)) || !bytes.Equal(headBlock(t, r, 1), block(0x42)) {
		t.Fatal("reopen did not publish the complete acknowledged image")
	}
}
