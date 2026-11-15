package durable_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
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

func TestRestoreCheckpointBeforeAssignmentPreservesRemoteBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	p := chunkProvider(t, t.TempDir(), remote, view, 0)
	s := openRecovered(t, p)
	ws := s.(*storage.WALStore)
	want := block(0xD7)
	lsn, err := s.Write(33, want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	// FlushCount increments after checkpoint persistence AND dirty-map removal.
	// The authority stays absent until that actual cycle has fully completed.
	awaitRestoreCondition(t, ctx, "checkpoint before attach", func() bool {
		return ws.CheckpointLSN() >= lsn && ws.FlushCount() > 0
	})
	if h, _ := remote.GetHead(ctx); h.Gen != 0 {
		t.Fatal("unexpected HEAD before assignment", h)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}))
	awaitRestoreCondition(t, ctx, "deferred attachment", func() bool {
		st := p.DurableStatuses()
		return len(st) == 1 && st[0].Operational
	})
	if h, err := p.CaptureVolume(ctx, "v1"); err != nil || h != lsn {
		t.Fatal("cut", h, err)
	}
	fresh := chunkProvider(t, t.TempDir(), remote, v1View(), 0)
	got, err := openRecovered(t, fresh).Read(33)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("remote restore lost checkpointed bytes: got=%x err=%v", sha256.Sum256(got), err)
	}
}

type publishAfterAbsentRead struct {
	storage.ChunkRemote
	once         sync.Once
	beforeReturn func()
}

func (r *publishAfterAbsentRead) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	h, err := r.ChunkRemote.GetHead(ctx)
	if err == nil {
		r.once.Do(r.beforeReturn)
	}
	return h, err
}

func TestRestoreHEADAppearsBetweenClaimAndAttach(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	data := bytes.Repeat([]byte{0xA7}, pcChunkBlocks*4096)
	wrapped := &publishAfterAbsentRead{ChunkRemote: remote, beforeReturn: func() {
		if err := remote.Put(ctx, "old-cut", data); err != nil {
			t.Fatal(err)
		}
		if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "old", RemoteLSN: 7,
			Chunks: map[uint64]storage.ChunkRef{0: {Key: "old-cut", SHA256: sha256.Sum256(data)}}}); err != nil {
			t.Fatal(err)
		}
	}}
	p := chunkProvider(t, t.TempDir(), wrapped, v1View(), 0)
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.RecoverVolume(ctx, "v1")
	if err != nil {
		backend, openErr := p.Open(ctx, "v1")
		if openErr != nil {
			t.Fatal(openErr)
		}
		if _, readErr := backend.Read(ctx, 0, make([]byte, 4096)); !errors.Is(readErr, frontend.ErrNotReady) {
			t.Fatal("failed attach admitted I/O", readErr)
		}
		if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
			t.Fatal("retry must claim and seed", err)
		}
	}
	h, err := remote.GetHead(ctx)
	if err != nil || h.Epoch != 3 || h.Holder != "r1" {
		t.Fatalf("ready without ownership: HEAD=%+v err=%v", h, err)
	}
	if r, _, h := s.(*storage.WALStore).Boundaries(); r != 7 || h != 7 {
		t.Fatalf("ready without seed: R=%d H=%d", r, h)
	}
	if got, err := s.Read(0); err != nil || !bytes.Equal(got, data[:4096]) {
		t.Fatal("exact lazy restore changed", err)
	}
	if cut, err := p.CaptureVolume(ctx, "v1"); err != nil || cut != 7 {
		t.Fatal("capture after settled restore", cut, err)
	}
}
