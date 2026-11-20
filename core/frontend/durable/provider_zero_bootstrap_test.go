package durable_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"sync"
	"testing"
	"time"
)

type bootstrapBlobs struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (b *bootstrapBlobs) Put(_ context.Context, data []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := fmt.Sprintf("fid-%d", len(b.objects)+1)
	b.objects[id] = bytes.Clone(data)
	return id, nil
}
func (b *bootstrapBlobs) Get(_ context.Context, id string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[id]
	if !ok {
		return nil, fmt.Errorf("missing blob %s", id)
	}
	return bytes.Clone(data), nil
}

func TestRestoreEnsureVolumeZeroHEADPreservesCheckpointedBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	filer := chunkstore.NewMemFiler()
	catalog, err := chunkstore.NewCatalog(filer, "/bootstrap")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := chunkstore.NewChunkRemote(filer, &bootstrapBlobs{objects: map[string][]byte{}}, "/bootstrap/v1")
	if err != nil {
		t.Fatal(err)
	}
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"})
	makeProvider := func(view *stubView) *durable.DurableProvider {
		p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks, ChunkHolder: "node-a/r1",
			ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
			ChunkAttachCheck: func(ctx context.Context, vol string, g durable.ChunkGeometry) error {
				_, err := catalog.EnsureVolume(ctx, vol, chunkstore.Geometry{NumBlocks: g.NumBlocks, BlockSize: g.BlockSize, ChunkBlocks: g.ChunkBlocks})
				return err
			},
		}, view)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	p := makeProvider(view)
	s := openRecovered(t, p)
	ws := s.(*storage.WALStore)
	want := block(0xD7)
	lsn, err := s.Write(33, want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	awaitRestoreCondition(t, ctx, "checkpointed local bootstrap", func() bool { return ws.CheckpointLSN() >= lsn && ws.FlushCount() > 0 })
	// This production attach hook, rather than a manufactured fixture HEAD,
	// creates the initial ZERO image after the write has left the dirty map.
	_, err = catalog.EnsureVolume(ctx, "v1", chunkstore.Geometry{NumBlocks: pcBlocks, BlockSize: 4096, ChunkBlocks: pcChunkBlocks})
	if err != nil {
		t.Fatal(err)
	}
	h, err := remote.GetHead(ctx)
	if err != nil || h.Gen == 0 || h.RemoteLSN != 0 || len(h.Chunks) != 0 {
		t.Fatalf("initial ZERO HEAD: %+v %v", h, err)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}))
	// The bootstrap's dirty set is unknown: it is reconciled against the
	// ZERO HEAD before cuts resume.
	awaitRestoreCondition(t, ctx, "ZERO HEAD bootstrap attachment and reconciliation", func() bool {
		st := p.DurableStatuses()
		return len(st) == 1 && st[0].Operational && st[0].Chunk != nil && st[0].Chunk.Attached && !st[0].Chunk.Stats.Reconciling
	})
	if got, err := p.CaptureVolume(ctx, "v1"); err != nil || got != lsn {
		t.Fatalf("capture=%d want=%d err=%v", got, lsn, err)
	}
	fresh := makeProvider(v1View())
	got, err := openRecovered(t, fresh).Read(33)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("checkpointed local bytes lost on remote restore: exact=%v err=%v", bytes.Equal(got, want), err)
	}
}
