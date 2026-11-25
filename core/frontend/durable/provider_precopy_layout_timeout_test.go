package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The detached commit applies, but its caller sees only the freeze deadline.
// Settlement must read the committed HEAD before any guest write can resume.
type appliedDetachWithoutReply struct {
	*chunkstore.LayoutRemote
	applied   bool
	appliedAt time.Time
	cutCause  error
}

func (r *appliedDetachWithoutReply) CASHead(ctx context.Context, want storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	gen, err := r.LayoutRemote.CASHead(ctx, want, next)
	if err != nil || next.State != storage.ChunkHeadDetached {
		return gen, err
	}
	r.applied = true
	r.appliedAt = time.Now()
	<-ctx.Done()
	r.cutCause = context.Cause(ctx)
	return 0, ctx.Err()
}

func TestPrecopyFreezeDeadlineSettlesAppliedDetachBeforeResume(t *testing.T) {
	ctx := context.Background()
	filer := chunkstore.NewMemFiler()
	cat, err := chunkstore.NewBCatalog(filer, &layoutProviderBlobs{blobs: map[string][]byte{}}, "/precopy-timeout")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	var remote *appliedDetachWithoutReply
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
		ChunkHolder: "s/r1", ChunkDetachPrecopy: true, DetachFreezeTimeout: 300 * time.Millisecond,
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return cat.Store(ref) },
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			inner, err := chunkstore.NewLayoutRemote(cat, ref, me)
			if err == nil {
				remote = &appliedDetachWithoutReply{LayoutRemote: inner}
			}
			return remote, err
		},
	}, newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(ctx, 4096, block(0x8a)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatal(err)
	}
	result, _, err := p.DetachVolume(ctx, "v1")
	if !remote.applied || err != nil || result.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("deadline after applied commit: applied=%v result=%+v err=%v; want settled detached", remote.applied, result, err)
	}
	if !errors.Is(remote.cutCause, context.DeadlineExceeded) || time.Since(remote.appliedAt) > time.Second {
		t.Fatalf("held commit ended with %v after %s; want the freeze deadline and bounded settlement", remote.cutCause, time.Since(remote.appliedAt))
	}
	if _, err := b.Write(ctx, 8192, block(0x9a)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write resumed after the commit applied: %v", err)
	}
	head, err := remote.ReadHead(ctx)
	if err != nil || head.State != layout.StateDetached {
		t.Fatalf("HEAD after settlement: %+v %v", head, err)
	}
}
