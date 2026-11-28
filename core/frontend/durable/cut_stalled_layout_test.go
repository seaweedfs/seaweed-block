package durable_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type stalledLayoutUpload struct {
	*chunkstore.LayoutRemote
	puts atomic.Int32
}

func (r *stalledLayoutUpload) Put(context.Context, string, []byte) error {
	r.puts.Add(1)
	return context.DeadlineExceeded
}

// Three actual failed cut attempts must reach guarded discard, the provider's
// maintenance kick, and the real kernel's conditional removal in one run.
func TestStalledBackgroundCutsCleanRealLayout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
		ready := make(chan struct{})
		cleaned := make(chan int, 1)
		var remote *stalledLayoutUpload
		var p *durable.DurableProvider
		cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: pcBlocks, BlockSize: 4096, ChunkBlocks: pcChunkBlocks, ChunkCOWSpillLimit: 2 * pcChunkBlocks * 4096,
			ChunkSnapshotPeriod: time.Second, ChunkCutTimeout: 100 * time.Millisecond, ChunkMaintenanceInterval: 24 * time.Hour}
		fixture.Configure(&cfg)
		original := cfg.ChunkLayoutRemoteFor
		cfg.ChunkLayoutRemoteFor = func(v string, me layout.HolderID) (storage.ChunkRemote, error) {
			r, err := original(v, me)
			if err != nil {
				return nil, err
			}
			remote = &stalledLayoutUpload{LayoutRemote: r.(*chunkstore.LayoutRemote)}
			return remote, nil
		}
		cfg.ChunkScheduledSnapshot = func(ctx context.Context, _ string, _ time.Time, cut func(context.Context) (storage.ChunkHead, error)) error {
			select {
			case <-ready:
			case <-ctx.Done():
				return ctx.Err()
			}
			_, err := cut(ctx)
			return err
		}
		cfg.ChunkMaintenance = func(ctx context.Context, _ string) error {
			if remote.puts.Load() < 3 || fcutStats(t, p).CutsFailed != 3 {
				t.Error("cleaning ran before K actual failed store attempts")
			}
			return remote.Clean(ctx, func(ctx context.Context, me layout.HolderID, session string) error {
				st, err := fixture.Catalog.Store(fixture.Ref)
				if err != nil {
					return err
				}
				reader, err := layout.NewReader("/provider-test", fixture.Ref.Dir(), st.Fetch)
				if err != nil {
					return err
				}
				result, err := layout.RemoveUnreachable(ctx, st, reader, st, me, session, layout.Shard(15), []layout.ChunkName{layout.NameChunk(15, block(99))})
				select {
				case cleaned <- len(result):
				default:
				}
				return err
			})
		}
		p, err := durable.NewDurableProvider(cfg, v1View())
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		st := fcutOpen(t, context.Background(), p)
		for i := 0; i < 4; i++ {
			if _, err := st.Write(uint32(i*pcChunkBlocks), block(byte(i+1))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.Sync(); err != nil {
			t.Fatal(err)
		}
		data := block(99)
		if err := remote.LayoutRemote.Put(context.Background(), remote.ChunkKey(15, layout.Sum(data)), data); err != nil {
			t.Fatal(err)
		}
		close(ready)
		time.Sleep(4 * time.Minute)
		synctest.Wait()
		select {
		case n := <-cleaned:
			if n != 1 {
				t.Fatalf("cleaning removed=%d, want the orphan", n)
			}
		default:
			t.Fatal("K failed attempts never kicked real cleaning")
		}
	})
}
