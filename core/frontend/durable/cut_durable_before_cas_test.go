package durable_test

import (
	"bytes"
	"context"
	"runtime"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// casObservedRemote counts HEAD reads and CAS attempts; onGetHead runs
// before each HEAD read (n counts from 1), onCAS right after each successful
// HEAD CAS.
type casObservedRemote struct {
	storage.ChunkRemote
	onCAS       func(next storage.ChunkHead)
	onGetHead   func(n int)
	reads, cass int
}

// The layout adapter supplies content keys as well as the observed CAS path.
type casObservedLayoutRemote struct {
	*casObservedRemote
	keyer storage.ChunkKeyer
}

func (r *casObservedLayoutRemote) ChunkKey(index uint64, sum [32]byte) string {
	return r.keyer.ChunkKey(index, sum)
}

func (r *casObservedRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	r.reads++
	if r.onGetHead != nil {
		r.onGetHead(r.reads)
	}
	return r.ChunkRemote.GetHead(ctx)
}

func (r *casObservedRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	r.cass++
	gen, err := r.ChunkRemote.CASHead(ctx, expected, next)
	if err == nil && r.onCAS != nil {
		r.onCAS(next)
	}
	return gen, err
}

// The durable-reopen rule rests on this: a cut publishes only an LSN its root has
// already made durable (the barrier sync before the HEAD CAS), so a crash
// right after the CAS leaves a root at or above HEAD's remote_lsn. The
// root's crash image taken at the CAS reattaches under that HEAD and serves
// the cut's data.
func TestCutPublishesOnlyLocallyDurableLSN(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the open store's byte-range lock keeps its files unreadable for the crash image")
	}
	ctx := context.Background()
	fixture := newLayoutCase(t, pcBlocks, pcChunkBlocks)
	root, crash := t.TempDir(), t.TempDir()
	var ws *storage.WALStore
	var images int
	remote := &casObservedRemote{}
	remote.onCAS = func(next storage.ChunkHead) {
		if ws == nil || next.RemoteLSN == 0 {
			return
		}
		if durable, _, _ := ws.Boundaries(); durable < next.RemoteLSN {
			t.Errorf("HEAD CAS published remote_lsn %d above the root's durable LSN %d", next.RemoteLSN, durable)
		}
		copyRootFiles(t, root, crash) // the root as a crash right after this CAS leaves it
		images++
	}
	a := fixture.provider(t, root, v1View(), "s/r1", func(cfg *durable.ProviderConfig) {
		cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			inner, err := chunkstore.NewLayoutRemote(fixture.cat, fixture.ref, me)
			if err != nil {
				return nil, err
			}
			remote.ChunkRemote = inner
			return &casObservedLayoutRemote{casObservedRemote: remote, keyer: inner}, nil
		}
	})
	s := openRecovered(t, a)
	ws = s.(*storage.WALStore)
	for lba := uint32(0); lba < 3; lba++ {
		if _, err := s.Write(lba, block(byte(lba+1))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if images == 0 {
		t.Fatal("no cut published a HEAD")
	}
	head := fixture.head(t)

	b := fixture.provider(t, crash, v1View(), "s/r1", nil)
	got := openRecovered(t, b) // the refusal must not fire on the crash image
	for lba := uint32(0); lba < 3; lba++ {
		buf, err := got.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, block(byte(lba+1))) {
			t.Fatalf("crash image under HEAD position %d lost block %d", head.Position, lba)
		}
	}
}
