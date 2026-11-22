package durable_test

import (
	"bytes"
	"context"
	"runtime"
	"testing"

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

// V1-SPEC r2.45 (2) rests on this: a cut publishes only an LSN its root has
// already made durable (the barrier sync before the HEAD CAS), so a crash
// right after the CAS leaves a root at or above HEAD's remote_lsn. The
// root's crash image taken at the CAS reattaches under that HEAD and serves
// the cut's data.
func TestCutPublishesOnlyLocallyDurableLSN(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the open store's byte-range lock keeps its files unreadable for the crash image")
	}
	ctx := context.Background()
	inner := storage.NewMemChunkRemote()
	root, crash := t.TempDir(), t.TempDir()
	var ws *storage.WALStore
	var images int
	remote := &casObservedRemote{ChunkRemote: inner}
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
	a := chunkProvider(t, root, remote, v1View(), 0)
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
	head, _ := inner.GetHead(ctx)

	b := chunkProvider(t, crash, inner, v1View(), 0)
	got := openRecovered(t, b) // the refusal must not fire on the crash image
	for lba := uint32(0); lba < 3; lba++ {
		buf, err := got.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, block(byte(lba+1))) {
			t.Fatalf("crash image under HEAD remote_lsn %d lost block %d", head.RemoteLSN, lba)
		}
	}
}
