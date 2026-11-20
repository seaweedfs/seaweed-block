package durable_test

// The restored frontier seed moves the store's next LSN; the installed
// write observer's ship order must follow it inside PrepareRestore, after
// the seed and before the takeover CAS. A refused rebase fails the restore
// with HEAD still owned by its previous holder and I/O closed.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// countingRebaser records every ship-LSN rebase; refuseFrom > 0 refuses
// that call and every later one (1-based).
type countingRebaser struct {
	mu         sync.Mutex
	lsns       []uint64
	refuseFrom int
}

func (o *countingRebaser) Observe(context.Context, uint32, uint64, []byte) error { return nil }
func (o *countingRebaser) Sync(context.Context, uint64) error                    { return nil }
func (o *countingRebaser) RebaseShipLSN(lsn uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lsns = append(o.lsns, lsn)
	if o.refuseFrom > 0 && len(o.lsns) >= o.refuseFrom {
		return errors.New("injected: writes pending")
	}
	return nil
}

func (o *countingRebaser) seen() []uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]uint64(nil), o.lsns...)
}

// restoreWithObserver restores a fresh host from an epoch-1 HEAD at
// remote_lsn 7 with obs installed before recovery. atCAS runs right
// before the takeover CAS. Returns the recovery error.
func restoreWithObserver(t *testing.T, obs *countingRebaser, atCAS func(p *durable.DurableProvider)) (storage.ChunkRemote, *durable.DurableProvider, *atomic.Int32, error) {
	t.Helper()
	ctx := hangGuard(t)
	remote := storage.NewMemChunkRemote()
	if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "old", RemoteLSN: 7}); err != nil {
		t.Fatal(err)
	}
	var p *durable.DurableProvider
	calls := &atomic.Int32{}
	wrapped := &takeoverOrderRemote{ChunkRemote: remote, beforeCAS: func() {
		calls.Add(1)
		atCAS(p)
	}}
	var err error
	p, err = durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
		ChunkRemoteFor:   func(string) (storage.ChunkRemote, error) { return wrapped, nil },
		ChunkAttachCheck: func(context.Context, string, durable.ChunkGeometry) error { return nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Backend("v1").SetWriteObserver(obs); err != nil {
		t.Fatalf("observer install before recovery: %v", err)
	}
	_, err = p.RecoverVolume(ctx, "v1")
	return remote, p, calls, err
}

func TestRestoreShipRebasePrecedesTakeoverCAS(t *testing.T) {
	obs := &countingRebaser{}
	var atCAS []uint64
	var nextAtCAS uint64
	_, _, calls, err := restoreWithObserver(t, obs, func(p *durable.DurableProvider) {
		atCAS = obs.seen()
		nextAtCAS = p.LogicalStorage("v1").NextLSN()
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || nextAtCAS != 8 || len(atCAS) == 0 || atCAS[len(atCAS)-1] != nextAtCAS {
		t.Fatalf("ship order not rebased to the seeded next LSN before the takeover CAS: CAS calls=%d nextLSN=%d rebases seen at CAS=%v; want 1, 8, last rebase 8",
			calls.Load(), nextAtCAS, atCAS)
	}
	t.Logf("rebases before CAS=%v (install, then post-seed); nextLSN at CAS=%d", atCAS, nextAtCAS)
}

func TestRestoreRefusedShipRebaseKeepsHeadAndIOClosed(t *testing.T) {
	obs := &countingRebaser{refuseFrom: 2} // install accepted, post-seed rebase refused
	remote, p, calls, err := restoreWithObserver(t, obs, func(*durable.DurableProvider) {})
	if err == nil {
		t.Fatal("restore succeeded although the observer refused the post-seed ship-LSN rebase")
	}
	ctx := hangGuard(t)
	h, herr := remote.GetHead(ctx)
	if herr != nil || calls.Load() != 0 || h.Epoch != 1 || h.Holder != "old" {
		t.Fatalf("HEAD claimed despite a refused ship rebase: CAS calls=%d head=%+v err=%v; want 0 calls, epoch 1 holder old", calls.Load(), h, herr)
	}
	if _, rerr := p.Backend("v1").Read(ctx, 0, make([]byte, 4096)); !errors.Is(rerr, frontend.ErrNotReady) {
		t.Fatalf("I/O after a refused ship rebase: %v; want ErrNotReady", rerr)
	}
	if got := obs.seen(); len(got) != 2 {
		t.Fatalf("rebases=%v; want install + post-seed", got)
	}
	t.Logf("restore refused: %v", err)
}
