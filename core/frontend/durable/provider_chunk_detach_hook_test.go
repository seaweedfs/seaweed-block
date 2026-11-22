package durable_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// detachHooks records the detach hooks: each detach gets the next token.
type detachHooks struct {
	mu      sync.Mutex
	next    uint64
	started []string
	resumed []string
}

func (k *detachHooks) detach(id string) uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.next++
	k.started = append(k.started, id)
	return k.next
}

func (k *detachHooks) resume(id string, attempt uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.resumed = append(k.resumed, fmt.Sprintf("%s#%d", id, attempt))
}

func (k *detachHooks) calls() (started, resumed []string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.started...), append([]string(nil), k.resumed...)
}

// hookedProvider is detachFixture's volume (one cut published, one dirty
// block) with both detach hooks wired.
func hookedProvider(t *testing.T) (*faultRemote, *durable.DurableProvider, frontend.Backend, *detachHooks) {
	t.Helper()
	remote := &faultRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	k := &detachHooks{}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, ChunkHolder: "r1",
		StorageRoot:          t.TempDir(),
		BlockSize:            4096,
		NumBlocks:            pcBlocks,
		ChunkBlocks:          pcChunkBlocks,
		ChunkRemoteFor:       func(string) (storage.ChunkRemote, error) { return remote, nil },
		OnChunkDetach:        k.detach,
		OnChunkDetachResumed: k.resume,
	}, v1View())
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	openRecovered(t, p)
	ctx := context.Background()
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(ctx, 4096, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(ctx, 8192, block(2)); err != nil {
		t.Fatal(err)
	}
	return remote, p, b, k
}

// A detach (graceful stop) tells the host once, with the volume id; the
// lease-fence drain, a StopIO of its own, does not.
func TestWALStoreChunk_DetachCallsStopHook(t *testing.T) {
	ctx := context.Background()
	t.Run("detach", func(t *testing.T) {
		_, p, _, k := hookedProvider(t)
		if _, _, err := p.DetachVolume(ctx, "v1"); err != nil {
			t.Fatal(err)
		}
		if started, resumed := k.calls(); len(started) != 1 || started[0] != "v1" || len(resumed) != 0 {
			t.Fatalf("detach hook calls %q, resumed %q; want [v1], none", started, resumed)
		}
	})
	t.Run("fence-stop", func(t *testing.T) {
		_, p, _, k := hookedProvider(t)
		if err := p.Backend("v1").StopIO(ctx, "workspace lease fenced"); err != nil {
			t.Fatal(err)
		}
		if started, resumed := k.calls(); len(started) != 0 || len(resumed) != 0 {
			t.Fatalf("lease-fence StopIO called the detach hooks: %q %q", started, resumed)
		}
	})
}

// The hook runs before the drain starts (and so before the final cut, which
// follows the drain): the host learns of the stop while guest I/O is still
// being drained, not after.
func TestWALStoreChunk_DetachHookPrecedesDrain(t *testing.T) {
	_, p, _, k := hookedProvider(t)
	drains, hookedAtDrain := 0, -1
	durable.SetTestDetachStopIO(p, func(ctx context.Context) error {
		drains++
		started, _ := k.calls()
		hookedAtDrain = len(started)
		return p.Backend("v1").StopIO(ctx, "chunk detach: I/O stopped until a new attach")
	})
	r, _, err := p.DetachVolume(context.Background(), "v1")
	if err != nil || r.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("detach: %+v %v", r, err)
	}
	if drains != 1 {
		t.Fatalf("drain ran %d times, want 1", drains)
	}
	if hookedAtDrain != 1 {
		t.Fatalf("detach hook calls when the drain started: %d, want 1 (the hook precedes the drain)", hookedAtDrain)
	}
}

// The resume hook is called with the attempt's own token exactly when a
// failed detach provably did not happen and serving resumed; a detach that
// applied, is fenced, or whose outcome is unknown never calls it.
func TestWALStoreChunk_DetachResumeHookOnlyOnProvenAbort(t *testing.T) {
	ctx := context.Background()
	t.Run("refused-cas-aborts", func(t *testing.T) {
		remote, p, b, k := hookedProvider(t)
		remote.set(func(r *faultRemote) { r.casMode = "precondition" })
		if _, _, err := p.DetachVolume(ctx, "v1"); !errors.Is(err, durable.ErrDetachAborted) {
			t.Fatalf("detach with a refused CAS: %v; want ErrDetachAborted", err)
		}
		if _, resumed := k.calls(); len(resumed) != 1 || resumed[0] != "v1#1" {
			t.Fatalf("resume hook calls %q, want [v1#1]", resumed)
		}
		assertServing(t, p, b, remote)
	})
	t.Run("failed-drain-aborts-each-attempt", func(t *testing.T) {
		remote, p, b, k := hookedProvider(t)
		durable.SetTestDetachStopIO(p, func(context.Context) error { return context.Canceled })
		for i := 0; i < 2; i++ {
			if _, _, err := p.DetachVolume(ctx, "v1"); !errors.Is(err, durable.ErrDetachAborted) {
				t.Fatalf("detach %d with a failed drain: %v; want ErrDetachAborted", i+1, err)
			}
		}
		if _, resumed := k.calls(); len(resumed) != 2 || resumed[0] != "v1#1" || resumed[1] != "v1#2" {
			t.Fatalf("resume hook calls %q, want [v1#1 v1#2] (each attempt's own token)", resumed)
		}
		durable.SetTestDetachStopIO(p, nil)
		assertServing(t, p, b, remote)
	})
	t.Run("applied-lost-reply", func(t *testing.T) {
		remote, p, _, k := hookedProvider(t)
		remote.set(func(r *faultRemote) { r.casMode = "applied-lost" })
		if r, _, err := p.DetachVolume(ctx, "v1"); err != nil || r.Head.State != storage.ChunkHeadDetached {
			t.Fatalf("detach whose CAS applied: %+v %v; want the detached result", r, err)
		}
		if _, resumed := k.calls(); len(resumed) != 0 {
			t.Fatalf("resume hook called on an applied detach: %q", resumed)
		}
	})
	t.Run("unknown", func(t *testing.T) {
		remote, p, _, k := hookedProvider(t)
		remote.set(func(r *faultRemote) { r.casMode = "lost"; r.getHeadErr = errInjected })
		if _, _, err := p.DetachVolume(ctx, "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
			t.Fatalf("detach with an undecidable outcome: %v; want ErrDetachOutcomeUnknown", err)
		}
		if _, resumed := k.calls(); len(resumed) != 0 {
			t.Fatalf("resume hook called on an unknown detach outcome: %q", resumed)
		}
		remote.set(func(r *faultRemote) { r.getHeadErr = nil })
	})
	t.Run("fenced", func(t *testing.T) {
		remote, p, _, k := hookedProvider(t)
		cur, err := remote.MemChunkRemote.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		newer := cur
		newer.Epoch, newer.Holder = cur.Epoch+1, "another-holder"
		if _, err := remote.MemChunkRemote.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, newer); err != nil {
			t.Fatal(err)
		}
		if _, _, err := p.DetachVolume(ctx, "v1"); !errors.Is(err, storage.ErrChunkLayerFenced) {
			t.Fatalf("detach against a newer epoch: %v; want fenced", err)
		}
		if _, resumed := k.calls(); len(resumed) != 0 {
			t.Fatalf("resume hook called on a fenced detach: %q", resumed)
		}
	})
}
