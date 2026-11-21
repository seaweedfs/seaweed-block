package durable_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type plainSealReplayRemote struct {
	*storage.MemChunkRemote
	mu                   sync.Mutex
	loseSeal, unreadable bool
	reads, cas           int
	corrupt              func(*storage.ChunkHead)
}

func (r *plainSealReplayRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	r.mu.Lock()
	r.reads++
	unreadable, corrupt := r.unreadable, r.corrupt
	r.mu.Unlock()
	if unreadable {
		return storage.ChunkHead{}, errInjected
	}
	h, err := r.MemChunkRemote.GetHead(ctx)
	if corrupt != nil {
		corrupt(&h)
	}
	return h, err
}

func (r *plainSealReplayRemote) CASHead(ctx context.Context, v storage.ChunkHeadVersion, h storage.ChunkHead) (uint64, error) {
	r.mu.Lock()
	r.cas++
	lose := r.loseSeal && h.State == storage.ChunkHeadDetached
	r.mu.Unlock()
	gen, err := r.MemChunkRemote.CASHead(ctx, v, h)
	if err == nil && lose {
		r.mu.Lock()
		r.unreadable = true
		r.mu.Unlock()
		return 0, errInjected
	}
	return gen, err
}

func TestPlainSealLostAnswerRestartReplaysFilerProof(t *testing.T) {
	ctx := hangGuard(t)
	r := &plainSealReplayRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	root := t.TempDir()
	first := chunkProvider(t, root, r, v1View(), 0)
	s := openRecovered(t, first)
	writeSync(t, s, 1, 0x42)
	r.mu.Lock()
	r.loseSeal = true
	r.mu.Unlock()
	if _, _, err := first.DetachVolumePlain(ctx, "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatal("seal did not lose its committed answer", err)
	}
	sealed := headHolder(t, r.MemChunkRemote)
	if sealed.State != storage.ChunkHeadDetached {
		t.Fatal("seal was not actually committed")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.loseSeal, r.unreadable = false, false
	r.mu.Unlock()
	view := v1View()
	second := chunkProvider(t, root, r, view, 0)
	if _, err := second.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrChunkLayerDetached) {
		t.Fatal("restart must keep the sealed incarnation non-serving", err)
	}
	// Startup reports the sealed recovery failure and remains non-serving;
	// the assigned identity must still be able to answer this read-only retry.
	notServing := view.Projection()
	notServing.Healthy = false
	view.set(notServing)
	r.mu.Lock()
	reads, cas := r.reads, r.cas
	r.mu.Unlock()
	result, last, err := second.DetachVolumePlain(ctx, "v1")
	if err != nil || !reflect.DeepEqual(result.Head, sealed) || result.H != sealed.RemoteLSN || last != sealed.RemoteLSN {
		t.Fatalf("re-sent seal did not return the original filer proof: result=%+v last=%d err=%v", result, last, err)
	}
	r.mu.Lock()
	gotReads, gotCAS := r.reads, r.cas
	r.mu.Unlock()
	if gotReads <= reads || gotCAS != cas || !reflect.DeepEqual(headHolder(t, r.MemChunkRemote), sealed) {
		t.Fatal("replay used no filer read or published a new generation")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*storage.ChunkHead)
	}{
		{"active", func(h *storage.ChunkHead) { h.State = storage.ChunkHeadActive; h.DetachedBy = nil }},
		{"holder", func(h *storage.ChunkHead) { h.Holder = "other" }},
		{"epoch", func(h *storage.ChunkHead) { h.Epoch++ }},
		{"missing-proof", func(h *storage.ChunkHead) { h.DetachedBy = nil }},
		{"proof-generation", func(h *storage.ChunkHead) { h.DetachedBy.Gen++ }},
		{"proof-holder", func(h *storage.ChunkHead) { h.DetachedBy.Holder = "other" }},
		{"proof-epoch", func(h *storage.ChunkHead) { h.DetachedBy.Epoch++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.mu.Lock()
			r.corrupt = tc.mutate
			r.mu.Unlock()
			defer func() { r.mu.Lock(); r.corrupt = nil; r.mu.Unlock() }()
			if _, _, err := second.DetachVolumePlain(ctx, "v1"); err == nil {
				t.Fatal("invalid filer proof returned success")
			}
		})
	}
	initial := view.Projection()
	r.mu.Lock()
	r.corrupt = func(*storage.ChunkHead) { moved := initial; moved.EndpointVersion++; view.set(moved) }
	r.mu.Unlock()
	if _, _, err := second.DetachVolumePlain(ctx, "v1"); !errors.Is(err, durable.ErrChunkEpochStale) {
		t.Fatal("replay ignored authority movement during its read", err)
	}
	view.set(initial)
	r.mu.Lock()
	r.corrupt = nil
	r.unreadable = true
	r.mu.Unlock()
	if _, _, err := second.DetachVolumePlain(ctx, "v1"); err == nil {
		t.Fatal("unreadable filer returned a cached seal")
	}
	r.mu.Lock()
	r.unreadable = false
	r.mu.Unlock()
	ended, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := second.DetachVolumePlain(ended, "v1"); !errors.Is(err, context.Canceled) {
		t.Fatal("replay hid caller cancellation", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := second.DetachVolumePlain(ctx, "v1"); err == nil {
		t.Fatal("closed provider replayed a seal")
	}
}

func TestPlainSealReplayNeverUsesProcessMemory(t *testing.T) {
	ctx := hangGuard(t)
	r := &plainSealReplayRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	p := chunkProvider(t, t.TempDir(), r, v1View(), 0)
	s := openRecovered(t, p)
	writeSync(t, s, 1, 0x53)
	if _, _, err := p.DetachVolumePlain(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.unreadable = true
	r.mu.Unlock()
	if _, _, err := p.DetachVolumePlain(ctx, "v1"); err == nil {
		t.Fatal("re-sent plain seal answered from process memory despite unreadable filer")
	}
}
