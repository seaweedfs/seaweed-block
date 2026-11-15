package storage

import (
	"errors"
	"testing"
)

// Detach publishes its final cut with State = detached and DetachedBy =
// {holder, epoch, the generation that CAS installs} in that one CAS (HEAD
// schema ruling, 2026-11-15). Afterwards the layer refuses every cut; a
// repeated Detach returns the first result.
func TestChunkDetach_OneCASPublishesDetachedFinalCut(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 3, 0x33)
	w.write(t, s, 17, 0x17)

	r, err := layer.Detach(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	heads := f.installedHeads()
	if len(heads) != 1 {
		t.Fatalf("installed %d HEADs, want the one detach cut", len(heads))
	}
	h := heads[0]
	if h.State != chunkHeadDetached || h.DetachedBy == nil || *h.DetachedBy != (chunkDetachedBy{Holder: "host-a", Epoch: 1, Gen: h.Gen}) {
		t.Fatalf("detach cut HEAD state %q detachedBy %+v, want detached by {host-a 1 %d}", h.State, h.DetachedBy, h.Gen)
	}
	if h.RemoteLSN != s.NextLSN()-1 || !sameHead(r.Head, h) || r.Head.State != chunkHeadDetached || r.H != h.RemoteLSN {
		t.Fatalf("Detach returned %+v H %d, the remote holds %+v", r.Head, r.H, h)
	}
	assertEveryHeadIsPrefixImage(t, f, w)

	casBefore := f.stats().cas
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("Cut after Detach: %v, want errChunkLayerDetached", err)
	}
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("Capture after Detach: %v, want errChunkLayerDetached", err)
	}
	if err := layer.MakeRemoteDurable(ctTimeout(t), 1); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("MakeRemoteDurable after Detach: %v, want errChunkLayerDetached", err)
	}
	again, err := layer.Detach(ctTimeout(t))
	if err != nil || !sameHead(again.Head, h) || again.H != r.H || again.Head.State != chunkHeadDetached {
		t.Fatalf("repeated Detach: %+v %v, want the first result", again, err)
	}
	if n := f.stats().cas; n != casBefore {
		t.Fatalf("a detached layer issued %d more CAS calls", n-casBefore)
	}
}

// With nothing new since the last HEAD, Detach still publishes one CAS.
func TestChunkDetach_NothingNewStillPublishes(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	newWriteLog().write(t, s, 2, 0x22)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	before := f.stats().head
	if before.State != chunkHeadActive || before.DetachedBy != nil {
		t.Fatalf("a normal cut published state %q detachedBy %+v", before.State, before.DetachedBy)
	}
	if _, err := layer.Detach(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	after := f.stats().head
	if after.State != chunkHeadDetached || after.Gen != before.Gen+1 || after.DetachedBy == nil || after.DetachedBy.Gen != after.Gen ||
		!sameChunks(after, before) || after.RemoteLSN != before.RemoteLSN {
		t.Fatalf("detach with nothing new: before %+v after %+v", before, after)
	}
}

// A detach CAS whose response is lost is resolved by re-reading HEAD.
func TestChunkDetach_LostResponseResolvedByReread(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	newWriteLog().write(t, s, 2, 0x22)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casAppliedLost}
	f.mu.Unlock()
	r, err := layer.Detach(ctTimeout(t))
	if err != nil {
		t.Fatalf("detach whose CAS applied but lost its response: %v", err)
	}
	if h := f.stats().head; h.State != chunkHeadDetached || !sameHead(r.Head, h) {
		t.Fatalf("Detach returned %+v, remote %+v", r.Head, h)
	}
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("Cut after the resolved detach: %v", err)
	}
}

// A new holder's normal cut publishes State = active and clears DetachedBy.
func TestChunkCut_NewHolderClearsDetachedState(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	copy(src, makeBlock(ctBlockSize, 0x44))
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	f.mu.Lock()
	h := copyChunkHead(f.head)
	h.State = chunkHeadDetached
	h.DetachedBy = &chunkDetachedBy{Holder: h.Holder, Epoch: h.Epoch, Gen: h.Gen + 1}
	f.installLocked(h)
	f.mu.Unlock()

	s, _ := newChunkTestStore(t)
	s.AdvanceFrontier(10) // the log continues from HEAD's remote_lsn (W4b's job in production)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, f, cfg)
	r, err := layer.Cut(ctTimeout(t)) // nothing new locally: still a CAS, the base is detached
	if err != nil {
		t.Fatal(err)
	}
	if got := f.stats().head; r.Noop || got.State != chunkHeadActive || got.DetachedBy != nil || got.Holder != "host-a" {
		t.Fatalf("the new holder's cut left state %q detachedBy %+v holder %q (noop %v)", got.State, got.DetachedBy, got.Holder, r.Noop)
	}
}

func sameChunks(a, b chunkHead) bool {
	if len(a.Chunks) != len(b.Chunks) {
		return false
	}
	for k, v := range a.Chunks {
		if b.Chunks[k] != v {
			return false
		}
	}
	return true
}
