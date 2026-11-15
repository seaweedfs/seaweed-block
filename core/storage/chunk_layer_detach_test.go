package storage

import (
	"errors"
	"testing"
)

// Detach publishes a final cut, then the same image once more with Detached
// set, so the next holder and the block master can tell from HEAD alone
// that this holder stopped after its last cut. Afterwards the layer refuses
// every cut; a repeated Detach returns the first result.
func TestChunkDetach_FinalCutThenDetachedHead(t *testing.T) {
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
	if len(heads) != 2 {
		t.Fatalf("installed %d HEADs, want the final cut and the detached mark", len(heads))
	}
	cut, mark := heads[0], heads[1]
	if cut.Detached || !mark.Detached || mark.Gen != cut.Gen+1 {
		t.Fatalf("final cut gen %d detached=%v, mark gen %d detached=%v", cut.Gen, cut.Detached, mark.Gen, mark.Detached)
	}
	if mark.RemoteLSN != cut.RemoteLSN || mark.RemoteLSN != s.NextLSN()-1 || mark.Holder != cut.Holder || mark.Epoch != cut.Epoch || !sameChunks(mark, cut) {
		t.Fatalf("the detached mark changed the image: %+v vs %+v", mark, cut)
	}
	if !r.Head.Detached || !sameHead(r.Head, mark) || r.H != cut.RemoteLSN {
		t.Fatalf("Detach returned %+v H %d, the remote holds %+v", r.Head, r.H, mark)
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
	if err != nil || !sameHead(again.Head, mark) || again.H != r.H {
		t.Fatalf("repeated Detach: %+v %v, want the first result", again, err)
	}
	if n := f.stats().cas; n != casBefore {
		t.Fatalf("a detached layer issued %d more CAS calls", n-casBefore)
	}
}

// With nothing new since the last HEAD, Detach still marks HEAD detached.
func TestChunkDetach_NothingNewStillMarks(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	newWriteLog().write(t, s, 2, 0x22)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	before := f.stats().head
	r, err := layer.Detach(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	after := f.stats().head
	if !after.Detached || after.Gen != before.Gen+1 || !sameChunks(after, before) || after.RemoteLSN != before.RemoteLSN || !r.Head.Detached {
		t.Fatalf("detach with nothing new: before %+v after %+v", before, after)
	}
}

// A detached-mark CAS whose response is lost is resolved by re-reading HEAD.
func TestChunkDetach_LostResponseResolvedByReread(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	newWriteLog().write(t, s, 2, 0x22)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.casModes = []fakeCASMode{casAppliedLost}
	f.mu.Unlock()
	r, err := layer.Detach(ctTimeout(t))
	if err != nil {
		t.Fatalf("detach whose mark CAS applied but lost its response: %v", err)
	}
	if h := f.stats().head; !h.Detached || !sameHead(r.Head, h) {
		t.Fatalf("Detach returned %+v, remote %+v", r.Head, h)
	}
}

// A normal cut by a new holder clears a predecessor's Detached mark.
func TestChunkCut_ClearsDetachedMark(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	copy(src, makeBlock(ctBlockSize, 0x44))
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	f.mu.Lock()
	h := copyChunkHead(f.head)
	h.Detached = true
	f.installLocked(h)
	f.mu.Unlock()

	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, f, cfg)
	if _, err := s.Write(6, makeBlock(ctBlockSize, 0x66)); err != nil {
		t.Fatal(err)
	}
	r, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.Head.Detached || f.stats().head.Detached {
		t.Fatalf("the new holder's cut kept the predecessor's Detached mark: %+v", f.stats().head)
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
