package storage

import (
	"errors"
	"fmt"
	"testing"
)

func sameHead(a, b chunkHead) bool {
	if a.Gen != b.Gen || a.Epoch != b.Epoch || a.Holder != b.Holder || a.RemoteLSN != b.RemoteLSN || len(a.Chunks) != len(b.Chunks) {
		return false
	}
	for k, v := range a.Chunks {
		if b.Chunks[k] != v {
			return false
		}
	}
	return true
}

// Cut returns the HEAD its own CAS installed, so a snapshot can copy exactly
// the image of cut H; a cut with nothing new does no CAS and returns the
// current HEAD.
func TestChunkCut_ReturnsInstalledHead(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	w.write(t, s, 9, 0x29)

	r, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	heads := f.installedHeads()
	if len(heads) != 1 {
		t.Fatalf("installed %d HEADs, want 1", len(heads))
	}
	if !sameHead(r.Head, heads[0]) {
		t.Fatalf("Cut returned HEAD %+v, the remote installed %+v", r.Head, heads[0])
	}
	if r.Noop || r.H != heads[0].RemoteLSN || r.H != s.NextLSN()-1 {
		t.Fatalf("Cut result noop=%v H=%d; installed remote_lsn %d, last LSN %d", r.Noop, r.H, heads[0].RemoteLSN, s.NextLSN()-1)
	}

	casBefore := f.stats().cas
	again, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Noop || f.stats().cas != casBefore {
		t.Fatalf("a cut with nothing new: noop=%v, CAS calls %d -> %d", again.Noop, casBefore, f.stats().cas)
	}
	if !sameHead(again.Head, heads[0]) || again.H != r.H {
		t.Fatalf("no-op cut returned HEAD %+v H %d, want the current HEAD %+v H %d", again.Head, again.H, heads[0], r.H)
	}

	// Capture keeps returning the cut LSN.
	w.write(t, s, 2, 0x23)
	h, err := layer.Capture(ctTimeout(t))
	if err != nil || h != s.NextLSN()-1 {
		t.Fatalf("Capture after Cut: H=%d err=%v, want H=%d", h, err, s.NextLSN()-1)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// When the CAS was applied but its response lost, the layer resolves it by
// re-reading HEAD; Cut must then return the HEAD actually installed.
func TestChunkCut_ReturnsInstalledHeadAfterLostResponse(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	f.casModes = []fakeCASMode{casAppliedLost}
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 5, 0x55)

	r, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatalf("applied CAS with a lost response: %v", err)
	}
	cur := f.stats().head
	if !sameHead(r.Head, cur) || r.Head.Gen == 0 {
		t.Fatalf("Cut returned HEAD %+v, the remote holds %+v", r.Head, cur)
	}
}

// Cut reports a refused capture with the owner's typed cause, like Capture
// (TestChunkCapture_RefusalIsTyped).
func TestChunkCut_RefusalIsTyped(t *testing.T) {
	errNotPrimary := errors.New("test: not primary")
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.CaptureAllowed = func() error { return fmt.Errorf("volume v1: %w", errNotPrimary) }
	layer := mustAttach(t, s, f, cfg)
	newWriteLog().write(t, s, 1, 0x11)

	_, err := layer.Cut(ctTimeout(t))
	if !errors.Is(err, errChunkCaptureRefused) || !errors.Is(err, errNotPrimary) {
		t.Fatalf("Cut refusal %v: want both errChunkCaptureRefused and the owner's cause", err)
	}
	if n := f.stats().cas; n != 0 {
		t.Fatalf("a refused cut issued %d CAS calls", n)
	}
}
