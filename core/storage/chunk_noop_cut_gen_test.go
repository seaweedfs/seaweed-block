package storage

import "testing"

// A no-op cut fences only when HEAD's identity or image differs from the one
// this holder published, as the CAS path does. HEAD's generation alone may
// move without either (a settlement that aborts the holder's session
// rewrites HEAD as it was): the holder stays healthy and its no-op cut
// completes on the newer generation.
func TestNoopCutDoesNotFenceOnAGenerationOnlyMove(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 0)
	w.write(t, s, 1, 0x41)
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	h, err := f.GetHead(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	// The same HEAD written again: a new generation, same identity and image.
	if _, err := f.CASHead(ctTimeout(t), ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, copyHead(h)); err != nil {
		t.Fatal(err)
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil || l.Fenced() {
		t.Fatalf("a no-op cut after a generation-only move: %v (fenced %v); want it to complete", err, l.Fenced())
	}
	if !r.Noop {
		t.Fatalf("cut %+v; want a no-op", r)
	}
	// A different image (or identity) still fences, as before.
	h2, _ := f.GetHead(ctTimeout(t))
	moved := copyHead(h2)
	moved.Holder = "another-host"
	if _, err := f.CASHead(ctTimeout(t), ChunkHeadVersion{Gen: h2.Gen, Epoch: h2.Epoch}, moved); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Cut(ctTimeout(t)); err == nil || !l.Fenced() {
		t.Fatalf("a no-op cut over another holder's HEAD: %v (fenced %v); want fenced", err, l.Fenced())
	}
}
