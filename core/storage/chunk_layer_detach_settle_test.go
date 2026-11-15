package storage

import (
	"errors"
	"testing"
)

// unresolvedMark runs a Detach whose final cut succeeds and whose mark CAS
// has mode m while HEAD cannot be re-read, so the mark stays unresolved.
func unresolvedMark(t *testing.T, m fakeCASMode) (*WALStore, *fakeChunkRemote, chunkLayer) {
	t.Helper()
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	newWriteLog().write(t, s, 2, 0x22)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casNormal, m}
	f.afterCAS = nil
	f.mu.Unlock()
	// Fail HEAD re-reads only after the final cut's CAS.
	f.mu.Lock()
	f.beforeCAS = func(f *fakeChunkRemote) {
		f.beforeCAS = func(f *fakeChunkRemote) { f.failGet = errFakeRemoteDown }
	}
	f.mu.Unlock()
	if _, err := layer.Detach(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("Detach with an unresolved mark: %v, want errChunkCASOutcomeUnknown", err)
	}
	return s, f, layer
}

// An applied mark whose outcome was unresolved must not let a later cut
// succeed at the pre-mark generation; once HEAD is readable the layer is
// detached (review of #111, P2).
func TestChunkDetach_UnresolvedAppliedMarkIsSettled(t *testing.T) {
	_, f, layer := unresolvedMark(t, casAppliedLost)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("Cut while the mark is unresolved: %v, want errChunkCASOutcomeUnknown", err)
	}
	f.mu.Lock()
	f.failGet = nil
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("Cut after the applied mark settles: %v, want errChunkLayerDetached", err)
	}
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("Capture: %v", err)
	}
	if err := layer.MakeRemoteDurable(ctTimeout(t), 1); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("MakeRemoteDurable: %v", err)
	}
	r, err := layer.Detach(ctTimeout(t))
	if err != nil || !r.Head.Detached || !sameHead(r.Head, f.stats().head) {
		t.Fatalf("Detach after settlement: %+v %v, remote %+v", r.Head, err, f.stats().head)
	}
}

// A mark that did not apply leaves the layer as it was: Detach marks anew.
func TestChunkDetach_UnresolvedNotAppliedMarkRetries(t *testing.T) {
	_, f, layer := unresolvedMark(t, casUnknownNotApplied)
	f.mu.Lock()
	f.failGet = nil
	f.mu.Unlock()
	r, err := layer.Detach(ctTimeout(t))
	if err != nil || !r.Head.Detached || !f.stats().head.Detached {
		t.Fatalf("Detach after a mark that did not apply: %+v %v", r.Head, err)
	}
}

// A takeover while the mark is unresolved fences the layer.
func TestChunkDetach_UnresolvedMarkThenTakeoverFences(t *testing.T) {
	_, f, layer := unresolvedMark(t, casUnknownNotApplied)
	f.mu.Lock()
	f.failGet = nil
	f.takeOverLocked("host-b")
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("Cut after a takeover: %v, want errChunkLayerFenced", err)
	}
}
