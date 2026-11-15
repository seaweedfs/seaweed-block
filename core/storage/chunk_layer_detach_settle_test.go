package storage

import (
	"errors"
	"testing"
)

// unresolvedDetach runs a Detach whose detach CAS has mode m while HEAD
// cannot be re-read, so its outcome stays unresolved.
func unresolvedDetach(t *testing.T, m fakeCASMode) (*fakeChunkRemote, chunkLayer) {
	t.Helper()
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	newWriteLog().write(t, s, 2, 0x22)
	f.mu.Lock()
	f.casModes = []fakeCASMode{m}
	f.beforeCAS = func(f *fakeChunkRemote) { f.failGet = errFakeRemoteDown }
	f.mu.Unlock()
	if _, err := layer.Detach(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("Detach with an unresolved CAS: %v, want errChunkCASOutcomeUnknown", err)
	}
	return f, layer
}

func healRemote(f *fakeChunkRemote) {
	f.mu.Lock()
	f.failGet = nil
	f.mu.Unlock()
}

// An applied detach CAS whose outcome was unresolved must not let a later
// cut succeed; once HEAD is readable the layer is detached (review
// of #111, P2).
func TestChunkDetach_UnresolvedAppliedDetachIsSettled(t *testing.T) {
	f, layer := unresolvedDetach(t, casAppliedLost)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("Cut while the detach is unresolved: %v, want errChunkCASOutcomeUnknown", err)
	}
	healRemote(f)
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("Cut after the applied detach settles: %v, want errChunkLayerDetached", err)
	}
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("Capture: %v", err)
	}
	if err := layer.MakeRemoteDurable(ctTimeout(t), 1); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("MakeRemoteDurable: %v", err)
	}
	r, err := layer.Detach(ctTimeout(t))
	if err != nil || r.Head.State != chunkHeadDetached || !sameHead(r.Head, f.stats().head) {
		t.Fatalf("Detach after settlement: %+v %v, remote %+v", r.Head, err, f.stats().head)
	}
}

// Detach itself settles an unresolved detach that had applied.
func TestChunkDetach_RepeatedDetachSettlesAppliedDetach(t *testing.T) {
	f, layer := unresolvedDetach(t, casAppliedLost)
	healRemote(f)
	casBefore := f.stats().cas
	r, err := layer.Detach(ctTimeout(t))
	if err != nil || r.Head.State != chunkHeadDetached || !sameHead(r.Head, f.stats().head) || f.stats().cas != casBefore {
		t.Fatalf("Detach settling an applied detach: %+v %v (CAS %d -> %d)", r.Head, err, casBefore, f.stats().cas)
	}
}

// A detach CAS that did not apply leaves the layer as it was: Detach runs anew.
func TestChunkDetach_UnresolvedNotAppliedDetachRetries(t *testing.T) {
	f, layer := unresolvedDetach(t, casUnknownNotApplied)
	healRemote(f)
	r, err := layer.Detach(ctTimeout(t))
	if err != nil || r.Head.State != chunkHeadDetached || f.stats().head.State != chunkHeadDetached {
		t.Fatalf("Detach after a detach that did not apply: %+v %v", r.Head, err)
	}
}

// A takeover while the detach is unresolved fences the layer.
func TestChunkDetach_UnresolvedDetachThenTakeoverFences(t *testing.T) {
	f, layer := unresolvedDetach(t, casUnknownNotApplied)
	f.mu.Lock()
	f.failGet = nil
	f.takeOverLocked("host-b")
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerFenced) {
		t.Fatalf("Cut after a takeover: %v, want errChunkLayerFenced", err)
	}
}
