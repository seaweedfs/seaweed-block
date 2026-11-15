package storage

import (
	"errors"
	"fmt"
	"testing"
)

// A detach CAS the server retains after the caller saw an unknown outcome
// must never let a later cut succeed at the old generation, and must not be
// able to land after the layer has moved on (second review of #111, P2).
func TestChunkDetach_DeferredDetachSettledBeforeNextCut(t *testing.T) {
	for _, rereadFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("rereadFails=%v", rereadFails), func(t *testing.T) { deferredDetachSchedule(t, rereadFails) })
	}
}

func deferredDetachSchedule(t *testing.T, rereadFails bool) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	base := f.stats().head
	f.mu.Lock()
	f.casModes = []fakeCASMode{casDeferred}
	if rereadFails {
		f.beforeCAS = func(f *fakeChunkRemote) { f.failGet = errFakeRemoteDown }
	}
	f.mu.Unlock()
	if _, err := layer.Detach(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("Detach with a retained CAS: %v, want errChunkCASOutcomeUnknown", err)
	}
	healRemote(f)
	// HEAD still reads as the old active generation; the retained CAS may
	// still land. The next cut must settle it, not take the no-op path.
	if r, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLayerDetached) {
		t.Fatalf("Cut after a retained detach CAS: %+v %v, want errChunkLayerDetached", r.Head, err)
	}
	if n := f.releaseDeferredCAS(); n != 0 {
		t.Fatalf("the retained detach CAS still landed after settlement (%d applied)", n)
	}
	h := f.stats().head
	if h.State != chunkHeadDetached || h.Gen != base.Gen+1 || h.DetachedBy == nil || h.DetachedBy.Gen != h.Gen {
		t.Fatalf("HEAD after settlement %+v (base gen %d)", h, base.Gen)
	}
	r, err := layer.Detach(ctTimeout(t))
	if err != nil || !sameHead(r.Head, h) || r.Head.State != chunkHeadDetached {
		t.Fatalf("Detach after settlement: %+v %v", r.Head, err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// A normal cut's retained CAS: released before settlement it applies and is
// adopted; released after settlement it cannot apply. HEAD stays a prefix
// image either way.
func TestChunkCut_DeferredCASSettled(t *testing.T) {
	for _, releaseFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("releaseFirst=%v", releaseFirst), func(t *testing.T) { deferredCutSchedule(t, releaseFirst) })
	}
}

func deferredCutSchedule(t *testing.T, releaseFirst bool) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 5, 0x55)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casDeferred}
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("Cut with a retained CAS: %v, want errChunkCASOutcomeUnknown", err)
	}
	if releaseFirst {
		if n := f.releaseDeferredCAS(); n != 1 {
			t.Fatalf("the retained CAS did not apply when released first (%d)", n)
		}
	}
	r, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatalf("Cut after the retained CAS: %v", err)
	}
	if !releaseFirst {
		if n := f.releaseDeferredCAS(); n != 0 {
			t.Fatalf("the retained CAS landed after settlement (%d applied)", n)
		}
	}
	h := f.stats().head
	if h.RemoteLSN != s.NextLSN()-1 || !sameHead(r.Head, h) {
		t.Fatalf("HEAD %+v, cut %+v", h, r.Head)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}
