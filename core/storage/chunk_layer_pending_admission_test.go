package storage

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

var errTestDemoted = errors.New("test: demoted during the HEAD read")

// Re-proposing a held CAS is a new publication: it passes the capture
// admission again right before it is sent, after the blocking HEAD read.
// A demotion or lineage change during that read sends no CAS and keeps the
// CAS unresolved; once authority is back, the same proposal settles at the
// same base (review of #111 on #114, P1).
func TestChunkCut_HeldCASReproposalRechecksAdmission(t *testing.T) {
	for _, releaseWhileRefused := range []bool{false, true} {
		t.Run(fmt.Sprintf("releaseWhileRefused=%v", releaseWhileRefused), func(t *testing.T) {
			heldReproposalSchedule(t, releaseWhileRefused)
		})
	}
}

func heldReproposalSchedule(t *testing.T, releaseWhileRefused bool) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	var refuse atomic.Bool
	cfg := ctConfig(1)
	cfg.CaptureAllowed = func() error {
		if refuse.Load() {
			return errTestDemoted
		}
		return nil
	}
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 5, 0x55)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casDeferred}
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("Cut with a retained CAS: %v, want errChunkCASOutcomeUnknown", err)
	}
	base := f.stats().head
	f.mu.Lock()
	held := copyChunkHead(f.deferred[0].next)
	// The next cut passes its first admission; the projection is demoted
	// while its HEAD read is in flight.
	f.onGetHead = func(*fakeChunkRemote) { refuse.Store(true) }
	f.mu.Unlock()
	casBefore := f.stats().cas

	_, err := layer.Cut(ctTimeout(t))
	if !errors.Is(err, errChunkCaptureRefused) || !errors.Is(err, errTestDemoted) || !errors.Is(err, errChunkCASOutcomeUnknown) {
		t.Fatalf("Cut demoted during its HEAD read: %v; want refused (demoted) with the CAS still unresolved", err)
	}
	if n := f.stats().cas - casBefore; n != 0 {
		t.Fatalf("a layer demoted during its HEAD read sent %d new CAS calls", n)
	}
	if h := f.stats().head; !sameHead(h, base) {
		t.Fatalf("HEAD moved under refused authority: %+v, base %+v", h, base)
	}
	f.mu.Lock()
	f.onGetHead = nil
	f.mu.Unlock()
	if releaseWhileRefused {
		if n := f.releaseDeferredCAS(); n != 1 {
			t.Fatalf("the retained CAS did not apply when released (%d)", n)
		}
	}
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
		t.Fatalf("Cut while still demoted: %v; want refused", err)
	}
	if n := f.stats().cas - casBefore; n != 0 {
		t.Fatalf("a demoted layer sent %d new CAS calls", n)
	}

	// Authority restored: the retained proposal settles at its base.
	refuse.Store(false)
	r, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatalf("Cut after authority returned: %v", err)
	}
	if !releaseWhileRefused {
		if n := f.releaseDeferredCAS(); n != 0 {
			t.Fatalf("the retained CAS landed after settlement (%d applied)", n)
		}
	}
	h := f.stats().head
	if h.Gen != base.Gen+1 || !sameChunks(h, held) || h.RemoteLSN != s.NextLSN()-1 || !sameHead(r.Head, h) {
		t.Fatalf("HEAD after recovery %+v (cut %+v); want the retained proposal at gen %d", h, r.Head, base.Gen+1)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}
