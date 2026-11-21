package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type settledLog struct {
	mu  sync.Mutex
	got [][2]uint64 // {prev gen, next gen}
	// capFree records, per call, whether the capture lock was free.
	capFree []bool
}

func (s *settledLog) hook(l *chunkLayerImpl) func(context.Context, ChunkHead, ChunkHead) {
	return func(_ context.Context, prev, next ChunkHead) {
		free := l.capMu.TryLock()
		if free {
			l.capMu.Unlock()
		}
		s.mu.Lock()
		s.got = append(s.got, [2]uint64{prev.Gen, next.Gen})
		s.capFree = append(s.capFree, free)
		s.mu.Unlock()
	}
}

func (s *settledLog) snapshot() ([][2]uint64, []bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][2]uint64(nil), s.got...), append([]bool(nil), s.capFree...)
}

func attachWithSettled(t *testing.T, f *fakeChunkRemote) (*WALStore, *chunkLayerImpl, *settledLog) {
	t.Helper()
	s, _ := newChunkTestStore(t)
	layer := mustAttach(t, s, f, ctConfig(1)).(*chunkLayerImpl)
	log := &settledLog{}
	layer.cfg.OnPublishSettled = log.hook(layer)
	return s, layer, log
}

// Each confirmed publication is reported once, with the HEAD before and
// after it, after the capture lock is released; a no-op cut reports nothing.
func TestChunkPublishSettled_ReportsEachPublication(t *testing.T) {
	f := newFakeChunkRemote()
	s, layer, log := attachWithSettled(t, f)
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil { // nothing new: no-op
		t.Fatal(err)
	}
	w.write(t, s, 9, 0x29)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	got, free := log.snapshot()
	if len(got) != 2 || got[0] != [2]uint64{0, 1} || got[1] != [2]uint64{1, 2} {
		t.Fatalf("reports %v; want [0->1 1->2]", got)
	}
	for i, ok := range free {
		if !ok {
			t.Fatalf("report %d ran under the capture lock", i)
		}
	}
}

// A CAS whose outcome was unknown, found applied by a later cut, is reported
// when it is settled, before that cut's own publication.
func TestChunkPublishSettled_ReportsALatePublication(t *testing.T) {
	f := newFakeChunkRemote()
	s, layer, log := attachWithSettled(t, f)
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	f.mu.Lock()
	f.casModes = []fakeCASMode{casAppliedLost}
	f.afterCAS = func(f *fakeChunkRemote) { f.failGet = errors.New("HEAD unreadable") }
	f.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); err == nil {
		t.Fatal("cut with an unresolved CAS succeeded")
	}
	if got, _ := log.snapshot(); len(got) != 0 {
		t.Fatalf("reports %v before the CAS was resolved", got)
	}
	f.mu.Lock()
	f.failGet = nil
	f.mu.Unlock()
	w.write(t, s, 9, 0x29)
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	got, _ := log.snapshot()
	if len(got) != 2 || got[0] != [2]uint64{0, 1} || got[1] != [2]uint64{1, 2} {
		t.Fatalf("reports %v; want the late publication 0->1, then 1->2", got)
	}
}

// A closing layer drops reports it has not run; without a hook nothing is
// recorded.
func TestChunkPublishSettled_ClosedAndUnset(t *testing.T) {
	f := newFakeChunkRemote()
	_, layer, log := attachWithSettled(t, f)
	layer.stMu.Lock()
	layer.settled = append(layer.settled, headTransition{prev: chunkHead{Gen: 1}, next: chunkHead{Gen: 2}})
	layer.stMu.Unlock()
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	layer.runPublishSettled()
	if got, _ := log.snapshot(); len(got) != 0 {
		t.Fatalf("reports %v after Close", got)
	}

	s2, _ := newChunkTestStore(t)
	plain := mustAttach(t, s2, newFakeChunkRemote(), ctConfig(1)).(*chunkLayerImpl)
	w := newWriteLog()
	w.write(t, s2, 2, 0x22)
	if _, err := plain.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	plain.stMu.Lock()
	n := len(plain.settled)
	plain.stMu.Unlock()
	if n != 0 {
		t.Fatalf("%d publications recorded without a hook", n)
	}
}
