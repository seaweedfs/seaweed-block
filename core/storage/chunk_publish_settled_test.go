package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
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

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// Overlapping publishers: while the first report's hook is held, a second
// publisher's cut completes and returns; its report is delivered after the
// first, never concurrently with it.
func TestChunkPublishSettled_OverlappingPublishersStayOrdered(t *testing.T) {
	f := newFakeChunkRemote()
	s, _ := newChunkTestStore(t)
	layer := mustAttach(t, s, f, ctConfig(1)).(*chunkLayerImpl)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, rel sync.Once
	t.Cleanup(func() { rel.Do(func() { close(release) }) })
	var mu sync.Mutex
	var got [][2]uint64
	active, maxActive := 0, 0
	layer.cfg.OnPublishSettled = func(_ context.Context, prev, next ChunkHead) {
		mu.Lock()
		active++
		maxActive = max(maxActive, active)
		got = append(got, [2]uint64{prev.Gen, next.Gen})
		first := len(got) == 1
		mu.Unlock()
		if first {
			once.Do(func() { close(entered) })
			<-release
		}
		mu.Lock()
		active--
		mu.Unlock()
	}
	w := newWriteLog()
	w.write(t, s, 2, 0x22)
	aDone := make(chan error, 1)
	go func() { _, err := layer.Cut(context.Background()); aDone <- err }()
	t.Cleanup(func() {
		rel.Do(func() { close(release) })
		select {
		case <-aDone:
		case <-time.After(10 * time.Second):
			t.Error("the first publisher never returned")
		}
	})
	waitClosed(t, entered, "the first report")
	w.write(t, s, 9, 0x29)
	bDone := make(chan error, 1)
	go func() { _, err := layer.Cut(context.Background()); bDone <- err }()
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second publisher waited on the first report's hook")
	}
	mu.Lock()
	n := len(got)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("%d reports delivered while the first was held; want 1", n)
	}
	rel.Do(func() { close(release) })
	select {
	case err := <-aDone:
		aDone <- err
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the first publisher did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != [2]uint64{0, 1} || got[1] != [2]uint64{1, 2} || maxActive != 1 {
		t.Fatalf("reports %v (max concurrent %d); want [0->1 1->2], one at a time", got, maxActive)
	}
}

// Close during the first of two queued reports: the first hook sees its
// context end, Close waits for it, and the second report is never delivered.
func TestChunkPublishSettled_CloseDropsUnstartedReports(t *testing.T) {
	f := newFakeChunkRemote()
	s, _ := newChunkTestStore(t)
	layer := mustAttach(t, s, f, ctConfig(1)).(*chunkLayerImpl)
	entered, release := make(chan struct{}), make(chan struct{})
	var rel sync.Once
	t.Cleanup(func() { rel.Do(func() { close(release) }) })
	var hookCtx context.Context
	var mu sync.Mutex
	calls := 0
	layer.cfg.OnPublishSettled = func(ctx context.Context, _, _ ChunkHead) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			hookCtx = ctx
			close(entered)
			<-release
		}
	}
	layer.stMu.Lock()
	layer.settled = append(layer.settled, headTransition{prev: chunkHead{Gen: 0}, next: chunkHead{Gen: 1}}, headTransition{prev: chunkHead{Gen: 1}, next: chunkHead{Gen: 2}})
	layer.stMu.Unlock()
	drained := make(chan struct{})
	go func() { layer.runPublishSettled(); close(drained) }()
	waitClosed(t, entered, "the first report")
	closeDone := make(chan error, 1)
	go func() { closeDone <- layer.Close() }()
	t.Cleanup(func() {
		rel.Do(func() { close(release) })
		select {
		case <-closeDone:
		case <-time.After(10 * time.Second):
			t.Error("Close never returned")
		}
	})
	waitClosed(t, hookCtx.Done(), "Close cancelling the hook's context")
	select {
	case err := <-closeDone:
		closeDone <- err
		t.Fatal("Close returned while a report hook was running")
	case <-time.After(200 * time.Millisecond):
	}
	rel.Do(func() { close(release) })
	waitClosed(t, drained, "the drain to end")
	select {
	case err := <-closeDone:
		closeDone <- err
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("%d reports delivered; want the second dropped after Close began", calls)
	}
}

// A hook may publish again: its own report follows, in order, with no
// deadlock.
func TestChunkPublishSettled_HookThatPublishes(t *testing.T) {
	f := newFakeChunkRemote()
	s, _ := newChunkTestStore(t)
	layer := mustAttach(t, s, f, ctConfig(1)).(*chunkLayerImpl)
	w := newWriteLog()
	var got [][2]uint64
	nested := false
	layer.cfg.OnPublishSettled = func(ctx context.Context, prev, next ChunkHead) {
		got = append(got, [2]uint64{prev.Gen, next.Gen})
		if !nested {
			nested = true
			w.write(t, s, 9, 0x29)
			if _, err := layer.Cut(ctx); err != nil {
				t.Errorf("cut from the hook: %v", err)
			}
		}
	}
	w.write(t, s, 2, 0x22)
	done := make(chan error, 1)
	go func() { _, err := layer.Cut(context.Background()); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a hook that publishes deadlocked")
	}
	if len(got) != 2 || got[0] != [2]uint64{0, 1} || got[1] != [2]uint64{1, 2} {
		t.Fatalf("reports %v; want [0->1 1->2]", got)
	}
}
