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

// worker is a goroutine a test launches. Its bounded join (after release,
// if given) is registered as cleanup before it starts, so a failing test
// never leaves it running; the result is read once and kept.
type worker struct {
	name string
	done chan error
	once sync.Once
	err  error
	ok   bool
}

func launch(t *testing.T, name string, release func(), f func() error) *worker {
	t.Helper()
	w := &worker{name: name, done: make(chan error, 1)}
	t.Cleanup(func() {
		if release != nil {
			release()
		}
		if _, ok := w.result(10 * time.Second); !ok {
			t.Errorf("%s never returned", name)
		}
	})
	go func() { w.done <- f() }()
	return w
}

// result waits up to d for the worker; ok is false on timeout.
func (w *worker) result(d time.Duration) (error, bool) {
	w.once.Do(func() {
		select {
		case w.err = <-w.done:
			w.ok = true
		case <-time.After(d):
		}
	})
	if !w.ok {
		w.once = sync.Once{} // a timed-out wait may be retried
	}
	return w.err, w.ok
}

func (w *worker) mustReturn(t *testing.T) {
	t.Helper()
	err, ok := w.result(10 * time.Second)
	if !ok {
		t.Fatalf("%s did not return", w.name)
	}
	if err != nil {
		t.Fatalf("%s: %v", w.name, err)
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
	releaseHook := func() { rel.Do(func() { close(release) }) }
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
	a := launch(t, "the first publisher", releaseHook, func() error { _, err := layer.Cut(context.Background()); return err })
	waitClosed(t, entered, "the first report")
	w.write(t, s, 9, 0x29)
	b := launch(t, "the second publisher", releaseHook, func() error { _, err := layer.Cut(context.Background()); return err })
	if err, ok := b.result(10 * time.Second); !ok {
		t.Fatal("the second publisher waited on the first report's hook")
	} else if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	n := len(got)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("%d reports delivered while the first was held; want 1", n)
	}
	releaseHook()
	a.mustReturn(t)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != [2]uint64{0, 1} || got[1] != [2]uint64{1, 2} || maxActive != 1 {
		t.Fatalf("reports %v (max concurrent %d); want [0->1 1->2], one at a time", got, maxActive)
	}
}

// Close during the first of two queued reports: the first hook sees its
// context end, Close reaches its join of the active drainer and waits there
// (witnessed, not timed), and the second report is never delivered.
func TestChunkPublishSettled_CloseDropsUnstartedReports(t *testing.T) {
	f := newFakeChunkRemote()
	s, _ := newChunkTestStore(t)
	layer := mustAttach(t, s, f, ctConfig(1)).(*chunkLayerImpl)
	entered, release := make(chan struct{}), make(chan struct{})
	var rel sync.Once
	releaseHook := func() { rel.Do(func() { close(release) }) }
	joining := make(chan struct{})
	var joinOnce sync.Once
	var order []string // "hook returned" and "Close joined", in the order they happened
	var orderMu sync.Mutex
	note := func(e string) { orderMu.Lock(); order = append(order, e); orderMu.Unlock() }
	testCloseJoinHook = func() { joinOnce.Do(func() { close(joining) }) }
	testCloseJoinedHook = func() { note("Close joined") }
	t.Cleanup(func() { testCloseJoinHook, testCloseJoinedHook = nil, nil })
	var hookCtx context.Context
	var mu sync.Mutex
	calls := 0
	layer.cfg.OnPublishSettled = func(ctx context.Context, _, _ ChunkHead) {
		mu.Lock()
		calls++
		n := calls
		if n == 1 {
			hookCtx = ctx
		}
		mu.Unlock()
		if n == 1 {
			close(entered)
			<-release
			note("hook returned")
		}
	}
	layer.stMu.Lock()
	layer.settled = append(layer.settled, headTransition{prev: chunkHead{Gen: 0}, next: chunkHead{Gen: 1}}, headTransition{prev: chunkHead{Gen: 1}, next: chunkHead{Gen: 2}})
	layer.stMu.Unlock()
	drainer := launch(t, "the drainer", releaseHook, func() error { layer.runPublishSettled(); return nil })
	waitClosed(t, entered, "the first report")
	closer := launch(t, "Close", releaseHook, layer.Close)
	mu.Lock()
	ctx := hookCtx
	mu.Unlock()
	waitClosed(t, ctx.Done(), "Close cancelling the hook's context")
	waitClosed(t, joining, "Close reaching its join of the drainer")
	releaseHook()
	drainer.mustReturn(t)
	closer.mustReturn(t)
	orderMu.Lock()
	seq := append([]string(nil), order...)
	orderMu.Unlock()
	if len(seq) != 2 || seq[0] != "hook returned" || seq[1] != "Close joined" {
		t.Fatalf("order %v; want the running hook to return before Close's join does", seq)
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
	var mu sync.Mutex
	var got [][2]uint64
	nested := false
	// One cancellation covers the outer cut and the cut its hook makes;
	// cleanup cancels it before the bounded join.
	cutCtx, cancelCuts := context.WithCancel(context.Background())
	layer.cfg.OnPublishSettled = func(_ context.Context, prev, next ChunkHead) {
		mu.Lock()
		got = append(got, [2]uint64{prev.Gen, next.Gen})
		again := !nested
		nested = true
		mu.Unlock()
		if again {
			w.write(t, s, 9, 0x29)
			if _, err := layer.Cut(cutCtx); err != nil {
				t.Errorf("cut from the hook: %v", err)
			}
		}
	}
	w.write(t, s, 2, 0x22)
	cut := launch(t, "a cut whose hook publishes", cancelCuts, func() error { _, err := layer.Cut(cutCtx); return err })
	if err, ok := cut.result(10 * time.Second); !ok {
		t.Fatal("a hook that publishes deadlocked")
	} else if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != [2]uint64{0, 1} || got[1] != [2]uint64{1, 2} {
		t.Fatalf("reports %v; want [0->1 1->2]", got)
	}
}

// A periodic cut (CutUnlessCutSince) reports its publication like any other
// cut, after the capture lock is released, before it returns.
func TestChunkPublishSettled_PeriodicCutReports(t *testing.T) {
	f := newFakeChunkRemote()
	s, layer, log := attachWithSettled(t, f)
	w := newWriteLog()
	w.write(t, s, 3, 0x33)
	if _, skipped, err := layer.CutUnlessCutSince(ctTimeout(t), 0); err != nil || skipped {
		t.Fatalf("periodic cut: skipped=%v err=%v", skipped, err)
	}
	got, free := log.snapshot()
	if len(got) != 1 || got[0] != [2]uint64{0, 1} || !free[0] {
		t.Fatalf("periodic cut reports %v (capture lock free %v); want one report 0->1 with the lock free", got, free)
	}
}
