package storage

import (
	"fmt"
	"os"

	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Held-caller controls for the sticky I/O fault: callers that passed the
// entry check before the fault was latched, and publications running when
// another path's fsync fails, must still be refused.

const holdBound = 5 * time.Second

// gate blocks a hooked operation until released; release is idempotent and
// is registered as a cleanup before the store's Close, so a failing test
// never leaves a hooked caller holding a store lock.
type gate struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	inOnce  sync.Once
}

func newGate(t *testing.T) *gate {
	g := &gate{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *gate) open()  { g.once.Do(func() { close(g.release) }) }
func (g *gate) block() { g.inOnce.Do(func() { close(g.entered) }); <-g.release }

func (g *gate) waitEntered(t *testing.T, what string) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(holdBound):
		t.Fatalf("%s never entered", what)
	}
}

// enteredWithin reports whether the hooked operation entered within d (the
// fixed code may hold the caller elsewhere instead).
func (g *gate) enteredWithin(d time.Duration) bool {
	select {
	case <-g.entered:
		return true
	case <-time.After(d):
		return false
	}
}

func recvErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(holdBound):
		t.Fatalf("%s did not return", what)
		return nil
	}
}

// workerTB is the part of testing.TB startWorker uses (a recorder stands in
// for it in the control below).
type workerTB interface {
	Helper()
	Cleanup(func())
	Errorf(format string, args ...any)
}

// joinBound bounds startWorker's join; the leak control shortens it.
var joinBound = holdBound

// leakedStores marks the tests whose held worker never settled: their store
// is left open rather than closed under a live worker.
var leakedStores sync.Map // workerTB -> struct{}

// startWorker runs fn on its own goroutine and returns its result channel.
// Its cleanup, which runs before the store's Close (registered later than
// heldStore's), opens the given gates and joins the worker within
// joinBound. If the worker does not settle, it fails the test with every
// goroutine's stack and marks the store leaked: the store is never closed
// under a live worker (that Close could block on the worker's locks).
func startWorker(t workerTB, fn func() error, gates ...*gate) <-chan error {
	t.Helper()
	res := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		res <- fn()
	}()
	t.Cleanup(func() {
		for _, g := range gates {
			g.open()
		}
		select {
		case <-done:
		case <-time.After(joinBound):
			leakedStores.Store(t, struct{}{})
			buf := make([]byte, 1<<20)
			buf = buf[:runtime.Stack(buf, true)]
			t.Errorf("a held worker did not settle within %v; the store is left open (not closed under it). goroutines:\n%s", joinBound, buf)
		}
	})
	return res
}

func diskCheckpoint(t *testing.T, path string) uint64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb, err := readSuperblock(f)
	if err != nil {
		t.Fatal(err)
	}
	return sb.WALCheckpointLSN
}

func waitQueuedSync(t *testing.T, s *WALStore, n int) {
	t.Helper()
	deadline := time.Now().Add(holdBound)
	for time.Now().Before(deadline) {
		s.committer.mu.Lock()
		q := len(s.committer.pending)
		s.committer.mu.Unlock()
		if q >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no queued Sync after %v", holdBound)
}

// heldMetadata blocks BeforeCheckpointAdvance once armed.
type heldMetadata struct {
	armed atomic.Bool
	g     *gate
}

func (m *heldMetadata) OnExtentWrite(uint32, []byte) error { return nil }
func (m *heldMetadata) VerifyExtent(uint32, []byte) error  { return nil }
func (m *heldMetadata) BeforeCheckpointAdvance(uint64) error {
	if m.armed.Load() {
		m.g.block()
	}
	return nil
}

// extentHeldMetadata blocks the first OnExtentWrite (the flusher after its
// snapshot, before its extent fsync) and, once armed, BeforeCheckpointAdvance.
type extentHeldMetadata struct {
	extent  *gate
	barrier *gate
	armed   atomic.Bool
	seen    atomic.Bool
}

func (m *extentHeldMetadata) OnExtentWrite(uint32, []byte) error {
	if m.seen.CompareAndSwap(false, true) {
		m.extent.block()
	}
	return nil
}
func (m *extentHeldMetadata) VerifyExtent(uint32, []byte) error { return nil }
func (m *extentHeldMetadata) BeforeCheckpointAdvance(uint64) error {
	if m.armed.Load() {
		m.barrier.block()
	}
	return nil
}

// leakRecorder records cleanups and errors in place of a *testing.T.
type leakRecorder struct {
	mu       sync.Mutex
	cleanups []func()
	errs     []string
}

func (r *leakRecorder) Helper() {}
func (r *leakRecorder) Cleanup(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, f)
}
func (r *leakRecorder) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

// A worker that never returns fails the test, with every goroutine's stack,
// within the join bound, and marks the store leaked instead of closing it
// under the worker; nothing hangs.
func TestWALStoreStartWorkerReportsAWorkerThatNeverSettles(t *testing.T) {
	old := joinBound
	joinBound = 200 * time.Millisecond
	defer func() { joinBound = old }()
	stuck := make(chan struct{})
	defer close(stuck) // release the stuck worker when the control ends
	rec := &leakRecorder{}
	startWorker(rec, func() error { <-stuck; return nil })

	start := time.Now()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for i := len(rec.cleanups) - 1; i >= 0; i-- {
			rec.cleanups[i]()
		}
	}()
	select {
	case <-finished:
	case <-holdBoundTimer():
		t.Fatal("startWorker's cleanup hung on a worker that never returns")
	}
	if d := time.Since(start); d > 5*joinBound {
		t.Fatalf("cleanup took %v, bound %v", d, joinBound)
	}
	rec.mu.Lock()
	errs := append([]string(nil), rec.errs...)
	rec.mu.Unlock()
	if len(errs) != 1 || !strings.Contains(errs[0], "did not settle") || !strings.Contains(errs[0], "goroutine ") {
		t.Fatalf("errors %q; want one 'did not settle' error with a goroutine dump", errs)
	}
	if _, leaked := leakedStores.LoadAndDelete(rec); !leaked {
		t.Fatal("the store was not marked leaked")
	}
}

func holdBoundTimer() <-chan time.Time { return time.After(holdBound) }
