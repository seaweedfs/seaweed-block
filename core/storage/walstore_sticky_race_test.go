package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

func heldStore(t *testing.T) (*WALStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, 4096, 64*1024)
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { // runs after the workers' and gates' cleanups (LIFO)
		if _, leaked := leakedStores.LoadAndDelete(t); leaked {
			return // a worker is still inside the store: never close under it
		}
		_ = s.Close()
	})
	return s, path
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

// A Sync queued behind an fsync that then fails must not be acknowledged by
// a second fsync of the same batch run.
func TestWALStoreQueuedSyncAfterAFailedFsyncIsRefused(t *testing.T) {
	s, _ := heldStore(t)
	if _, err := s.Write(0, makeBlock(4096, 0x10)); err != nil {
		t.Fatal(err)
	}
	g := newGate(t)
	real := s.syncWALFile
	var calls atomic.Int32
	fault := errors.New("injected fsync failure")
	s.syncWALFile = func() error {
		if calls.Add(1) == 1 {
			g.block()
			return fault
		}
		return real() // a later fsync would "succeed"
	}

	errA := startWorker(t, func() error {
		_, err := s.Sync()
		return err
	}, g)
	g.waitEntered(t, "Sync A's fsync")
	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil {
		t.Fatal(err)
	}
	errB := startWorker(t, func() error {
		_, err := s.Sync()
		return err
	}, g)
	waitQueuedSync(t, s, 1) // B passed the entry check and waits in the committer
	g.open()

	if err := recvErr(t, errA, "Sync A"); !errors.Is(err, fault) {
		t.Fatalf("Sync A: %v; want the injected fault", err)
	}
	if err := recvErr(t, errB, "Sync B"); !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("queued Sync B after the failed fsync: %v; want ErrFsyncFailed", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("physical fsyncs=%d; want 1 (no fsync after the fault)", n)
	}
	if r, _, _ := s.Boundaries(); r != 0 {
		t.Fatalf("published R=%d after the fault", r)
	}
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

// A writer admitted (past its entry check) while an append fails, and then
// waiting for the append lock, must not reuse the failed append's LSNs or
// offsets. One subtest per write path, each with its own oracle.
func TestWALStoreWritersQueuedBehindAFailedAppendAreRefused(t *testing.T) {
	paths := []struct {
		name string
		run  func(s *WALStore, lsn uint64) error
	}{
		{"Write", func(s *WALStore, _ uint64) error { _, err := s.Write(11, makeBlock(4096, 0xAC)); return err }},
		{"WriteBatch", func(s *WALStore, _ uint64) error {
			_, err := s.WriteBatch(20, [][]byte{makeBlock(4096, 0xAD)})
			return err
		}},
		{"ApplyEntry", func(s *WALStore, lsn uint64) error { return s.ApplyEntry(21, makeBlock(4096, 0xAE), lsn) }},
	}
	for _, p := range paths {
		t.Run(p.name, func(t *testing.T) {
			s, _ := heldStore(t)
			// WAL admission reads usage under the WAL writer's lock, which the
			// held append owns; without it the queued writer reaches its
			// admission point (and then the append lock) deterministically.
			s.admission = nil
			appendGate := newGate(t)
			eio := errors.New("injected EIO after the batch bytes were written")
			var batchCalls atomic.Int32
			s.wal.batchWriteAt = func(b []byte, off int64) (int, error) {
				if batchCalls.Add(1) == 1 {
					appendGate.block()
				}
				n, _ := s.fd.WriteAt(b, off)
				return n, eio
			}
			nextBefore := s.NextLSN()
			headBefore := s.wal.logicalHeadValue()

			errA := startWorker(t, func() error {
				_, err := s.WriteBatch(10, [][]byte{makeBlock(4096, 0x5B), makeBlock(4096, 0x5A)})
				return err
			}, appendGate)
			appendGate.waitEntered(t, "the failing batch append")

			// The queued writer acknowledges that it passed its entry check
			// before the fault exists, then waits for the append lock.
			admitted := make(chan struct{})
			var admitOnce sync.Once
			s.testAfterAdmit = func() { admitOnce.Do(func() { close(admitted) }) }
			errB := startWorker(t, func() error { return p.run(s, nextBefore) }, appendGate)
			select {
			case <-admitted:
			case <-time.After(holdBound):
				t.Fatalf("%s was never admitted", p.name)
			}
			appendGate.open()

			if err := recvErr(t, errA, "the failing batch"); !errors.Is(err, eio) {
				t.Fatalf("failing batch: %v", err)
			}
			if err := recvErr(t, errB, p.name); !errors.Is(err, ErrAppendFailed) {
				t.Fatalf("%s admitted before the fault: %v; want ErrAppendFailed", p.name, err)
			}
			s.testAfterAdmit = nil
			if n := batchCalls.Load(); n != 1 {
				t.Fatalf("batch pwrites=%d; want 1 (no append after the fault)", n)
			}
			if got := s.NextLSN(); got != nextBefore {
				t.Fatalf("NextLSN %d -> %d after a refused %s", nextBefore, got, p.name)
			}
			if got := s.wal.logicalHeadValue(); got != headBefore {
				t.Fatalf("WAL head %d -> %d after a refused %s", headBefore, got, p.name)
			}
		})
	}
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

// A direct-frontier publication must not complete after the flusher's
// extent fsync failed, even when that failure lands while the publication
// is past its fault check (in its metadata barrier).
func TestWALStoreExtentFsyncFaultDuringDirectFrontierPublication(t *testing.T) {
	s, path := heldStore(t)
	meta := &heldMetadata{g: newGate(t)}
	s.metadata = meta
	if _, err := s.Write(0, makeBlock(4096, 0x10)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	ckptBefore, diskBefore := s.CheckpointLSN(), diskCheckpoint(t, path)

	extentGate := newGate(t)
	eio := errors.New("injected extent fsync failure")
	s.syncExtentFile = func() error { extentGate.block(); return eio }
	errFlush := startWorker(t, func() error { return s.flusher.flushOnce() }, extentGate, meta.g)
	extentGate.waitEntered(t, "the flusher's extent fsync")

	meta.armed.Store(true)
	s.AdvanceFrontier(10)
	errSync := startWorker(t, func() error {
		_, err := s.Sync()
		return err
	}, extentGate, meta.g)
	inBarrier := meta.g.enteredWithin(300 * time.Millisecond)
	extentGate.open() // the extent fsync fails and latches the fault
	if err := recvErr(t, errFlush, "the flush"); !errors.Is(err, eio) {
		t.Fatalf("flush: %v; want the injected extent fault", err)
	}
	meta.g.open()
	err := recvErr(t, errSync, "the direct-frontier Sync")
	t.Logf("Sync reached its metadata barrier before the fault: %t", inBarrier)
	if !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("direct-frontier Sync after the extent fsync failed: %v; want ErrFsyncFailed", err)
	}
	if got := s.CheckpointLSN(); got != ckptBefore {
		t.Fatalf("checkpoint %d -> %d after the fault", ckptBefore, got)
	}
	if got := diskCheckpoint(t, path); got != diskBefore {
		t.Fatalf("superblock checkpoint %d -> %d after the fault", diskBefore, got)
	}
}

// A checkpoint publication must not complete after a WAL fsync failed, even
// when that failure lands while the flusher is past its fault check (in its
// metadata barrier).
func TestWALStoreWALFsyncFaultDuringCheckpointPublication(t *testing.T) {
	s, path := heldStore(t)
	meta := &heldMetadata{g: newGate(t)}
	s.metadata = meta
	if _, err := s.Write(0, makeBlock(4096, 0x10)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil { // dirty WAL
		t.Fatal(err)
	}
	ckptBefore, diskBefore := s.CheckpointLSN(), diskCheckpoint(t, path)

	walGate := newGate(t)
	fault := errors.New("injected WAL fsync failure")
	s.syncWALFile = func() error { walGate.block(); return fault }
	errSync := startWorker(t, func() error {
		_, err := s.Sync()
		return err
	}, walGate, meta.g)
	walGate.waitEntered(t, "the WAL fsync")

	meta.armed.Store(true)
	errFlush := startWorker(t, func() error { return s.flusher.flushOnce() }, walGate, meta.g)
	inBarrier := meta.g.enteredWithin(300 * time.Millisecond)
	walGate.open() // the WAL fsync fails and latches the fault
	if err := recvErr(t, errSync, "the Sync"); !errors.Is(err, fault) {
		t.Fatalf("Sync: %v; want the injected WAL fault", err)
	}
	meta.g.open()
	err := recvErr(t, errFlush, "the flush")
	t.Logf("the flusher reached its metadata barrier before the fault: %t", inBarrier)
	if !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("flush after the WAL fsync failed: %v; want ErrFsyncFailed", err)
	}
	if got := s.CheckpointLSN(); got != ckptBefore {
		t.Fatalf("checkpoint %d -> %d after the fault", ckptBefore, got)
	}
	if got := diskCheckpoint(t, path); got != diskBefore {
		t.Fatalf("superblock checkpoint %d -> %d after the fault", diskBefore, got)
	}
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

// The fault check at the publication's own superblock fsync: a direct-
// frontier Sync is already in its metadata barrier (past every earlier
// check) when the flusher's extent fsync fails; its superblock publication
// must be refused.
func TestWALStoreExtentFsyncFaultAfterThePublicationCheck(t *testing.T) {
	s, path := heldStore(t)
	meta := &extentHeldMetadata{extent: newGate(t), barrier: newGate(t)}
	s.metadata = meta
	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	ckptBefore, diskBefore := s.CheckpointLSN(), diskCheckpoint(t, path)

	errFlush := startWorker(t, func() error { return s.flusher.flushOnce() }, meta.extent, meta.barrier)
	meta.extent.waitEntered(t, "the flusher's extent write") // before its extent fsync

	meta.armed.Store(true)
	s.AdvanceFrontier(10)
	errSync := startWorker(t, func() error {
		_, err := s.Sync()
		return err
	}, meta.extent, meta.barrier)
	meta.barrier.waitEntered(t, "the Sync's metadata barrier") // past the store-lock check

	eio := errors.New("injected extent fsync failure")
	s.syncExtentFile = func() error { return eio }
	meta.extent.open()
	if err := recvErr(t, errFlush, "the flush"); !errors.Is(err, eio) {
		t.Fatalf("flush: %v; want the injected extent fault", err)
	}
	meta.barrier.open()
	if err := recvErr(t, errSync, "the direct-frontier Sync"); !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("Sync whose barrier outlived the extent fault: %v; want ErrFsyncFailed", err)
	}
	if got := s.CheckpointLSN(); got != ckptBefore {
		t.Fatalf("checkpoint %d -> %d after the fault", ckptBefore, got)
	}
	if got := diskCheckpoint(t, path); got != diskBefore {
		t.Fatalf("superblock checkpoint %d -> %d after the fault", diskBefore, got)
	}
}

// No two fsyncs of the store's file overlap: an extent fsync waits for a
// WAL fsync in flight (so a failure of either is latched before the other
// can complete as a success).
func TestWALStoreFsyncsOfTheFileAreSerialized(t *testing.T) {
	s, _ := heldStore(t)
	if _, err := s.Write(0, makeBlock(4096, 0x10)); err != nil {
		t.Fatal(err)
	}
	walGate := newGate(t)
	real := s.syncWALFile
	s.syncWALFile = func() error { walGate.block(); return real() }
	errSync := startWorker(t, func() error {
		_, err := s.Sync()
		return err
	}, walGate)
	walGate.waitEntered(t, "the WAL fsync")

	extentEntered := make(chan struct{})
	var once sync.Once
	realExtent := s.syncExtentFile
	s.syncExtentFile = func() error { once.Do(func() { close(extentEntered) }); return realExtent() }
	arrived := make(chan struct{})
	var arriveOnce sync.Once
	s.testBeforeFileSync = func() { arriveOnce.Do(func() { close(arrived) }) } // the next syncFile caller: the flusher
	errFlush := startWorker(t, func() error { return s.flusher.flushOnce() }, walGate)
	select { // the flusher reached the serialization boundary...
	case <-arrived:
	case <-time.After(holdBound):
		t.Fatal("the flusher never reached the file sync boundary")
	}
	select { // ...and does not pass it while the WAL fsync is in flight
	case <-extentEntered:
		t.Fatal("the extent fsync ran while a WAL fsync of the same file was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	walGate.open()
	if err := recvErr(t, errSync, "the Sync"); err != nil {
		t.Fatal(err)
	}
	if err := recvErr(t, errFlush, "the flush"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-extentEntered:
	default:
		t.Fatal("the extent fsync never ran")
	}
}

// The failure path's cleanup settles: a test that stops (here SkipNow, which
// ends the test the same way as Fatal) while a worker is held inside the
// store still opens the gate, joins the worker and then closes the store.
func TestWALStoreHeldWorkersSettleOnAFailurePath(t *testing.T) {
	var s *WALStore
	var res <-chan error
	t.Run("stops-while-held", func(t *testing.T) {
		s, _ = heldStore(t)
		if _, err := s.Write(0, makeBlock(4096, 0x10)); err != nil {
			t.Fatal(err)
		}
		g := newGate(t)
		// After its gate opens the flush still takes a while, so only a
		// cleanup that joins it (not one that just opens the gate) sees it end.
		s.syncExtentFile = func() error { g.block(); time.Sleep(200 * time.Millisecond); return nil }
		res = startWorker(t, func() error { return s.flusher.flushOnce() }, g)
		g.waitEntered(t, "the held flush")
		t.SkipNow() // leaves the flush held inside the store
	})
	select {
	case err := <-res:
		if err != nil {
			t.Fatalf("held flush after cleanup: %v", err)
		}
	default:
		t.Fatal("cleanup returned while the held flush was still running")
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if !closed {
		t.Fatal("cleanup did not close the store")
	}
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
