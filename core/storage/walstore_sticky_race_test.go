package storage

import (
	"errors"
	"os"
	"path/filepath"
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

func heldStore(t *testing.T) (*WALStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, 4096, 64*1024)
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() }) // runs after the gates' cleanups (LIFO)
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

	errA := make(chan error, 1)
	go func() { _, err := s.Sync(); errA <- err }()
	g.waitEntered(t, "Sync A's fsync")
	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil {
		t.Fatal(err)
	}
	errB := make(chan error, 1)
	go func() { _, err := s.Sync(); errB <- err }()
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

			errA := make(chan error, 1)
			go func() {
				_, err := s.WriteBatch(10, [][]byte{makeBlock(4096, 0x5B), makeBlock(4096, 0x5A)})
				errA <- err
			}()
			appendGate.waitEntered(t, "the failing batch append")

			// The queued writer acknowledges that it passed its entry check
			// before the fault exists, then waits for the append lock.
			admitted := make(chan struct{})
			var admitOnce sync.Once
			s.testAfterAdmit = func() { admitOnce.Do(func() { close(admitted) }) }
			errB := make(chan error, 1)
			go func() { errB <- p.run(s, nextBefore) }()
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
	errFlush := make(chan error, 1)
	go func() { errFlush <- s.flusher.flushOnce() }()
	extentGate.waitEntered(t, "the flusher's extent fsync")

	meta.armed.Store(true)
	s.AdvanceFrontier(10)
	errSync := make(chan error, 1)
	go func() { _, err := s.Sync(); errSync <- err }()
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
	errSync := make(chan error, 1)
	go func() { _, err := s.Sync(); errSync <- err }()
	walGate.waitEntered(t, "the WAL fsync")

	meta.armed.Store(true)
	errFlush := make(chan error, 1)
	go func() { errFlush <- s.flusher.flushOnce() }()
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
