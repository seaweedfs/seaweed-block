package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Row 5/5b store-level controls on the REAL *WALStore in its v3 mode
// (DESIGN r15 6.1 to 6.6, section 11 item 5 and the G1 fence). A "crash"
// stops the committer and closes the file without any flush or further
// write (crashRecoveryHardeningStore), so every pwritten byte is still
// visible on reopen; damage is injected into the file directly.

const v3TestBlock = 4096
const v3TestWAL = 64 * 1024

func newStoreV3(t *testing.T, mode WMode) (*WALStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.v3")
	s, err := createWALStoreV3ForTest(path, 64, v3TestBlock, v3TestWAL, mode)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	return s, path
}

// reopenV3 opens and recovers, as a restart would.
func reopenV3(t *testing.T, path string) (*WALStore, error) {
	t.Helper()
	s, err := openWALStoreV3ForTest(path)
	if err != nil {
		return nil, err
	}
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Recover(); err != nil {
		crashRecoveryHardeningStore(t, s)
		return nil, err
	}
	return s, nil
}

func mustReopenV3(t *testing.T, path string) *WALStore {
	t.Helper()
	s, err := reopenV3(t, path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func blk(b byte) []byte { return bytes.Repeat([]byte{b}, v3TestBlock) }

func readV3(t *testing.T, s *WALStore, lba uint32) []byte {
	t.Helper()
	b, err := s.Read(lba)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// writeSynced writes one block per LSN, each in its own Sync (batch).
func writeSynced(t *testing.T, s *WALStore, from, to uint32) {
	t.Helper()
	for lba := from; lba <= to; lba++ {
		if _, err := s.Write(lba, blk(byte(lba))); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
}

// recordOffset returns the absolute file offset of the record with lsn,
// found by scanning the file as a reopen would.
func recordOffset(t *testing.T, path string, lsn uint64) int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	a, b := make([]byte, slotSizeV3), make([]byte, slotSizeV3)
	if _, err := f.ReadAt(a, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(b, slotBOffsetV3); err != nil {
		t.Fatal(err)
	}
	d, _, err := selectSlotV3(a, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := scanWALV3(f, d)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range r.Records {
		if rec.Entry.LSN == lsn {
			size := uint64(walEntryHeaderSizeV3 + len(rec.Entry.Data))
			return int64(d.WALOffset + (rec.End-size)%d.WALSize)
		}
	}
	t.Fatalf("no record with LSN %d", lsn)
	return 0
}

func damageRecord(t *testing.T, path string, lsn uint64) {
	t.Helper()
	off := recordOffset(t, path, lsn) + walEntryPrefixSizeV3 + 100 // a payload byte
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := []byte{0}
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func isWALCorrupt(err error) bool {
	var rf *RecoveryFailure
	return errors.As(err, &rf) && rf.Kind == StorageRecoveryFailureWALIntegrity
}

func TestWALStoreV3RecoversEveryAcknowledgedWriteInBothModes(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		s, path := newStoreV3(t, mode)
		writeSynced(t, s, 1, 5)
		crashRecoveryHardeningStore(t, s)
		r := mustReopenV3(t, path)
		for lba := uint32(1); lba <= 5; lba++ {
			if !bytes.Equal(readV3(t, r, lba), blk(byte(lba))) {
				t.Fatalf("mode %d: acknowledged LBA %d lost", mode, lba)
			}
		}
		// Writes continue after recovery and survive another crash.
		writeSynced(t, r, 6, 7)
		crashRecoveryHardeningStore(t, r)
		r2 := mustReopenV3(t, path)
		if !bytes.Equal(readV3(t, r2, 7), blk(7)) || !bytes.Equal(readV3(t, r2, 1), blk(1)) {
			t.Fatalf("mode %d: writes after recovery lost", mode)
		}
		_ = r2.Close()
	}
}

func TestWALStoreV3ModeCWindowIsExactlyTheNewestBatch(t *testing.T) {
	// The newest acknowledged batch corrupted before any later Sync or
	// refresh: mode C truncates it silently (the documented window); mode A
	// refuses.
	for _, c := range []struct {
		mode    WMode
		corrupt bool
	}{{WModeLagged, false}, {WModeStrict, true}} {
		s, path := newStoreV3(t, c.mode)
		writeSynced(t, s, 1, 3)
		crashRecoveryHardeningStore(t, s)
		damageRecord(t, path, 3)
		r, err := reopenV3(t, path)
		if c.corrupt {
			if !isWALCorrupt(err) {
				t.Fatalf("mode A, newest batch corrupt: err = %v, want wal_corrupt", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("mode C, newest batch corrupt: err = %v, want the documented silent truncation", err)
		}
		if !bytes.Equal(readV3(t, r, 2), blk(2)) || bytes.Equal(readV3(t, r, 3), blk(3)) {
			t.Fatal("mode C: want LSN 1-2 kept and the newest batch (3) truncated")
		}
		_ = r.Close()
	}
	// The second-newest batch is protected in both modes.
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		s, path := newStoreV3(t, mode)
		writeSynced(t, s, 1, 3)
		crashRecoveryHardeningStore(t, s)
		damageRecord(t, path, 2)
		if _, err := reopenV3(t, path); !isWALCorrupt(err) {
			t.Fatalf("mode %d, second-newest batch corrupt: err = %v, want wal_corrupt", mode, err)
		}
	}
}

func TestWALStoreV3RefreshAndSetWModeCloseTheWindow(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	writeSynced(t, s, 1, 3)
	if err := s.refreshW(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	damageRecord(t, path, 3)
	if _, err := reopenV3(t, path); !isWALCorrupt(err) {
		t.Fatalf("after the idle refresh: err = %v, want wal_corrupt", err)
	}

	s2, path2 := newStoreV3(t, WModeLagged)
	writeSynced(t, s2, 1, 3)
	if err := s2.SetWMode(WModeStrict); err != nil {
		t.Fatal(err)
	}
	if s2.v3.pub.current().WMode != WModeStrict {
		t.Fatal("SetWMode did not publish the new mode")
	}
	crashRecoveryHardeningStore(t, s2)
	damageRecord(t, path2, 3)
	if _, err := reopenV3(t, path2); !isWALCorrupt(err) {
		t.Fatalf("after SetWMode(A): err = %v, want wal_corrupt (C -> A closes the window)", err)
	}
}

func TestWALStoreV3TornUnsyncedTailIsNeverAFalseCorrupt(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		s, path := newStoreV3(t, mode)
		writeSynced(t, s, 1, 3)
		if _, err := s.Write(4, blk(4)); err != nil { // appended, never synced
			t.Fatal(err)
		}
		crashRecoveryHardeningStore(t, s)
		damageRecord(t, path, 4) // a torn unsynced record
		r, err := reopenV3(t, path)
		if err != nil {
			t.Fatalf("mode %d, torn unsynced tail: err = %v, want recovery", mode, err)
		}
		if !bytes.Equal(readV3(t, r, 3), blk(3)) || bytes.Equal(readV3(t, r, 4), blk(4)) {
			t.Fatalf("mode %d: want 1-3 kept and the unsynced 4 dropped", mode)
		}
		_ = r.Close()
	}
}

// resurrectionSchedule is the G1 schedule (DESIGN 6.6): records dropped by
// one recovery must not resurface after a second crash. Without the fence,
// re-filling the gap with a record of the same size re-chains the old
// record behind it.
func resurrectionSchedule(t *testing.T, mode WMode, skipFence bool) (resurrected bool) {
	t.Helper()
	s, path := newStoreV3(t, mode)
	writeSynced(t, s, 1, 3)
	for _, lba := range []uint32{4, 5} { // unsynced
		if _, err := s.Write(lba, blk(byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	crashRecoveryHardeningStore(t, s)
	damageRecord(t, path, 4) // a torn LSN 4: recovery keeps 1-3 and drops the trusted 5
	testSkipFenceOnRecoverV3 = skipFence
	r, err := reopenV3(t, path)
	testSkipFenceOnRecoverV3 = false
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(readV3(t, r, 5), blk(5)) {
		t.Fatal("first recovery kept LSN 5 above the gap")
	}
	if _, err := r.Write(9, blk(0x99)); err != nil { // re-fills LSN 4's place, same size
		t.Fatal(err)
	}
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, r)
	r2 := mustReopenV3(t, path)
	defer r2.Close()
	return bytes.Equal(readV3(t, r2, 5), blk(5))
}

func TestWALStoreV3DroppedRecordsNeverResurfaceAfterASecondCrash(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		if resurrectionSchedule(t, mode, false) {
			t.Fatalf("mode %d: a record dropped by recovery resurfaced after the second crash (G1)", mode)
		}
	}
}

func TestWALStoreV3NoFenceResurrectsInBothModes(t *testing.T) {
	// The schedule is a real counterexample: with the fence skipped, the
	// dropped record comes back. This keeps the G1 control discriminating.
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		if !resurrectionSchedule(t, mode, true) {
			t.Fatalf("mode %d: the no-fence schedule did not resurrect; the G1 control is not discriminating", mode)
		}
	}
}

func TestWALStoreV3FenceTouchesNoByteBeforeTheHead(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	writeSynced(t, s, 1, 3)
	if _, err := s.Write(4, blk(4)); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	damageRecord(t, path, 4)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	e := uint64(recordOffset(t, path, 3)) + walEntryHeaderSizeV3 + v3TestBlock // end of LSN 3
	r := mustReopenV3(t, path)
	crashRecoveryHardeningStore(t, r)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before[walOffsetV3:e], after[walOffsetV3:e]) {
		t.Fatal("the fence changed a byte before the recovered head E")
	}
	if !bytes.Equal(after[e:walOffsetV3+v3TestWAL], make([]byte, walOffsetV3+v3TestWAL-e)) {
		t.Fatal("the fence left a nonzero byte in [E, floor+WALSize)")
	}
	if e%4096 == 0 {
		t.Fatal("test setup: E is page-aligned; it should exercise an unaligned head")
	}
}

func TestWALStoreV3AppendWriteErrorLatchesTheStore(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	defer s.Close()
	writeSynced(t, s, 1, 1)
	boom := errors.New("injected WAL pwrite error")
	s.v3.w.writeAt = func([]byte, int64) (int, error) { return 0, boom }
	if _, err := s.Write(2, blk(2)); !errors.Is(err, boom) {
		t.Fatalf("append: err = %v, want the pwrite error", err)
	}
	if s.stickyIOFault() == nil {
		t.Fatal("an append pwrite error did not latch the store-global fault")
	}
	if _, err := s.Sync(); err == nil {
		t.Fatal("Sync acknowledged after an append write error")
	}
	if _, err := s.Write(3, blk(3)); err == nil {
		t.Fatal("a later Write was admitted after an append write error")
	}
}

// Row 5b: the v3 store uses the real flusher. Its extent writes happen,
// but without the row-7 map candidate the checkpoint and tail do not move
// (no fabricated coverage), and the flushed records stay retained.
func TestWALStoreV3FlusherWritesTheExtentButHoldsTheCheckpointUntilTheMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.v3")
	s, err := createWALStoreV3ForTest(path, 64, v3TestBlock, v3TestWAL, WModeLagged)
	if err != nil {
		t.Fatal(err)
	}
	writeSynced(t, s, 1, 3)
	before := s.v3.pub.current()
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	after := s.v3.pub.current()
	if after.CheckpointLSN != before.CheckpointLSN || after.RetainedTail != before.RetainedTail || s.CheckpointLSN() != 0 {
		t.Fatalf("checkpoint/tail advanced without a map candidate: %d/%d -> %d/%d",
			before.CheckpointLSN, before.RetainedTail, after.CheckpointLSN, after.RetainedTail)
	}
	// The data reads back (now from the extent) and survives a crash (from
	// the still-retained WAL).
	for lba := uint32(1); lba <= 3; lba++ {
		if !bytes.Equal(readV3(t, s, lba), blk(byte(lba))) {
			t.Fatalf("LBA %d unreadable after the v3 flush", lba)
		}
	}
	crashRecoveryHardeningStore(t, s)
	r := mustReopenV3(t, path)
	defer r.Close()
	for lba := uint32(1); lba <= 3; lba++ {
		if !bytes.Equal(readV3(t, r, lba), blk(byte(lba))) {
			t.Fatalf("LBA %d lost after flush and crash", lba)
		}
	}
}

func TestWALStoreV3RefusesPathsOfLaterRows(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	defer s.Close()
	if _, err := s.WriteBatch(0, [][]byte{blk(1)}); !errors.Is(err, errV3NotYet) {
		t.Fatalf("WriteBatch: err = %v, want errV3NotYet", err)
	}
	if err := s.ApplyEntry(0, blk(1), 10); !errors.Is(err, errV3NotYet) {
		t.Fatalf("ApplyEntry: err = %v, want errV3NotYet", err)
	}
	if err := s.WriteExtentDirect(0, blk(1)); !errors.Is(err, errV3NotYet) {
		t.Fatalf("WriteExtentDirect: err = %v, want errV3NotYet", err)
	}
}

// codex01 row 5 P1-1: a batch's acknowledgement rechecks the store fault
// under the store lock. Schedule, in both modes: batch A's slot write is
// parked (A holds descMu); B's Write takes s.mu and waits on descMu in the
// writer's snapshot; A is released, B's pwrite fails and latches while B
// still holds s.mu; A must then refuse without advancing the acknowledged W.
func TestWALStoreV3SyncRechecksTheFaultAtTheAckBoundary(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		s, _ := newStoreV3(t, mode)
		writeSynced(t, s, 1, 1)
		if _, err := s.Write(2, blk(2)); err != nil {
			t.Fatal(err)
		}
		prev := s.v3.wack.Load()
		parked, release := make(chan struct{}), make(chan struct{})
		rel := sync.OnceFunc(func() { close(release) })
		t.Cleanup(rel)
		var once sync.Once
		s.v3.testBeforeSlotWrite = func() error {
			once.Do(func() { close(parked); <-release })
			return nil
		}
		aErr := make(chan error, 1)
		go func() { _, err := s.Sync(); aErr <- err }()
		waitV3(t, parked, "the parked publication")
		boom := errors.New("injected append write error")
		s.v3.w.writeAt = func([]byte, int64) (int, error) { return 0, boom }
		bErr := make(chan error, 1)
		go func() { _, err := s.Write(3, blk(3)); bErr <- err }()
		deadline := time.Now().Add(10 * time.Second)
		for s.mu.TryLock() { // wait until B holds s.mu (it then blocks on descMu)
			s.mu.Unlock()
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for B to take s.mu")
			}
			runtime.Gosched()
		}
		rel()
		if err := recvErrV3(t, bErr, "B"); !errors.Is(err, boom) {
			t.Fatalf("mode %d: B: err = %v, want the append write error", mode, err)
		}
		if err := recvErrV3(t, aErr, "A"); err == nil {
			t.Fatalf("mode %d: A acknowledged after B latched the store fault", mode)
		}
		if got := s.v3.wack.Load(); got != prev {
			t.Fatalf("mode %d: acknowledged W advanced to %d after the fault (want %d)", mode, got, prev)
		}
		crashRecoveryHardeningStore(t, s)
	}
}

// codex01 row 5 P2: a history ending at the last representable cursor
// recovers; only further appends are refused.
func TestWALStoreV3RecoversAHistoryEndingAtTheLastRepresentableCursor(t *testing.T) {
	const recSize = uint64(walEntryHeaderSizeV3 + v3TestBlock)
	cursor := counterMaxV3 - 3*recSize // 3 records end exactly at 2^64-2, no ring-end crossing
	path := filepath.Join(t.TempDir(), "store.v3")
	s, err := createWALStoreV3AtCursorForTest(path, 64, v3TestBlock, v3TestWAL, WModeStrict, cursor)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	writeSynced(t, s, 1, 3)
	if _, err := s.Write(4, blk(4)); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("append past the last cursor: err = %v, want ErrCounterExhausted", err)
	}
	crashRecoveryHardeningStore(t, s)
	r := mustReopenV3(t, path)
	defer r.Close()
	for lba := uint32(1); lba <= 3; lba++ {
		if !bytes.Equal(readV3(t, r, lba), blk(byte(lba))) {
			t.Fatalf("LBA %d of the terminal history lost", lba)
		}
	}
	if _, err := r.Write(5, blk(5)); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("append after recovery at the last cursor: err = %v, want ErrCounterExhausted", err)
	}
}

// codex01 row 5 P2 (SetWMode evidence): a held mode-C batch completes and
// acknowledges under C before SetWMode(A) publishes the new mode.
func TestWALStoreV3SetWModeWaitsForAHeldBatch(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 1)
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	parked, release := make(chan struct{}), make(chan struct{})
	rel := sync.OnceFunc(func() { close(release) })
	t.Cleanup(rel)
	var once sync.Once
	s.v3.testBeforeSlotWrite = func() error {
		once.Do(func() { close(parked); <-release })
		return nil
	}
	aErr := make(chan error, 1)
	go func() { _, err := s.Sync(); aErr <- err }()
	waitV3(t, parked, "the parked publication")
	setErr := make(chan error, 1)
	go func() { setErr <- s.SetWMode(WModeStrict) }()
	select {
	case err := <-setErr:
		t.Fatalf("SetWMode returned (%v) while a C batch was held", err)
	default:
	}
	rel()
	if err := recvErrV3(t, aErr, "A"); err != nil {
		t.Fatal(err)
	}
	if err := recvErrV3(t, setErr, "SetWMode"); err != nil {
		t.Fatal(err)
	}
	cur := s.v3.pub.current()
	if cur.WMode != WModeStrict || cur.W != 2 {
		t.Fatalf("after SetWMode: mode %d W %d, want A and W 2", cur.WMode, cur.W)
	}
	// The held batch's own publication (now the fallback slot) was mode C
	// with the lagged W 1.
	if fb := s.v3.pub.other; fb.WMode != WModeLagged || fb.W != 1 || fb.SlotGen != cur.SlotGen-1 {
		t.Fatalf("held batch publication: mode %d W %d gen %d, want C, W 1, gen %d", fb.WMode, fb.W, fb.SlotGen, cur.SlotGen-1)
	}
}

// No batch acknowledges under a stale mode: a batch that reaches the
// leader role while SetWMode(A) holds it publishes with A's rule (W = H)
// once SetWMode has committed.
func TestWALStoreV3NoBatchAcksUnderAStaleModeAfterSetWMode(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 1)
	parked, release := make(chan struct{}), make(chan struct{})
	rel := sync.OnceFunc(func() { close(release) })
	t.Cleanup(rel)
	s.v3.testSetWModeHoldingRole = func() { close(parked); <-release }
	setErr := make(chan error, 1)
	go func() { setErr <- s.SetWMode(WModeStrict) }() // parks holding the leader role, before publishing
	waitV3(t, parked, "the parked publication")
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{})
	var wonce sync.Once
	s.v3.testLeaderWaiting = func() { wonce.Do(func() { close(waiting) }) }
	bErr := make(chan error, 1)
	go func() { _, err := s.Sync(); bErr <- err }()
	waitV3(t, waiting, "B at the leader role") // B is about to take the leader role SetWMode holds
	rel()
	if err := recvErrV3(t, setErr, "SetWMode"); err != nil {
		t.Fatal(err)
	}
	if err := recvErrV3(t, bErr, "B"); err != nil {
		t.Fatal(err)
	}
	if cur := s.v3.pub.current(); cur.WMode != WModeStrict || cur.W != 2 {
		t.Fatalf("batch after SetWMode(A): mode %d slot W %d, want A with W = H = 2 (a stale C ack would leave W 1)", cur.WMode, cur.W)
	}
}

// SetWMode crash cases: a committed mode survives a crash; a failed mode
// publication leaves the old mode on disk and latches the store.
func TestWALStoreV3SetWModeCrashCases(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	writeSynced(t, s, 1, 2)
	if err := s.SetWMode(WModeStrict); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r := mustReopenV3(t, path)
	if m := r.v3.pub.current().WMode; m != WModeStrict {
		t.Fatalf("committed SetWMode lost across a crash: mode %d", m)
	}
	crashRecoveryHardeningStore(t, r)

	s2, path2 := newStoreV3(t, WModeLagged)
	writeSynced(t, s2, 1, 2)
	boom := errors.New("injected mode slot write error")
	s2.v3.testBeforeSlotWrite = func() error { return boom }
	if err := s2.SetWMode(WModeStrict); !errors.Is(err, boom) {
		t.Fatalf("SetWMode with a failing slot write: err = %v, want the write error", err)
	}
	if s2.stickyIOFault() == nil {
		t.Fatal("a failed mode publication did not latch the store")
	}
	crashRecoveryHardeningStore(t, s2)
	r2 := mustReopenV3(t, path2)
	defer r2.Close()
	if m := r2.v3.pub.current().WMode; m != WModeLagged {
		t.Fatalf("a failed SetWMode changed the mode on disk: %d", m)
	}
}

func waitV3(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func recvErrV3(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// codex01 5b P1: a SAME-instance recovery that drops mode C's newest batch
// resets the acknowledged frontier to R, so a later refresh cannot publish
// a W the WAL no longer holds and the next Recover does not report a false
// corruption. Fresh-open tests mask this (they start from the slot W).
func TestWALStoreV3SameOpenRecoverResetsTheAcknowledgedFrontier(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	defer crashRecoveryHardeningStore(t, s)
	writeSynced(t, s, 1, 2) // slot W 1 (lagged), acknowledged W 2
	damageRecord(t, path, 2)
	rec1 := recordOffset(t, path, 1)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatalf("same-open Recover with the newest C batch damaged: %v (want the documented truncation)", err)
	}
	if got := s.v3.wack.Load(); got != 1 {
		t.Fatalf("acknowledged W after the same-open recovery = %d, want R = 1", got)
	}
	if err := s.refreshW(); err != nil {
		t.Fatal(err)
	}
	if w := s.v3.pub.current().W; w > 1 {
		t.Fatalf("refresh published W %d over a recovered prefix of 1", w)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatalf("second same-open Recover: %v (a false wal_corrupt from a stale W)", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	end1 := rec1 + int64(walEntryHeaderSizeV3+v3TestBlock)
	if !bytes.Equal(before[rec1:end1], after[rec1:end1]) {
		t.Fatal("the kept record 1 changed")
	}
	if !bytes.Equal(after[end1:walOffsetV3+v3TestWAL], make([]byte, walOffsetV3+v3TestWAL-int(end1))) {
		t.Fatal("the dropped record 2 was not fenced")
	}
	if !bytes.Equal(readV3(t, s, 1), blk(1)) {
		t.Fatal("LBA 1 lost")
	}
}

// codex01 5b P2: a scan failure under an installing descriptor keeps the
// typed not-ready outcome and leaves the file unchanged.
func TestWALStoreV3InstallingScanErrorIsTypedNotReady(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	if _, err := s.v3.pub.publish(nil, nil, func(n *descriptorV3) error {
		n.BaseState, n.BaseLSN = BaseInstalling, 0
		n.BaseCursor = n.RetainedHead
		n.BaseInstallID[0] = 7
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	if err := os.Truncate(path, walOffsetV3+100); err != nil { // the WAL region is now short
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = reopenV3(t, path)
	if !errors.Is(err, ErrBaseIncomplete) || errors.Is(err, ErrInstallSuffixDamaged) {
		t.Fatalf("installing scan error: err = %v, want ErrBaseIncomplete (not labelled damaged)", err)
	}
	after, err2 := os.ReadFile(path)
	if err2 != nil {
		t.Fatal(err2)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("a refused installing recovery changed the file")
	}
}

// codex01 5b P2: the real flusher cycle performs the mode-C idle refresh,
// also when no dirty entries remain, so the window closes without a Sync.
func TestWALStoreV3FlusherCycleRunsTheIdleRefresh(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	writeSynced(t, s, 1, 3) // slot W 2 (lagged), acknowledged W 3
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if w := s.v3.pub.current().W; w != 3 {
		t.Fatalf("slot W after a flusher cycle = %d, want 3 (the idle refresh)", w)
	}
	if err := s.flusher.flushOnceInternal(false); err != nil { // now no dirty entries
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	damageRecord(t, path, 3)
	if _, err := reopenV3(t, path); !isWALCorrupt(err) {
		t.Fatalf("after the flusher's refresh: err = %v, want wal_corrupt", err)
	}
}

// heldV3 owns a parked hook and the workers a held control starts. Its
// cleanup, registered before any worker starts and after the store crash
// cleanup, first releases the hold, then joins every worker with a bound,
// so an early Fatal never strands store shutdown (codex01 row 5d P2).
type heldV3 struct {
	t       *testing.T
	parked  chan struct{}
	release chan struct{}
	rel     func()
	once    sync.Once
	workers []chan error
}

func newHeldV3(t *testing.T) *heldV3 {
	h := &heldV3{t: t, parked: make(chan struct{}), release: make(chan struct{})}
	h.rel = sync.OnceFunc(func() { close(h.release) })
	t.Cleanup(func() {
		h.rel()
		for _, w := range h.workers {
			select {
			case <-w:
			case <-time.After(10 * time.Second):
				t.Errorf("a held worker did not finish within 10s")
			}
		}
	})
	return h
}

// hold parks the caller the first time it runs.
func (h *heldV3) hold() { h.once.Do(func() { close(h.parked); <-h.release }) }

// start runs f on a worker whose result is joined at cleanup.
func (h *heldV3) start(f func() error) chan error {
	c := make(chan error, 1)
	h.workers = append(h.workers, c)
	go func() { c <- f() }()
	return c
}

// result takes a worker's result with a bound, and leaves a copy so the
// cleanup join does not wait on a consumed channel.
func (h *heldV3) result(c chan error, what string) error {
	h.t.Helper()
	err := recvErrV3(h.t, c, what)
	c <- err
	return err
}

// notDoneV3 asserts a worker is still waiting on the leader role. It waits
// up to 300ms for the worker to (wrongly) finish: the wait only gives a
// broken implementation time to show itself, so a mutant that lets the
// worker run is caught reliably; a correct implementation passes whatever
// the timing.
func notDoneV3(t *testing.T, c chan error, what string) {
	t.Helper()
	select {
	case err := <-c:
		c <- err
		t.Fatalf("%s returned (%v) while it should be waiting on the leader role", what, err)
	case <-time.After(300 * time.Millisecond):
	}
}

// codex01 row 5d schedule A: a refresh attempted DURING a same-open
// recovery waits for it (Recover holds the leader role), then sees the
// recovered frontier and publishes nothing.
func TestWALStoreV3RefreshDuringRecoverWaitsAndPublishesNothing(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	h := newHeldV3(t)
	writeSynced(t, s, 1, 2) // slot W 1 (lagged), acknowledged W 2
	damageRecord(t, path, 2)
	s.v3.testRecoverHold = h.hold
	rec := h.start(func() error { _, err := s.Recover(); return err })
	waitV3(t, h.parked, "Recover after its decision")
	ref := h.start(s.refreshW)
	notDoneV3(t, ref, "refreshW")
	h.rel()
	if err := h.result(rec, "Recover"); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if err := h.result(ref, "refreshW"); err != nil {
		t.Fatalf("refreshW: %v", err)
	}
	if w := s.v3.pub.current().W; w > 1 {
		t.Fatalf("a refresh during recovery published W %d over the recovered prefix 1", w)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatalf("next Recover: %v (a false wal_corrupt)", err)
	}
}

// codex01 row 5d schedule B: a recovery attempted while a C batch is parked
// after its publication and before its acknowledgement waits for the batch;
// the recovery then lowers the frontier and nothing revives the dropped cut.
func TestWALStoreV3RecoverWaitsForABatchAckAndNothingRevivesTheCut(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	h := newHeldV3(t)
	writeSynced(t, s, 1, 1)
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	s.v3.testBatchBeforeAck = h.hold
	syn := h.start(func() error { _, err := s.Sync(); return err })
	waitV3(t, h.parked, "the batch before its acknowledgement")
	s.v3.testBatchBeforeAck = nil
	damageRecord(t, path, 2)
	rec := h.start(func() error { _, err := s.Recover(); return err })
	notDoneV3(t, rec, "Recover")
	h.rel()
	// The batch acknowledged before the recovery. The outer Sync either
	// completed before the recovery took the leader role, or found the
	// recovery generation moved and refused its ack: both are correct.
	if err := h.result(syn, "Sync"); err != nil && !errors.Is(err, errV3RecoveredMeanwhile) {
		t.Fatalf("Sync: %v", err)
	}
	if err := h.result(rec, "Recover"); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := s.v3.wack.Load(); got != 1 {
		t.Fatalf("acknowledged W after the recovery = %d, want R = 1 (the dropped cut revived)", got)
	}
	if err := s.refreshW(); err != nil {
		t.Fatal(err)
	}
	if w := s.v3.pub.current().W; w > 1 {
		t.Fatalf("W %d covers the fenced suffix", w)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatalf("next Recover: %v (a false wal_corrupt)", err)
	}
}

// codex01 row 5d: the outer Sync() completion is validated against the
// recovery generation. A Sync whose batch finished, then waits while a
// recovery drops its cut, must not acknowledge it; a fresh Sync after the
// recovery acknowledges normally.
func TestWALStoreV3OuterSyncCompletionIsRefusedAcrossARecovery(t *testing.T) {
	s, path := newStoreV3(t, WModeLagged)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	h := newHeldV3(t)
	writeSynced(t, s, 1, 1)
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	s.v3.testSyncBeforeCompletion = h.hold
	syn := h.start(func() error { _, err := s.Sync(); return err })
	waitV3(t, h.parked, "the outer Sync before its completion")
	s.v3.testSyncBeforeCompletion = nil
	damageRecord(t, path, 2)
	if _, err := s.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	h.rel()
	if err := h.result(syn, "Sync"); !errors.Is(err, errV3RecoveredMeanwhile) {
		t.Fatalf("outer Sync across a recovery: err = %v, want errV3RecoveredMeanwhile", err)
	}
	if R, _, _ := s.Boundaries(); R > 1 {
		t.Fatalf("synced frontier %d revived past the recovered prefix 1", R)
	}
	// Fresh lifetime: a new write and Sync after the recovery acknowledge.
	if _, err := s.Write(3, blk(3)); err != nil {
		t.Fatal(err)
	}
	if lsn, err := s.Sync(); err != nil || lsn < 2 {
		t.Fatalf("fresh Sync after the recovery: %d, %v", lsn, err)
	}
}

// codex01 row 5c P2: the empty-dirty-set branch of the flusher cycle runs the
// idle refresh on its own (discriminated separately from the flush branch).
func TestWALStoreV3FlusherEmptyCycleRunsTheIdleRefresh(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 2) // slot W 1 (lagged), acknowledged W 2
	s.dm.clear()            // nothing dirty: the cycle takes the empty branch
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if w := s.v3.pub.current().W; w != 2 {
		t.Fatalf("slot W after an empty flusher cycle = %d, want 2 (the idle refresh)", w)
	}
}
