package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
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
		var once sync.Once
		s.v3.testBeforeSlotWrite = func() error {
			once.Do(func() { close(parked); <-release })
			return nil
		}
		aErr := make(chan error, 1)
		go func() { _, err := s.Sync(); aErr <- err }()
		<-parked
		boom := errors.New("injected append write error")
		s.v3.w.writeAt = func([]byte, int64) (int, error) { return 0, boom }
		bErr := make(chan error, 1)
		go func() { _, err := s.Write(3, blk(3)); bErr <- err }()
		for s.mu.TryLock() { // wait until B holds s.mu (it then blocks on descMu)
			s.mu.Unlock()
			runtime.Gosched()
		}
		close(release)
		if err := <-bErr; !errors.Is(err, boom) {
			t.Fatalf("mode %d: B: err = %v, want the append write error", mode, err)
		}
		if err := <-aErr; err == nil {
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
	defer crashRecoveryHardeningStore(t, s)
	writeSynced(t, s, 1, 1)
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	parked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.v3.testBeforeSlotWrite = func() error {
		once.Do(func() { close(parked); <-release })
		return nil
	}
	aErr := make(chan error, 1)
	go func() { _, err := s.Sync(); aErr <- err }()
	<-parked
	setErr := make(chan error, 1)
	go func() { setErr <- s.SetWMode(WModeStrict) }()
	select {
	case err := <-setErr:
		t.Fatalf("SetWMode returned (%v) while a C batch was held", err)
	default:
	}
	close(release)
	if err := <-aErr; err != nil {
		t.Fatal(err)
	}
	if err := <-setErr; err != nil {
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
	defer crashRecoveryHardeningStore(t, s)
	writeSynced(t, s, 1, 1)
	parked, release := make(chan struct{}), make(chan struct{})
	s.v3.testSetWModeHoldingRole = func() { close(parked); <-release }
	setErr := make(chan error, 1)
	go func() { setErr <- s.SetWMode(WModeStrict) }() // parks holding the leader role, before publishing
	<-parked
	if _, err := s.Write(2, blk(2)); err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{})
	var wonce sync.Once
	s.v3.testLeaderWaiting = func() { wonce.Do(func() { close(waiting) }) }
	bErr := make(chan error, 1)
	go func() { _, err := s.Sync(); bErr <- err }()
	<-waiting // B is about to take the leader role SetWMode holds
	close(release)
	if err := <-setErr; err != nil {
		t.Fatal(err)
	}
	if err := <-bErr; err != nil {
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
