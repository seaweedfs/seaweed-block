package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Row 5 store-level controls (DESIGN r15 6.1 to 6.6, section 11 item 5 and
// the G1 fence). "Crash" here closes the file without any further write, so
// every pwritten byte is still visible on reopen; damage is injected into
// the file directly.

const v3TestBlock = 4096
const v3TestWAL = 64 * 1024

func newStoreV3(t *testing.T, mode WMode) (*walStoreV3, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.v3")
	s, err := createWALStoreV3ForTest(path, 64, v3TestBlock, v3TestWAL, mode)
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func blk(b byte) []byte { return bytes.Repeat([]byte{b}, v3TestBlock) }

// writeSynced writes one block per LSN, each in its own Sync (batch).
func writeSynced(t *testing.T, s *walStoreV3, from, to uint32) {
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
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		r, err := openWALStoreV3(path)
		if err != nil {
			t.Fatalf("mode %d: reopen: %v", mode, err)
		}
		for lba := uint32(1); lba <= 5; lba++ {
			if !bytes.Equal(r.Read(lba), blk(byte(lba))) {
				t.Fatalf("mode %d: acknowledged LBA %d lost", mode, lba)
			}
		}
		// Writes continue after recovery and survive another reopen.
		writeSynced(t, r, 6, 7)
		_ = r.Close()
		r2, err := openWALStoreV3(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(r2.Read(7), blk(7)) || !bytes.Equal(r2.Read(1), blk(1)) {
			t.Fatalf("mode %d: writes after recovery lost", mode)
		}
		_ = r2.Close()
	}
}

func TestWALStoreV3ModeCWindowIsExactlyTheNewestBatch(t *testing.T) {
	// Newest acknowledged batch corrupted before any later Sync or refresh:
	// mode C truncates it silently (the documented window); mode A refuses.
	for _, c := range []struct {
		mode    WMode
		corrupt bool
	}{{WModeLagged, false}, {WModeStrict, true}} {
		s, path := newStoreV3(t, c.mode)
		writeSynced(t, s, 1, 3)
		_ = s.Close()
		damageRecord(t, path, 3)
		r, err := openWALStoreV3(path)
		if c.corrupt {
			if !isWALCorrupt(err) {
				t.Fatalf("mode A, newest batch corrupt: err = %v, want wal_corrupt", err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("mode C, newest batch corrupt: err = %v, want the documented silent truncation", err)
		}
		if !bytes.Equal(r.Read(2), blk(2)) || r.Read(3) != nil {
			t.Fatal("mode C: want LSN 1-2 kept and the newest batch (3) truncated")
		}
		_ = r.Close()
	}
	// The second-newest batch is protected in both modes.
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		s, path := newStoreV3(t, mode)
		writeSynced(t, s, 1, 3)
		_ = s.Close()
		damageRecord(t, path, 2)
		if _, err := openWALStoreV3(path); !isWALCorrupt(err) {
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
	_ = s.Close()
	damageRecord(t, path, 3)
	if _, err := openWALStoreV3(path); !isWALCorrupt(err) {
		t.Fatalf("after the idle refresh: err = %v, want wal_corrupt", err)
	}

	s2, path2 := newStoreV3(t, WModeLagged)
	writeSynced(t, s2, 1, 3)
	if err := s2.SetWMode(WModeStrict); err != nil {
		t.Fatal(err)
	}
	if s2.pub.current().WMode != WModeStrict {
		t.Fatal("SetWMode did not publish the new mode")
	}
	_ = s2.Close()
	damageRecord(t, path2, 3)
	if _, err := openWALStoreV3(path2); !isWALCorrupt(err) {
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
		_ = s.Close()
		damageRecord(t, path, 4) // a torn unsynced record
		r, err := openWALStoreV3(path)
		if err != nil {
			t.Fatalf("mode %d, torn unsynced tail: err = %v, want recovery", mode, err)
		}
		if !bytes.Equal(r.Read(3), blk(3)) || r.Read(4) != nil {
			t.Fatalf("mode %d: want 1-3 kept and the unsynced 4 dropped", mode)
		}
		_ = r.Close()
	}
}

// G1 (DESIGN 6.6): records dropped by one recovery must not resurface after
// a second crash. Without the fence, re-filling the gap with a record of the
// same size re-chains the old record behind it.
func resurrectionSchedule(t *testing.T, mode WMode, skipFence bool) (resurrected bool) {
	t.Helper()
	s, path := newStoreV3(t, mode)
	writeSynced(t, s, 1, 3)
	if _, err := s.Write(4, blk(4)); err != nil { // unsynced
		t.Fatal(err)
	}
	if _, err := s.Write(5, blk(5)); err != nil { // unsynced
		t.Fatal(err)
	}
	_ = s.Close()
	damageRecord(t, path, 4) // a torn LSN 4: recovery keeps 1-3 and drops the trusted 5
	r := openV3WithFence(t, path, skipFence)
	if r.Read(5) != nil {
		t.Fatal("first recovery kept LSN 5 above the gap")
	}
	if _, err := r.Write(9, blk(0x99)); err != nil { // re-fills LSN 4's place, same size
		t.Fatal(err)
	}
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
	r2, err := openWALStoreV3(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	return r2.Read(5) != nil
}

// openV3WithFence opens with the fence optionally disabled (mutant switch),
// by re-running recovery on a store whose test switch is set first.
func openV3WithFence(t *testing.T, path string, skipFence bool) *walStoreV3 {
	t.Helper()
	if !skipFence {
		r, err := openWALStoreV3(path)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	fd, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	testSkipFenceOnRecoverV3 = true
	defer func() { testSkipFenceOnRecoverV3 = false }()
	r, err := recoverWALStoreV3(fd)
	if err != nil {
		t.Fatal(err)
	}
	return r
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
	_ = s.Close()
	damageRecord(t, path, 4)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	e := uint64(recordOffset(t, path, 3)) + walEntryHeaderSizeV3 + v3TestBlock // end of LSN 3
	r, err := openWALStoreV3(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.Close()
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
	// E = 3 x 4166 = 12498 is not page-aligned; the byte just before it
	// belongs to LSN 3 and is unchanged (checked above), so nothing rounded down.
	if e%4096 == 0 {
		t.Fatal("test setup: E is page-aligned; it should exercise an unaligned head")
	}
}

func TestWALStoreV3AppendWriteErrorLatchesTheStore(t *testing.T) {
	s, _ := newStoreV3(t, WModeLagged)
	defer s.Close()
	writeSynced(t, s, 1, 1)
	boom := errors.New("injected WAL pwrite error")
	s.w.writeAt = func([]byte, int64) (int, error) { return 0, boom }
	if _, err := s.Write(2, blk(2)); !errors.Is(err, boom) {
		t.Fatalf("append: err = %v, want the pwrite error", err)
	}
	if s.sticky() == nil {
		t.Fatal("an append pwrite error did not latch the store-global fault")
	}
	if _, err := s.Sync(); err == nil {
		t.Fatal("Sync acknowledged after an append write error")
	}
	if _, err := s.pub.publish(&s.mu, nil, nil); err == nil {
		t.Fatal("a descriptor publication succeeded after an append write error")
	}
}
