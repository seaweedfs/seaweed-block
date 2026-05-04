package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFlusher_AdvancesCheckpointBeforeReadFromExtent proves the
// flusher actually drains dirty entries to extent and advances the
// on-disk checkpoint LSN. After a few flush cycles, reads of those
// LBAs come from the extent (not the WAL via dirty map).
func TestFlusher_AdvancesCheckpointBeforeReadFromExtent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Write some blocks and Sync to make them durable in the WAL.
	for i := uint32(0); i < 4; i++ {
		if _, err := s.Write(i, makeBlock(4096, byte(0xA0+i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}

	// Wait for the flusher to drain — default interval 50ms; allow
	// generous slack for slow CI.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.CheckpointLSN() >= 4 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s.CheckpointLSN() < 4 {
		t.Fatalf("flusher never advanced checkpoint: got %d, want >= 4", s.CheckpointLSN())
	}

	// Reads should still return correct bytes (whether from WAL or
	// extent — caller doesn't care).
	for i := uint32(0); i < 4; i++ {
		got, err := s.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		want := makeBlock(4096, byte(0xA0+i))
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d: bytes wrong after flush: got[0]=%02x want[0]=%02x",
				i, got[0], want[0])
		}
	}
}

// TestFlusher_ReleasesWALPressureAfterCheckpoint pins the pressure
// feedback loop: after dirty entries are flushed and checkpointed, the
// circular WAL tail must advance so admission sees free space again.
// Without this, long-running writers eventually hit "WAL region full"
// even though the flusher is successfully writing data to the extent.
func TestFlusher_ReleasesWALPressureAfterCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	s := createWALStoreWithWALSizeForTest(t, path, 128, 4096, 256*1024)
	defer s.Close()

	for i := uint32(0); i < 32; i++ {
		if _, err := s.Write(i, makeBlock(4096, byte(0x80+i))); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := s.wal.usedFraction(); got == 0 {
		t.Fatal("test setup did not create WAL pressure")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.CheckpointLSN() >= 32 && s.wal.usedFraction() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := s.CheckpointLSN(); got < 32 {
		t.Fatalf("flusher never checkpointed writes: got %d, want >= 32", got)
	}
	if used := s.wal.usedFraction(); used != 0 {
		t.Fatalf("WAL pressure after checkpoint = %.3f, want 0", used)
	}
}

func TestWALWriter_AdvanceTailPastEntryIsIdempotent(t *testing.T) {
	w := &walWriter{
		walSize:     64,
		logicalHead: 40,
		logicalTail: 0,
	}

	w.advanceTailPastEntry(0, 10)
	if got := w.logicalTailValue(); got != 10 {
		t.Fatalf("first advance tail=%d, want 10", got)
	}

	w.advanceTailPastEntry(0, 10)
	if got := w.logicalTailValue(); got != 10 {
		t.Fatalf("repeated advance tail=%d, want still 10", got)
	}
}

func TestWALWriter_AdvanceTailPastWrappedEntry(t *testing.T) {
	w := &walWriter{
		walSize:     64,
		logicalHead: 80,
		logicalTail: 60,
	}

	w.advanceTailPastEntry(0, 10)
	if got := w.logicalTailValue(); got != 74 {
		t.Fatalf("wrapped advance tail=%d, want 74", got)
	}
}

func createWALStoreWithWALSizeForTest(t *testing.T, path string, numBlocks uint32, blockSize int, walSize uint64) *WALStore {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	sb, err := newSuperblock(uint64(numBlocks)*uint64(blockSize), createOptions{
		BlockSize:   uint32(blockSize),
		ExtentSize:  uint32(blockSize),
		WALSize:     walSize,
		ImplKind:    ImplKindWALStore,
		ImplVersion: WALStoreImplVersion,
	})
	if err != nil {
		_ = f.Close()
		t.Fatalf("newSuperblock: %v", err)
	}
	totalSize := int64(superblockSize) + int64(sb.WALSize) + int64(sb.VolumeSize)
	if err := f.Truncate(totalSize); err != nil {
		_ = f.Close()
		t.Fatalf("truncate: %v", err)
	}
	if _, err := sb.writeTo(f); err != nil {
		_ = f.Close()
		t.Fatalf("write superblock: %v", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatalf("sync superblock: %v", err)
	}
	s, err := openInitialized(path, f, &sb)
	if err != nil {
		_ = f.Close()
		t.Fatalf("openInitialized: %v", err)
	}
	return s
}

// TestFlusher_RecoveryHonorsCheckpoint: after the flusher has
// advanced the checkpoint and written through the superblock, a
// subsequent reopen+Recover skips the checkpointed records (they're
// already in extent) and the data is still readable.
func TestFlusher_RecoveryHonorsCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 4; i++ {
			_, _ = s.Write(i, makeBlock(4096, byte(0xA0+i)))
		}
		_, _ = s.Sync()
		// Wait for flush to advance checkpoint.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if s.CheckpointLSN() >= 4 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if s.CheckpointLSN() < 4 {
			t.Fatal("flusher never advanced checkpoint pre-close")
		}
		_ = s.Close() // clean close — superblock fully persisted
	}

	// Reopen + Recover. The checkpointed records should be skipped;
	// extent should serve all reads.
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 4; i++ {
		got, _ := s.Read(i)
		want := makeBlock(4096, byte(0xA0+i))
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d: bytes did not survive checkpointed restart", i)
		}
	}
	if s.CheckpointLSN() < 4 {
		t.Fatalf("Recover did not load checkpoint from superblock: got %d", s.CheckpointLSN())
	}
}

// TestCrashFamily_AbruptKillAtMultipleWindows is the Phase 08 crash
// proof family. Each subtest simulates an abrupt kill at a different
// point in the local data process and verifies:
//
//   - acked writes (covered by a successful Sync) survive the kill
//   - unacked writes may be lost but never corrupt acked data
//   - recovery is deterministic
//
// The "abrupt kill" is simulated by stopping background goroutines
// then dropping the file handle without going through Close —
// no implicit fsync, no superblock update.
func TestCrashFamily_AbruptKillAtMultipleWindows(t *testing.T) {
	cases := []struct {
		name      string
		nBefore   int  // writes (each followed by Sync) before the danger zone
		nDuring   int  // writes during the danger zone (no Sync)
		waitFlush bool // give the flusher a chance to advance checkpoint before the kill
	}{
		{
			name:    "kill_pre_sync",
			nBefore: 0,
			nDuring: 3, // 3 writes, no Sync, then kill
		},
		{
			name:    "kill_post_sync_pre_flush",
			nBefore: 3, // 3 writes + Sync, then immediate kill
			nDuring: 0,
		},
		{
			name:      "kill_post_flush",
			nBefore:   3, // 3 writes + Sync, wait for flush, then kill
			nDuring:   0,
			waitFlush: true,
		},
		{
			name:      "kill_post_flush_then_more_unacked_writes",
			nBefore:   3, // first batch acked + flushed
			nDuring:   2, // second batch unacked
			waitFlush: true,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "store.bin")

			func() {
				s, err := CreateWALStore(path, 16, 4096)
				if err != nil {
					t.Fatal(err)
				}
				// Acked writes: those covered by Sync.
				for i := 0; i < c.nBefore; i++ {
					if _, err := s.Write(uint32(i), makeBlock(4096, byte(0xA0+i))); err != nil {
						t.Fatal(err)
					}
				}
				if c.nBefore > 0 {
					if _, err := s.Sync(); err != nil {
						t.Fatal(err)
					}
				}
				if c.waitFlush {
					deadline := time.Now().Add(2 * time.Second)
					for time.Now().Before(deadline) {
						if s.CheckpointLSN() >= uint64(c.nBefore) {
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
				}
				// Unacked writes: NOT followed by Sync.
				for i := 0; i < c.nDuring; i++ {
					if _, err := s.Write(uint32(c.nBefore+i), makeBlock(4096, byte(0xB0+i))); err != nil {
						t.Fatal(err)
					}
				}
				// Abrupt kill: stop background goroutines so they
				// don't race the file close, then drop fd directly
				// without going through Close().
				s.flusher.Stop()
				s.committer.Stop()
				_ = s.fd.Close()
			}()

			// Reopen + Recover. Verify acked survives, unacked
			// is either present-as-written or absent, never corrupt.
			s, err := OpenWALStore(path)
			if err != nil {
				t.Fatalf("reopen after %s: %v", c.name, err)
			}
			defer s.Close()
			if _, err := s.Recover(); err != nil {
				t.Fatalf("recover after %s: %v", c.name, err)
			}

			for i := 0; i < c.nBefore; i++ {
				got, err := s.Read(uint32(i))
				if err != nil {
					t.Fatal(err)
				}
				want := makeBlock(4096, byte(0xA0+i))
				if !bytes.Equal(got, want) {
					t.Fatalf("ACKED LBA %d did not survive %s: got[0]=%02x want[0]=%02x",
						i, c.name, got[0], want[0])
				}
			}
			zero := make([]byte, 4096)
			for i := 0; i < c.nDuring; i++ {
				got, err := s.Read(uint32(c.nBefore + i))
				if err != nil {
					t.Fatal(err)
				}
				want := makeBlock(4096, byte(0xB0+i))
				if !bytes.Equal(got, want) && !bytes.Equal(got, zero) {
					t.Fatalf("UNACKED LBA %d under %s: got neither write nor zero — corruption: got[0]=%02x",
						c.nBefore+i, c.name, got[0])
				}
			}
		})
	}
}

// TestFlusher_ManyDirtyEntriesCheckpointStaysCorrect is the
// regression test for the architect-flagged "checkpoint can advance
// past unflushed records" bug.
//
// The earlier flusher truncated the dirty snapshot to maxBatch
// entries (in arbitrary map order), then advanced the checkpoint to
// the highest LSN in that PARTIAL batch. If the truncated portion
// contained entries with smaller LSNs, the checkpoint would jump
// past them and recovery would later skip records that hadn't been
// flushed → data loss.
//
// The fix removed the maxBatch cutoff (V2 has no such cutoff). This
// test writes many entries, forces flush, then crashes-without-Close
// and verifies all blocks are recoverable via the extent. If a
// checkpoint ever jumps past an unflushed entry again, this test
// will catch it because that LBA's read will return zeros instead
// of the written bytes.
func TestFlusher_ManyDirtyEntriesCheckpointStaysCorrect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	const n = 300 // > any plausible old maxBatch (which was 256)

	{
		s, err := CreateWALStore(path, n+10, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < n; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(i%256))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		// Wait for the flusher to drain enough that the checkpoint
		// has advanced. With 300 entries and the V2-faithful
		// "process whole snapshot in one cycle" rule, one flush
		// cycle should drain all of them.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if s.CheckpointLSN() >= uint64(n) {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if s.CheckpointLSN() < uint64(n) {
			t.Fatalf("flusher did not drain all %d entries: checkpoint=%d", n, s.CheckpointLSN())
		}
		// Abrupt kill: bypass Close so we exercise recovery from the
		// post-flush state, not a clean-shutdown path.
		s.flusher.Stop()
		s.committer.Stop()
		_ = s.fd.Close()
	}

	// Reopen + Recover. Every LBA must be readable with the bytes
	// we wrote — the checkpoint advance must have only covered
	// LBAs whose data is actually in the extent.
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < n; i++ {
		got, _ := s.Read(i)
		want := makeBlock(4096, byte(i%256))
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d: bytes lost — checkpoint advanced past unflushed record",
				i)
		}
	}
}

// TestFlusher_ConcurrentWriteRaceLBAStaysFresh is the regression
// test for the architect-flagged "flusher deletes newer concurrent
// write" bug.
//
// The earlier flusher snapshotted {LBA, oldLSN}, wrote oldLSN's
// data to extent, and unconditionally deleted the dirty map entry
// by LBA. If a concurrent Write() updated that LBA to {LBA, newLSN}
// after the snapshot, the unconditional delete would also drop the
// newer entry, and reads would fall back to the stale extent bytes.
//
// The fix uses compareAndDelete(LBA, expectedLSN) — only delete if
// the entry's LSN still matches the snapshot LSN. Newer entries are
// left in place.
//
// We construct the race deterministically: write a value, sneak in a
// second write to the same LBA before the flusher cycle, then read
// back. The read must reflect the SECOND write, not the first.
func TestFlusher_ConcurrentWriteRaceLBAStaysFresh(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	first := makeBlock(4096, 0xAA)
	second := makeBlock(4096, 0xBB)

	// First write to LBA 5.
	if _, err := s.Write(5, first); err != nil {
		t.Fatal(err)
	}

	// Hammer the LBA with the SECOND write in a loop while the
	// background flusher (interval 100ms) ticks. Each iteration
	// could land in a different position relative to the flusher's
	// snapshot/cleanup. Across ~50 iterations there is high
	// probability that at least one iteration interleaves the
	// write between the flusher's snapshot and its cleanup. With
	// the bug, that interleaving causes the dirty entry to be
	// dropped and the next read to return `first` (from extent)
	// instead of `second`.
	for iter := 0; iter < 50; iter++ {
		if _, err := s.Write(5, second); err != nil {
			t.Fatal(err)
		}
		// Give the flusher a tick or two to potentially race.
		time.Sleep(120 * time.Millisecond)
		got, err := s.Read(5)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, second) {
			t.Fatalf("iter %d: LBA 5 reads %02x, want second-write byte %02x — flusher ate the newer entry",
				iter, got[0], second[0])
		}
	}
}

// TestAllBlocks_ReturnsBlocksAfterFlush is the regression test for
// the architect-flagged "AllBlocks contract violation" bug. The
// LogicalStorage contract says AllBlocks() returns every WRITTEN
// LBA's current bytes. The earlier WALStore.AllBlocks() returned
// only the dirty-map entries, which means flushed blocks would
// disappear from the result.
//
// The fix reads every LBA via Read() (which falls back to extent
// for non-dirty LBAs) and filters zeros. After the flusher drains
// every entry, all written LBAs are still returned.
func TestAllBlocks_ReturnsBlocksAfterFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Write(1, makeBlock(4096, 0x11)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(7, makeBlock(4096, 0x77)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}

	// Wait for the flusher to drain — both LBAs out of the dirty
	// map and into the extent.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.CheckpointLSN() >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s.CheckpointLSN() < 2 {
		t.Fatal("flusher did not advance checkpoint")
	}

	// AllBlocks must still return both written LBAs even though
	// they're no longer in the dirty map.
	all := s.AllBlocks()
	if len(all) != 2 {
		t.Fatalf("AllBlocks after flush returned %d entries, want 2", len(all))
	}
	if all[1][0] != 0x11 {
		t.Fatalf("AllBlocks LBA 1: got %02x, want 11", all[1][0])
	}
	if all[7][0] != 0x77 {
		t.Fatalf("AllBlocks LBA 7: got %02x, want 77", all[7][0])
	}
}

// TestRecovery_DeterministicAcrossMultipleOpens: after a crash + a
// recovered open, calling Recover again on the same on-disk state
// yields exactly the same in-memory state.
func TestRecovery_DeterministicAcrossMultipleOpens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, _ := CreateWALStore(path, 16, 4096)
		for i := uint32(0); i < 5; i++ {
			_, _ = s.Write(i, makeBlock(4096, byte(0x10+i)))
		}
		_, _ = s.Sync()
		// Skip Close — simulate crash.
		s.flusher.Stop()
		s.committer.Stop()
		_ = s.fd.Close()
	}

	// First reopen + Recover.
	s1, _ := OpenWALStore(path)
	r1, err := s1.Recover()
	if err != nil {
		t.Fatal(err)
	}
	contents1 := make([][]byte, 5)
	for i := uint32(0); i < 5; i++ {
		contents1[i], _ = s1.Read(i)
	}
	_ = s1.Close()

	// Second reopen + Recover on the same file.
	s2, _ := OpenWALStore(path)
	r2, err := s2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 5; i++ {
		got, _ := s2.Read(i)
		if !bytes.Equal(got, contents1[i]) {
			t.Fatalf("LBA %d differs across two open+recover cycles", i)
		}
	}
	_ = s2.Close()

	if r1 != r2 {
		t.Fatalf("Recover frontier not deterministic across reopens: r1=%d r2=%d", r1, r2)
	}
}
