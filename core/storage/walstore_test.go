package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestWALStore_FailedAppendDoesNotConsumeLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := createLegacyWALStoreForTest(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	bad, err := os.CreateTemp(t.TempDir(), "closed-wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.Close(); err != nil {
		t.Fatal(err)
	}
	s.wal.fd = bad
	if _, err := s.Write(0, makeBlock(4096, 0xA1)); err == nil {
		t.Fatal("Write with closed WAL fd succeeded")
	}
	if got := s.NextLSN(); got != 1 {
		t.Fatalf("NextLSN after failed append=%d want 1", got)
	}

	// A failed append is sticky (ErrAppendFailed): no later write reuses
	// its LSN on this open store, single or batch.
	s.wal.fd = s.fd
	if _, err := s.Write(0, makeBlock(4096, 0xA2)); !errors.Is(err, ErrAppendFailed) {
		t.Fatalf("Write after a failed append: %v; want ErrAppendFailed", err)
	}
	if _, err := s.WriteBatch(1, [][]byte{makeBlock(4096, 0xC1), makeBlock(4096, 0xC2)}); !errors.Is(err, ErrAppendFailed) {
		t.Fatalf("WriteBatch after a failed append: %v; want ErrAppendFailed", err)
	}
	if got := s.NextLSN(); got != 1 {
		t.Fatalf("NextLSN after refused appends=%d want 1", got)
	}
	_ = s.Close()

	// A reopen starts from what is on disk (nothing of the failed append):
	// the first write gets LSN 1, the next batch LSNs 2 and 3.
	r, err := openLegacyWALStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	lsn, err := r.Write(0, makeBlock(4096, 0xA2))
	if err != nil || lsn != 1 {
		t.Fatalf("first write after a reopen LSN=%d err=%v want 1", lsn, err)
	}
	lsns, err := r.WriteBatch(1, [][]byte{makeBlock(4096, 0xC1), makeBlock(4096, 0xC2)})
	if err != nil || len(lsns) != 2 || lsns[0] != 2 || lsns[1] != 3 {
		t.Fatalf("batch after a reopen LSNs=%v err=%v want [2 3]", lsns, err)
	}
}

func TestWALStore_SyncDoesNotClaimConcurrentWrite(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if lsn, err := s.Write(0, makeBlock(4096, 0xA1)); err != nil || lsn != 1 {
		t.Fatalf("first Write lsn=%d err=%v", lsn, err)
	}

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	s.syncCache = func() error {
		close(syncStarted)
		<-releaseSync
		return nil
	}
	type syncResult struct {
		frontier uint64
		err      error
	}
	result := make(chan syncResult, 1)
	go func() {
		frontier, syncErr := s.Sync()
		result <- syncResult{frontier: frontier, err: syncErr}
	}()

	<-syncStarted
	if lsn, err := s.Write(1, makeBlock(4096, 0xA2)); err != nil || lsn != 2 {
		t.Fatalf("concurrent Write lsn=%d err=%v", lsn, err)
	}
	close(releaseSync)
	got := <-result
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.frontier != 1 {
		t.Fatalf("Sync frontier=%d want captured frontier 1", got.frontier)
	}
	R, _, H := s.Boundaries()
	if R != 1 || H != 2 {
		t.Fatalf("boundaries R=%d H=%d want R=1 H=2", R, H)
	}
}

func TestWALStore_DirectFrontierMetadataFailureRetainsRetryState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := createLegacyWALStoreForTest(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Write(0, makeBlock(4096, 0xA1)); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(10)

	savedSync := s.syncDirectFrontierMetadata
	s.syncDirectFrontierMetadata = func([]byte) error {
		return errors.New("injected superblock sync failure")
	}
	if _, err := s.Sync(); err == nil {
		t.Fatal("Sync with injected metadata failure succeeded")
	}
	if s.checkpointLSN != 0 {
		t.Fatalf("checkpointLSN=%d want 0 after failed metadata sync", s.checkpointLSN)
	}
	if s.pendingDirectFrontierLSN != 10 {
		t.Fatalf("pendingDirectFrontierLSN=%d want retry target 10", s.pendingDirectFrontierLSN)
	}
	if R, _, _ := s.Boundaries(); R != 0 {
		t.Fatalf("durable frontier=%d want 0 after failed metadata sync", R)
	}

	// The failed fsync is sticky (ErrFsyncFailed): the retry is refused, the
	// in-memory frontier is never published, and a reopen starts from what
	// is on disk.
	s.syncDirectFrontierMetadata = savedSync
	if _, err := s.Sync(); !errors.Is(err, ErrFsyncFailed) {
		t.Fatalf("retry after a failed metadata fsync: %v; want ErrFsyncFailed", err)
	}
	if s.checkpointLSN != 0 {
		t.Fatalf("refused retry published checkpoint %d", s.checkpointLSN)
	}
	_ = s.Close()
	reopened, err := openLegacyWALStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	reopenedOpen := true
	t.Cleanup(func() {
		if reopenedOpen {
			_ = reopened.Close()
		}
	})
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := reopened.CheckpointLSN(); got != 0 {
		t.Fatalf("reopened checkpointLSN=%d want 0 (the frontier was never durable)", got)
	}
	if reopened.wal.logicalHeadValue() == 0 {
		t.Fatal("recovery did not reconstruct the WAL head of the LSN 1 record")
	}
	reopened.AdvanceFrontier(10)
	frontier, err := reopened.Sync()
	if err != nil || frontier != 10 || reopened.CheckpointLSN() != 10 {
		t.Fatalf("frontier after a reopen=%d checkpoint=%d err=%v want 10/10", frontier, reopened.CheckpointLSN(), err)
	}
	if got, want := reopened.sb.WALHead, reopened.wal.logicalHeadValue(); got != want || got == 0 {
		t.Fatalf("superblock WALHead byte cursor=%d want %d (nonzero)", got, want)
	}
	reopened.committer.Stop()
	_ = reopened.fd.Close() // crash
	reopenedOpen = false
	again, err := openLegacyWALStoreForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = again.Close() })
	if _, err := again.Recover(); err != nil {
		t.Fatal(err)
	}
	if got := again.CheckpointLSN(); got != 10 {
		t.Fatalf("checkpoint after a crash=%d want the durable frontier 10", got)
	}
}

// TestWALStore_WriteSyncCloseReopenRead is the headline acceptance:
// a clean Sync → Close → Open → Recover round-trip preserves every
// acked block.
func TestWALStore_WriteSyncCloseReopenRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 4; i++ {
			data := makeBlock(4096, byte(0xA0+i))
			if _, err := s.Write(i, data); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	{
		s, err := OpenWALStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 4; i++ {
			got, err := s.Read(i)
			if err != nil {
				t.Fatal(err)
			}
			want := makeBlock(4096, byte(0xA0+i))
			if !bytes.Equal(got, want) {
				t.Fatalf("LBA %d: bytes did not survive close+reopen", i)
			}
		}
	}
}

func TestWALStore_WriteBatchSyncCloseReopenRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		blocks := [][]byte{
			makeBlock(4096, 0xA1),
			makeBlock(4096, 0xA2),
			makeBlock(4096, 0xA3),
		}
		lsns, err := s.WriteBatch(2, blocks)
		if err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
		if got, want := len(lsns), len(blocks); got != want {
			t.Fatalf("WriteBatch LSN count=%d want %d", got, want)
		}
		for i := 1; i < len(lsns); i++ {
			if lsns[i] != lsns[i-1]+1 {
				t.Fatalf("WriteBatch LSNs not consecutive: %v", lsns)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	{
		s, err := OpenWALStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		for i, want := range [][]byte{
			makeBlock(4096, 0xA1),
			makeBlock(4096, 0xA2),
			makeBlock(4096, 0xA3),
		} {
			got, err := s.Read(uint32(i + 2))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("LBA %d: batch bytes did not survive close+reopen", i+2)
			}
		}
	}
}

func TestWALStore_WriteCopiesCallerBufferIntoWALRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	original := makeBlock(4096, 0xC1)
	caller := append([]byte(nil), original...)
	if _, err := s.Write(3, caller); err != nil {
		t.Fatalf("Write: %v", err)
	}
	for i := range caller {
		caller[i] = 0x7E
	}
	got, err := s.Read(3)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("WALStore retained caller buffer instead of encoded WAL bytes")
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatalf("OpenWALStore: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	got, err = reopened.Read(3)
	if err != nil {
		t.Fatalf("Read after recover: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("recovered WAL bytes changed after caller buffer mutation")
	}
}

func TestWALStore_WriteBatchCopiesCallerBuffersIntoWALRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	originals := [][]byte{
		makeBlock(4096, 0xD1),
		makeBlock(4096, 0xD2),
		makeBlock(4096, 0xD3),
	}
	blocks := make([][]byte, len(originals))
	for i := range originals {
		blocks[i] = append([]byte(nil), originals[i]...)
	}
	if _, err := s.WriteBatch(4, blocks); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	for _, block := range blocks {
		for i := range block {
			block[i] = 0x7F
		}
	}
	for i, want := range originals {
		got, err := s.Read(uint32(4 + i))
		if err != nil {
			t.Fatalf("Read LBA %d: %v", 4+i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d retained caller buffer instead of encoded WAL bytes", 4+i)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatalf("OpenWALStore: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for i, want := range originals {
		got, err := reopened.Read(uint32(4 + i))
		if err != nil {
			t.Fatalf("Read after recover LBA %d: %v", 4+i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("recovered LBA %d changed after caller buffer mutation", 4+i)
		}
	}
}

// TestWALStore_AckedWritesSurviveSimulatedCrash is the real
// crash-consistency proof. A clean Close persists superblock state,
// which would mask any per-write durability bug. This test bypasses
// Close entirely, simulating a kill -9.
//
// Pattern:
//  1. Write blocks 0,1,2 + Sync (acked, must survive)
//  2. Write blocks 3,4   (NOT followed by Sync — may or may not survive)
//  3. Bypass Close — drop the file handle without finalizing
//     anything (no superblock update, no fsync, no new commit batch)
//  4. Reopen + Recover
//  5. Acked blocks 0,1,2 MUST be present and intact
//  6. Post-Sync blocks 3,4 may be present OR absent — never corrupt
//     versions of acked data
func TestWALStore_AckedWritesSurviveSimulatedCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	// Write + Sync + simulated crash. NO Close call.
	func() {
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		// A simulated process crash cannot leave an old flusher using the
		// descriptor. Stop it BEFORE writes: stopping after them could flush
		// the very WAL replay this test is intended to exercise.
		crash := sync.OnceFunc(func() {
			s.DisableAutoFlushForRecoveryTest()
			crashRecoveryHardeningStore(t, s)
		})
		t.Cleanup(crash)
		s.DisableAutoFlushForRecoveryTest()
		select {
		case <-s.flusher.doneCh:
		default:
			t.Fatal("crash fixture flusher is still running before writes")
		}
		for i := uint32(0); i < 3; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(0xA0+i))); err != nil {
				t.Fatal(err)
			}
		}
		// Acked: these MUST survive crash.
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if cp := s.CheckpointLSN(); cp != 0 {
			t.Fatalf("crash fixture checkpoint=%d, want uncheckpointed WAL", cp)
		}
		// Unacked: may or may not survive.
		for i := uint32(3); i < 5; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(0xA0+i))); err != nil {
				t.Fatal(err)
			}
		}
		// Drop descriptors without store.Close(), a final checkpoint or
		// another Sync. Cleanup also runs on an earlier assertion failure.
		crash()
	}()

	// Reopen + Recover. The acked blocks must be present.
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatalf("OpenWALStore after simulated crash: %v", err)
	}
	defer s.Close()

	if _, err := s.Recover(); err != nil {
		t.Fatalf("Recover after simulated crash: %v", err)
	}

	// Acked: must be intact.
	for i := uint32(0); i < 3; i++ {
		got, err := s.Read(i)
		if err != nil {
			t.Fatalf("Read LBA %d: %v", i, err)
		}
		want := makeBlock(4096, byte(0xA0+i))
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d: ACKED data did not survive simulated crash; got[0]=%02x want[0]=%02x len(got)=%d",
				i, got[0], want[0], len(got))
		}
	}

	// Unacked: may or may not be present, but if present must be
	// the bytes we wrote (never some other LBA's data).
	for i := uint32(3); i < 5; i++ {
		got, err := s.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		want := makeBlock(4096, byte(0xA0+i))
		zero := make([]byte, 4096)
		if !bytes.Equal(got, want) && !bytes.Equal(got, zero) {
			t.Fatalf("LBA %d: post-Sync write recovered to non-write/non-zero state — corruption: got[0]=%02x",
				i, got[0])
		}
	}
}

// TestWALStore_RecoverIsIdempotent: calling Recover twice on the same
// on-disk state must yield identical results, per the LogicalStorage
// contract.
func TestWALStore_RecoverIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 3; i++ {
			_, _ = s.Write(i, makeBlock(4096, byte(i+1)))
		}
		_, _ = s.Sync()
		_ = s.Close()
	}

	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	r1, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Fatalf("Recover not idempotent: r1=%d r2=%d", r1, r2)
	}
}

// TestWALStore_OpenRejectsBadMagic: corruption-detection sanity.
func TestWALStore_OpenRejectsBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bogus.bin")
	if err := os.WriteFile(path, []byte("not_a_valid_store"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenWALStore(path)
	if err == nil {
		t.Fatal("OpenWALStore on bogus file should error")
	}
}
