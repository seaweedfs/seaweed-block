package parallelwal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func testConfig() Config {
	return Config{
		NumBlocks:    32,
		BlockSize:    512,
		LaneCount:    4,
		StripeBlocks: 1,
		SlotsPerLane: 16,
		QueueDepth:   8,
	}
}

func testBlock(fill byte, size int) []byte {
	out := make([]byte, size)
	for i := range out {
		out[i] = fill
	}
	return out
}

func createTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "parallelwal.bin")
	s, err := CreateStoreWithConfig(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !s.closed {
			_ = s.Close()
		}
	})
	return s, path
}

func crashStore(t *testing.T, s *Store) {
	t.Helper()
	s.mu.Lock()
	if len(s.pending) != 0 {
		s.mu.Unlock()
		t.Fatal("crashStore called with writes in flight")
	}
	s.closed = true
	s.mu.Unlock()
	if err := s.fd.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateSyncRecoverAndScan(t *testing.T) {
	s, path := createTestStore(t)
	for lba := uint32(0); lba < 4; lba++ {
		lsn, err := s.Write(lba, testBlock(byte(lba+1), s.BlockSize()))
		if err != nil {
			t.Fatal(err)
		}
		if want := uint64(lba + 1); lsn != want {
			t.Fatalf("Write(%d) LSN=%d want=%d", lba, lsn, want)
		}
	}
	if got, err := s.Sync(); err != nil || got != 4 {
		t.Fatalf("Sync=(%d,%v) want=(4,nil)", got, err)
	}
	if R, S, H := s.Boundaries(); R != 4 || S != 1 || H != 4 {
		t.Fatalf("Boundaries=(%d,%d,%d) want=(4,1,4)", R, S, H)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 4 {
		t.Fatalf("Recover=(%d,%v) want=(4,nil)", got, err)
	}
	for lba := uint32(0); lba < 4; lba++ {
		got, err := reopened.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if want := testBlock(byte(lba+1), reopened.BlockSize()); string(got) != string(want) {
			t.Fatalf("Read(%d) mismatch", lba)
		}
	}
	var gotLSNs []uint64
	if err := reopened.ScanLBAs(0, func(entry storage.RecoveryEntry) error {
		gotLSNs = append(gotLSNs, entry.LSN)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(gotLSNs) != 4 {
		t.Fatalf("ScanLBAs emitted %v", gotLSNs)
	}
	for i, got := range gotLSNs {
		if want := uint64(i + 1); got != want {
			t.Fatalf("ScanLBAs[%d]=%d want=%d", i, got, want)
		}
	}
}

func TestCrossLaneCompletionPublishesContiguousLSNs(t *testing.T) {
	s, _ := createTestStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.lanes[0].beforeWrite = func(*writeRequest) {
		close(entered)
		<-release
	}

	type result struct {
		lsn uint64
		err error
	}
	first := make(chan result, 1)
	second := make(chan result, 1)
	go func() {
		lsn, err := s.Write(0, testBlock(1, s.BlockSize()))
		first <- result{lsn: lsn, err: err}
	}()
	<-entered
	go func() {
		lsn, err := s.Write(1, testBlock(2, s.BlockSize()))
		second <- result{lsn: lsn, err: err}
	}()

	select {
	case got := <-second:
		t.Fatalf("higher LSN returned before lower lane completed: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
	if _, _, H := s.Boundaries(); H != 0 {
		t.Fatalf("H=%d while LSN 1 is incomplete", H)
	}
	close(release)
	if got := <-first; got.err != nil || got.lsn != 1 {
		t.Fatalf("first=%+v", got)
	}
	if got := <-second; got.err != nil || got.lsn != 2 {
		t.Fatalf("second=%+v", got)
	}
	if _, _, H := s.Boundaries(); H != 2 {
		t.Fatalf("H=%d want=2", H)
	}
}

func TestSyncFencesWritesAdmittedBeforeCall(t *testing.T) {
	s, path := createTestStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.lanes[0].beforeWrite = func(*writeRequest) {
		close(entered)
		<-release
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := s.Write(0, testBlock(0x41, s.BlockSize()))
		writeDone <- err
	}()
	<-entered
	syncDone := make(chan error, 1)
	go func() {
		stable, err := s.Sync()
		if err == nil && stable != 1 {
			err = fmt.Errorf("stable=%d want=1", stable)
		}
		syncDone <- err
	}()
	select {
	case err := <-syncDone:
		t.Fatalf("Sync returned before admitted append completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-syncDone; err != nil {
		t.Fatal(err)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0x41 {
		t.Fatalf("recovered byte=%02x want=41", got[0])
	}
}

func TestLowerLSNFailureBlocksCompletedHigherLane(t *testing.T) {
	s, _ := createTestStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.lanes[0].beforeWrite = func(*writeRequest) {
		close(entered)
		<-release
		s.lanes[0].base = -int64(s.hdr.RecordSize)
	}

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		_, err := s.Write(0, testBlock(1, s.BlockSize()))
		first <- err
	}()
	<-entered
	go func() {
		_, err := s.Write(1, testBlock(2, s.BlockSize()))
		second <- err
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	if err := <-first; err == nil {
		t.Fatal("lower LSN unexpectedly succeeded")
	}
	if err := <-second; err == nil {
		t.Fatal("higher LSN escaped lower-LSN terminal failure")
	}
	if _, _, H := s.Boundaries(); H != 0 {
		t.Fatalf("H=%d after terminal lower-LSN failure", H)
	}
	if _, err := s.Sync(); err == nil {
		t.Fatal("Sync succeeded after terminal lane failure")
	}
}

func TestCloseDrainsActiveAppenderAfterTerminalFailure(t *testing.T) {
	s, _ := createTestStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.lanes[0].beforeWrite = func(*writeRequest) {
		close(entered)
		<-release
	}

	first := make(chan error, 1)
	go func() {
		_, err := s.Write(0, testBlock(1, s.BlockSize()))
		first <- err
	}()
	<-entered
	s.lanes[1].base = -int64(s.recordSize)
	if _, err := s.Write(1, testBlock(2, s.BlockSize())); err == nil {
		t.Fatal("terminal append unexpectedly succeeded")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- s.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while an appender still owned the fd: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-first; err == nil {
		t.Fatal("blocked append did not receive terminal failure")
	}
	if err := <-closeDone; err == nil {
		t.Fatal("Close after terminal append failure returned nil")
	}
}

func TestRecoverRejectsActiveAppender(t *testing.T) {
	s, _ := createTestStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.lanes[0].beforeWrite = func(*writeRequest) {
		close(entered)
		<-release
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := s.Write(0, testBlock(1, s.BlockSize()))
		writeDone <- err
	}()
	<-entered
	if _, err := s.Recover(); err == nil {
		t.Fatal("Recover accepted an active appender")
	}
	close(release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
}

func TestUnsyncedTailIgnoredAfterCrash(t *testing.T) {
	s, path := createTestStore(t)
	if _, err := s.Write(0, testBlock(1, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, testBlock(2, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 1 {
		t.Fatalf("Recover=(%d,%v) want=(1,nil)", got, err)
	}
	got, err := reopened.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(make([]byte, reopened.BlockSize())) {
		t.Fatal("unsynced tail became visible after recovery")
	}
	if lsn, err := reopened.Write(1, testBlock(3, reopened.BlockSize())); err != nil || lsn != 2 {
		t.Fatalf("replacement Write=(%d,%v) want=(2,nil)", lsn, err)
	}
}

func TestOpenFallsBackFromCorruptLatestHeader(t *testing.T) {
	s, path := createTestStore(t)
	if _, err := s.Write(0, testBlock(1, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	// Publish the same durable frontier into the alternate header so the
	// fallback retains the acknowledged state after corrupting the latest one.
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	latestSlot := s.headerSlot
	crashStore(t, s)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, int64(latestSlot*headerSize+100)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatalf("Recover from prior header: %v", err)
	}
	got, err := reopened.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(testBlock(1, reopened.BlockSize())) {
		t.Fatal("prior valid header did not recover committed data")
	}
}

func TestCommittedRecordCorruptionFailsClosed(t *testing.T) {
	s, path := createTestStore(t)
	if _, err := s.Write(0, testBlock(1, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	recordOffset := s.lanes[0].base + recordHeaderSize
	crashStore(t, s)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := f.ReadAt(one[:], recordOffset); err != nil {
		t.Fatal(err)
	}
	one[0] ^= 0xff
	if _, err := f.WriteAt(one[:], recordOffset); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer crashStore(t, reopened)
	if _, err := reopened.Recover(); !errors.Is(err, storage.ErrWALIntegrityFault) {
		t.Fatalf("Recover error=%v, want WAL integrity fault", err)
	}
}

func TestRecoveryRejectsInvalidCommittedRecordSemantics(t *testing.T) {
	tests := []struct {
		name  string
		lba   uint32
		flags uint16
	}{
		{name: "out-of-range LBA", lba: testConfig().NumBlocks, flags: flagWrite},
		{name: "unknown flags", lba: 0, flags: 0x7fff},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, path := createTestStore(t)
			if _, err := s.Write(0, testBlock(1, s.BlockSize())); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			replacement, err := encodeRecord(walRecord{
				LSN:   1,
				LBA:   tc.lba,
				Flags: tc.flags,
				Data:  testBlock(1, s.BlockSize()),
			}, s.BlockSize())
			if err != nil {
				t.Fatal(err)
			}
			recordOffset := s.lanes[0].base
			crashStore(t, s)
			f, err := os.OpenFile(path, os.O_RDWR, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt(replacement, recordOffset); err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer crashStore(t, reopened)
			if _, err := reopened.Recover(); !errors.Is(err, storage.ErrWALIntegrityFault) {
				t.Fatalf("Recover error=%v want WAL integrity fault", err)
			}
		})
	}
}

func TestApplyEntryAcceptsSourceFrontierJump(t *testing.T) {
	s, _ := createTestStore(t)
	if err := s.ApplyEntry(0, testBlock(1, s.BlockSize()), 100); err != nil {
		t.Fatal(err)
	}
	if _, _, head := s.Boundaries(); head != 100 {
		t.Fatalf("head=%d want=100", head)
	}
	if lsn, err := s.Write(1, testBlock(2, s.BlockSize())); err != nil || lsn != 101 {
		t.Fatalf("Write=(%d,%v) want=(101,nil)", lsn, err)
	}
}

func TestFailedApplyEntryDoesNotPublishSourceFrontierJump(t *testing.T) {
	s, _ := createTestStore(t)
	s.lanes[0].base = -int64(s.recordSize)
	if err := s.ApplyEntry(0, testBlock(1, s.BlockSize()), 100); err == nil {
		t.Fatal("ApplyEntry unexpectedly succeeded")
	}
	if _, _, head := s.Boundaries(); head != 0 {
		t.Fatalf("failed ApplyEntry published head=%d want=0", head)
	}
}

func TestSourceFrontierJumpPersistsWithoutFalseCheckpoint(t *testing.T) {
	s, path := createTestStore(t)
	if _, err := s.Write(0, testBlock(0x11, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyEntry(1, testBlock(0x77, s.BlockSize()), 100); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Sync(); err != nil || got != 100 {
		t.Fatalf("Sync=(%d,%v) want=(100,nil)", got, err)
	}
	if s.hdr.CheckpointLSN != 0 || s.hdr.WALTail != 100 {
		t.Fatalf("header checkpoint=%d walTail=%d want=0/100", s.hdr.CheckpointLSN, s.hdr.WALTail)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 100 {
		t.Fatalf("Recover=(%d,%v) want frontier 100", got, err)
	}
	for lba, want := range []byte{0x11, 0x77} {
		data, err := reopened.Read(uint32(lba))
		if err != nil {
			t.Fatal(err)
		}
		if data[0] != want {
			t.Fatalf("LBA %d byte=%02x want=%02x", lba, data[0], want)
		}
	}
}

func TestConcurrentSameLBAWritesRemainOrdered(t *testing.T) {
	s, _ := createTestStore(t)
	const writes = 8
	var wg sync.WaitGroup
	results := make(chan uint64, writes)
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(fill byte) {
			defer wg.Done()
			lsn, err := s.Write(7, testBlock(fill, s.BlockSize()))
			if err != nil {
				t.Errorf("Write: %v", err)
				return
			}
			results <- lsn
		}(byte(i + 1))
	}
	wg.Wait()
	close(results)
	seen := make(map[uint64]bool)
	for lsn := range results {
		seen[lsn] = true
	}
	for lsn := uint64(1); lsn <= writes; lsn++ {
		if !seen[lsn] {
			t.Fatalf("missing LSN %d", lsn)
		}
	}
	applied, err := s.AppliedLSNs()
	if err != nil {
		t.Fatal(err)
	}
	if applied[7] != writes {
		t.Fatalf("applied LSN=%d want=%d", applied[7], writes)
	}
}

func TestWriteBatchDispatchesAcrossLanesBeforePublishing(t *testing.T) {
	s, _ := createTestStore(t)
	entered := []chan struct{}{make(chan struct{}), make(chan struct{})}
	release := make(chan struct{})
	for laneID := 0; laneID < 2; laneID++ {
		id := laneID
		s.lanes[laneID].beforeWrite = func(*writeRequest) {
			close(entered[id])
			<-release
		}
	}
	result := make(chan error, 1)
	go func() {
		_, err := s.WriteBatch(0, [][]byte{
			testBlock(1, s.BlockSize()),
			testBlock(2, s.BlockSize()),
		})
		result <- err
	}()
	for _, ch := range entered {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("batch did not dispatch to both lanes")
		}
	}
	if _, _, H := s.Boundaries(); H != 0 {
		t.Fatalf("H=%d before either lane was released", H)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, _, H := s.Boundaries(); H != 2 {
		t.Fatalf("H=%d want=2", H)
	}
}

func TestRingWrapRecyclesOnlyCheckpointedPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrap.bin")
	s, err := CreateStoreWithConfig(path, Config{
		NumBlocks:     8,
		BlockSize:     512,
		LaneCount:     1,
		StripeBlocks:  1,
		SlotsPerLane:  4,
		RetainPerLane: 2,
		QueueDepth:    4,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := s.Write(0, testBlock(byte(i), s.BlockSize())); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if got, err := s.Sync(); err != nil || got != uint64(i) {
			t.Fatalf("Sync %d=(%d,%v)", i, got, err)
		}
	}
	R, S, H := s.Boundaries()
	if R != 10 || H != 10 || S != 9 {
		t.Fatalf("Boundaries=(%d,%d,%d) want=(10,9,10)", R, S, H)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 10 {
		t.Fatalf("Recover=(%d,%v)", got, err)
	}
	data, err := reopened.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 10 {
		t.Fatalf("latest byte=%d want=10", data[0])
	}
	if err := reopened.ScanLBAs(7, func(storage.RecoveryEntry) error { return nil }); !errors.Is(err, storage.ErrWALRecycled) {
		t.Fatalf("ScanLBAs below S error=%v want WAL recycled", err)
	}
	var lsns []uint64
	if err := reopened.ScanLBAs(8, func(entry storage.RecoveryEntry) error {
		lsns = append(lsns, entry.LSN)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(lsns) != 2 || lsns[0] != 9 || lsns[1] != 10 {
		t.Fatalf("retained LSNs=%v want=[9 10]", lsns)
	}
}

func TestAdvancedWALTailBeyondHeadSurvivesRecovery(t *testing.T) {
	s, path := createTestStore(t)
	for lba := uint32(0); lba < 2; lba++ {
		if _, err := s.Write(lba, testBlock(byte(lba+1), s.BlockSize())); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.AdvanceWALTail(20)
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 2 {
		t.Fatalf("Recover=(%d,%v) want=(2,nil)", got, err)
	}
	for lba := uint32(0); lba < 2; lba++ {
		data, err := reopened.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if data[0] != byte(lba+1) {
			t.Fatalf("LBA %d byte=%02x want=%02x", lba, data[0], byte(lba+1))
		}
	}
}

func TestRecoverReplaysDurableWALBeforeCheckpoint(t *testing.T) {
	s, path := createTestStore(t)
	if _, err := s.Write(3, testBlock(0x7a, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	heads := s.publishedHeads
	tails := s.hdr.LaneTails
	s.mu.RUnlock()
	if err := s.fd.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.persistHeader(1, 0, heads, tails, 1, s.activeExtent); err != nil {
		t.Fatal(err)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 1 {
		t.Fatalf("Recover=(%d,%v)", got, err)
	}
	data, err := reopened.Read(3)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0x7a {
		t.Fatalf("replayed byte=%02x want=7a", data[0])
	}
}

func TestDirectExtentFrontierPersistsWithoutSyntheticWAL(t *testing.T) {
	s, path := createTestStore(t)
	if err := s.WriteExtentDirect(5, testBlock(0x5a, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(100)
	if got, err := s.Sync(); err != nil || got != 100 {
		t.Fatalf("Sync=(%d,%v) want=(100,nil)", got, err)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 100 {
		t.Fatalf("Recover=(%d,%v) want=(100,nil)", got, err)
	}
	data, err := reopened.Read(5)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0x5a {
		t.Fatalf("direct extent byte=%02x want=5a", data[0])
	}
	if err := reopened.ScanLBAs(0, func(storage.RecoveryEntry) error { return nil }); !errors.Is(err, storage.ErrWALRecycled) {
		t.Fatalf("ScanLBAs below rebuilt frontier error=%v want WAL recycled", err)
	}
}

func TestRetainedPreCheckpointWALDoesNotOverrideRebuiltExtent(t *testing.T) {
	s, path := createTestStore(t)
	if _, err := s.Write(5, testBlock(0x11, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteExtentDirect(5, testBlock(0x77, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(100)
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	data, err := reopened.Read(5)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0x77 {
		t.Fatalf("retained WAL overrode rebuilt extent: byte=%02x want=77", data[0])
	}
}

func TestBaseExtentHeaderFailureKeepsPriorAcknowledgedExtent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "parallelwal.bin")
	cfg := testConfig()
	cfg.LaneCount = 1
	cfg.SlotsPerLane = 4
	cfg.RetainPerLane = 1
	s, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(0, testBlock(0x11, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(0, testBlock(0x22, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if s.checkpointLSN != 2 || s.activeExtent != 0 {
		t.Fatalf("baseline checkpoint=%d activeExtent=%d want=(2,0)", s.checkpointLSN, s.activeExtent)
	}

	if err := s.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteExtentDirect(0, testBlock(0x77, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(100)
	s.mu.Lock()
	s.headerSlot = headerSlots
	s.mu.Unlock()
	if _, err := s.Sync(); err == nil {
		t.Fatal("Sync succeeded with invalid final header slot")
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 2 {
		t.Fatalf("Recover=(%d,%v) want=(2,nil)", got, err)
	}
	data, err := reopened.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0x22 {
		t.Fatalf("failed BASE commit changed acknowledged extent: byte=%02x want=22", data[0])
	}
	if reopened.activeExtent != 0 {
		t.Fatalf("activeExtent=%d want old extent 0", reopened.activeExtent)
	}
}

func TestBeginBaseInstallClearsAbortedStage(t *testing.T) {
	s, path := createTestStore(t)
	if err := s.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteExtentDirect(3, testBlock(0x55, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(10)
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	data, err := s.Read(3)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0 {
		t.Fatalf("aborted BASE stage leaked into next session: byte=%02x", data[0])
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	data, err = reopened.Read(3)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0 {
		t.Fatalf("aborted BASE stage persisted after recovery: byte=%02x", data[0])
	}
}

func TestAdvanceFrontierWithoutBaseStageKeepsExistingData(t *testing.T) {
	s, path := createTestStore(t)
	if _, err := s.Write(2, testBlock(0x44, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(100)
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if s.hdr.CheckpointLSN != 0 || s.hdr.ActiveExtent != 0 {
		t.Fatalf("metadata-only frontier checkpoint=%d activeExtent=%d want=(0,0)",
			s.hdr.CheckpointLSN, s.hdr.ActiveExtent)
	}
	crashStore(t, s)

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 100 {
		t.Fatalf("Recover=(%d,%v) want=(100,nil)", got, err)
	}
	data, err := reopened.Read(2)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0x44 {
		t.Fatalf("metadata-only frontier lost existing data: byte=%02x want=44", data[0])
	}
}

func TestNextBaseStagePreservesHeaderFallbackToCurrentExtent(t *testing.T) {
	s, path := createTestStore(t)
	if err := s.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteExtentDirect(4, testBlock(0x66, s.BlockSize())); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(20)
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if s.activeExtent != 1 {
		t.Fatalf("first BASE activeExtent=%d want=1", s.activeExtent)
	}

	if err := s.BeginBaseInstall(); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	latestHeaderSlot := s.headerSlot
	s.mu.RUnlock()
	crashStore(t, s)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0}, int64(latestHeaderSlot*headerSize)); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 20 {
		t.Fatalf("Recover=(%d,%v) want=(20,nil)", got, err)
	}
	data, err := reopened.Read(4)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 0x66 {
		t.Fatalf("fallback header referenced reused extent: byte=%02x want=66", data[0])
	}
	if reopened.activeExtent != 1 {
		t.Fatalf("fallback activeExtent=%d want current extent 1", reopened.activeExtent)
	}
}

func TestRecycledSlotsRemainRecoverableThroughHeaderFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "parallelwal.bin")
	cfg := testConfig()
	cfg.LaneCount = 1
	cfg.SlotsPerLane = 4
	cfg.RetainPerLane = 1
	s, err := CreateStoreWithConfig(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for lba, fill := range []byte{0x11, 0x22} {
		if _, err := s.Write(uint32(lba), testBlock(fill, s.BlockSize())); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if s.hdr.LaneTails[0] != 1 || s.checkpointLSN != 2 {
		t.Fatalf("checkpoint tail=%d checkpoint=%d want=(1,2)", s.hdr.LaneTails[0], s.checkpointLSN)
	}

	// Fill through sequence 4 without Sync. Sequence 4 reuses physical slot 0,
	// which the pre-recycle header would still scan as committed LSN 1.
	for lba := uint32(2); lba <= 4; lba++ {
		if _, err := s.Write(lba, testBlock(byte(0x30+lba), s.BlockSize())); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.RLock()
	latestHeaderSlot := s.headerSlot
	s.mu.RUnlock()
	crashStore(t, s)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0}, int64(latestHeaderSlot*headerSize)); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Recover(); err != nil || got != 2 {
		t.Fatalf("Recover=(%d,%v) want=(2,nil)", got, err)
	}
	for lba, want := range []byte{0x11, 0x22} {
		data, err := reopened.Read(uint32(lba))
		if err != nil {
			t.Fatal(err)
		}
		if data[0] != want {
			t.Fatalf("LBA %d byte=%02x want=%02x", lba, data[0], want)
		}
	}
}

func TestHeaderForConfigRejectsPersistedGeometryOverflow(t *testing.T) {
	tests := []Config{
		{
			NumBlocks:    1,
			BlockSize:    int(uint64(^uint32(0)) + 1),
			LaneCount:    1,
			StripeBlocks: 1,
			SlotsPerLane: 2,
			QueueDepth:   1,
		},
		{
			NumBlocks:    1,
			BlockSize:    512,
			LaneCount:    1,
			StripeBlocks: int(^uint16(0)) + 1,
			SlotsPerLane: 2,
			QueueDepth:   1,
		},
	}
	for i, cfg := range tests {
		if _, err := headerForConfig(normalizeConfig(cfg)); !errors.Is(err, errBadGeometry) {
			t.Fatalf("case %d error=%v want invalid geometry", i, err)
		}
	}
}

func TestFileSizeRejectsMultiplicationOverflow(t *testing.T) {
	h := fileHeader{
		BlockSize:     512,
		NumBlocks:     1,
		LaneCount:     maxLaneCount,
		StripeBlocks:  1,
		RecordSize:    512 + recordHeaderSize,
		SlotsPerLane:  ^uint64(0),
		RetainPerLane: 1,
		WALTail:       1,
	}
	if _, err := fileSize(h); !errors.Is(err, errBadGeometry) {
		t.Fatalf("fileSize error=%v want invalid geometry", err)
	}
}

func TestHeaderValidationRejectsWrappedRecordSize(t *testing.T) {
	h := fileHeader{
		BlockSize:     ^uint32(0),
		NumBlocks:     1,
		LaneCount:     1,
		StripeBlocks:  1,
		RecordSize:    recordHeaderSize - 1,
		SlotsPerLane:  2,
		RetainPerLane: 1,
		WALTail:       1,
	}
	if err := h.validate(); !errors.Is(err, errBadGeometry) {
		t.Fatalf("validate error=%v want invalid geometry", err)
	}
}
