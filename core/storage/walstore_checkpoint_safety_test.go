package storage

import (
	"bytes"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFlusherCheckpointMetadataFailureDoesNotPublishOrRecycle(t *testing.T) {
	tests := []struct {
		name   string
		inject func(*WALStore)
	}{
		{
			name: "write",
			inject: func(s *WALStore) {
				s.writeSuperblockMetadata = func([]byte) error {
					return errors.New("injected checkpoint write failure")
				}
			},
		},
		{
			name: "sync",
			inject: func(s *WALStore) {
				s.syncSuperblockMetadata = func() error {
					return errors.New("injected checkpoint sync failure")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			s.flusher.Stop()

			want := makeBlock(4096, 0x71)
			lsn, err := s.Write(3, want)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}

			tailBefore := s.wal.logicalTailValue()
			savedWrite := s.writeSuperblockMetadata
			savedSync := s.syncSuperblockMetadata
			tt.inject(s)

			if err := s.flusher.flushOnce(); err == nil {
				t.Fatal("flush with injected checkpoint failure succeeded")
			}
			if got := s.CheckpointLSN(); got != 0 {
				t.Fatalf("checkpoint after failed metadata publication=%d want 0", got)
			}
			if got := s.sb.WALCheckpointLSN; got != 0 {
				t.Fatalf("superblock checkpoint after failed metadata publication=%d want 0", got)
			}
			if got := s.wal.logicalTailValue(); got != tailBefore {
				t.Fatalf("WAL tail after failed metadata publication=%d want %d", got, tailBefore)
			}
			if got := s.dm.len(); got != 1 {
				t.Fatalf("dirty entries after failed metadata publication=%d want 1", got)
			}

			s.writeSuperblockMetadata = savedWrite
			s.syncSuperblockMetadata = savedSync
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatalf("retry flush: %v", err)
			}
			if got := s.CheckpointLSN(); got != lsn {
				t.Fatalf("checkpoint after retry=%d want %d", got, lsn)
			}
			if got := s.dm.len(); got != 0 {
				t.Fatalf("dirty entries after retry=%d want 0", got)
			}
			got, err := s.Read(3)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("extent bytes changed across checkpoint retry")
			}
		})
	}
}

func TestCheckpointPublicationCrashWindowsRemainRecoverable(t *testing.T) {
	tests := []struct {
		name              string
		inject            func(*WALStore)
		wantFlushError    bool
		wantCheckpointMin uint64
		wantCheckpointMax uint64
	}{
		{
			name: "checkpoint_write_failed",
			inject: func(s *WALStore) {
				s.writeSuperblockMetadata = func([]byte) error {
					return errors.New("injected checkpoint write failure")
				}
			},
			wantFlushError:    true,
			wantCheckpointMin: 0,
			wantCheckpointMax: 0,
		},
		{
			name: "checkpoint_sync_failed",
			inject: func(s *WALStore) {
				s.syncSuperblockMetadata = func() error {
					return errors.New("injected checkpoint sync failure")
				}
			},
			wantFlushError:    true,
			wantCheckpointMin: 0,
			wantCheckpointMax: 1,
		},
		{
			name:              "checkpoint_synced",
			inject:            func(*WALStore) {},
			wantCheckpointMin: 1,
			wantCheckpointMax: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.bin")
			s, err := CreateWALStore(path, 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			s.flusher.Stop()
			want := makeBlock(4096, 0x73)
			if _, err := s.Write(3, want); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			tt.inject(s)
			flushErr := s.flusher.flushOnce()
			if tt.wantFlushError && flushErr == nil {
				t.Fatal("flush succeeded despite injected checkpoint failure")
			}
			if !tt.wantFlushError && flushErr != nil {
				t.Fatal(flushErr)
			}

			// Simulate process loss without the WALStore Close metadata path.
			s.committer.Stop()
			if err := s.fd.Close(); err != nil {
				t.Fatal(err)
			}
			s.fd = nil

			reopened, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			checkpoint := reopened.CheckpointLSN()
			if checkpoint < tt.wantCheckpointMin || checkpoint > tt.wantCheckpointMax {
				t.Fatalf("reopened checkpoint=%d want in [%d,%d]",
					checkpoint, tt.wantCheckpointMin, tt.wantCheckpointMax)
			}
			if _, err := reopened.Recover(); err != nil {
				t.Fatal(err)
			}
			got, err := reopened.Read(3)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("checkpoint crash window lost the acknowledged block")
			}
		})
	}
}

func TestFlusherWALSlotMismatchFailsClosed(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.flusher.Stop()

	if _, err := s.Write(4, makeBlock(4096, 0x44)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	walOffset, _, lsn, _, ok := s.dm.get(4)
	if !ok {
		t.Fatal("dirty entry missing")
	}

	header := make([]byte, walEntryHeaderSize)
	absoluteOffset := int64(s.sb.WALOffset + walOffset)
	if _, err := s.fd.ReadAt(header, absoluteOffset); err != nil {
		t.Fatal(err)
	}
	mutated := append([]byte(nil), header...)
	mutated[0] ^= 0x7f
	if _, err := s.fd.WriteAt(mutated, absoluteOffset); err != nil {
		t.Fatal(err)
	}

	err = s.flusher.flushOnce()
	if err == nil || !strings.Contains(err.Error(), "WAL slot mismatch") {
		t.Fatalf("flush error=%v want WAL slot mismatch", err)
	}
	if got := s.CheckpointLSN(); got != 0 {
		t.Fatalf("checkpoint after WAL slot mismatch=%d want 0", got)
	}
	if got := s.dm.len(); got != 1 {
		t.Fatalf("dirty entries after WAL slot mismatch=%d want 1", got)
	}
	instr := s.FlusherInstrumentation()
	if instr.ValidationFailures != 1 || instr.ValidatedRecords != 0 ||
		instr.CyclesFailed != 1 {
		t.Fatalf("validation failures/validated/cycles failed=%d/%d/%d want 1/0/1",
			instr.ValidationFailures, instr.ValidatedRecords, instr.CyclesFailed)
	}
	_, _, currentLSN, _, ok := s.dm.get(4)
	if !ok || currentLSN != lsn {
		t.Fatalf("dirty entry after mismatch ok=%t lsn=%d want true/%d", ok, currentLSN, lsn)
	}

	if _, err := s.fd.WriteAt(header, absoluteOffset); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatalf("flush after restoring WAL header: %v", err)
	}
}

func TestFlusherRejectsCorruptOrUnsupportedDirtyRecord(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func([]byte)
		wantErr string
	}{
		{
			name: "crc",
			mutate: func(record []byte) {
				record[walEntryPrefixSize] ^= 0x7f
			},
			wantErr: "CRC mismatch",
		},
		{
			name: "unsupported_type",
			mutate: func(record []byte) {
				record[16] = walEntryBarrier
			},
			wantErr: "invalid dirty WAL record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			s.flusher.Stop()

			if _, err := s.Write(2, makeBlock(4096, 0x22)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			walOffset, _, _, length, ok := s.dm.get(2)
			if !ok {
				t.Fatal("dirty entry missing")
			}
			record := make([]byte, walEntryHeaderSize+length)
			absoluteOffset := int64(s.sb.WALOffset + walOffset)
			if _, err := s.fd.ReadAt(record, absoluteOffset); err != nil {
				t.Fatal(err)
			}
			original := append([]byte(nil), record...)
			tt.mutate(record)
			if _, err := s.fd.WriteAt(record, absoluteOffset); err != nil {
				t.Fatal(err)
			}

			err = s.flusher.flushOnce()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("flush error=%v want substring %q", err, tt.wantErr)
			}
			if got := s.CheckpointLSN(); got != 0 {
				t.Fatalf("checkpoint after invalid record=%d want 0", got)
			}
			if got := s.dm.len(); got != 1 {
				t.Fatalf("dirty entries after invalid record=%d want 1", got)
			}
			instr := s.FlusherInstrumentation()
			if instr.ValidationFailures != 1 || instr.ValidatedRecords != 0 ||
				instr.CyclesFailed != 1 {
				t.Fatalf("validation failures/validated/cycles failed=%d/%d/%d want 1/0/1",
					instr.ValidationFailures, instr.ValidatedRecords, instr.CyclesFailed)
			}

			if _, err := s.fd.WriteAt(original, absoluteOffset); err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatalf("flush after restoring record: %v", err)
			}
		})
	}
}

func TestWALStoreClosePerformsFinalFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	replaceFlusherForTest(s, time.Hour)

	want := makeBlock(4096, 0xa5)
	lsn, err := s.Write(7, want)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got := reopened.CheckpointLSN(); got != lsn {
		t.Fatalf("checkpoint after close=%d want final-flushed LSN %d", got, lsn)
	}
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Read(7)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("final-flushed bytes did not survive reopen")
	}
}

func TestWALStoreCloseWaitsForInflightSync(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	replaceFlusherForTest(s, time.Hour)
	if _, err := s.Write(1, makeBlock(4096, 0x61)); err != nil {
		t.Fatal(err)
	}

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	s.syncCache = func() error {
		close(syncStarted)
		<-releaseSync
		return nil
	}
	syncDone := make(chan error, 1)
	go func() {
		_, err := s.Sync()
		syncDone <- err
	}()
	<-syncStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- s.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before in-flight Sync completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseSync)
	if err := <-syncDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestWALStoreMutationAPIsRejectAfterClose(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyEntry(1, makeBlock(4096, 0x11), 1); err == nil {
		t.Fatal("ApplyEntry after Close succeeded")
	}
	if err := s.WriteExtentDirect(1, makeBlock(4096, 0x22)); err == nil {
		t.Fatal("WriteExtentDirect after Close succeeded")
	}
}

func TestWALStoreCloseReturnsFinalMetadataFailure(t *testing.T) {
	tests := []struct {
		name   string
		inject func(*WALStore)
	}{
		{
			name: "write",
			inject: func(s *WALStore) {
				s.writeSuperblockMetadata = func([]byte) error {
					return errors.New("injected final metadata write failure")
				}
			},
		},
		{
			name: "sync",
			inject: func(s *WALStore) {
				s.syncSuperblockMetadata = func() error {
					return errors.New("injected final metadata sync failure")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			replaceFlusherForTest(s, time.Hour)
			if _, err := s.Write(1, makeBlock(4096, 0x51)); err != nil {
				t.Fatal(err)
			}
			tt.inject(s)
			err = s.Close()
			if err == nil || !strings.Contains(err.Error(), "injected final metadata") {
				t.Fatalf("Close error=%v want injected final metadata failure", err)
			}
			if s.fd != nil {
				t.Fatal("Close metadata failure left file open")
			}
		})
	}
}

func TestWriteExtentDirectSupersedesPriorDirtyWAL(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.flusher.Stop()

	if _, err := s.Write(5, makeBlock(4096, 0x55)); err != nil {
		t.Fatal(err)
	}
	base := makeBlock(4096, 0xb5)
	if err := s.WriteExtentDirect(5, base); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, ok := s.dm.get(5); ok {
		t.Fatal("direct BASE install left the superseded dirty WAL mapping visible")
	}
	got, err := s.Read(5)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, base) {
		t.Fatal("read after direct BASE install returned superseded WAL bytes")
	}
}

func TestWriteExtentDirectSupersededWALDoesNotReappearAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	s.flusher.Stop()

	if _, err := s.Write(9, makeBlock(4096, 0x39)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	base := makeBlock(4096, 0xb9)
	if err := s.WriteExtentDirect(9, base); err != nil {
		t.Fatal(err)
	}
	s.AdvanceFrontier(10)
	if frontier, err := s.Sync(); err != nil || frontier != 10 {
		t.Fatalf("Sync frontier=%d err=%v want 10/nil", frontier, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Read(9)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, base) {
		t.Fatal("superseded WAL bytes reappeared after direct BASE restart")
	}
}

func TestRunningFlusherCannotOverwriteDirectBase(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for iteration := 0; iteration < 50; iteration++ {
		if _, err := s.Write(10, makeBlock(4096, byte(iteration))); err != nil {
			t.Fatal(err)
		}
		s.flusher.Notify()
		base := makeBlock(4096, byte(0x80+iteration))
		if err := s.WriteExtentDirect(10, base); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
		got, err := s.Read(10)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, base) {
			t.Fatalf("iteration %d: running flusher overwrote direct BASE", iteration)
		}
	}
}

func TestFlusherSnapshotCannotOverwriteLaterDirectBase(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.flusher.Stop()

	old := makeBlock(4096, 0x31)
	if _, err := s.Write(6, old); err != nil {
		t.Fatal(err)
	}
	snapshot := s.dm.snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("dirty snapshot entries=%d want 1", len(snapshot))
	}

	base := makeBlock(4096, 0xb6)
	if err := s.WriteExtentDirect(6, base); err != nil {
		t.Fatal(err)
	}
	written, err := s.writeExtentIfCurrent(snapshot[0].LBA, snapshot[0].LSN, old)
	if err != nil {
		t.Fatal(err)
	}
	if written {
		t.Fatal("stale flusher snapshot overwrote a later direct BASE block")
	}
	got, err := s.readFromExtent(6)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, base) {
		t.Fatal("extent does not contain the direct BASE bytes")
	}
}

func TestWriteExtentDirectFailureRetainsDirtyWAL(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	s.flusher.Stop()

	if _, err := s.Write(8, makeBlock(4096, 0x48)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.committer.Stop()
	if err := s.fd.Close(); err != nil {
		t.Fatal(err)
	}

	if err := s.WriteExtentDirect(8, makeBlock(4096, 0xb8)); err == nil {
		t.Fatal("direct BASE write on closed file succeeded")
	}
	if _, _, _, _, ok := s.dm.get(8); !ok {
		t.Fatal("failed direct BASE write removed the recoverable dirty WAL mapping")
	}
}

func replaceFlusherForTest(s *WALStore, interval time.Duration) {
	s.flusher.Stop()
	s.flusher = newFlusher(s, flusherConfig{Interval: interval})
	go s.flusher.run()
}
