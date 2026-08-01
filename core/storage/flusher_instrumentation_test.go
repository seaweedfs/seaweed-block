package storage

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestBoundedExtentWriteOpportunity(t *testing.T) {
	entries := []snapshotEntry{
		{LBA: 300},
		{LBA: 2},
		{LBA: 0},
		{LBA: 6},
		{LBA: 1},
		{LBA: 5},
	}
	got := boundedExtentWriteOpportunity(entries, 4096)
	if got.minimumOps != 3 || got.runCount != 3 || got.singletonRuns != 1 ||
		got.coalescibleEntries != 5 || got.maxRun != 3 {
		t.Fatalf("opportunity=%+v want minimum=3 runs=3 singleton=1 coalescible=5 max=3", got)
	}

	entries = make([]snapshotEntry, 257)
	for index := range entries {
		entries[index].LBA = uint64(index)
	}
	got = boundedExtentWriteOpportunity(entries, 4096)
	if got.minimumOps != 2 || got.runCount != 1 || got.singletonRuns != 0 ||
		got.coalescibleEntries != 257 || got.maxRun != 257 {
		t.Fatalf("bounded opportunity=%+v want minimum=2 runs=1 singleton=0 coalescible=257 max=257", got)
	}
}

func TestFlusherInstrumentationCountsCompleteCycle(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.flusher.Stop()

	block := makeBlock(4096, 0x41)
	if _, err := s.WriteBatch(0, [][]byte{block, block, block, block}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	got := s.FlusherInstrumentation()
	if got.CyclesStarted != 1 || got.CyclesSucceeded != 1 || got.CyclesFailed != 0 {
		t.Fatalf("cycles started/succeeded/failed=%d/%d/%d want 1/1/0",
			got.CyclesStarted, got.CyclesSucceeded, got.CyclesFailed)
	}
	if got.SnapshotEntries != 4 || got.ValidatedRecords != 4 || got.SupersededEntries != 0 {
		t.Fatalf("snapshot/validated/superseded=%d/%d/%d want 4/4/0",
			got.SnapshotEntries, got.ValidatedRecords, got.SupersededEntries)
	}
	if got.WALHeaderReadOps != 4 || got.WALRecordReadOps != 4 {
		t.Fatalf("header/record reads=%d/%d want 4/4",
			got.WALHeaderReadOps, got.WALRecordReadOps)
	}
	if got.ExtentWriteOps != 4 || got.ExtentWriteBytes != 4*4096 ||
		got.ExtentWriteMaxBytes != 4096 || got.ExtentWriteFailures != 0 {
		t.Fatalf("extent ops/bytes/max/failures=%d/%d/%d/%d want 4/%d/4096/0",
			got.ExtentWriteOps, got.ExtentWriteBytes, got.ExtentWriteMaxBytes,
			got.ExtentWriteFailures, 4*4096)
	}
	if got.SnapshotBoundedWriteMinimum != 1 || got.SnapshotRunCount != 1 ||
		got.SnapshotSingletonRuns != 0 || got.SnapshotCoalescibleEntries != 4 ||
		got.SnapshotMaxContiguousRunBlocks != 4 {
		t.Fatalf("opportunity minimum/runs/singletons/coalescible/max=%d/%d/%d/%d/%d want 1/1/0/4/4",
			got.SnapshotBoundedWriteMinimum, got.SnapshotRunCount,
			got.SnapshotSingletonRuns, got.SnapshotCoalescibleEntries,
			got.SnapshotMaxContiguousRunBlocks)
	}
	if got.WrittenBoundedWriteMinimum != 1 || got.WrittenRunCount != 1 ||
		got.WrittenSingletonRuns != 0 || got.WrittenCoalescibleEntries != 4 ||
		got.WrittenMaxContiguousRunBlocks != 4 {
		t.Fatalf("written opportunity minimum/runs/singletons/coalescible/max=%d/%d/%d/%d/%d want 1/1/0/4/4",
			got.WrittenBoundedWriteMinimum, got.WrittenRunCount,
			got.WrittenSingletonRuns, got.WrittenCoalescibleEntries,
			got.WrittenMaxContiguousRunBlocks)
	}
	if got.ValidationFailures != 0 ||
		got.WALHeaderReadFailures != 0 ||
		got.WALRecordReadFailures != 0 {
		t.Fatalf("validation/header-read/record-read failures=%d/%d/%d want 0/0/0",
			got.ValidationFailures, got.WALHeaderReadFailures,
			got.WALRecordReadFailures)
	}
	if got.ExtentSyncOps != 1 || got.ExtentSyncFailures != 0 ||
		got.CheckpointMetadataWriteOps != 1 ||
		got.CheckpointMetadataSyncOps != 1 {
		t.Fatalf("extent-sync/failures/checkpoint-write/checkpoint-sync=%d/%d/%d/%d want 1/0/1/1",
			got.ExtentSyncOps, got.ExtentSyncFailures,
			got.CheckpointMetadataWriteOps, got.CheckpointMetadataSyncOps)
	}
	if got.CheckpointMetadataWriteFailures != 0 ||
		got.CheckpointMetadataSyncFailures != 0 {
		t.Fatalf("checkpoint write/sync failures=%d/%d want 0/0",
			got.CheckpointMetadataWriteFailures, got.CheckpointMetadataSyncFailures)
	}
	if got.CycleDurationNanos == 0 || got.CycleMaxDurationNanos == 0 {
		t.Fatal("cycle timing evidence is zero")
	}
	if got.CycleDurationNanos < got.SnapshotDurationNanos+got.OpportunityAnalysisNanos {
		t.Fatalf("cycle duration=%d excludes snapshot+opportunity=%d",
			got.CycleDurationNanos,
			got.SnapshotDurationNanos+got.OpportunityAnalysisNanos)
	}
}

func TestFlusherInstrumentationSeparatesSnapshotAndWrittenOpportunity(t *testing.T) {
	entries := []snapshotEntry{{LBA: 0}, {LBA: 1}, {LBA: 2}}
	var instr flusherInstrumentation
	finish := instr.recordCycle(time.Now(), time.Nanosecond, entries, 4096)
	instr.recordWrittenOpportunity([]snapshotEntry{entries[0], entries[2]}, 4096)
	finish(true)

	got := instr.snapshot()
	if got.SnapshotBoundedWriteMinimum != 1 ||
		got.SnapshotCoalescibleEntries != 3 {
		t.Fatalf("snapshot minimum/coalescible=%d/%d want 1/3",
			got.SnapshotBoundedWriteMinimum, got.SnapshotCoalescibleEntries)
	}
	if got.WrittenBoundedWriteMinimum != 2 ||
		got.WrittenRunCount != 2 ||
		got.WrittenSingletonRuns != 2 ||
		got.WrittenCoalescibleEntries != 0 {
		t.Fatalf("written minimum/runs/singletons/coalescible=%d/%d/%d/%d want 2/2/2/0",
			got.WrittenBoundedWriteMinimum, got.WrittenRunCount,
			got.WrittenSingletonRuns, got.WrittenCoalescibleEntries)
	}
}

func TestFlusherInstrumentationCountsCheckpointFailure(t *testing.T) {
	tests := []struct {
		name             string
		inject           func(*WALStore)
		wantWriteFailure uint64
		wantWriteBytes   uint64
		wantSyncOps      uint64
		wantSyncFailure  uint64
	}{
		{
			name: "write",
			inject: func(s *WALStore) {
				s.writeSuperblockMetadata = func([]byte) error {
					return errors.New("injected checkpoint write failure")
				}
			},
			wantWriteFailure: 1,
		},
		{
			name: "sync",
			inject: func(s *WALStore) {
				s.syncSuperblockMetadata = func() error {
					return errors.New("injected checkpoint sync failure")
				}
			},
			wantWriteBytes:  superblockSize,
			wantSyncOps:     1,
			wantSyncFailure: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			if err := s.flusher.Stop(); err != nil {
				t.Fatal(err)
			}

			if _, err := s.Write(0, makeBlock(4096, 0x52)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			savedWrite := s.writeSuperblockMetadata
			savedSync := s.syncSuperblockMetadata
			tt.inject(s)
			err = s.flusher.flushOnce()
			s.writeSuperblockMetadata = savedWrite
			s.syncSuperblockMetadata = savedSync
			if err == nil {
				t.Fatal("flush succeeded despite checkpoint failure")
			}

			got := s.FlusherInstrumentation()
			if got.CyclesStarted != 1 || got.CyclesSucceeded != 0 || got.CyclesFailed != 1 {
				t.Fatalf("cycles started/succeeded/failed=%d/%d/%d want 1/0/1",
					got.CyclesStarted, got.CyclesSucceeded, got.CyclesFailed)
			}
			if got.ExtentWriteOps != 1 || got.ExtentWriteFailures != 0 ||
				got.ExtentSyncOps != 1 || got.ExtentSyncFailures != 0 {
				t.Fatalf("extent writes/failures/syncs/failures=%d/%d/%d/%d want 1/0/1/0",
					got.ExtentWriteOps, got.ExtentWriteFailures,
					got.ExtentSyncOps, got.ExtentSyncFailures)
			}
			if got.CheckpointMetadataWriteOps != 1 ||
				got.CheckpointMetadataWriteFailures != tt.wantWriteFailure ||
				got.CheckpointMetadataWriteBytes != tt.wantWriteBytes ||
				got.CheckpointMetadataSyncOps != tt.wantSyncOps ||
				got.CheckpointMetadataSyncFailures != tt.wantSyncFailure {
				t.Fatalf("checkpoint write ops/failures/bytes/sync ops/failures=%d/%d/%d/%d/%d want 1/%d/%d/%d/%d",
					got.CheckpointMetadataWriteOps, got.CheckpointMetadataWriteFailures,
					got.CheckpointMetadataWriteBytes, got.CheckpointMetadataSyncOps,
					got.CheckpointMetadataSyncFailures, tt.wantWriteFailure,
					tt.wantWriteBytes, tt.wantSyncOps, tt.wantSyncFailure)
			}
		})
	}
}

func TestFlusherInstrumentationCountsExtentFailures(t *testing.T) {
	var instr flusherInstrumentation
	injected := errors.New("injected extent failure")
	instr.recordExtentWrite(4096, time.Nanosecond, injected)
	instr.recordExtentSync(time.Nanosecond, injected)
	instr.recordWALHeaderRead(12, time.Nanosecond, injected)
	instr.recordWALRecordRead(34, time.Nanosecond, injected)

	got := instr.snapshot()
	if got.ExtentWriteOps != 1 || got.ExtentWriteFailures != 1 ||
		got.ExtentWriteBytes != 0 || got.ExtentWriteMaxBytes != 4096 {
		t.Fatalf("extent writes/failures/bytes/max=%d/%d/%d/%d want 1/1/0/4096",
			got.ExtentWriteOps, got.ExtentWriteFailures,
			got.ExtentWriteBytes, got.ExtentWriteMaxBytes)
	}
	if got.ExtentSyncOps != 1 || got.ExtentSyncFailures != 1 {
		t.Fatalf("extent syncs/failures=%d/%d want 1/1",
			got.ExtentSyncOps, got.ExtentSyncFailures)
	}
	if got.WALHeaderReadOps != 1 || got.WALHeaderReadFailures != 1 ||
		got.WALHeaderReadBytes != 12 ||
		got.WALRecordReadOps != 1 || got.WALRecordReadFailures != 1 ||
		got.WALRecordReadBytes != 34 {
		t.Fatalf("WAL header ops/failures/bytes and record ops/failures/bytes=%d/%d/%d %d/%d/%d want 1/1/12 1/1/34",
			got.WALHeaderReadOps, got.WALHeaderReadFailures, got.WALHeaderReadBytes,
			got.WALRecordReadOps, got.WALRecordReadFailures, got.WALRecordReadBytes)
	}
}

func TestFlusherInstrumentationCountsWALReadFailures(t *testing.T) {
	tests := []struct {
		name                   string
		breakFile              func(*testing.T, *WALStore, int64)
		wantHeaderReadFailures uint64
		wantRecordReadFailures uint64
	}{
		{
			name: "header",
			breakFile: func(t *testing.T, s *WALStore, _ int64) {
				t.Helper()
				if err := s.fd.Close(); err != nil {
					t.Fatal(err)
				}
			},
			wantHeaderReadFailures: 1,
		},
		{
			name: "record",
			breakFile: func(t *testing.T, s *WALStore, absoluteOffset int64) {
				t.Helper()
				if err := s.fd.Truncate(absoluteOffset + walEntryHeaderSize); err != nil {
					t.Fatal(err)
				}
			},
			wantRecordReadFailures: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.Stop(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(0, makeBlock(4096, 0x63)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			walOffset, _, _, _, ok := s.dm.get(0)
			if !ok {
				t.Fatal("dirty entry missing")
			}
			s.committer.Stop()
			absoluteOffset := int64(s.sb.WALOffset + walOffset)
			tt.breakFile(t, s, absoluteOffset)

			if err := s.flusher.flushOnce(); err == nil {
				t.Fatal("flush succeeded despite WAL read failure")
			}
			got := s.FlusherInstrumentation()
			if got.WALHeaderReadFailures != tt.wantHeaderReadFailures ||
				got.WALRecordReadFailures != tt.wantRecordReadFailures ||
				got.ValidationFailures != 0 ||
				got.CyclesFailed != 1 {
				t.Fatalf("header/record/validation/cycle failures=%d/%d/%d/%d want %d/%d/0/1",
					got.WALHeaderReadFailures, got.WALRecordReadFailures,
					got.ValidationFailures, got.CyclesFailed,
					tt.wantHeaderReadFailures, tt.wantRecordReadFailures)
			}
			if tt.name == "record" {
				if err := s.fd.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
