package storage

import (
	"errors"

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

func TestFlusherInstrumentationCountsExtentFailures(t *testing.T) {
	var instr flusherInstrumentation
	injected := errors.New("injected extent failure")
	instr.recordExtentWrite(4096, time.Nanosecond, injected)
	instr.recordExtentSync(time.Nanosecond, injected)
	instr.recordWALHeaderRead(12, time.Nanosecond, injected)
	instr.recordWALRecordRead(34, time.Nanosecond, injected)
	instr.recordWALRecordDecode(34, time.Nanosecond, injected)

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
		got.WALRecordReadBytes != 34 ||
		got.WALRecordDecodeOps != 1 || got.WALRecordDecodeFailures != 1 ||
		got.WALRecordDecodeBytes != 34 {
		t.Fatalf("WAL header/read/decode ops/failures/bytes=%d/%d/%d %d/%d/%d %d/%d/%d want 1/1/12 1/1/34 1/1/34",
			got.WALHeaderReadOps, got.WALHeaderReadFailures, got.WALHeaderReadBytes,
			got.WALRecordReadOps, got.WALRecordReadFailures, got.WALRecordReadBytes,
			got.WALRecordDecodeOps, got.WALRecordDecodeFailures, got.WALRecordDecodeBytes)
	}
}
