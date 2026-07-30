package storage

import (
	"sort"
	"sync/atomic"
	"time"
)

const maxCheckpointWriteBytes = 1 << 20

// FlusherInstrumentationStatus is a cumulative diagnostic snapshot of
// checkpoint work. Operation counters include failed attempts; byte counters
// include only successful writes.
type FlusherInstrumentationStatus struct {
	CyclesStarted                   uint64
	CyclesSucceeded                 uint64
	CyclesFailed                    uint64
	CycleDurationNanos              uint64
	CycleMaxDurationNanos           uint64
	SnapshotEntries                 uint64
	SnapshotUniqueWALRecords        uint64
	SnapshotRecordReuseCandidates   uint64
	SnapshotDurationNanos           uint64
	OpportunityAnalysisNanos        uint64
	SnapshotBoundedWriteMinimum     uint64
	SnapshotRunCount                uint64
	SnapshotSingletonRuns           uint64
	SnapshotCoalescibleEntries      uint64
	SnapshotMaxContiguousRunBlocks  uint64
	WrittenBoundedWriteMinimum      uint64
	WrittenRunCount                 uint64
	WrittenSingletonRuns            uint64
	WrittenCoalescibleEntries       uint64
	WrittenMaxContiguousRunBlocks   uint64
	ValidatedRecords                uint64
	ValidationFailures              uint64
	SupersededEntries               uint64
	WALHeaderReadOps                uint64
	WALHeaderReadFailures           uint64
	WALHeaderReadBytes              uint64
	WALHeaderReadDurationNanos      uint64
	WALRecordReadOps                uint64
	WALRecordReadFailures           uint64
	WALRecordReadBytes              uint64
	WALRecordReadDurationNanos      uint64
	MaterializationReadOps          uint64
	MaterializationReadBytes        uint64
	MaterializationRecordReuseHits  uint64
	ExtentWriteOps                  uint64
	ExtentWriteFailures             uint64
	ExtentWriteBytes                uint64
	ExtentWriteMaxBytes             uint64
	ExtentWriteDurationNanos        uint64
	ExtentSyncOps                   uint64
	ExtentSyncFailures              uint64
	ExtentSyncDurationNanos         uint64
	CheckpointMetadataWriteOps      uint64
	CheckpointMetadataWriteBytes    uint64
	CheckpointMetadataWriteFailures uint64
	CheckpointMetadataWriteNanos    uint64
	CheckpointMetadataSyncOps       uint64
	CheckpointMetadataSyncFailures  uint64
	CheckpointMetadataSyncNanos     uint64
}

type flusherInstrumentation struct {
	cyclesStarted                   atomic.Uint64
	cyclesSucceeded                 atomic.Uint64
	cyclesFailed                    atomic.Uint64
	cycleDurationNanos              atomic.Uint64
	cycleMaxDurationNanos           atomic.Uint64
	snapshotEntries                 atomic.Uint64
	snapshotUniqueWALRecords        atomic.Uint64
	snapshotRecordReuseCandidates   atomic.Uint64
	snapshotDurationNanos           atomic.Uint64
	opportunityAnalysisNanos        atomic.Uint64
	snapshotBoundedWriteMinimum     atomic.Uint64
	snapshotRunCount                atomic.Uint64
	snapshotSingletonRuns           atomic.Uint64
	snapshotCoalescibleEntries      atomic.Uint64
	snapshotMaxContiguousRunBlocks  atomic.Uint64
	writtenBoundedWriteMinimum      atomic.Uint64
	writtenRunCount                 atomic.Uint64
	writtenSingletonRuns            atomic.Uint64
	writtenCoalescibleEntries       atomic.Uint64
	writtenMaxContiguousRunBlocks   atomic.Uint64
	validatedRecords                atomic.Uint64
	validationFailures              atomic.Uint64
	supersededEntries               atomic.Uint64
	walHeaderReadOps                atomic.Uint64
	walHeaderReadFailures           atomic.Uint64
	walHeaderReadBytes              atomic.Uint64
	walHeaderReadDurationNanos      atomic.Uint64
	walRecordReadOps                atomic.Uint64
	walRecordReadFailures           atomic.Uint64
	walRecordReadBytes              atomic.Uint64
	walRecordReadDurationNanos      atomic.Uint64
	materializationRecordReuseHits  atomic.Uint64
	extentWriteOps                  atomic.Uint64
	extentWriteFailures             atomic.Uint64
	extentWriteBytes                atomic.Uint64
	extentWriteMaxBytes             atomic.Uint64
	extentWriteDurationNanos        atomic.Uint64
	extentSyncOps                   atomic.Uint64
	extentSyncFailures              atomic.Uint64
	extentSyncDurationNanos         atomic.Uint64
	checkpointMetadataWriteOps      atomic.Uint64
	checkpointMetadataWriteBytes    atomic.Uint64
	checkpointMetadataWriteFailures atomic.Uint64
	checkpointMetadataWriteNanos    atomic.Uint64
	checkpointMetadataSyncOps       atomic.Uint64
	checkpointMetadataSyncFailures  atomic.Uint64
	checkpointMetadataSyncNanos     atomic.Uint64
}

func (i *flusherInstrumentation) recordCycle(
	cycleStart time.Time,
	snapshotDuration time.Duration,
	entries []snapshotEntry,
	blockSize uint32,
) func(bool) {
	i.cyclesStarted.Add(1)
	i.snapshotEntries.Add(uint64(len(entries)))
	i.snapshotDurationNanos.Add(storageDurationNanos(snapshotDuration))
	opportunityStart := time.Now()
	opportunity := boundedExtentWriteOpportunity(entries, blockSize)
	recordShape := walRecordMaterializationShape(entries)
	i.opportunityAnalysisNanos.Add(storageDurationNanos(time.Since(opportunityStart)))
	i.snapshotUniqueWALRecords.Add(recordShape.uniqueRecords)
	i.snapshotRecordReuseCandidates.Add(recordShape.reuseCandidates)
	i.snapshotBoundedWriteMinimum.Add(opportunity.minimumOps)
	i.snapshotRunCount.Add(opportunity.runCount)
	i.snapshotSingletonRuns.Add(opportunity.singletonRuns)
	i.snapshotCoalescibleEntries.Add(opportunity.coalescibleEntries)
	updateAtomicMax(&i.snapshotMaxContiguousRunBlocks, opportunity.maxRun)
	return func(success bool) {
		duration := storageDurationNanos(time.Since(cycleStart))
		i.cycleDurationNanos.Add(duration)
		updateAtomicMax(&i.cycleMaxDurationNanos, duration)
		if success {
			i.cyclesSucceeded.Add(1)
		} else {
			i.cyclesFailed.Add(1)
		}
	}
}

type walRecordShape struct {
	uniqueRecords   uint64
	reuseCandidates uint64
}

func walRecordMaterializationShape(entries []snapshotEntry) walRecordShape {
	type identity struct {
		offset uint64
		size   uint64
	}
	records := make(map[identity]struct{}, len(entries))
	for _, entry := range entries {
		records[identity{offset: entry.WALOffset, size: entry.RecordSize}] = struct{}{}
	}
	uniqueRecords := uint64(len(records))
	return walRecordShape{
		uniqueRecords:   uniqueRecords,
		reuseCandidates: uint64(len(entries)) - uniqueRecords,
	}
}

func (i *flusherInstrumentation) recordWrittenOpportunity(entries []snapshotEntry, blockSize uint32) {
	opportunityStart := time.Now()
	opportunity := boundedExtentWriteOpportunity(entries, blockSize)
	i.opportunityAnalysisNanos.Add(storageDurationNanos(time.Since(opportunityStart)))
	i.writtenBoundedWriteMinimum.Add(opportunity.minimumOps)
	i.writtenRunCount.Add(opportunity.runCount)
	i.writtenSingletonRuns.Add(opportunity.singletonRuns)
	i.writtenCoalescibleEntries.Add(opportunity.coalescibleEntries)
	updateAtomicMax(&i.writtenMaxContiguousRunBlocks, opportunity.maxRun)
}

type extentWriteOpportunity struct {
	minimumOps         uint64
	runCount           uint64
	singletonRuns      uint64
	coalescibleEntries uint64
	maxRun             uint64
}

func boundedExtentWriteOpportunity(entries []snapshotEntry, blockSize uint32) extentWriteOpportunity {
	if len(entries) == 0 {
		return extentWriteOpportunity{}
	}
	lbas := make([]uint64, len(entries))
	for index, entry := range entries {
		lbas[index] = entry.LBA
	}
	sort.Slice(lbas, func(left, right int) bool { return lbas[left] < lbas[right] })
	maxBlocks := uint64(maxCheckpointWriteBytes) / uint64(blockSize)
	if maxBlocks == 0 {
		maxBlocks = 1
	}
	var result extentWriteOpportunity
	runBlocks := uint64(1)
	for index := 1; index <= len(lbas); index++ {
		if index < len(lbas) && lbas[index] == lbas[index-1]+1 {
			runBlocks++
			continue
		}
		result.runCount++
		if runBlocks == 1 {
			result.singletonRuns++
		} else {
			result.coalescibleEntries += runBlocks
		}
		if runBlocks > result.maxRun {
			result.maxRun = runBlocks
		}
		result.minimumOps += (runBlocks + maxBlocks - 1) / maxBlocks
		runBlocks = 1
	}
	return result
}

func (i *flusherInstrumentation) recordWALHeaderRead(bytes int, duration time.Duration, err error) {
	i.walHeaderReadOps.Add(1)
	i.walHeaderReadBytes.Add(uint64(bytes))
	i.walHeaderReadDurationNanos.Add(storageDurationNanos(duration))
	if err != nil {
		i.walHeaderReadFailures.Add(1)
	}
}

func (i *flusherInstrumentation) recordWALRecordRead(bytes int, duration time.Duration, err error) {
	i.walRecordReadOps.Add(1)
	i.walRecordReadBytes.Add(uint64(bytes))
	i.walRecordReadDurationNanos.Add(storageDurationNanos(duration))
	if err != nil {
		i.walRecordReadFailures.Add(1)
	}
}

func (i *flusherInstrumentation) recordValidatedRecord() {
	i.validatedRecords.Add(1)
}

func (i *flusherInstrumentation) recordValidationFailure() {
	i.validationFailures.Add(1)
}

func (i *flusherInstrumentation) recordSupersededEntry() {
	i.supersededEntries.Add(1)
}

func (i *flusherInstrumentation) recordMaterializationReuseHit() {
	i.materializationRecordReuseHits.Add(1)
}

func (i *flusherInstrumentation) recordExtentWrite(bytes int, duration time.Duration, err error) {
	value := uint64(bytes)
	i.extentWriteOps.Add(1)
	i.extentWriteDurationNanos.Add(storageDurationNanos(duration))
	updateAtomicMax(&i.extentWriteMaxBytes, value)
	if err != nil {
		i.extentWriteFailures.Add(1)
		return
	}
	i.extentWriteBytes.Add(value)
}

func (i *flusherInstrumentation) recordExtentSync(duration time.Duration, err error) {
	i.extentSyncOps.Add(1)
	i.extentSyncDurationNanos.Add(storageDurationNanos(duration))
	if err != nil {
		i.extentSyncFailures.Add(1)
	}
}

func (i *flusherInstrumentation) recordCheckpointWrite(bytes int, duration time.Duration, err error) {
	i.checkpointMetadataWriteOps.Add(1)
	i.checkpointMetadataWriteNanos.Add(storageDurationNanos(duration))
	if err != nil {
		i.checkpointMetadataWriteFailures.Add(1)
		return
	}
	i.checkpointMetadataWriteBytes.Add(uint64(bytes))
}

func (i *flusherInstrumentation) recordCheckpointSync(duration time.Duration, err error) {
	i.checkpointMetadataSyncOps.Add(1)
	i.checkpointMetadataSyncNanos.Add(storageDurationNanos(duration))
	if err != nil {
		i.checkpointMetadataSyncFailures.Add(1)
	}
}

func updateAtomicMax(target *atomic.Uint64, value uint64) {
	for {
		old := target.Load()
		if value <= old || target.CompareAndSwap(old, value) {
			return
		}
	}
}

func (i *flusherInstrumentation) snapshot() FlusherInstrumentationStatus {
	walHeaderReadOps := i.walHeaderReadOps.Load()
	walHeaderReadBytes := i.walHeaderReadBytes.Load()
	walRecordReadOps := i.walRecordReadOps.Load()
	walRecordReadBytes := i.walRecordReadBytes.Load()
	return FlusherInstrumentationStatus{
		CyclesStarted:                   i.cyclesStarted.Load(),
		CyclesSucceeded:                 i.cyclesSucceeded.Load(),
		CyclesFailed:                    i.cyclesFailed.Load(),
		CycleDurationNanos:              i.cycleDurationNanos.Load(),
		CycleMaxDurationNanos:           i.cycleMaxDurationNanos.Load(),
		SnapshotEntries:                 i.snapshotEntries.Load(),
		SnapshotUniqueWALRecords:        i.snapshotUniqueWALRecords.Load(),
		SnapshotRecordReuseCandidates:   i.snapshotRecordReuseCandidates.Load(),
		SnapshotDurationNanos:           i.snapshotDurationNanos.Load(),
		OpportunityAnalysisNanos:        i.opportunityAnalysisNanos.Load(),
		SnapshotBoundedWriteMinimum:     i.snapshotBoundedWriteMinimum.Load(),
		SnapshotRunCount:                i.snapshotRunCount.Load(),
		SnapshotSingletonRuns:           i.snapshotSingletonRuns.Load(),
		SnapshotCoalescibleEntries:      i.snapshotCoalescibleEntries.Load(),
		SnapshotMaxContiguousRunBlocks:  i.snapshotMaxContiguousRunBlocks.Load(),
		WrittenBoundedWriteMinimum:      i.writtenBoundedWriteMinimum.Load(),
		WrittenRunCount:                 i.writtenRunCount.Load(),
		WrittenSingletonRuns:            i.writtenSingletonRuns.Load(),
		WrittenCoalescibleEntries:       i.writtenCoalescibleEntries.Load(),
		WrittenMaxContiguousRunBlocks:   i.writtenMaxContiguousRunBlocks.Load(),
		ValidatedRecords:                i.validatedRecords.Load(),
		ValidationFailures:              i.validationFailures.Load(),
		SupersededEntries:               i.supersededEntries.Load(),
		WALHeaderReadOps:                walHeaderReadOps,
		WALHeaderReadFailures:           i.walHeaderReadFailures.Load(),
		WALHeaderReadBytes:              walHeaderReadBytes,
		WALHeaderReadDurationNanos:      i.walHeaderReadDurationNanos.Load(),
		WALRecordReadOps:                walRecordReadOps,
		WALRecordReadFailures:           i.walRecordReadFailures.Load(),
		WALRecordReadBytes:              walRecordReadBytes,
		WALRecordReadDurationNanos:      i.walRecordReadDurationNanos.Load(),
		MaterializationReadOps:          walHeaderReadOps + walRecordReadOps,
		MaterializationReadBytes:        walHeaderReadBytes + walRecordReadBytes,
		MaterializationRecordReuseHits:  i.materializationRecordReuseHits.Load(),
		ExtentWriteOps:                  i.extentWriteOps.Load(),
		ExtentWriteFailures:             i.extentWriteFailures.Load(),
		ExtentWriteBytes:                i.extentWriteBytes.Load(),
		ExtentWriteMaxBytes:             i.extentWriteMaxBytes.Load(),
		ExtentWriteDurationNanos:        i.extentWriteDurationNanos.Load(),
		ExtentSyncOps:                   i.extentSyncOps.Load(),
		ExtentSyncFailures:              i.extentSyncFailures.Load(),
		ExtentSyncDurationNanos:         i.extentSyncDurationNanos.Load(),
		CheckpointMetadataWriteOps:      i.checkpointMetadataWriteOps.Load(),
		CheckpointMetadataWriteBytes:    i.checkpointMetadataWriteBytes.Load(),
		CheckpointMetadataWriteFailures: i.checkpointMetadataWriteFailures.Load(),
		CheckpointMetadataWriteNanos:    i.checkpointMetadataWriteNanos.Load(),
		CheckpointMetadataSyncOps:       i.checkpointMetadataSyncOps.Load(),
		CheckpointMetadataSyncFailures:  i.checkpointMetadataSyncFailures.Load(),
		CheckpointMetadataSyncNanos:     i.checkpointMetadataSyncNanos.Load(),
	}
}
