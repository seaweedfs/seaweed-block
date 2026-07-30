package storage

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestWALStore_WriteInstrumentationCountsAppendLockWait(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	block := make([]byte, 4096)
	if _, err := s.Write(0, block); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(1, [][]byte{block, block}); err != nil {
		t.Fatal(err)
	}

	got := s.WriteInstrumentation()
	if got.WALAppendLockWaitOps != 2 {
		t.Fatalf("WALAppendLockWaitOps=%d want 2", got.WALAppendLockWaitOps)
	}
	if got.WALAppendLockWaitNanos == 0 {
		t.Fatal("WALAppendLockWaitNanos=0 want diagnostic evidence")
	}
	if got.WriteCommitLockWaitOps != 2 {
		t.Fatalf("WriteCommitLockWaitOps=%d want 2", got.WriteCommitLockWaitOps)
	}
	if got.WriteCommitLockWaitNanos == 0 {
		t.Fatal("WriteCommitLockWaitNanos=0 want diagnostic evidence")
	}
}

func BenchmarkPhase167WALStoreContention(b *testing.B) {
	benchmarkWALStoreContention(b, func(index, numBlocks int) uint32 {
		return uint32(index % numBlocks)
	})
}

func BenchmarkPhase171WALStoreScatteredContention(b *testing.B) {
	benchmarkWALStoreContention(b, func(index, numBlocks int) uint32 {
		return uint32((index * 7919) % numBlocks)
	})
}

func benchmarkWALStoreContention(b *testing.B, lbaForIndex func(int, int) uint32) {
	const (
		blockSize = 4096
		numBlocks = 16384
	)
	for _, writers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			s, err := CreateWALStore(filepath.Join(b.TempDir(), "store.bin"), numBlocks, blockSize)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = s.Close() })
			b.ReportAllocs()

			data := make([][]byte, writers)
			for i := range data {
				data[i] = make([]byte, blockSize)
				data[i][0] = byte(i + 1)
			}
			latencies := make([]int64, b.N)
			workerOps := make([]int, writers)
			var firstErr error
			var errOnce sync.Once
			start := make(chan struct{})
			var wg sync.WaitGroup

			b.SetBytes(blockSize)
			b.ResetTimer()
			foregroundStart := time.Now()
			for worker := 0; worker < writers; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for idx := worker; idx < b.N; idx += writers {
						opStart := time.Now()
						_, writeErr := s.Write(lbaForIndex(idx, numBlocks), data[worker])
						latencies[idx] = time.Since(opStart).Nanoseconds()
						if writeErr != nil {
							errOnce.Do(func() { firstErr = writeErr })
							return
						}
						workerOps[worker]++
					}
				}(worker)
			}
			close(start)
			wg.Wait()
			foregroundDuration := time.Since(foregroundStart)
			b.StopTimer()
			var syncedLSN uint64
			syncStart := time.Now()
			if firstErr == nil {
				syncedLSN, firstErr = s.Sync()
			}
			syncDuration := time.Since(syncStart)
			drainStart := time.Now()
			if firstErr == nil {
				firstErr = s.flusher.Stop()
			}
			drainDuration := time.Since(drainStart)
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			assertWALStoreContentionDrained(b, s, syncedLSN)
			if b.N >= writers {
				for worker, ops := range workerOps {
					if ops == 0 {
						b.Fatalf("worker %d completed zero writes", worker)
					}
				}
			}

			status := s.WriteInstrumentation()
			flusherStatus := s.FlusherInstrumentation()
			reportLatencyPercentiles(b, latencies)
			reportWALStoreContentionMetrics(
				b, s, status, flusherStatus, uint64(b.N), 1,
				foregroundDuration, syncDuration, drainDuration,
			)
			b.ReportMetric(0, "multi_block_records")
			b.ReportMetric(float64(b.N)/foregroundDuration.Seconds(), "write_ops/s")
		})
	}
}

func BenchmarkPhase170WALStoreBatchContention(b *testing.B) {
	benchmarkWALStoreBatchContention(b, false)
}

func BenchmarkPhase172WALStoreMultiBlockContention(b *testing.B) {
	benchmarkWALStoreBatchContention(b, true)
}

func benchmarkWALStoreBatchContention(b *testing.B, multiBlockRecords bool) {
	const (
		blockSize   = 4096
		numBlocks   = 16384
		batchBlocks = 16
	)
	for _, writers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			s, err := CreateWALStore(filepath.Join(b.TempDir(), "store.bin"), numBlocks, blockSize)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = s.Close() })
			s.enableMultiBlockRecordsForTest(multiBlockRecords)
			b.ReportAllocs()

			data := make([][][]byte, writers)
			for worker := range data {
				data[worker] = make([][]byte, batchBlocks)
				for block := range data[worker] {
					data[worker][block] = make([]byte, blockSize)
					data[worker][block][0] = byte(worker + block + 1)
				}
			}
			latencies := make([]int64, b.N)
			workerOps := make([]int, writers)
			var firstErr error
			var errOnce sync.Once
			start := make(chan struct{})
			var wg sync.WaitGroup

			b.SetBytes(blockSize * batchBlocks)
			b.ResetTimer()
			foregroundStart := time.Now()
			for worker := 0; worker < writers; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for idx := worker; idx < b.N; idx += writers {
						startLBA := uint32((idx * batchBlocks) % (numBlocks - batchBlocks))
						opStart := time.Now()
						_, writeErr := s.WriteBatch(startLBA, data[worker])
						latencies[idx] = time.Since(opStart).Nanoseconds()
						if writeErr != nil {
							errOnce.Do(func() { firstErr = writeErr })
							return
						}
						workerOps[worker]++
					}
				}(worker)
			}
			close(start)
			wg.Wait()
			foregroundDuration := time.Since(foregroundStart)
			b.StopTimer()
			var syncedLSN uint64
			syncStart := time.Now()
			if firstErr == nil {
				syncedLSN, firstErr = s.Sync()
			}
			syncDuration := time.Since(syncStart)
			drainStart := time.Now()
			if firstErr == nil {
				firstErr = s.flusher.Stop()
			}
			drainDuration := time.Since(drainStart)
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			assertWALStoreContentionDrained(b, s, syncedLSN)
			if b.N >= writers {
				for worker, ops := range workerOps {
					if ops == 0 {
						b.Fatalf("worker %d completed zero batches", worker)
					}
				}
			}

			status := s.WriteInstrumentation()
			flusherStatus := s.FlusherInstrumentation()
			logicalEntries := uint64(b.N * batchBlocks)
			reportLatencyPercentiles(b, latencies)
			reportWALStoreContentionMetrics(
				b, s, status, flusherStatus, logicalEntries, batchBlocks,
				foregroundDuration, syncDuration, drainDuration,
			)
			b.ReportMetric(batchBlocks, "batch_blocks")
			if multiBlockRecords {
				b.ReportMetric(1, "multi_block_records")
			} else {
				b.ReportMetric(0, "multi_block_records")
			}
			b.ReportMetric(float64(logicalEntries)/foregroundDuration.Seconds(), "block_ops/s")
		})
	}
}

func reportWALStoreContentionMetrics(
	b *testing.B,
	s *WALStore,
	status WriteInstrumentationStatus,
	flusherStatus FlusherInstrumentationStatus,
	logicalEntries uint64,
	entriesPerAPICall int,
	foregroundDuration time.Duration,
	syncDuration time.Duration,
	drainDuration time.Duration,
) {
	b.Helper()
	perOp := func(total, count uint64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count)
	}
	perEntry := func(total uint64) float64 {
		if logicalEntries == 0 {
			return 0
		}
		return float64(total) / float64(logicalEntries)
	}
	perValidatedRecord := func(total uint64) float64 {
		if flusherStatus.ValidatedRecords == 0 {
			return 0
		}
		return float64(total) / float64(flusherStatus.ValidatedRecords)
	}
	perSnapshotEntry := func(total uint64) float64 {
		if flusherStatus.SnapshotEntries == 0 {
			return 0
		}
		return float64(total) / float64(flusherStatus.SnapshotEntries)
	}

	_, _, headLSN := s.Boundaries()
	checkpointLSN := s.CheckpointLSN()

	b.ReportMetric(perOp(status.WALCopyDurationNanos, status.WALCopyOps), "wal_copy_ns/record")
	b.ReportMetric(perOp(status.WALEncodeDurationNanos, status.WALEncodeOps), "wal_encode_ns/record")
	b.ReportMetric(perOp(status.WALChecksumDurationNanos, status.WALChecksumOps), "wal_checksum_ns/record")
	b.ReportMetric(perOp(status.WALAppendDurationNanos, status.WALAppendOps), "wal_append_ns/writeat")
	b.ReportMetric(perOp(status.WALAppendLockWaitNanos, status.WALAppendLockWaitOps), "wal_lock_wait_ns/append_call")
	b.ReportMetric(perOp(status.WriteCommitLockWaitNanos, status.WriteCommitLockWaitOps), "commit_lock_wait_ns/api_call")
	b.ReportMetric(perOp(status.DirtyMapUpdateDurationNanos, status.DirtyMapUpdateOps), "dirty_map_ns/record")
	b.ReportMetric(perEntry(status.WALAppendWriteAtCalls), "writeat_calls/entry")
	b.ReportMetric(perEntry(status.WALAppendWriteAtBytes), "writeat_bytes/entry")
	b.ReportMetric(float64(status.WALAppendWriteAtMaxBytes), "writeat_max_bytes")
	b.ReportMetric(float64(status.WALAppendWrapCount), "wal_wraps")
	b.ReportMetric(float64(status.WALAppendPaddingBytes), "wal_padding_bytes")
	b.ReportMetric(float64(s.FlushCount()), "flushes")
	b.ReportMetric(float64(s.dm.len()), "dirty_entries")
	b.ReportMetric(float64(checkpointLSN), "checkpoint_lsn")
	b.ReportMetric(float64(headLSN), "head_lsn")
	b.ReportMetric(float64(checkpointLSN)/float64(headLSN), "checkpoint_coverage")
	b.ReportMetric(float64(s.syncs.Load()), "explicit_sync_calls")
	b.ReportMetric(float64(entriesPerAPICall), "entries/api_call")
	b.ReportMetric(float64(foregroundDuration.Nanoseconds()), "foreground_ns")
	b.ReportMetric(float64(syncDuration.Nanoseconds()), "final_sync_ns")
	b.ReportMetric(float64(drainDuration.Nanoseconds()), "final_drain_ns")
	b.ReportMetric(perEntry(flusherStatus.SnapshotEntries), "flush_snapshot_entries/entry")
	b.ReportMetric(perEntry(flusherStatus.SnapshotUniqueWALRecords), "flush_unique_wal_records/entry")
	b.ReportMetric(perEntry(flusherStatus.SnapshotRecordReuseCandidates), "flush_record_reuse_opportunities/entry")
	b.ReportMetric(perSnapshotEntry(flusherStatus.SnapshotUniqueWALRecords), "flush_unique_wal_records/snapshot_entry")
	b.ReportMetric(perSnapshotEntry(flusherStatus.SnapshotRecordReuseCandidates), "flush_record_reuse_opportunities/snapshot_entry")
	b.ReportMetric(perEntry(flusherStatus.SnapshotDurationNanos), "flush_snapshot_ns/entry")
	b.ReportMetric(perEntry(flusherStatus.OpportunityAnalysisNanos), "flush_opportunity_ns/entry")
	b.ReportMetric(perEntry(flusherStatus.ValidatedRecords), "flush_validated_records/entry")
	b.ReportMetric(float64(flusherStatus.ValidationFailures), "flush_validation_failures")
	b.ReportMetric(perEntry(flusherStatus.SupersededEntries), "flush_superseded_entries/entry")
	b.ReportMetric(perEntry(flusherStatus.WALHeaderReadOps), "flush_header_reads/entry")
	b.ReportMetric(perValidatedRecord(flusherStatus.WALHeaderReadOps), "flush_header_reads/validated_record")
	b.ReportMetric(float64(flusherStatus.WALHeaderReadFailures), "flush_header_read_failures")
	b.ReportMetric(perEntry(flusherStatus.WALHeaderReadBytes), "flush_header_read_bytes/entry")
	b.ReportMetric(perEntry(flusherStatus.WALHeaderReadDurationNanos), "flush_header_read_ns/entry")
	b.ReportMetric(perEntry(flusherStatus.WALRecordReadOps), "flush_record_reads/entry")
	b.ReportMetric(perValidatedRecord(flusherStatus.WALRecordReadOps), "flush_record_reads/validated_record")
	b.ReportMetric(float64(flusherStatus.WALRecordReadFailures), "flush_record_read_failures")
	b.ReportMetric(perEntry(flusherStatus.WALRecordReadBytes), "flush_record_read_bytes/entry")
	b.ReportMetric(perEntry(flusherStatus.WALRecordReadDurationNanos), "flush_record_read_ns/entry")
	b.ReportMetric(perEntry(flusherStatus.MaterializationReadOps), "flush_materialization_reads/entry")
	b.ReportMetric(perValidatedRecord(flusherStatus.MaterializationReadOps), "flush_materialization_reads/validated_record")
	b.ReportMetric(perEntry(flusherStatus.MaterializationReadBytes), "flush_materialization_read_bytes/entry")
	b.ReportMetric(perValidatedRecord(flusherStatus.MaterializationRecordReuseHits), "flush_record_reuse_hits/validated_record")
	b.ReportMetric(perEntry(flusherStatus.ExtentWriteOps), "extent_write_ops/entry")
	b.ReportMetric(float64(flusherStatus.ExtentWriteFailures), "extent_write_failures")
	b.ReportMetric(perEntry(flusherStatus.ExtentWriteBytes), "extent_write_bytes/entry")
	b.ReportMetric(perEntry(flusherStatus.ExtentWriteDurationNanos), "extent_write_ns/entry")
	b.ReportMetric(perEntry(flusherStatus.SnapshotBoundedWriteMinimum), "extent_snapshot_min_write_ops/entry")
	b.ReportMetric(perEntry(flusherStatus.SnapshotRunCount), "extent_snapshot_runs/entry")
	b.ReportMetric(perEntry(flusherStatus.SnapshotSingletonRuns), "extent_snapshot_singleton_runs/entry")
	b.ReportMetric(perEntry(flusherStatus.SnapshotCoalescibleEntries), "extent_snapshot_coalescible_entries/entry")
	b.ReportMetric(float64(flusherStatus.SnapshotMaxContiguousRunBlocks), "extent_snapshot_max_run_blocks")
	b.ReportMetric(perEntry(flusherStatus.WrittenBoundedWriteMinimum), "extent_written_min_write_ops/entry")
	b.ReportMetric(perEntry(flusherStatus.WrittenRunCount), "extent_written_runs/entry")
	b.ReportMetric(perEntry(flusherStatus.WrittenSingletonRuns), "extent_written_singleton_runs/entry")
	b.ReportMetric(perEntry(flusherStatus.WrittenCoalescibleEntries), "extent_written_coalescible_entries/entry")
	b.ReportMetric(float64(flusherStatus.WrittenMaxContiguousRunBlocks), "extent_written_max_run_blocks")
	b.ReportMetric(float64(flusherStatus.ExtentWriteMaxBytes), "extent_write_max_bytes")
	b.ReportMetric(float64(flusherStatus.ExtentSyncOps), "extent_sync_ops")
	b.ReportMetric(float64(flusherStatus.ExtentSyncFailures), "extent_sync_failures")
	b.ReportMetric(
		perOp(flusherStatus.ExtentSyncDurationNanos, flusherStatus.ExtentSyncOps),
		"extent_sync_ns/op",
	)
	b.ReportMetric(float64(flusherStatus.CheckpointMetadataWriteOps), "checkpoint_write_ops")
	b.ReportMetric(float64(flusherStatus.CheckpointMetadataWriteFailures), "checkpoint_write_failures")
	b.ReportMetric(
		perOp(flusherStatus.CheckpointMetadataWriteNanos, flusherStatus.CheckpointMetadataWriteOps),
		"checkpoint_write_ns/op",
	)
	b.ReportMetric(float64(flusherStatus.CheckpointMetadataSyncOps), "checkpoint_sync_ops")
	b.ReportMetric(float64(flusherStatus.CheckpointMetadataSyncFailures), "checkpoint_sync_failures")
	b.ReportMetric(
		perOp(flusherStatus.CheckpointMetadataSyncNanos, flusherStatus.CheckpointMetadataSyncOps),
		"checkpoint_sync_ns/op",
	)
	b.ReportMetric(float64(flusherStatus.CyclesStarted), "flush_cycles_started")
	b.ReportMetric(float64(flusherStatus.CyclesSucceeded), "flush_cycles_succeeded")
	b.ReportMetric(float64(flusherStatus.CyclesFailed), "flush_cycles_failed")
	b.ReportMetric(perEntry(flusherStatus.CycleDurationNanos), "flush_cycle_ns/entry")
	b.ReportMetric(float64(flusherStatus.CycleMaxDurationNanos), "flush_cycle_max_ns")
}

func assertWALStoreContentionDrained(b *testing.B, s *WALStore, syncedLSN uint64) {
	b.Helper()
	if dirty := s.dm.len(); dirty != 0 {
		b.Fatalf("dirty entries after timed drain=%d", dirty)
	}
	checkpointLSN := s.CheckpointLSN()
	_, _, headLSN := s.Boundaries()
	if checkpointLSN != syncedLSN || headLSN != syncedLSN {
		b.Fatalf("drained frontier checkpoint=%d head=%d synced=%d",
			checkpointLSN, headLSN, syncedLSN)
	}
}

func reportLatencyPercentiles(b *testing.B, samples []int64) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	at := func(percent int) float64 {
		idx := (len(samples)*percent + 99) / 100
		if idx > 0 {
			idx--
		}
		return float64(samples[idx])
	}
	b.ReportMetric(at(50), "p50_ns")
	b.ReportMetric(at(95), "p95_ns")
	b.ReportMetric(at(99), "p99_ns")
}
