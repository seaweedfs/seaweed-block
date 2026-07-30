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
			wallStart := time.Now()
			for worker := 0; worker < writers; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for idx := worker; idx < b.N; idx += writers {
						opStart := time.Now()
						_, writeErr := s.Write(uint32(idx%numBlocks), data[worker])
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
			var syncedLSN uint64
			if firstErr == nil {
				syncedLSN, firstErr = s.Sync()
			}
			if firstErr == nil {
				s.flusher.Stop()
			}
			wallDuration := time.Since(wallStart)
			b.StopTimer()
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
			reportLatencyPercentiles(b, latencies)
			reportWALStoreContentionMetrics(b, s, status, uint64(b.N), 1)
			b.ReportMetric(float64(b.N)/wallDuration.Seconds(), "write_ops/s")
		})
	}
}

func BenchmarkPhase170WALStoreBatchContention(b *testing.B) {
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
			wallStart := time.Now()
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
			var syncedLSN uint64
			if firstErr == nil {
				syncedLSN, firstErr = s.Sync()
			}
			if firstErr == nil {
				s.flusher.Stop()
			}
			wallDuration := time.Since(wallStart)
			b.StopTimer()
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
			logicalEntries := uint64(b.N * batchBlocks)
			reportLatencyPercentiles(b, latencies)
			reportWALStoreContentionMetrics(b, s, status, logicalEntries, batchBlocks)
			b.ReportMetric(batchBlocks, "batch_blocks")
			b.ReportMetric(float64(logicalEntries)/wallDuration.Seconds(), "block_ops/s")
		})
	}
}

func reportWALStoreContentionMetrics(
	b *testing.B,
	s *WALStore,
	status WriteInstrumentationStatus,
	logicalEntries uint64,
	entriesPerAPICall int,
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
