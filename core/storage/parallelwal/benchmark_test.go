package parallelwal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func BenchmarkPhase169SegmentedWALContention(b *testing.B) {
	const (
		blockSize  = 4096
		numBlocks  = 16384
		maxLogSize = int64(4 << 30)
	)
	for _, writers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "store.bin")
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
			if err != nil {
				b.Fatal(err)
			}
			if err := file.Truncate(segmentDurableLogOffset + maxLogSize); err != nil {
				_ = file.Close()
				b.Fatal(err)
			}
			engine, err := newSegmentDurableEngine(file, segmentOwnerConfig{
				BlockSize:            blockSize,
				NumBlocks:            numBlocks,
				QueueDepth:           256,
				MaxEntriesPerSegment: maxSegmentEntries,
				MaxLogBytes:          maxLogSize,
			})
			if err != nil {
				_ = file.Close()
				b.Fatal(err)
			}
			b.Cleanup(func() {
				_ = engine.Close()
				_ = file.Close()
			})

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
						_, writeErr := engine.Submit(uint32(idx%numBlocks), data[worker])
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
			if firstErr == nil {
				_, firstErr = engine.Sync()
			}
			wallDuration := time.Since(wallStart)
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			if b.N >= writers {
				for worker, ops := range workerOps {
					if ops == 0 {
						b.Fatalf("worker %d completed zero writes", worker)
					}
				}
			}
			metrics := engine.owner.Metrics()
			reportParallelLatencyPercentiles(b, latencies)
			b.ReportMetric(float64(metrics.SegmentsWritten), "segments")
			b.ReportMetric(float64(metrics.EntriesWritten), "entries")
			if metrics.SegmentsWritten != 0 {
				b.ReportMetric(float64(metrics.EntriesWritten)/float64(metrics.SegmentsWritten),
					"entries/segment")
			}
			b.ReportMetric(float64(metrics.BytesWritten), "wal_bytes")
			b.ReportMetric(1, "sync_calls")
			b.ReportMetric(float64(b.N)/wallDuration.Seconds(), "write_ops/s")
		})
	}
}

func BenchmarkPhase167ParallelWALContention(b *testing.B) {
	const (
		blockSize = 4096
		numBlocks = 16384
	)
	for _, writers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			s, err := CreateStoreWithConfig(filepath.Join(b.TempDir(), "store.bin"), Config{
				NumBlocks:     numBlocks,
				BlockSize:     blockSize,
				LaneCount:     4,
				StripeBlocks:  1,
				SlotsPerLane:  65536,
				RetainPerLane: 65535,
				QueueDepth:    256,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = s.Close() })

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
			if firstErr == nil {
				_, firstErr = s.Sync()
			}
			wallDuration := time.Since(wallStart)
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			if b.N >= writers {
				for worker, ops := range workerOps {
					if ops == 0 {
						b.Fatalf("worker %d completed zero writes", worker)
					}
				}
			}
			activeLanes := 0
			s.mu.RLock()
			for laneID := 0; laneID < int(s.hdr.LaneCount); laneID++ {
				if s.publishedHeads[laneID] > 0 {
					activeLanes++
				}
			}
			s.mu.RUnlock()
			_, walTail, _ := s.Boundaries()
			s.mu.RLock()
			checkpointWriteOps := s.checkpointWriteOps
			recycleReadOps := s.recycleReadOps
			walWriteOps := s.walWriteOps
			s.mu.RUnlock()
			reportParallelLatencyPercentiles(b, latencies)
			b.ReportMetric(float64(activeLanes), "active_lanes")
			b.ReportMetric(float64(checkpointWriteOps), "checkpoint_write_ops")
			b.ReportMetric(float64(recycleReadOps), "recycle_read_ops")
			b.ReportMetric(float64(walWriteOps), "wal_write_ops")
			b.ReportMetric(float64(walTail), "wal_tail")
			b.ReportMetric(1, "sync_calls")
			b.ReportMetric(float64(b.N)/wallDuration.Seconds(), "write_ops/s")
		})
	}
}

func BenchmarkPhase167ParallelWALBatchContention(b *testing.B) {
	const (
		blockSize   = 4096
		numBlocks   = 16384
		batchBlocks = 16
	)
	for _, writers := range []int{1, 4} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			s, err := CreateStoreWithConfig(filepath.Join(b.TempDir(), "store.bin"), Config{
				NumBlocks:     numBlocks,
				BlockSize:     blockSize,
				LaneCount:     4,
				StripeBlocks:  1,
				SlotsPerLane:  32768,
				RetainPerLane: 64,
				QueueDepth:    256,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = s.Close() })

			data := make([][][]byte, writers)
			for worker := range data {
				data[worker] = make([][]byte, batchBlocks)
				for block := range data[worker] {
					data[worker][block] = make([]byte, blockSize)
					data[worker][block][0] = byte(worker + block + 1)
				}
			}
			latencies := make([]int64, b.N)
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
					}
				}(worker)
			}
			close(start)
			wg.Wait()
			if firstErr == nil {
				_, firstErr = s.Sync()
			}
			wallDuration := time.Since(wallStart)
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			s.mu.RLock()
			checkpointWriteOps := s.checkpointWriteOps
			recycleReadOps := s.recycleReadOps
			walWriteOps := s.walWriteOps
			s.mu.RUnlock()
			reportParallelLatencyPercentiles(b, latencies)
			b.ReportMetric(batchBlocks, "batch_blocks")
			b.ReportMetric(float64(checkpointWriteOps), "checkpoint_write_ops")
			b.ReportMetric(float64(recycleReadOps), "recycle_read_ops")
			b.ReportMetric(float64(walWriteOps), "wal_write_ops")
			b.ReportMetric(float64(b.N*batchBlocks)/wallDuration.Seconds(), "block_ops/s")
		})
	}
}

func BenchmarkPhase167LegacyWALBatchContentionControl(b *testing.B) {
	const (
		blockSize   = 4096
		numBlocks   = 16384
		batchBlocks = 16
	)
	for _, writers := range []int{1, 4} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			s, err := storage.CreateWALStore(filepath.Join(b.TempDir(), "store.bin"), numBlocks, blockSize)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = s.Close() })

			data := make([][][]byte, writers)
			for worker := range data {
				data[worker] = make([][]byte, batchBlocks)
				for block := range data[worker] {
					data[worker][block] = make([]byte, blockSize)
					data[worker][block][0] = byte(worker + block + 1)
				}
			}
			latencies := make([]int64, b.N)
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
					}
				}(worker)
			}
			close(start)
			wg.Wait()
			if firstErr == nil {
				_, firstErr = s.Sync()
			}
			wallDuration := time.Since(wallStart)
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			reportParallelLatencyPercentiles(b, latencies)
			b.ReportMetric(batchBlocks, "batch_blocks")
			b.ReportMetric(float64(b.N*batchBlocks)/wallDuration.Seconds(), "block_ops/s")
		})
	}
}

func BenchmarkPhase167LegacyWALContentionControl(b *testing.B) {
	const (
		blockSize = 4096
		numBlocks = 16384
	)
	for _, writers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			s, err := storage.CreateWALStore(filepath.Join(b.TempDir(), "store.bin"), numBlocks, blockSize)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = s.Close() })
			s.DisableAutoFlushForRecoveryTest()

			data := make([][]byte, writers)
			for i := range data {
				data[i] = make([]byte, blockSize)
				data[i][0] = byte(i + 1)
			}
			latencies := make([]int64, b.N)
			start := make(chan struct{})
			var firstErr error
			var errOnce sync.Once
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
					}
				}(worker)
			}
			close(start)
			wg.Wait()
			if firstErr == nil {
				_, firstErr = s.Sync()
			}
			wallDuration := time.Since(wallStart)
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
			reportParallelLatencyPercentiles(b, latencies)
			b.ReportMetric(1, "sync_calls")
			b.ReportMetric(float64(b.N)/wallDuration.Seconds(), "write_ops/s")
		})
	}
}

func reportParallelLatencyPercentiles(b *testing.B, samples []int64) {
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
