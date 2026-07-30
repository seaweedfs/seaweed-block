package parallelwal

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

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
				SlotsPerLane:  4096,
				RetainPerLane: 64,
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
			s.mu.RUnlock()
			reportParallelLatencyPercentiles(b, latencies)
			b.ReportMetric(float64(activeLanes), "active_lanes")
			b.ReportMetric(float64(checkpointWriteOps), "checkpoint_write_ops")
			b.ReportMetric(float64(walTail), "wal_tail")
			b.ReportMetric(float64(b.N)/wallDuration.Seconds(), "write_ops/s")
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
