package storage

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
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

			data := make([][]byte, writers)
			for i := range data {
				data[i] = make([]byte, blockSize)
				data[i][0] = byte(i + 1)
			}
			latencies := make([]int64, b.N)
			var next atomic.Int64
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
					for {
						idx := int(next.Add(1) - 1)
						if idx >= b.N {
							return
						}
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

			status := s.WriteInstrumentation()
			reportLatencyPercentiles(b, latencies)
			b.ReportMetric(float64(status.WALAppendLockWaitNanos)/float64(status.WALAppendLockWaitOps), "wal_lock_wait_ns/op")
			b.ReportMetric(float64(b.N)/wallDuration.Seconds(), "write_ops/s")
		})
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
