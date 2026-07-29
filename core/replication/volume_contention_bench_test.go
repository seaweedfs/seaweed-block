package replication

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

type phase167HealthyView struct {
	projection frontend.Projection
}

func (v *phase167HealthyView) Projection() frontend.Projection {
	return v.projection
}

func BenchmarkPhase167RF3SyncQuorumContention(b *testing.B) {
	const (
		blockSize = 4096
		numBlocks = 16384
	)
	if os.Getenv("SW_BLOCK_BENCH_VERBOSE") == "" {
		oldLogWriter := log.Writer()
		log.SetOutput(io.Discard)
		b.Cleanup(func() { log.SetOutput(oldLogWriter) })
	}

	for _, writers := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers_%d", writers), func(b *testing.B) {
			primary, err := storage.CreateWALStore(filepath.Join(b.TempDir(), "primary.bin"), numBlocks, blockSize)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = primary.Close() })

			targets := make([]ReplicaTarget, 0, 2)
			for i := 0; i < 2; i++ {
				replicaStore := storage.NewBlockStore(numBlocks, blockSize)
				listener, listenErr := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
				if listenErr != nil {
					b.Fatal(listenErr)
				}
				listener.Serve()
				b.Cleanup(listener.Stop)
				targets = append(targets, ReplicaTarget{
					ReplicaID:       fmt.Sprintf("r%d", i+1),
					DataAddr:        listener.Addr(),
					ControlAddr:     listener.Addr(),
					Epoch:           1,
					EndpointVersion: 1,
				})
			}

			id := frontend.Identity{
				VolumeID:        "phase167",
				ReplicaID:       "primary",
				Epoch:           1,
				EndpointVersion: 1,
			}
			view := &phase167HealthyView{projection: frontend.Projection{
				VolumeID:        id.VolumeID,
				ReplicaID:       id.ReplicaID,
				Epoch:           id.Epoch,
				EndpointVersion: id.EndpointVersion,
				Healthy:         true,
			}}
			backend := durable.NewStorageBackend(primary, view, id)
			backend.SetOperational(true, "phase167 contention baseline")
			backend.SetWriteAckPolicy(durable.WriteAckRequireObserverAck)
			repVolume := NewReplicationVolume(id.VolumeID, primary)
			repVolume.SetDurabilityMode(DurabilitySyncQuorum)
			if err := repVolume.UpdateReplicaSet(1, targets); err != nil {
				b.Fatal(err)
			}
			backend.SetWriteObserver(repVolume)
			b.Cleanup(func() {
				_ = backend.Close()
				_ = repVolume.Close()
			})

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
						_, writeErr := backend.Write(
							context.Background(),
							int64(idx%numBlocks)*blockSize,
							data[worker],
						)
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
				firstErr = backend.Sync(context.Background())
			}
			wallDuration := time.Since(wallStart)
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}

			stats := repVolume.Stats()
			if stats.WriteOps != uint64(b.N) {
				b.Fatalf("replication write ops=%d want %d", stats.WriteOps, b.N)
			}
			reportReplicationLatencyPercentiles(b, latencies)
			b.ReportMetric(float64(stats.WriteLockWaitNanos)/float64(stats.WriteOps), "repl_lock_wait_ns/op")
			b.ReportMetric(float64(stats.WriteFanoutNanos)/float64(stats.WriteOps), "repl_fanout_ns/op")
			b.ReportMetric(float64(b.N)/wallDuration.Seconds(), "write_ops/s")
		})
	}
}

func reportReplicationLatencyPercentiles(b *testing.B, samples []int64) {
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
