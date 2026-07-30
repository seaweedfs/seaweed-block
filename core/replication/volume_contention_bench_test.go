package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
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
			replicaStores := make([]*storage.BlockStore, 0, 2)
			for i := 0; i < 2; i++ {
				replicaStore := storage.NewBlockStore(numBlocks, blockSize)
				replicaStores = append(replicaStores, replicaStore)
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
						fillPhase167Block(data[worker], idx)
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
						workerOps[worker]++
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
			if b.N >= writers {
				for worker, ops := range workerOps {
					if ops == 0 {
						b.Fatalf("worker %d completed zero writes", worker)
					}
				}
			}
			for replicaIndex, replicaStore := range replicaStores {
				deadline := time.Now().Add(3 * time.Second)
				for {
					_, _, head := replicaStore.Boundaries()
					if head == uint64(b.N) {
						break
					}
					if time.Now().After(deadline) {
						b.Fatalf("replica %d head=%d want %d after queued-work drain", replicaIndex, head, b.N)
					}
					time.Sleep(time.Millisecond)
				}
				if b.N <= numBlocks {
					for idx := 0; idx < b.N; idx++ {
						got, readErr := replicaStore.Read(uint32(idx))
						if readErr != nil {
							b.Fatal(readErr)
						}
						want := make([]byte, blockSize)
						fillPhase167Block(want, idx)
						if !bytes.Equal(got, want) {
							b.Fatalf("replica %d LBA %d data mismatch", replicaIndex, idx)
						}
					}
				}
			}
			reportReplicationLatencyPercentiles(b, latencies)
			b.ReportMetric(float64(stats.WriteLockWaitNanos)/float64(stats.WriteOps), "repl_lock_wait_ns/op")
			b.ReportMetric(float64(stats.WriteFanoutNanos)/float64(stats.WriteOps), "repl_fanout_ns/op")
			b.ReportMetric(float64(stats.WriteAckWaitNanos)/float64(stats.WriteOps), "repl_ack_wait_ns/op")
			b.ReportMetric(float64(stats.PeerQueueMaxDepth), "peer_queue_max_depth")
			b.ReportMetric(float64(stats.PeerQueueSaturated), "peer_queue_saturated")
			b.ReportMetric(float64(b.N)/wallDuration.Seconds(), "write_ops/s")
		})
	}
}

func fillPhase167Block(block []byte, operation int) {
	for offset := 0; offset+8 <= len(block); offset += 8 {
		value := uint64(uint32(operation+1))<<32 | uint64(uint32(offset/8))
		binary.LittleEndian.PutUint64(block[offset:offset+8], value)
	}
	for offset := len(block) &^ 7; offset < len(block); offset++ {
		block[offset] = byte(operation + offset + 1)
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
