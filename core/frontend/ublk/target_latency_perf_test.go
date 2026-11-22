package ublk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestTargetDurabilityLatencyPerf(t *testing.T) {
	if os.Getenv("SW_BLOCK_TARGET_LATENCY_PERF") != "1" {
		t.Skip("opt-in actual Target provider-path proxy")
	}
	for _, kind := range []durable.ImplName{durable.ImplWALStore, durable.ImplWALStoreChunk} {
		t.Run(string(kind), func(t *testing.T) {
			cfg := durable.ProviderConfig{Impl: kind, StorageRoot: t.TempDir(), NumBlocks: 256, BlockSize: 4096}
			if kind == durable.ImplWALStoreChunk {
				cfg.ChunkBlocks = 16
				remote := storage.NewMemChunkRemote()
				cfg.ChunkRemoteFor = func(string) (storage.ChunkRemote, error) { return remote, nil }
				cfg.ChunkHolder = "r1"
			}
			p, err := durable.NewDurableProvider(cfg, targetPerfView{})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatal(err)
			}
			tg := NewTarget(TargetConfig{Provider: p, VolumeID: "v1", VolumeSize: 256 * 4096, BlockSize: 4096})
			var h handler
			tg.startDevice = func(cfg Config) (targetDevice, error) {
				h = handler{backend: cfg.Backend, size: cfg.Size}
				return &targetPerfDevice{}, nil
			}
			if _, err := tg.Start(); err != nil {
				t.Fatal(err)
			}
			defer tg.Close()
			var samples []int64
			data := make([]byte, 4096)
			for i := 0; i < 110; i++ {
				data[0] = byte(i)
				start := time.Now()
				if n := h.handle(context.Background(), request{op: ioOpWrite, nrSectors: 8, startSector: uint64(i%128) * 8}, data); n != 4096 {
					t.Fatal(n)
				}
				for j := 0; j < 2; j++ {
					if n := h.handle(context.Background(), request{op: ioOpFlush}, nil); n != 0 {
						t.Fatal(n)
					}
				}
				if i >= 10 {
					samples = append(samples, time.Since(start).Nanoseconds())
				}
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			var sum int64
			for _, n := range samples {
				sum += n
			}
			dataJSON, _ := json.Marshal(map[string]any{"impl": kind, "samples": len(samples), "mean_ns": sum / int64(len(samples)), "p50_ns": samples[49], "p99_ns": samples[98]})
			fmt.Printf("TARGET_LATENCY %s\n", dataJSON)
		})
	}
}

type targetPerfView struct{}

func (targetPerfView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, Healthy: true}
}

type targetPerfDevice struct{}

func (*targetPerfDevice) Path() string { return "/test-device" }
func (*targetPerfDevice) Stats() Stats { return Stats{} }
func (*targetPerfDevice) Close() error { return nil }
