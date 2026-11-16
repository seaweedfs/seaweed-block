package durable_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Identical file compiled on main and the feature head. Includes the full
// 4K write plus paired Flush, handler and backend, not only the fsync syscall.
func TestDurabilityLatencyPerf(t *testing.T) {
	if os.Getenv("SW_BLOCK_LATENCY_PERF") != "1" {
		t.Skip("opt-in file-backed fsync-heavy proxy")
	}
	for _, kind := range []string{"walstore", "walstore-chunk"} {
		t.Run(kind, func(t *testing.T) {
			var b *durable.StorageBackend
			if kind == "walstore" {
				b = openDurableBackend(t, durable.ImplWALStore, 256, 4096)
			} else {
				p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), v1View(), 0)
				openRecovered(t, p)
				if _, err := p.Open(context.Background(), "v1"); err != nil {
					t.Fatal(err)
				}
				b = p.Backend("v1")
			}
			h := nvme.NewIOHandler(nvme.HandlerConfig{Backend: b, BlockSize: 4096, VolumeSize: 256 * 4096, NSID: 1})
			data := make([]byte, 4096)
			ns := make([]int64, 0, 100)
			for i := 0; i < 110; i++ {
				data[0] = byte(i)
				start := time.Now()
				cmds := []nvme.IOCommand{{Opcode: 1, NSID: 1, NLB: 1, SLBA: uint64(i % 128), Data: data}, {Opcode: 0, NSID: 1}, {Opcode: 0, NSID: 1}}
				for _, cmd := range cmds {
					if r := h.Handle(context.Background(), cmd); r.AsError() != nil {
						t.Fatal(r.AsError())
					}
				}
				if i >= 10 {
					ns = append(ns, time.Since(start).Nanoseconds())
				}
			}
			sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
			var sum int64
			for _, n := range ns {
				sum += n
			}
			report := map[string]any{"impl": kind, "samples": 100, "mean_ns": sum / 100, "p50_ns": ns[49], "p99_ns": ns[98], "scope": "file-backed 4K Write + paired Flush; no guest/frontend transport"}
			encoded, _ := json.Marshal(report)
			fmt.Printf("LATENCY_WORKLOAD %s\n", encoded)
		})
	}
}
