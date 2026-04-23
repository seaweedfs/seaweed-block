// T3c perf baseline benchmark — characterization only, no gate.
//
// Publishes first-light numbers for both LogicalStorage impls so
// later changes have a comparison point. No threshold / no
// pass-fail. Matches the workload shape pinned in
// testrunner/perf/t3c-durable-baseline.md.
//
// Run (matrix walks both impls):
//   go test -run '^$' -bench 'BenchmarkT3c_DurablePerf' \
//     ./core/frontend/durable/ -benchtime=60s -count=1

package durable_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
)

// BenchmarkT3c_DurablePerf drives 4 KiB random writes against a
// DurableProvider-sourced backend. One variant per impl.
//
// Scope caveat: this is NOT a sustained-throughput benchmark —
// it caps iterations at maxIters below so the walstore's WAL
// doesn't fill during a long -benchtime run (the flusher is
// eventually-consistent and can't drain faster than this
// benchmark writes). For real sustained numbers, run m01 fio.
// This benchmark exists to publish first-light relative
// per-op cost between walstore and smartwal; it does NOT claim
// to saturate either impl.
func BenchmarkT3c_DurablePerf(b *testing.B) {
	for _, impl := range implMatrix() {
		impl := impl
		b.Run(string(impl), func(b *testing.B) {
			// Cap N so WAL capacity is not the bottleneck.
			// Characterization only; not a saturation benchmark.
			const maxIters = 8000
			if b.N > maxIters {
				b.N = maxIters
			}
			root := b.TempDir()
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			view := newStubView(healthyProj(id))
			const blockSize = 4096
			const numBlocks = uint32(65536) // 256 MiB
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl:        impl,
				StorageRoot: root,
				BlockSize:   blockSize,
				NumBlocks:   numBlocks,
			}, view)
			if err != nil {
				b.Fatalf("NewDurableProvider: %v", err)
			}
			defer p.Close()
			ctx := context.Background()
			backend, err := p.Open(ctx, "v1")
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
				b.Fatalf("RecoverVolume: %v", err)
			}

			payload := make([]byte, blockSize)
			for i := range payload {
				payload[i] = byte(i)
			}
			rng := rand.New(rand.NewSource(42))

			b.ResetTimer()
			b.SetBytes(int64(blockSize))
			// Sync every syncBatch writes so the flusher has room
			// to drain the WAL — mirrors real workloads where
			// apps periodically fsync. Without this, walstore
			// fills its WAL faster than the flusher drains and
			// admission control errors. Benchmark measures write
			// + periodic-sync cost, not burst throughput; documented
			// in testrunner/perf/t3c-durable-baseline.md.
			const syncBatch = 64
			for i := 0; i < b.N; i++ {
				lba := rng.Int31n(int32(numBlocks))
				offset := int64(lba) * int64(blockSize)
				if _, err := backend.Write(ctx, offset, payload); err != nil {
					b.Fatalf("Write: %v", err)
				}
				if (i+1)%syncBatch == 0 {
					if err := backend.Sync(ctx); err != nil {
						b.Fatalf("Sync: %v", err)
					}
				}
			}
		})
	}
}
