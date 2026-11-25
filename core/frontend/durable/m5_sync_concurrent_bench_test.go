package durable_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

func BenchmarkConcurrentBackendSync(b *testing.B) {
	for _, impl := range []durable.ImplName{durable.ImplSmartWAL, durable.ImplWALStoreChunk} {
		for _, qd := range []int{8, 32} {
			b.Run(fmt.Sprintf("%s/QD%d", impl, qd), func(b *testing.B) {
				id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
				cfg := durable.ProviderConfig{Impl: impl, StorageRoot: b.TempDir(), NumBlocks: 32768, BlockSize: 4096, ChunkBlocks: 1024}
				if impl == durable.ImplWALStoreChunk {
					testutil.NewLayoutFixture(b, "v1", 32768, 1024).Configure(&cfg)
				}
				p, err := durable.NewDurableProvider(cfg, newStubView(healthyProj(id)))
				if err != nil {
					b.Fatal(err)
				}
				defer p.Close()
				raw, err := p.Open(context.Background(), "v1")
				if err != nil {
					b.Fatal(err)
				}
				if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
					b.Fatal(err)
				}
				backend := raw.(*durable.StorageBackend)
				profile := backend.WriteProfile()
				workloadCtx, cancelWorkload := context.WithTimeout(context.Background(), 30*time.Second)
				b.ResetTimer()
				elapsed, workloadErr := runConcurrentSyncProxy(b, workloadCtx, backend, qd, b.N)
				b.StopTimer()
				cancelWorkload()
				if workloadErr != nil {
					b.Fatal(workloadErr)
				}
				post := backend.WriteProfile()
				n := uint64(qd * b.N)
				if post.BackendSyncOps-profile.BackendSyncOps != n {
					b.Fatal("Sync count changed")
				}
				for w := 0; w < qd; w++ {
					got := make([]byte, 4096)
					if _, err := backend.Read(context.Background(), int64(w)*4096, got); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(w + 1)}, 4096)) {
						b.Fatal("readback", w, err)
					}
				}
				row := map[string]any{"impl": impl, "qd": qd, "iterations": b.N, "syncs": n, "elapsed_ns": elapsed.Nanoseconds(), "syncs_per_second": float64(n) / elapsed.Seconds(), "variant": os.Getenv("STUDY_VARIANT")}
				data, _ := json.Marshal(row)
				fmt.Printf("M5_CONCURRENT %s\n", data)
				b.ReportMetric(float64(n)/elapsed.Seconds(), "Sync/s")
			})
		}
	}
}

// Local file I/O is not interruptible by context. Bound the completion wait;
// a stalled operation fails the benchmark with its goroutine stacks. The caller
// registers backend cleanup before starting this workload.
type concurrentSyncProxyBackend interface {
	Write(context.Context, int64, []byte) (int, error)
	Sync(context.Context) error
}

func runConcurrentSyncProxy(tb testing.TB, ctx context.Context, backend concurrentSyncProxyBackend, qd, iterations int) (time.Duration, error) {
	tb.Helper()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, qd)
	var wg sync.WaitGroup
	for w := 0; w < qd; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			fail := func(err error) { errs <- err; cancel() }
			payload := bytes.Repeat([]byte{byte(w + 1)}, 4096)
			for i := 0; i < iterations; i++ {
				if err := ctx.Err(); err != nil {
					fail(err)
					return
				}
				if _, err := backend.Write(ctx, int64(w)*4096, payload); err != nil {
					fail(err)
					return
				}
				if err := ctx.Err(); err != nil {
					fail(err)
					return
				}
				if err := backend.Sync(ctx); err != nil {
					fail(err)
					return
				}
			}
		}(w)
	}
	started := time.Now()
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		dump := make([]byte, 1<<20)
		n := runtime.Stack(dump, true)
		tb.Fatalf("timed out waiting for backend Sync workers: %v\n%s", ctx.Err(), dump[:n])
	}
	elapsed := time.Since(started)
	close(errs)
	for err := range errs {
		return elapsed, err
	}
	return elapsed, ctx.Err()
}
