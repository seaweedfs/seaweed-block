package durable_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"os"
	"os/exec"
	"sort"
	"testing"
	"time"
)

func BenchmarkQD1PairedBackendSync(b *testing.B) {
	for _, impl := range []durable.ImplName{durable.ImplSmartWAL, durable.ImplWALStoreChunk} {
		b.Run(string(impl), func(b *testing.B) {
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			root := b.TempDir()
			diagnosticCtx, cancelDiagnostic := context.WithTimeout(context.Background(), 2*time.Second)
			out, diagnosticErr := exec.CommandContext(diagnosticCtx, "findmnt", "-T", root, "-no", "SOURCE,FSTYPE,TARGET").CombinedOutput()
			cancelDiagnostic()
			if diagnosticErr == nil {
				fmt.Printf("STUDY_FS %s", out)
			}
			cfg := durable.ProviderConfig{Impl: impl, StorageRoot: root, NumBlocks: 32768, BlockSize: 4096, ChunkBlocks: 1024}
			remote := storage.NewMemChunkRemote()
			if impl == durable.ImplWALStoreChunk {
				cfg.ChunkRemoteFor = func(string) (storage.ChunkRemote, error) { return remote, nil }
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
			payload := make([]byte, 4096)
			b.StopTimer()
			// Same QD1 request shape as the guest: one write followed by two Sync calls.
			for i := 0; i < 20; i++ {
				if _, err := backend.Write(context.Background(), 0, payload); err != nil {
					b.Fatal(err)
				}
				for j := 0; j < 2; j++ {
					if err := backend.Sync(context.Background()); err != nil {
						b.Fatal(err)
					}
				}
			}
			profile := backend.WriteProfile()
			samples := make([]int64, 0, 2*b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				payload[0] = byte(i)
				if _, err := backend.Write(context.Background(), int64(i%8192)*4096, payload); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for j := 0; j < 2; j++ {
					start := time.Now()
					if err := backend.Sync(context.Background()); err != nil {
						b.Fatal(err)
					}
					samples = append(samples, time.Since(start).Nanoseconds())
				}
			}
			b.StopTimer()
			post := backend.WriteProfile()
			n := uint64(2 * b.N)
			if post.BackendSyncOps-profile.BackendSyncOps != n {
				b.Fatal("D0 Sync count changed")
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			var total int64
			for _, v := range samples {
				total += v
			}
			var got [4096]byte
			if _, err := backend.Read(context.Background(), int64((b.N-1)%8192)*4096, got[:]); err != nil || !bytes.Equal(got[:], payload) {
				b.Fatal("last write bytes mismatch", err)
			}
			report := map[string]any{"impl": impl, "iterations": b.N, "syncs": n, "sync_mean_ns": float64(total) / float64(n), "sync_p50_ns": samples[len(samples)/2], "sync_p99_ns": samples[(len(samples)*99-1)/100], "variant": os.Getenv("STUDY_VARIANT")}
			data, _ := json.Marshal(report)
			fmt.Printf("M5_SYNC_STUDY %s\n", data)
			b.ReportMetric(float64(total)/float64(n), "ns/Sync")
		})
	}
}
