package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// File-backed backend proxy, not a guest/mount benchmark. Writers are released
// by the actual Sync/H handshake and issue one 4K write+Sync each per cut.
// This file is intentionally source-identical on the parent comparison ref.
func BenchmarkCutCOWProxy(b *testing.B) {
	for _, chunks := range []int{8, 64} {
		for _, qd := range []int{1, 8} {
			b.Run(fmt.Sprintf("chunks%d/QD%d", chunks, qd), func(b *testing.B) {
				const blocksPerChunk = 1024
				s, err := CreateWALStoreWithOptions(filepath.Join(b.TempDir(), "volume"), uint32(chunks*blocksPerChunk), 4096, WALStoreOptions{ChunkBlocks: blocksPerChunk})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = s.Close() })
				s.DisableAutoFlushForRecoveryTest()
				if _, err = s.Recover(); err != nil {
					b.Fatal(err)
				}
				remote := NewMemChunkRemote()
				layer, err := AttachChunkLayer(s, remote, ChunkLayerConfig{ChunkBlocks: blocksPerChunk, Epoch: 1, Holder: "proxy", UploadConcurrency: 8, FreshFromHead: true})
				if err != nil {
					b.Fatal(err)
				}
				l := layer.(*chunkLayerImpl)
				if _, err = l.Cut(context.Background()); err != nil {
					b.Fatal(err)
				}
				var latencies []int64
				var barrier, copyNs, syncNs, total, uploads, bytes int64
				b.ResetTimer()
				b.StopTimer()
				for n := 0; n < b.N; n++ {
					data := makeBlock(4096, byte(1+n%100))
					for c := 0; c < chunks; c++ {
						if _, err = s.Write(uint32(c*blocksPerChunk), data); err != nil {
							b.Fatal(err)
						}
					}
					if _, err = s.Sync(); err != nil {
						b.Fatal(err)
					}
					if err = s.flusher.flushOnce(); err != nil {
						b.Fatal(err)
					}
					start := make(chan struct{})
					done := make(chan struct{})
					durations := make([]int64, qd)
					errs := make([]error, qd)
					l.testAfterBarrierSyncHook = func(uint64) { close(start) }
					var wg sync.WaitGroup
					for writer := 0; writer < qd; writer++ {
						wg.Add(1)
						go func(i int) {
							defer wg.Done()
							<-start
							t := time.Now()
							_, e := s.Write(uint32((chunks-1-i%chunks)*blocksPerChunk+1), makeBlock(4096, byte(0xa0+i)))
							if e == nil {
								_, e = s.Sync()
							}
							durations[i] = time.Since(t).Nanoseconds()
							errs[i] = e
						}(writer)
					}
					go func() { wg.Wait(); close(done) }()
					b.StartTimer()
					r, e := l.Cut(context.Background())
					b.StopTimer()
					select {
					case <-done:
					case <-time.After(30 * time.Second):
						stack := make([]byte, 1<<20)
						nn := runtime.Stack(stack, true)
						b.Fatalf("proxy writers stalled\n%s", stack[:nn])
					}
					if e != nil {
						b.Fatal(e)
					}
					for _, e := range errs {
						if e != nil {
							b.Fatal(e)
						}
					}
					l.testAfterBarrierSyncHook = nil
					latencies = append(latencies, durations...)
					barrier += r.Stats.BarrierPauseNs
					copyNs += r.Stats.CopyNs
					syncNs += r.Stats.SyncNs
					total += r.Stats.TotalNs
					uploads += int64(r.Stats.UploadedChunks)
					bytes += r.Stats.UploadedBytes
					// The proxy is not measuring retained old remote versions. Retain only
					// current HEAD objects between iterations, identically in both arms.
					remote.mu.Lock()
					live := map[string]bool{}
					for _, ref := range remote.head.Chunks {
						live[ref.Key] = true
					}
					for key := range remote.objs {
						if !live[key] {
							delete(remote.objs, key)
						}
					}
					remote.mu.Unlock()
				}
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
				percentile := func(p int) float64 {
					i := (len(latencies)*p+99)/100 - 1
					if i < 0 {
						i = 0
					}
					return float64(latencies[i])
				}
				b.ReportMetric(percentile(50), "writer-p50-ns")
				b.ReportMetric(percentile(99), "writer-p99-ns")
				b.ReportMetric(float64(latencies[len(latencies)-1]), "writer-max-ns")
				for name, value := range map[string]int64{"barrier-ns": barrier, "copy-ns": copyNs, "sync-ns": syncNs, "cut-ns": total, "uploaded-chunks": uploads, "uploaded-bytes": bytes} {
					b.ReportMetric(float64(value)/float64(b.N), name)
				}
			})
		}
	}
}
