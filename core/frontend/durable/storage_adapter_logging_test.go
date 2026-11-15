package durable_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type writeLogCounter struct {
	dst   io.Writer
	calls atomic.Uint64
	bytes atomic.Uint64
}

func (w *writeLogCounter) Write(p []byte) (int, error) {
	w.calls.Add(1)
	n, err := w.dst.Write(p)
	w.bytes.Add(uint64(n))
	return n, err
}

// These tests are intentionally nonparallel: the standard logger is global.
func captureWriteLogs(tb testing.TB, dst io.Writer) *writeLogCounter {
	tb.Helper()
	old := log.Writer()
	w := &writeLogCounter{dst: dst}
	log.SetOutput(w)
	tb.Cleanup(func() { log.SetOutput(old) })
	return w
}

func TestStorageBackendSuccessfulWritesDoNotLog(t *testing.T) {
	for _, factory := range logicalStorageFactories() {
		t.Run(factory.name, func(t *testing.T) {
			backend, _, _ := newTestBackend(t, factory, 16, 4096)
			logs := captureWriteLogs(t, io.Discard)
			for _, observed := range []bool{false, true} {
				if observed {
					backend.SetWriteObserver(writeObserverFunc{})
				}
				for _, payload := range [][]byte{bytes.Repeat([]byte{0x5A}, 4096), {0xA5}, bytes.Repeat([]byte{0xC3}, 8192)} {
					for i := 0; i < 4; i++ {
						n, err := backend.Write(context.Background(), 0, payload)
						if err != nil || n != len(payload) {
							t.Fatalf("Write=%d,%v", n, err)
						}
						got := make([]byte, len(payload))
						if n, err := backend.Read(context.Background(), 0, got); err != nil || n != len(got) || !bytes.Equal(got, payload) {
							t.Fatalf("readback=%d,%v exact=%v", n, err, bytes.Equal(got, payload))
						}
					}
				}
			}
			if err := backend.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls := logs.calls.Load(); calls != 0 {
				t.Fatalf("24 successful writes emitted %d log writes (%d bytes), want 0", calls, logs.bytes.Load())
			}
		})
	}
}

func TestStorageBackendRejectedWriteStillLogs(t *testing.T) {
	backend, _, _ := newTestBackend(t, logicalStorageFactories()[0], 16, 4096)
	backend.SetOperational(false, "test unavailable")
	var buf bytes.Buffer
	captureWriteLogs(t, &buf)
	if n, err := backend.Write(context.Background(), 0, make([]byte, 4096)); n != 0 || !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("Write=%d,%v", n, err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("gate-rejected")) {
		t.Fatalf("missing gate rejection log: %s", buf.Bytes())
	}
}

// Adapter/logging microbenchmark only: bounded in-memory storage isolates the
// removed log cost. The file sink uses Write, without fsync per log record.
func BenchmarkStorageBackendSuccessfulWriteLogging(b *testing.B) {
	for _, sink := range []string{"counter", "file"} {
		b.Run(sink, func(b *testing.B) {
			var dst io.Writer = io.Discard
			if sink == "file" {
				f, err := os.Create(filepath.Join(b.TempDir(), "write.log"))
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = f.Close() })
				dst = f
			}
			logs := captureWriteLogs(b, dst)
			store := storage.NewBlockStore(16, 4096)
			id := frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
			backend := durable.NewStorageBackend(store, newStubView(healthyProj(id)), id)
			backend.SetOperational(true, "benchmark")
			backend.SetWriteObserver(writeObserverFunc{})
			payload := bytes.Repeat([]byte{0x5A}, 4096)
			ctx := context.Background()
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if n, err := backend.Write(ctx, 0, payload); n != len(payload) || err != nil {
					b.Fatalf("Write=%d,%v", n, err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(logs.calls.Load())/float64(b.N), "logwrites/op")
			b.ReportMetric(float64(logs.bytes.Load())/float64(b.N), "logbytes/op")
			got := make([]byte, len(payload))
			if _, err := backend.Read(ctx, 0, got); err != nil || !bytes.Equal(got, payload) {
				b.Fatal("readback mismatch", err)
			}
		})
	}
}
