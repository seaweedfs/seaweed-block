package durable

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func hydrationProgress(t *testing.T, p *DurableProvider) (absent int, hydrated uint64) {
	t.Helper()
	for _, st := range p.DurableStatuses() {
		if st.VolumeID == "v1" && st.Chunk != nil && st.Chunk.Attached {
			return st.Chunk.Stats.AbsentChunks, st.Chunk.Stats.Hydration.Completed
		}
	}
	t.Fatal("v1 has no attached chunk layer")
	return 0, 0
}

// INV-CUT-HYDRATION-PROGRESS-001: the status reports, per volume, the
// chunks still to hydrate (AbsentChunks: HEAD-named, not yet local) and the
// chunks hydrated so far (Hydration.Completed, cumulative). A read of an
// un-hydrated chunk moves both by one; a second read of it moves neither. A
// caller can therefore prove that a read happened before its chunk was
// hydrated by reading the two before and after it.
func TestHydrationProgressCountersFollowFirstReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mem := storage.NewMemChunkRemote()
	src := kzProvider(t, t.TempDir(), mem)
	if _, err := src.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := src.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	data := map[int64][]byte{3: bytes.Repeat([]byte{0x33}, 4096), 7: bytes.Repeat([]byte{0x77}, 4096)}
	for chunk, b := range data {
		if _, err := src.Backend("v1").Write(ctx, chunk*16*4096, b); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Backend("v1").Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := src.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}

	p := kzProvider(t, t.TempDir(), mem)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	want := []struct {
		read     int64 // chunk read before the check (-1: none)
		absent   int
		hydrated uint64
	}{{-1, 2, 0}, {3, 1, 1}, {3, 1, 1}, {7, 0, 2}}
	got := make([]byte, 4096)
	for _, w := range want {
		if w.read >= 0 {
			if n, err := p.Backend("v1").Read(ctx, w.read*16*4096, got); err != nil || n != len(got) || !bytes.Equal(got, data[w.read]) {
				t.Fatalf("read of chunk %d: %d %v, data equal %v", w.read, n, err, bytes.Equal(got, data[w.read]))
			}
		}
		if absent, hydrated := hydrationProgress(t, p); absent != w.absent || hydrated != w.hydrated {
			t.Fatalf("after reading chunk %d: %d still to hydrate, %d hydrated; want %d, %d", w.read, absent, hydrated, w.absent, w.hydrated)
		}
	}
}
