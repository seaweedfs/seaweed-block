package durable_test

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
)

// INV-FE-PRIMARY-READY-OPERATIONAL-001 (condition A): a HEAD fence after
// startup, while the lease would still be valid, refuses I/O from the fence,
// latches the backend not operational with its evidence and marks the volume
// runtime-fenced with a wakeup, so the process owner chooses the no-restart
// exit. Another holder claims HEAD at a newer epoch, or at the same epoch
// under another holder string; this holder's next publication fences.
func TestWALStoreChunk_RuntimeHeadFenceLatchesNotOperational(t *testing.T) {
	for _, tc := range []struct {
		name   string
		epoch  func(uint64) uint64
		holder string
	}{
		{"newer epoch", func(e uint64) uint64 { return e + 1 }, "s2/r1"},
		{"same epoch, other holder", func(e uint64) uint64 { return e }, "s2/r1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			// The provider's own layout HEAD: the one its publications read.
			fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
			p := layoutChunkProvider(t, t.TempDir(), fixture, v1View(), 0)
			s := openRecovered(t, p)
			if _, err := s.Write(1, block(1)); err != nil {
				t.Fatal(err)
			}
			if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			if p.ChunkRuntimeFenced("v1") {
				t.Fatal("runtime-fenced while serving")
			}
			backend, err := p.Open(ctx, "v1")
			if err != nil {
				t.Fatal(err)
			}

			hs, err := fixture.Catalog.Store(fixture.Ref)
			if err != nil {
				t.Fatal(err)
			}
			head, _, err := hs.ReadHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			next := head
			next.Epoch, next.Holder = tc.epoch(head.Epoch), tc.holder
			if err := hs.UpdateHead(ctx, head.Gen, next); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(2, block(2)); err != nil {
				t.Fatal(err)
			}
			if _, err := p.CaptureVolume(ctx, "v1"); err == nil {
				t.Fatal("a capture over another holder's HEAD succeeded")
			}
			// From the fence on, no I/O is admitted (before the watcher runs).
			if n, err := backend.Write(ctx, 3*4096, block(3)); err == nil || n != 0 {
				t.Fatalf("a write after the fence: n=%d err=%v, want refused", n, err)
			}

			select {
			case <-p.ChunkRuntimeFenceUpdates():
			case <-time.After(5 * time.Second):
				t.Fatal("no runtime-fence wakeup within the cleanup bound")
			}
			if !p.ChunkRuntimeFenced("v1") {
				t.Fatal("woken but not runtime-fenced")
			}
			var st *durable.VolumeStatus
			for _, v := range p.DurableStatuses() {
				if v.VolumeID == "v1" {
					v := v
					st = &v
				}
			}
			if st == nil || st.Operational || st.Evidence != "chunk layer fenced after startup: HEAD has another publication owner" {
				t.Fatalf("durable status after a runtime fence: %+v, want not operational with the fence evidence", st)
			}
			if _, err := backend.Write(ctx, 3*4096, block(3)); err == nil {
				t.Fatal("a write after the runtime fence was admitted")
			}
		})
	}
}
