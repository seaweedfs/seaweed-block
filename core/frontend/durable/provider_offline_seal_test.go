package durable_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// G.GONE.SEALED: an idle plain detach seals under the caller's deadline,
// including after the frontend handle closes; no live-serving timer applies.
func TestOfflineSealUsesCallerDeadline(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "closed"}[closed], func(t *testing.T) {
			remote := storage.NewMemChunkRemote()
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
				NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks, DetachFreezeTimeout: time.Second,
				ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
			}, v1View())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Close() })
			openRecovered(t, p)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			b, err := p.Open(ctx, "v1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = b.Write(ctx, 4096, block(0x61)); err != nil {
				t.Fatal(err)
			}
			if closed {
				if err = b.Close(); err != nil {
					t.Fatal(err)
				}
			}
			deadline, _ := ctx.Deadline()
			observed := false
			durable.SetTestDetachStopIO(p, func(c context.Context) error {
				observed = true
				got, ok := c.Deadline()
				if !ok || !got.Equal(deadline) {
					t.Errorf("offline seal inherited live timer: got %v known=%t want caller %v", got, ok, deadline)
				}
				return b.(*durable.StorageBackend).StopIO(c, "offline seal test")
			})
			r, last, err := p.DetachVolumePlain(ctx, "v1")
			if err != nil || r.Head.State != storage.ChunkHeadDetached || r.H != last {
				t.Fatalf("offline seal failed: %+v last=%d err=%v", r, last, err)
			}
			if !observed {
				t.Error("offline seal did not drain")
			}
			if !bytes.Equal(headBlock(t, remote, 1), block(0x61)) {
				t.Fatal("offline seal lost acknowledged bytes")
			}
		})
	}
}
