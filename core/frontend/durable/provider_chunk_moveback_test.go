package durable_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func viewAs(replica string, epoch uint64) *stubView {
	return newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: replica, Epoch: epoch, EndpointVersion: 1}))
}

// A workspace moves A -> B -> A, and A's old durable root is still on disk.
// B's acknowledged, synced and published writes must be what A serves after
// the move back, and A's next cut must not overwrite them with A's stale
// image.
func TestWALStoreChunk_MoveBackOntoAnOldRootServesTheNewerHead(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	rootA, rootB := t.TempDir(), t.TempDir()

	// 1. A (epoch 3) cuts LBA 0 and LBA 16, then keeps writing without a cut.
	pA := chunkProvider(t, rootA, remote, viewAs("rA", 3), 0)
	sA := openRecovered(t, pA)
	for _, w := range []struct {
		lba  uint32
		fill byte
	}{{0, 0xA1}, {16, 0xA1}} {
		if _, err := sA.Write(w.lba, block(w.fill)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pA.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatalf("A capture: %v", err)
	}
	for _, lba := range []uint32{0, 16} {
		if _, err := sA.Write(lba, block(0xA2)); err != nil { // never cut
			t.Fatal(err)
		}
	}
	if _, err := sA.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := pA.Close(); err != nil { // A is lost; its root stays on disk
		t.Fatal(err)
	}

	// 2. A forced move to B (epoch 4, fresh root): B claims HEAD, writes,
	// syncs and cuts.
	pB := chunkProvider(t, rootB, remote, viewAs("rB", 4), 0)
	sB := openRecovered(t, pB)
	if got, err := sB.Read(0); err != nil || !bytes.Equal(got, block(0xA1)) {
		t.Fatalf("B reads A's published LBA 0: %v", err)
	}
	if _, err := sB.Write(0, block(0xB1)); err != nil {
		t.Fatal(err)
	}
	if _, err := sB.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := pB.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatalf("B capture: %v", err)
	}
	if err := pB.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(headBlock(t, remote, 0), block(0xB1)) {
		t.Fatal("HEAD does not hold B's write")
	}

	// 3. Back to A (epoch 5) over its old root.
	pA2 := chunkProvider(t, rootA, remote, viewAs("rA", 5), 0)
	sA2 := openRecovered(t, pA2)
	got, err := sA2.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, block(0xB1)) {
		t.Errorf("after the move back, A serves LBA 0 = %02x; want B's published %02x (A's stale image is %02x)", got[0], 0xB1, 0xA2)
	}
	// A's cut, retried while the layer reconciles (hash mode).
	var capErr error
	for i := 0; i < 100; i++ {
		if _, capErr = pA2.CaptureVolume(ctx, "v1"); capErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("A capture after the move back: %v", capErr)
	if h := headBlock(t, remote, 0); !bytes.Equal(h, block(0xB1)) {
		t.Errorf("after A's cut, HEAD LBA 0 = %02x; B's acked, synced and published %02x was overwritten", h[0], 0xB1)
	}
}
