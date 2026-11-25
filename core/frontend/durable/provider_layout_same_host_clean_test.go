package durable_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The authority's same-host clean-move test proves the detached HEAD and ACK
// admit epoch+1. This B-path test proves that the successor on that same
// host actually serves the sealed image, rather than its new empty root.
func TestLayoutSameHostCleanSuccessorReadsSealedImage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := newLayoutCase(t, 64, 4)
	locks := t.TempDir()
	configure := func(cfg *durable.ProviderConfig) {
		cfg.HostVolumeLock = true
		cfg.VolumeLockDirTestOnly = locks
	}
	old := c.provider(t, t.TempDir(), v1View(), "node-a/r1", configure)
	s := openRecovered(t, old)
	want0, want3 := block(0x31), block(0x73)
	writeSync(t, s, 0, 0x31)
	writeSync(t, s, 3, 0x73)
	sealed, _, err := old.DetachVolumePlain(ctx, "v1")
	if err != nil || sealed.Head.State != storage.ChunkHeadDetached || sealed.H < 2 {
		t.Fatalf("clean seal: %+v %v", sealed, err)
	}
	head := c.head(t)
	if head.State != layout.StateDetached || head.Current == nil || head.Position != sealed.H || head.Holder != "node-a/r1" || head.Epoch != 3 {
		t.Fatalf("B HEAD did not retain the sealed image: %+v", head)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	// A new root and the same holder name make this specifically a same-host
	// clean successor, not an own-epoch restart reading its old local WAL.
	next := c.provider(t, t.TempDir(), viewAs("r1", 4), "node-a/r1", configure)
	loaded, err := next.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := next.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("same-host clean successor did not Start from the seal: %v", err)
	}
	for lba, want := range map[uint32][]byte{0: want0, 3: want3} {
		got, err := loaded.Read(lba)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("same-host successor LBA %d: err=%v matches sealed bytes=%t", lba, err, bytes.Equal(got, want))
		}
	}
	after := c.head(t)
	if after.State != layout.StateLive || after.Epoch != 4 || after.Holder != "node-a/r1" || after.ClaimID == head.ClaimID || after.Position != head.Position {
		t.Fatalf("same-host successor did not consume the sealed lineage: before=%+v after=%+v", head, after)
	}
}
