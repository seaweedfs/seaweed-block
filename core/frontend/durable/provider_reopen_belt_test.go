package durable_test

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
)

func TestLocalReopenClaimsBeforeServing(t *testing.T) {
	ctx := hangGuard(t)
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	root := t.TempDir()
	first := layoutChunkProvider(t, root, fixture, v1View(), 0)
	s := openRecovered(t, first)
	writeSync(t, s, 0, 0x31)
	if _, err := first.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	before := layoutHead(t, fixture)
	writeSync(t, s, 1, 0x42) // locally acknowledged, not yet in HEAD
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := layoutChunkProvider(t, root, fixture, v1View(), 0)
	s = openRecovered(t, second)
	after := layoutHead(t, fixture)
	if after.Epoch != before.Epoch || after.Holder != before.Holder || after.ClaimID != before.ClaimID || after.Position != before.Position {
		t.Errorf("local reopen changed the holder identity or published position: before=%+v after=%+v", before, after)
	}
	if got, err := s.Read(1); err != nil || !bytes.Equal(got, block(0x42)) {
		t.Fatal("claim replaced locally acknowledged bytes with HEAD", err)
	}
	if _, err := second.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(layoutHeadBlock(t, fixture, 0), block(0x31)) || !bytes.Equal(layoutHeadBlock(t, fixture, 1), block(0x42)) {
		t.Fatal("reopen did not publish the complete acknowledged image")
	}
}
