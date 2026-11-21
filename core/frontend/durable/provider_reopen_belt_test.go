package durable_test

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestLocalReopenClaimsBeforeServing(t *testing.T) {
	ctx := hangGuard(t)
	r := storage.NewMemChunkRemote()
	root := t.TempDir()
	first := chunkProvider(t, root, r, v1View(), 0)
	s := openRecovered(t, first)
	writeSync(t, s, 0, 0x31)
	if _, err := first.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	before := headHolder(t, r)
	writeSync(t, s, 1, 0x42) // locally acknowledged, not yet in HEAD
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := chunkProvider(t, root, r, v1View(), 0)
	s = openRecovered(t, second)
	after := headHolder(t, r)
	if after.Gen <= before.Gen || after.Epoch != before.Epoch || after.Holder != before.Holder || after.RemoteLSN != before.RemoteLSN {
		t.Errorf("local reopen served without replacing the old CAS guard: before=%+v after=%+v", before, after)
	}
	if got, err := s.Read(1); err != nil || !bytes.Equal(got, block(0x42)) {
		t.Fatal("claim replaced locally acknowledged bytes with HEAD", err)
	}
	if _, err := second.CaptureVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(headBlock(t, r, 0), block(0x31)) || !bytes.Equal(headBlock(t, r, 1), block(0x42)) {
		t.Fatal("reopen did not publish the complete acknowledged image")
	}
}
