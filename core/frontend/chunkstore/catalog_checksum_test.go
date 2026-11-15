package chunkstore

import (
	"context"
	"testing"
)

func TestCatalogC12_ProtectionSurvivesSnapshotAndFork(t *testing.T) {
	ctx := context.Background()
	f, b := newFakeFiler(), newFakeBlobs()
	c := newCat(t, f)
	policy := VolumeProtection{StorageFormatVersion: 2, DataChecksums: true}
	vol, err := c.EnsureVolume(ctx, "v1", catGeo, policy)
	if err != nil {
		t.Fatal(err)
	}
	if vol.VolumeProtection != policy {
		t.Fatalf("VOLUME protection missing: %+v", vol)
	}
	h := publishSource(t, f, b, "v1", map[uint64][]byte{0: chunkBytes(0x41)})
	snap, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "c12-snap"}, h)
	if err != nil {
		t.Fatal(err)
	}
	if snap.VolumeProtection != policy {
		t.Fatalf("snapshot protection missing: %+v", snap)
	}
	if _, err = c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "c12-fork"}); err != nil {
		t.Fatal(err)
	}
	fork, ok, err := c.ReadVolume(ctx, "v2")
	if err != nil || !ok || fork.VolumeProtection != policy {
		t.Fatalf("fork protection missing: %+v %v", fork, err)
	}
}
