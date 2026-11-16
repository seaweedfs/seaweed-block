package main

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

// r4 P2: the cluster-spec import never re-creates (or places) an id a
// volume delete reserved, also after a restart with the original spec;
// unreserved ids keep importing idempotently.
func TestClusterSpecImportSkipsAReservedVolume(t *testing.T) {
	cfg := master.Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir()}
	open := func() *master.Host {
		t.Helper()
		h, err := master.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	// Imports run before Start, as in run(); a host is started before Close
	// (Close waits on a server that Start launches).
	closeHost := func(h *master.Host) {
		h.Start()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	}
	specs := []lifecycle.VolumeSpec{
		{VolumeID: "gone", SizeBytes: 1 << 20, ReplicationFactor: 1},
		{VolumeID: "kept", SizeBytes: 1 << 20, ReplicationFactor: 1},
	}
	h := open()
	if err := importLifecycleVolumes(h, specs); err != nil {
		t.Fatal(err)
	}
	// "gone" is deleted: reservation committed, its record removed by the
	// ordinary lifecycle delete.
	if _, err := h.Lifecycle().Deletions.Admit("gone", "d1"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Lifecycle().Deletions.Commit("gone", "d1"); err != nil {
		t.Fatal(err)
	}
	if err := h.Lifecycle().Volumes.DeleteVolume("gone"); err != nil {
		t.Fatal(err)
	}
	closeHost(h)

	h = open() // restart with the original spec
	defer closeHost(h)
	for i := 0; i < 2; i++ {
		if err := importLifecycleVolumes(h, specs); err != nil {
			t.Fatalf("import %d: %v", i, err)
		}
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("gone"); ok {
		t.Fatal("the import re-created a reserved id")
	}
	if _, ok := h.Lifecycle().Volumes.GetVolume("kept"); !ok {
		t.Fatal("an unreserved volume was not imported")
	}
	intents := []lifecycle.PlacementIntent{
		{VolumeID: "gone", DesiredRF: 1, Slots: []lifecycle.PlacementSlotIntent{{ServerID: "s1", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}},
	}
	if err := importLifecyclePlacementIntents(h, intents); err != nil {
		t.Fatal(err)
	}
	if _, ok := h.Lifecycle().Placements.GetPlacement("gone"); ok {
		t.Fatal("the import placed a reserved id")
	}
}
