package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// A volume created on the filer store reads back its VOLUME and its
// unclaimed HEAD, re-creates as a replay, and the first Start claims it
// from a fresh root through the same store.
func TestLayoutStoreCreateVolumeThenStart(t *testing.T) {
	s, _, _ := layoutStoreFixture(t)
	ctx := context.Background()
	v := layout.Volume{ID: s.id, Name: "v", Format: layout.VolumeFormat}
	if _, err := layout.CreateVolume(ctx, s, v); err != nil {
		t.Fatal(err)
	}
	if got, found, err := s.ReadVolumeRecord(ctx); err != nil || !found || got != v {
		t.Fatalf("VOLUME read back %+v %t %v", got, found, err)
	}
	if _, err := layout.CreateVolume(ctx, s, v); err != nil {
		t.Fatalf("re-create: %v", err)
	}
	h, err := layout.Start(ctx, s, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{ClaimID: "root", Fresh: true})
	if err != nil || h.Epoch != 1 || h.ClaimID != "root" || h.Gen != 2 {
		t.Fatalf("first Start: %+v %v", h, err)
	}
	if err := s.CreateHead(ctx, layout.Unclaimed(s.id, nil)); !errors.Is(err, layout.ErrExists) {
		t.Fatalf("second HEAD create: %v; want ErrExists", err)
	}
}

// The store creates only the two shapes the kernel creates, of its own
// volume id: an unclaimed live HEAD, and a deleted HEAD as a tombstone; a
// tombstone makes a later create of the volume's HEAD fail.
func TestLayoutStoreCreatesOnlyKernelShapes(t *testing.T) {
	s, f, _ := layoutStoreFixture(t)
	ctx := context.Background()
	claimed := layout.Unclaimed(s.id, nil)
	claimed.Epoch, claimed.Holder, claimed.ClaimID = 1, "s/r", "k"
	other, _ := layout.NewVolumeID()
	for name, err := range map[string]error{
		"claimed HEAD":             s.CreateHead(ctx, claimed),
		"live HEAD as a tombstone": s.CreateDeletedHead(ctx, layout.Unclaimed(s.id, nil)),
		"another volume's HEAD":    s.CreateHead(ctx, layout.Unclaimed(other, nil)),
		"another volume's VOLUME":  s.CreateVolumeRecord(ctx, layout.Volume{ID: other, Name: "v", Format: layout.VolumeFormat}),
	} {
		if err == nil {
			t.Errorf("%s created", name)
		}
	}
	if len(f.written) != 0 {
		t.Fatalf("refused creates wrote %d entries", len(f.written))
	}
	tomb := layout.Unclaimed(s.id, nil)
	tomb.State = layout.StateDeleted
	if err := s.CreateDeletedHead(ctx, tomb); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.CreateVolume(ctx, s, layout.Volume{ID: s.id, Name: "v", Format: layout.VolumeFormat}); !errors.Is(err, layout.ErrVolumeDeleting) {
		t.Fatalf("create after the tombstone: %v; want ErrVolumeDeleting", err)
	}
}
