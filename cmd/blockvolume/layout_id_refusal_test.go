package main

import (
	"bytes"
	"context"
	"errors"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The directory suffix is only id8: the full master id must match both records.
func TestLayoutIDMismatchIsTypedAndExits78(t *testing.T) {
	for _, record := range []string{"VOLUME", "HEAD"} {
		t.Run(record, func(t *testing.T) {
			ctx := context.Background()
			actual, _ := layout.ParseVolumeID("1234567890abcdef1234567890abcdef")
			wrong, _ := layout.ParseVolumeID("1234567890abcdef1234567890abcdee")
			f := &layoutAttachFiler{MemFiler: chunkstore.NewMemFiler()}
			cat, err := chunkstore.NewBCatalog(f, f, "/identity")
			if err != nil {
				t.Fatal(err)
			}
			launch, err := parseFlags(append(phase150RequiredBlockvolumeArgs(), "--volume-id=v", "--durable-root="+t.TempDir(), "--layout-id="+actual.String()))
			if err != nil {
				t.Fatal(err)
			}
			supplied, err := layout.ParseVolumeID(launch.layoutID)
			if err != nil {
				t.Fatal(err)
			}
			ref := chunkstore.VolumeRef{Name: launch.volumeID, ID: supplied}
			foreign := chunkstore.VolumeRef{Name: "v", ID: wrong}
			if ref.Dir() != foreign.Dir() {
				t.Fatal("fixture needs an id8 collision")
			}
			geometry := layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}
			if _, err := cat.CreateVolume(ctx, ref, geometry); err != nil {
				t.Fatal(err)
			}
			// Leave the other record correct to prove each boundary independently.
			entry, err := f.LookupEntry(ctx, path.Join("/identity", ref.Dir()), record)
			if err != nil {
				t.Fatal(err)
			}
			if record == "VOLUME" {
				entry.Content, err = (layout.Volume{ID: wrong, Name: "v", Format: layout.VolumeFormat, Geometry: geometry}).Encode()
			} else {
				entry.Content, err = layout.Unclaimed(wrong, nil).Encode()
				entry.Extended["swb.layout.volume"] = []byte(wrong.String())
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := f.UpdateEntry(ctx, path.Join("/identity", ref.Dir()), entry, nil); err != nil {
				t.Fatal(err)
			}
			g := durable.ChunkGeometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4}
			if err := chunkAttachCheck(cat, ref)(ctx, "v", g); !errors.Is(err, layout.ErrIdentity) {
				t.Errorf("%s mismatch attach check = %v; want typed ErrIdentity", record, err)
			}
			if record == "VOLUME" {
				if _, code := geometryFlagsRefusal(ctx, cat, ref, 64, 4096, 4); code != 78 {
					t.Errorf("pre-open refusal exit = %d, want 78", code)
				}
			}
			before, err := f.LookupEntry(ctx, path.Join("/identity", ref.Dir()), "HEAD")
			if err != nil {
				t.Fatal(err)
			}
			st, _ := cat.Store(ref)
			view := &claimExitView{}
			view.assigned.Store(true)
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r",
				ChunkAttachCheck:   chunkAttachCheck(cat, ref),
				ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return st, nil },
				ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
					return chunkstore.NewLayoutRemote(cat, ref, me)
				},
			}, view)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.EnsureStorage("v"); err != nil {
				t.Fatal(err)
			}
			if _, err := p.RecoverVolume(ctx, "v"); !errors.Is(err, layout.ErrIdentity) {
				t.Fatalf("recover = %v, want ErrIdentity", err)
			}
			h, _ := exitAuthorityHost(t)
			e := newProcessExit(ctx)
			if !chooseChunkStartupExit(e, flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v"}, h, p) {
				t.Fatal("identity refusal did not terminate startup")
			}
			if out := <-e.decision; out.code != 78 || out.reason != "layout_identity_mismatch" {
				t.Fatalf("identity refusal exit = %+v", out)
			}
			gen, ended := p.ChunkStartupOutcome("v")
			if !ended || p.ChunkStartupIdentityRefused("v", gen+1) {
				t.Fatal("identity refusal escaped its recovery generation")
			}
			after, err := f.LookupEntry(ctx, path.Join("/identity", ref.Dir()), "HEAD")
			if err != nil || !bytes.Equal(after.Content, before.Content) {
				t.Fatal("refusal changed HEAD", err)
			}

		})
	}
}
