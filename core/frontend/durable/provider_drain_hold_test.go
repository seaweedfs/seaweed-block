package durable

import (
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type drainHoldView struct{}

func (drainHoldView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}
}

// Both production sites that open a walstore-chunk store (create and open)
// hold its extent drain until the chunk layer attaches, before Recover; a
// plain walstore store is never held.
func TestProviderHoldsTheDrainOfAChunkStoreOnCreateAndOpen(t *testing.T) {
	for _, c := range []struct {
		impl ImplName
		held bool
	}{{ImplWALStoreChunk, true}, {ImplWALStore, false}} {
		t.Run(string(c.impl), func(t *testing.T) {
			p := &DurableProvider{cfg: ProviderConfig{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4}, view: drainHoldView{}}
			path := filepath.Join(t.TempDir(), "v1.bin")
			h, err := p.createFresh("v1", path, c.impl)
			if err != nil {
				t.Fatal(err)
			}
			ws := h.storage.(*storage.WALStore)
			if ws.ExtentDrainHeld() != c.held {
				t.Fatalf("create: drain held %v; want %v", ws.ExtentDrainHeld(), c.held)
			}
			if err := ws.Close(); err != nil {
				t.Fatal(err)
			}
			h, err = p.openExisting("v1", path, c.impl)
			if err != nil {
				t.Fatal(err)
			}
			ws = h.storage.(*storage.WALStore)
			defer ws.Close()
			if ws.ExtentDrainHeld() != c.held {
				t.Fatalf("open: drain held %v; want %v", ws.ExtentDrainHeld(), c.held)
			}
		})
	}
}
