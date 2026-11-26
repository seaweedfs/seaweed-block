package volume

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// A primary of a replicated line serves only under the ack profile its
// assignment names (the master's): a volume started with the default
// best-effort and assigned primary of a sync-quorum line is not Healthy,
// refuses writes through the real backend, and says why, naming both
// profiles. The same line under its own profile serves; a line without
// peers (RF1) is not held by the profile.
func TestPrimaryOfAReplicatedLineServesOnlyUnderTheMastersAckProfile(t *testing.T) {
	peer := []*control.ReplicaDescriptor{{ReplicaId: "rC", Epoch: 2, EndpointVersion: 1, DataAddr: "127.0.0.1:3", CtrlAddr: "127.0.0.1:4"}}
	for _, c := range []struct {
		name   string
		master string
		peers  []*control.ReplicaDescriptor
		serves bool
	}{
		{"replicated line under another profile", "sync_quorum", peer, false},
		{"replicated line from a master that names none", "", peer, false},
		{"replicated line under its own profile", "best_effort", peer, true},
		{"RF1 line under another profile", "sync_quorum", nil, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, _, _ := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
			h.cfg.AckProfile = "best_effort" // blockvolume's default --replication-ack
			proj := &mutableProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 2, EndpointVersion: 1}}
			h.view = NewAdapterProjectionView(proj, "vol1", "rB", h)
			if err := h.SetSelfFence(&switchFencer{}); err != nil {
				t.Fatal(err)
			}
			store := memorywal.NewStore(8, 4096)
			t.Cleanup(func() { _ = store.Close() })
			backend := durable.NewStorageBackend(store, h.view, frontend.Identity{VolumeID: "vol1", ReplicaID: "rB", Epoch: 2, EndpointVersion: 1})
			backend.SetOperational(true, "ack profile")

			fact := selfFact(2)
			fact.Peers, fact.AckProfile = c.peers, c.master
			h.applyFixtureFact(fact)

			_, werr := backend.Write(context.Background(), 0, []byte("w"))
			s := NewStatusServer(h.view).statusProjection()
			if c.serves {
				if !h.view.Projection().Healthy || werr != nil {
					t.Fatalf("does not serve: healthy=%t write=%v reason=%q", h.view.Projection().Healthy, werr, s.NotReadyReason)
				}
				return
			}
			if h.view.Projection().Healthy {
				t.Fatal("a primary in another ack profile than its line's is Healthy")
			}
			if werr == nil {
				t.Fatal("a write was acknowledged by a primary in another ack profile than its line's")
			}
			if s.FrontendPrimaryReady || !strings.Contains(s.NotReadyReason, "ack profile mismatch") || !strings.Contains(s.NotReadyReason, "best_effort") {
				t.Fatalf("status: ready=%t reason=%q; want the mismatch naming both profiles", s.FrontendPrimaryReady, s.NotReadyReason)
			}
		})
	}
}
