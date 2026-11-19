package volume

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

func rf2PrimaryFact(epoch uint64) *control.AssignmentFact {
	return &control.AssignmentFact{
		VolumeId: "vol1", ReplicaId: "r1", Epoch: epoch, EndpointVersion: 1,
		DataAddr: "127.0.0.1:1000", CtrlAddr: "127.0.0.1:1001", PeerSetGeneration: epoch,
		Peers: []*control.ReplicaDescriptor{{ReplicaId: "r2", Epoch: epoch, EndpointVersion: 1, DataAddr: "127.0.0.1:2000", CtrlAddr: "127.0.0.1:2001"}},
	}
}

// The production start schedule of an RF1-only (walstore-chunk) volume: the
// host starts with local readiness blocked, a retained RF2 fact arrives and is
// stored, and installing the replication volume replays it with no self fence
// installed yet. The host must refuse it before any peer set, adoption or
// readiness marker, and latch the refusal; a later RF1 fact is adopted.
func TestRF1Only_RetainedReplicatedFactReplayedBeforeSelfFence(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r1", rec)
	ready := make(chan adapter.AssignmentInfo, 4)
	h.cfg.ReadyMarker, h.cfg.RF1Only = ready, "chunk snapshots need RF1"
	rv := h.replication
	h.replication = nil
	h.BlockLocalReadiness("awaiting durable recovery")

	h.applyFact(rf2PrimaryFact(3)) // stored for replay
	h.ClearLocalReadinessBlock()
	h.replicationMu.Lock()
	h.setReplicationLocked(rv) // what SetReplicationVolume does, then it replays
	h.replicationMu.Unlock()
	h.replayLocalReadinessFactIfReady()

	if h.selfFence != nil {
		t.Fatal("fixture: a self fence is installed")
	}
	if r.callCount.Load() != 0 || a.callCount.Load() != 0 || len(ready) != 0 {
		t.Fatalf("replayed RF2 fact installed something: peers=%d adopt=%d ready=%d %v", r.callCount.Load(), a.callCount.Load(), len(ready), rec.snapshot())
	}
	failed, reason := h.RoleAdmissionFailure()
	if !failed || !strings.Contains(reason, "refuses a primary role with 1 replica peers at epoch 3") || !strings.Contains(reason, "chunk snapshots need RF1") {
		t.Fatalf("no refusal latch: %v %q", failed, reason)
	}

	// A supporting-replica fact (another primary lists this replica) is refused too.
	sup := rf2PrimaryFact(4)
	sup.ReplicaId, sup.Peers[0].ReplicaId = "r2", "r1"
	h.applyFact(sup)
	if r.callCount.Load() != 0 || a.callCount.Load() != 0 || len(ready) != 0 {
		t.Fatal("supporting replica admitted on an RF1-only volume")
	}
	if _, reason := h.RoleAdmissionFailure(); !strings.Contains(reason, "refuses a supporting replica role at epoch 4") {
		t.Fatalf("supporting refusal not latched: %q", reason)
	}

	// The master moves the volume to RF1: adopted, latch cleared, ready emitted.
	rf1 := rf2PrimaryFact(5)
	rf1.Peers = nil
	h.applyFact(rf1)
	if a.callCount.Load() != 1 || len(ready) != 1 {
		t.Fatalf("RF1 primary not adopted: adopt=%d ready=%d", a.callCount.Load(), len(ready))
	}
	if failed, reason := h.RoleAdmissionFailure(); failed {
		t.Fatalf("latch not cleared by an RF1 fact: %q", reason)
	}
}

// Control: without the rule the same replayed RF2 fact installs its peer set
// and is adopted (today's behaviour for replicated volumes).
func TestRF1Only_UnsetKeepsReplicatedAdoption(t *testing.T) {
	h, a, r := newStubbedHost(t, "vol1", "r1", &orderingRecorder{})
	h.applyFact(rf2PrimaryFact(3))
	if r.callCount.Load() != 1 || a.callCount.Load() != 1 {
		t.Fatalf("replicated fact not applied: peers=%d adopt=%d", r.callCount.Load(), a.callCount.Load())
	}
	if failed, _ := h.RoleAdmissionFailure(); failed {
		t.Fatal("latched without the rule")
	}
}

// A refusal is bound to its epoch: RF1@2 healthy and writing, RF2@3 refused
// (writes stop), then a stale RF1@2 replay must neither adopt nor clear the
// refusal (writes stay refused, no peers installed); only RF1 at the refused
// epoch or newer, once its role is admitted, clears it.
func TestRF1Only_StaleRF1FactDoesNotClearANewerRefusal(t *testing.T) {
	h, a, r := newStubbedHost(t, "vol1", "r1", &orderingRecorder{})
	h.cfg.RF1Only = "chunk snapshots need RF1"
	proj := &mutableProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 2, EndpointVersion: 1}}
	h.view = NewAdapterProjectionView(proj, "vol1", "r1", h)
	fencer := &switchFencer{}
	if err := h.SetSelfFence(fencer); err != nil {
		t.Fatal(err)
	}
	rf1 := func(epoch uint64) *control.AssignmentFact { f := rf2PrimaryFact(epoch); f.Peers = nil; return f }
	h.applyFact(rf1(2))
	if a.callCount.Load() != 1 {
		t.Fatal("fixture: RF1@2 not adopted")
	}
	store := memorywal.NewStore(8, 4096)
	t.Cleanup(func() { _ = store.Close() })
	backend := durable.NewStorageBackend(store, h.view, frontend.Identity{VolumeID: "vol1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 1})
	backend.SetOperational(true, "rf1 baseline")
	ctx := context.Background()
	if _, err := backend.Write(ctx, 0, []byte("epoch-2")); err != nil {
		t.Fatalf("baseline write: %v", err)
	}

	h.applyFact(rf2PrimaryFact(3))
	if _, err := backend.Write(ctx, 0, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("write after the RF2@3 refusal: %v", err)
	}
	h.applyFact(rf1(2)) // stale replay below the refused epoch
	if failed, reason := h.RoleAdmissionFailure(); !failed || !strings.Contains(reason, "at epoch 3") {
		t.Fatalf("stale RF1@2 cleared the RF2@3 refusal: %v %q", failed, reason)
	}
	if _, err := backend.Write(ctx, 0, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("write reopened by a stale RF1@2: %v", err)
	}
	if a.callCount.Load() != 1 || r.callCount.Load() != 1 {
		t.Fatalf("stale RF1@2 adopted or RF2 peers installed: adopt=%d peerSets=%d", a.callCount.Load(), r.callCount.Load())
	}
	if len(r.lastTargetIDs) != 0 {
		t.Fatalf("forbidden peers installed: %v", r.lastTargetIDs)
	}

	fencer.fail.Store(true) // RF1@3 whose role fence fails: a failed attempt does not clear the refusal
	h.applyFact(rf1(3))
	if _, reason := h.RoleAdmissionFailure(); !strings.Contains(reason, "refuses a primary role") || a.callCount.Load() != 1 {
		t.Fatalf("failed RF1@3 attempt cleared the refusal: %q adopt=%d", reason, a.callCount.Load())
	}
	fencer.fail.Store(false)
	h.applyFact(rf1(3))
	if failed, reason := h.RoleAdmissionFailure(); failed || a.callCount.Load() != 2 {
		t.Fatalf("RF1@3 did not reconcile: %v %q adopt=%d", failed, reason, a.callCount.Load())
	}
}
