package volume

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
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
