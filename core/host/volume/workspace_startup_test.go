package volume

import (
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"testing"
)

func TestWorkspaceExitLineageIncludesBlockedOwnAssignment(t *testing.T) {
	h, _, _ := newStubbedHost(t, "v", "r", &orderingRecorder{})
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "other", Epoch: 6, EndpointVersion: 1})
	if !h.IsCurrentReplicaSuperseded() {
		t.Fatal("never-assigned newer other line did not supersede")
	}
	h.BlockLocalReadiness("local recovery pending")
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "r", Epoch: 7, EndpointVersion: 1})
	if h.IsCurrentReplicaSuperseded() {
		t.Fatal("blocked own assignment left historical other line current for exit")
	}
	// Exit classification must not change frontend admission at its old lineage.
	if !h.IsSuperseded("r", 5, 1) {
		t.Fatal("exit observation weakened serving supersede predicate")
	}
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "r", Epoch: 4, EndpointVersion: 1})
	if h.IsCurrentReplicaSuperseded() {
		t.Fatal("older own delivery lowered the exit lineage")
	}
	h.recordOtherLine(&control.AssignmentFact{VolumeId: "other-volume", ReplicaId: "other", Epoch: 99})
	if h.IsCurrentReplicaSuperseded() {
		t.Fatal("another volume produced a no-retry outcome")
	}
}

func TestWorkspaceEmptyOtherReplicaNeverSelectsNoRetry(t *testing.T) {
	h, _, _ := newStubbedHost(t, "v", "r", &orderingRecorder{})
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "r", Epoch: 5, EndpointVersion: 1})
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "", Epoch: 7, EndpointVersion: 1})
	if h.IsCurrentReplicaSuperseded() {
		t.Fatal("empty replica id is not another holder; must retry")
	}
}

// The two Part T facts are intentionally different: a validated assignment can
// be observed while local recovery prevents the engine from adopting it.
func TestWorkspaceExitAndFenceUseNamedLineages(t *testing.T) {
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "self", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	fencer := &holderFenceCounter{}
	if err := h.SetSelfFence(fencer); err != nil {
		t.Fatal(err)
	}
	h.BlockLocalReadiness("local recovery pending")
	foreign := holderAssignment(t, "other")
	foreign.Epoch = 6
	h.applyFact(foreign)
	if !h.IsCurrentReplicaSuperseded() || h.IsSuperseded("r1", 0, 0) || fencer.calls.Load() != 0 {
		t.Fatal("foreign same-replica server must select exit78 without fencing an unadopted incarnation")
	}
	own := holderAssignment(t, "self")
	own.Epoch = 7
	h.applyFact(own)
	if h.view.EngineProjection().Epoch != 0 || h.IsCurrentReplicaSuperseded() || fencer.calls.Load() != 0 {
		t.Fatal("observed own7 must suppress historical foreign6 for exit while adopted lineage stays zero")
	}
	foreign.Epoch = 8
	h.applyFact(foreign)
	if !h.IsCurrentReplicaSuperseded() || h.IsSuperseded("r1", 0, 0) || fencer.calls.Load() != 0 {
		t.Fatal("foreign8 must supersede observed own7 without changing the unadopted fencing rule")
	}
}
