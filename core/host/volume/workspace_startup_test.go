package volume

import (
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"testing"
)

func TestWorkspaceExitLineageIncludesBlockedOwnAssignment(t *testing.T) {
	h, _, _ := newStubbedHost(t, "v", "r", &orderingRecorder{})
	h.applyFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "other", Epoch: 6, EndpointVersion: 1})
	if !h.IsCurrentReplicaSuperseded() {
		t.Fatal("never-assigned newer other line did not supersede")
	}
	h.BlockLocalReadiness("local recovery pending")
	h.applyFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "r", Epoch: 7, EndpointVersion: 1})
	if h.IsCurrentReplicaSuperseded() {
		t.Fatal("blocked own assignment left historical other line current for exit")
	}
	// Exit classification must not change frontend admission at its old lineage.
	if !h.IsSuperseded("r", 5, 1) {
		t.Fatal("exit observation weakened serving supersede predicate")
	}
	h.applyFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "r", Epoch: 4, EndpointVersion: 1})
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
	h.applyFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "r", Epoch: 5, EndpointVersion: 1})
	h.applyFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "", Epoch: 7, EndpointVersion: 1})
	if h.IsCurrentReplicaSuperseded() {
		t.Fatal("empty replica id is not another holder; must retry")
	}
}
