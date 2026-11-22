package volume

import (
	"github.com/seaweedfs/seaweed-block/core/adapter"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"testing"
)

func TestWorkspaceExitLineageIncludesBlockedOwnAssignment(t *testing.T) {
	h, _, _ := newStubbedHost(t, "v", "r", &orderingRecorder{})
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v", ReplicaId: "other", Epoch: 6, EndpointVersion: 1})
	// Zero observed-own lineage inherits the fencing predicate's zero rule:
	// a never-assigned process retries75; it has no own incarnation to supersede.
	if h.IsCurrentReplicaSuperseded() {
		t.Fatal("never-assigned process must retry75 despite a foreign line")
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
	if h.IsCurrentReplicaSuperseded() || h.IsSuperseded("r1", 0, 0) || fencer.calls.Load() != 0 {
		t.Fatal("foreign same-replica server must select retry75 and no fence before any own assignment")
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

// Check at the adoption handoff, not just after applyFact: moving observation
// after adapter adoption must fail even when the eventual snapshots match.
type observationCheckedAdoption struct {
	assignmentConsumer
	before func(adapter.AssignmentInfo)
}

func (a observationCheckedAdoption) OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog {
	a.before(info)
	return a.assignmentConsumer.OnAssignment(info)
}

func TestWorkspaceObservedLineageNeverTrailsAdoption(t *testing.T) {
	h, err := New(Config{MasterAddr: "127.0.0.1:1", ServerID: "self", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:19303", CtrlAddr: "127.0.0.1:19304"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	check := func(epoch, ev uint64) {
		t.Helper()
		h.ownAssignmentMu.RLock()
		defer h.ownAssignmentMu.RUnlock()
		if h.ownAssignmentEpoch < epoch || h.ownAssignmentEpoch == epoch && h.ownAssignmentEV < ev {
			t.Fatalf("observation trails adoption handoff: observed=%d/%d adopted=%d/%d", h.ownAssignmentEpoch, h.ownAssignmentEV, epoch, ev)
		}
	}
	adoptions := 0
	h.adpt = observationCheckedAdoption{h.adpt, func(info adapter.AssignmentInfo) {
		check(info.Epoch, info.EndpointVersion)
		adoptions++
	}}
	assertAdopted := func(epoch, ev uint64) {
		t.Helper()
		p := h.view.EngineProjection()
		if p.Epoch != epoch || p.EndpointVersion != ev {
			t.Fatalf("adopted=%d/%d want=%d/%d", p.Epoch, p.EndpointVersion, epoch, ev)
		}
		check(p.Epoch, p.EndpointVersion)
	}
	own := holderAssignment(t, "self")
	own.Epoch = 3
	h.applyFact(own)
	assertAdopted(3, 1)
	h.BlockLocalReadiness("local recovery pending")
	own.Epoch = 7
	h.applyFact(own)
	own.EndpointVersion = 2
	h.applyFact(own)
	check(7, 2)
	assertAdopted(3, 1)
	h.ClearLocalReadinessBlock()
	h.replayLocalReadinessFactIfReady()
	assertAdopted(7, 2)
	foreign := holderAssignment(t, "other")
	foreign.Epoch = 8
	h.applyFact(foreign)
	assertAdopted(7, 2)
	own.Epoch = 9
	h.applyFact(own)
	assertAdopted(9, 2)
	own.Epoch = 7
	h.applyFact(own)
	assertAdopted(9, 2)
	if adoptions != 3 {
		t.Fatalf("adoption handoffs=%d want3 (initial, readiness replay, move back)", adoptions)
	}
}
