package volume

import (
	"testing"
)

type retainedBaseFencer struct {
	recordingFencer
	reason string
}

func (f *retainedBaseFencer) RebuildRetainedBase() string { return f.reason }

// A supporting replica whose listener refused its rebuild for a retained
// BASE reports rebuild_retained_base on its heartbeat slot (ineligible), so
// the master and the operator see which replica to wipe and re-add; with no
// refusal it reports nothing.
func TestReplicaHeartbeatNamesARetainedBaseRebuildRefusal(t *testing.T) {
	h := newTestVolumeHost(t)
	f := &retainedBaseFencer{}
	if err := h.SetSelfFence(f); err != nil {
		t.Fatal(err)
	}
	if s := reportSlot(t, h); s.UnhealthyReason != "" {
		t.Fatalf("no refusal: reason=%q; want none", s.UnhealthyReason)
	}
	f.reason = "rebuild_retained_base"
	if s := reportSlot(t, h); s.Eligible || s.UnhealthyReason != UnhealthyRebuildRetainedBase || s.ReplicaId == "" {
		t.Fatalf("refused rebuild: eligible=%t reason=%q replica=%q; want ineligible, %s, the replica named", s.Eligible, s.UnhealthyReason, s.ReplicaId, UnhealthyRebuildRetainedBase)
	}
}

// A latched replica never answers a probe or fencing round ready, so no
// promotion (automatic, the operator's failover, or a planned move) can make
// it primary: every one requires the target's ready answer.
func TestRetainedBaseReplicaIsNeverReadyForPromotion(t *testing.T) {
	h, _, rep := newStubbedHost(t, "vol1", "rB", &orderingRecorder{})
	h.replicationMu.Lock()
	h.setReplicationLocked(requiringReplication{rep})
	h.replicationMu.Unlock()
	h.SetProbeRoleSource(func() (bool, bool) { return true, false }) // its own view: ready
	f := &retainedBaseFencer{}
	if err := h.SetSelfFence(f); err != nil {
		t.Fatal(err)
	}
	if !h.probeAnswer(1).GetReplicaReady() {
		t.Fatal("fixture: an unlatched ready replica answered not ready")
	}
	f.reason = "rebuild_retained_base"
	if h.probeAnswer(2).GetReplicaReady() {
		t.Fatal("a replica refused a rebuild for a retained base answered ready for promotion")
	}
}
