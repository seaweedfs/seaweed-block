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
