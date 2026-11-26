package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
)

// Every line names the process of its primary, also when the ask that
// minted it named none: the initial bind of the product tick, and a direct
// reassign (as the heal, a planned or workspace move, or an operator's
// request submits it) record the incarnation the target reports. So the
// promoted process's first report re-issues nothing, and a restarted one
// is told apart from it.
func TestLinesNameTheirPrimarysProcess(t *testing.T) {
	h := newBootGraceMaster(t, Config{})
	defer closeTestMaster(t, h)
	if line := waitAuthorityLine(t, h.Publisher(), "vol-rf3"); line.ReplicaID != "r1" || line.Incarnation != "r1-a" {
		t.Fatalf("initial line %+v; want r1 issued for r1-a", line)
	}
	ingestRF3Incarnations(t, h, readySlot("r1-a"), readySlot("r2-a"), readySlot("r3-a")) // fresh reports
	if err := h.Controller().SubmitAssignmentAsk(authority.AssignmentAsk{
		VolumeID: "vol-rf3", ReplicaID: "r2", DataAddr: "127.0.0.1:19201", CtrlAddr: "127.0.0.1:19202",
		Intent: authority.IntentReassign, Evidence: adapter.PrimaryEvidence{Kind: adapter.PrimaryKindForced},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		line, ok := h.Publisher().VolumeAuthorityLine("vol-rf3")
		if ok && line.ReplicaID == "r2" {
			if line.Incarnation != "r2-a" {
				t.Fatalf("direct reassign: line %+v; want it issued for r2-a", line)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the direct reassign was not published: %+v", line)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
