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

// Overlapping reports of one replica from two hosts (a workspace move keeps
// the replica id), the source's the newer: a line for the target's
// endpoints is issued for the target's process.
func TestLineForTheTargetsEndpointsNamesTheTargetsProcess(t *testing.T) {
	h := newBootGraceMaster(t, Config{})
	defer closeTestMaster(t, h)
	now := time.Now().UTC()
	for _, r := range []struct {
		server, data, ctrl, incarnation string
		at                              time.Time
	}{
		{"node-d", "127.0.0.1:19401", "127.0.0.1:19402", "r2-target", now.Add(-time.Second)},
		{"node-b", "127.0.0.1:19201", "127.0.0.1:19202", "r2-source", now},
	} {
		if err := h.ObservationHost().Ingest(authority.Observation{ServerID: r.server, ObservedAt: r.at,
			Server: authority.ServerFact{Reachable: true, Eligible: true},
			Slots: []authority.SlotFact{{VolumeID: "vol-rf3", ReplicaID: "r2", DataAddr: r.data, CtrlAddr: r.ctrl,
				Reachable: true, ReadyForPrimary: true, Eligible: true, Incarnation: r.incarnation}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.Controller().SubmitAssignmentAsk(authority.AssignmentAsk{
		VolumeID: "vol-rf3", ReplicaID: "r2", DataAddr: "127.0.0.1:19401", CtrlAddr: "127.0.0.1:19402",
		Intent: authority.IntentReassign, Evidence: adapter.PrimaryEvidence{Kind: adapter.PrimaryKindForced},
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		line, ok := h.Publisher().VolumeAuthorityLine("vol-rf3")
		if ok && line.ReplicaID == "r2" {
			if line.Incarnation != "r2-target" {
				t.Fatalf("line at the target's endpoints issued for %q; want r2-target", line.Incarnation)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reassign was not published: %+v", line)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
