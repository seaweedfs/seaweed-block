package authority

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// An endpoint refresh decided against a line (a restarted holder at the
// source addresses) is not applied to the line a move put in its place: a
// planned move keeps the replica id and changes the host, so without the
// condition the refresh would write the source's addresses into the moved
// line. The publisher refuses it under its lock; the moved line stays. The
// same refresh against the unchanged line is applied.
func TestRefreshIsConditionalOnTheLineItWasDecidedAgainst(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "src-d", CtrlAddr: "src-c", Intent: IntentBind, Incarnation: "proc-a"}); err != nil {
		t.Fatal(err)
	}
	decided, _ := pub.VolumeAuthorityLine("v1")
	refresh := AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "src-d", CtrlAddr: "src-c", Intent: IntentRefreshEndpoint, Incarnation: "proc-b",
		ExpectCurrent: &ExpectedLine{ReplicaID: decided.ReplicaID, Epoch: decided.Epoch, DataAddr: decided.DataAddr, CtrlAddr: decided.CtrlAddr}}

	// A planned move lands first: the same replica at the next epoch, on the
	// target's addresses.
	if err := pub.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "dst-d", CtrlAddr: "dst-c", Intent: IntentReassign,
		Evidence: adapter.PrimaryEvidence{Kind: adapter.PrimaryKindPromotion, DurableLSN: 1, Basis: adapter.BasisPlanned}}); err != nil {
		t.Fatalf("move: %v", err)
	}
	moved, _ := pub.VolumeAuthorityLine("v1")

	if err := pub.apply(refresh); !errors.Is(err, ErrExpectedLineMoved) {
		t.Fatalf("stale refresh: err=%v; want ErrExpectedLineMoved", err)
	}
	if line, _ := pub.VolumeAuthorityLine("v1"); line.Epoch != moved.Epoch || line.EndpointVersion != moved.EndpointVersion || line.DataAddr != "dst-d" || line.CtrlAddr != "dst-c" {
		t.Fatalf("line %+v after the stale refresh; want the moved line %+v unchanged", line, moved)
	}

	// Decided against the current (moved) line, a refresh for a restarted
	// target is applied.
	fresh := AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "dst-d", CtrlAddr: "dst-c", Intent: IntentRefreshEndpoint, Incarnation: "proc-c",
		ExpectCurrent: &ExpectedLine{ReplicaID: moved.ReplicaID, Epoch: moved.Epoch, DataAddr: moved.DataAddr, CtrlAddr: moved.CtrlAddr}}
	if err := pub.apply(fresh); err != nil {
		t.Fatalf("refresh against the current line: %v", err)
	}
	if line, _ := pub.VolumeAuthorityLine("v1"); line.Epoch != moved.Epoch || line.EndpointVersion != moved.EndpointVersion+1 || line.Incarnation != "proc-c" {
		t.Fatalf("line %+v; want %d/%d naming proc-c", line, moved.Epoch, moved.EndpointVersion+1)
	}
}

// The topology controller's refresh names the line it was decided against.
func TestControllerRefreshNamesTheLineItWasDecidedAgainst(t *testing.T) {
	line := AuthorityBasis{ReplicaID: "r1", Epoch: 3, DataAddr: "d", CtrlAddr: "c"}
	ask := refreshAsk("v1", ReplicaCandidate{ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Incarnation: "proc-b"}, line)
	if e := ask.ExpectCurrent; e == nil || e.ReplicaID != "r1" || e.Epoch != 3 || e.DataAddr != "d" || e.CtrlAddr != "c" {
		t.Fatalf("refresh expects %+v; want the line r1@3 at d/c", ask.ExpectCurrent)
	}
}
