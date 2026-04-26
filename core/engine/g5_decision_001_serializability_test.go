package engine

import (
	"encoding/json"
	"testing"
)

// G5-DECISION-001 obligation per round-47: keep engine recovery
// state struct serializable so G5 can choose Path A (persist
// recovery state across primary restart) without a structural
// rewrite. Path B (rebuild from probe after restart) is also open;
// either path stays available as long as the struct is JSON-marshal-
// able with stable shape.
//
// T4d-4 obligation: STRUCTURAL — no implementation of persistence
// in T4d. Just the property that, should G5 pick Path A, the engine
// state can be marshaled + unmarshaled round-trip cleanly.

func TestG5Decision001_ReplicaState_RoundTripJSON(t *testing.T) {
	original := ReplicaState{
		Identity: IdentityTruth{
			VolumeID:        "v1",
			ReplicaID:       "r1",
			Epoch:           7,
			EndpointVersion: 3,
			DataAddr:        "127.0.0.1:9000",
			CtrlAddr:        "127.0.0.1:9001",
			MemberPresent:   true,
		},
		Reachability: ReachabilityTruth{
			Status:                  ProbeReachable,
			LastContactKind:         ContactProbe,
			ObservedEndpointVersion: 3,
			TransportEpoch:          7,
			FencedEpoch:             7,
		},
		Recovery: RecoveryTruth{
			R:              50,
			S:              10,
			H:              100,
			Decision:       DecisionCatchUp,
			DecisionReason: "gap_within_wal",
			Attempts:       2,
		},
		Session: SessionTruth{
			SessionID:   42,
			Kind:        SessionCatchUp,
			TargetLSN:   100,
			Phase:       PhaseRunning,
			AchievedLSN: 75,
		},
	}

	b, err := json.Marshal(&original)
	if err != nil {
		t.Fatalf("FAIL: G5-DECISION-001 obligation — engine state must be JSON-marshalable; got err: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("Marshal produced empty bytes")
	}

	var roundtrip ReplicaState
	if err := json.Unmarshal(b, &roundtrip); err != nil {
		t.Fatalf("FAIL: G5-DECISION-001 round-trip Unmarshal: %v", err)
	}

	// Spot-check critical fields survived round-trip.
	if roundtrip.Identity.Epoch != 7 {
		t.Errorf("Identity.Epoch round-trip: got %d, want 7", roundtrip.Identity.Epoch)
	}
	if roundtrip.Recovery.Decision != DecisionCatchUp {
		t.Errorf("Recovery.Decision round-trip: got %q, want %q", roundtrip.Recovery.Decision, DecisionCatchUp)
	}
	if roundtrip.Recovery.Attempts != 2 {
		t.Errorf("Recovery.Attempts round-trip: got %d, want 2", roundtrip.Recovery.Attempts)
	}
	if roundtrip.Session.Phase != PhaseRunning {
		t.Errorf("Session.Phase round-trip: got %q, want %q", roundtrip.Session.Phase, PhaseRunning)
	}
}

// TestG5Decision001_ReplicaState_ZeroValueStable — fence: zero-value
// engine state must also marshal cleanly (no panics on uninitialized
// fields). Common in restart-from-empty scenarios.
func TestG5Decision001_ReplicaState_ZeroValueStable(t *testing.T) {
	var zero ReplicaState
	b, err := json.Marshal(&zero)
	if err != nil {
		t.Fatalf("zero-value Marshal: %v", err)
	}
	var rt ReplicaState
	if err := json.Unmarshal(b, &rt); err != nil {
		t.Fatalf("zero-value Unmarshal: %v", err)
	}
}
