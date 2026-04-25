package adapter

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/engine"
)

// TestAdapter_ProbeReplica_MintsTransientSessionID pins T4c-1 architect
// Option D: when the engine emits `engine.ProbeReplica`, the adapter
// mints a non-zero transient sessionID (parallel to FenceAtEpoch),
// passes it to executor.Probe, and does NOT emit any session-lifecycle
// event (no SessionPrepared / SessionStarted / SessionClosed for probe).
//
// Probe is non-mutating; it must not appear in engine session truth.
func TestAdapter_ProbeReplica_MintsTransientSessionID(t *testing.T) {
	exec := newMockExecutor()
	a := NewVolumeReplicaAdapter(exec)
	a.startTimeout = 0 // disable watchdog (probes don't go through start lifecycle)

	// Drive an assignment so engine emits ProbeReplica.
	_ = a.OnAssignment(AssignmentInfo{
		ReplicaID:       "r1",
		Epoch:           5,
		EndpointVersion: 3,
		DataAddr:        "127.0.0.1:1",
		CtrlAddr:        "127.0.0.1:2",
	})

	// Probe runs in a goroutine; give it a tick to land.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		exec.mu.Lock()
		hasProbe := len(exec.probeCalls) > 0
		exec.mu.Unlock()
		if hasProbe {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	exec.mu.Lock()
	probes := append([]probeRecord(nil), exec.probeCalls...)
	exec.mu.Unlock()

	if len(probes) == 0 {
		t.Fatal("expected adapter to dispatch ProbeReplica → executor.Probe; got 0 probe calls")
	}
	p := probes[0]
	if p.replicaID != "r1" {
		t.Errorf("replicaID = %q, want r1", p.replicaID)
	}
	if p.sessionID == 0 {
		t.Errorf("transient probe sessionID must be non-zero (architect Option D); got 0")
	}
	if p.epoch != 5 || p.endpointVersion != 3 {
		t.Errorf("identity passthrough wrong: epoch=%d endpoint=%d, want 5/3", p.epoch, p.endpointVersion)
	}

	// Probe must NOT appear in engine session truth (no SessionPrepared
	// / SessionStarted / SessionClosed lifecycle). Engine's
	// ReplicaProjection surfaces session state via SessionPhase
	// (PhaseNone = no session). Probe must leave it at PhaseNone.
	proj := a.Projection()
	if proj.SessionPhase != engine.PhaseNone {
		t.Errorf("probe must NOT create session lifecycle; got SessionPhase=%q",
			proj.SessionPhase)
	}
}

// TestAdapter_ProbeReplica_MintsDistinctSessionIDs pins that two
// successive probe-emit triggers receive distinct, non-zero sessionIDs.
// The mint itself is monotonic (sessionIDCounter.Add(1)), but the probe
// runs in a goroutine so capture order is not stable — assert
// distinctness, not arrival ordering.
func TestAdapter_ProbeReplica_MintsDistinctSessionIDs(t *testing.T) {
	exec := newMockExecutor()
	a := NewVolumeReplicaAdapter(exec)
	a.startTimeout = 0

	// First assignment → first probe.
	_ = a.OnAssignment(AssignmentInfo{
		ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "x", CtrlAddr: "y",
	})
	// Second assignment with bumped endpoint → reachability reset → second probe.
	_ = a.OnAssignment(AssignmentInfo{
		ReplicaID: "r1", Epoch: 1, EndpointVersion: 2,
		DataAddr: "x", CtrlAddr: "y",
	})

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		exec.mu.Lock()
		count := len(exec.probeCalls)
		exec.mu.Unlock()
		if count >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	exec.mu.Lock()
	probes := append([]probeRecord(nil), exec.probeCalls...)
	exec.mu.Unlock()

	if len(probes) < 2 {
		t.Fatalf("expected >=2 probes, got %d", len(probes))
	}
	if probes[0].sessionID == probes[1].sessionID {
		t.Errorf("sessionIDs must be distinct: both probes got sid=%d", probes[0].sessionID)
	}
	if probes[0].sessionID == 0 || probes[1].sessionID == 0 {
		t.Errorf("sessionIDs must be non-zero: %d, %d",
			probes[0].sessionID, probes[1].sessionID)
	}
}
