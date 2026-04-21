package host_test

// D4 — Go-native replay of testrunner/scenarios/t0-hosting-smoke.yaml.
//
// Final testrunner integration is deferred to Final Gate; this
// test executes the scenario's phases in-process against the real
// compiled blockmaster / blockvolume binaries using the same L2
// harness the D1 subprocess tests use. The phase sequence here
// MUST mirror the YAML skeleton — if they drift, update both.
//
// Ledger coverage:
//   PG8 scenario harness — end-to-end smoke (volume-scoped
//   subscription, durable authority, 20-cycle kill-restart
//   stability).

import (
	"path/filepath"
	"testing"
	"time"
)

// TestT0Scenario_HostingSmoke_Replay walks the four phases of
// testrunner/scenarios/t0-hosting-smoke.yaml:
//   setup            — master + 3 volumes
//   initial_assignment — baseline authority tuple
//   kill_restart_cycles — 20× master SIGKILL + restart, tuple stable
//   verify_final     — all cycles produced identical tuple
//
// Budget: master kill-restart loop is the slow part (~25-30s on
// Windows). Whole test fits within a 90s go-test timeout.
func TestT0Scenario_HostingSmoke_Replay(t *testing.T) {
	if testing.Short() {
		t.Skip("scenario replay uses L2 subprocess harness; skipped under -short")
	}
	const cycles = 20

	bins := buildL2Binaries(t)
	art := mkArtifactDir(t)
	storeDir := filepath.Join(art, "store")
	topoPath := writeL2Topology(t, art)

	// Phase: setup
	m := startMaster(t, bins, storeDir, topoPath, art)
	_ = startVolume(t, bins, m.addr, art, volumeOpts{
		ServerID: "s1", VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "127.0.0.1:7041", CtrlAddr: "127.0.0.1:7141",
	})
	_ = startVolume(t, bins, m.addr, art, volumeOpts{
		ServerID: "s2", VolumeID: "v1", ReplicaID: "r2",
		DataAddr: "127.0.0.1:7042", CtrlAddr: "127.0.0.1:7142",
	})
	_ = startVolume(t, bins, m.addr, art, volumeOpts{
		ServerID: "s3", VolumeID: "v1", ReplicaID: "r3",
		DataAddr: "127.0.0.1:7043", CtrlAddr: "127.0.0.1:7143",
	})

	// Phase: initial_assignment
	initial := pollAssigned(t, m.addr, "v1", 15*time.Second)
	baseline := tupleOf(initial)
	if baseline.Epoch != 1 {
		t.Fatalf("initial_assignment: epoch=%d want 1", baseline.Epoch)
	}
	if baseline.ReplicaID != "r1" {
		t.Fatalf("initial_assignment: replica=%q want r1 (tiebreak)", baseline.ReplicaID)
	}
	timeline := []authorityTuple{baseline}

	// Phase: kill_restart_cycles (20×)
	for i := 1; i <= cycles; i++ {
		m.sigKill(t)
		time.Sleep(200 * time.Millisecond)
		m = startMaster(t, bins, storeDir, topoPath, art)

		resp := pollAssigned(t, m.addr, "v1", 15*time.Second)
		got := tupleOf(resp)
		timeline = append(timeline, got)
		if got != baseline {
			dumpTimelineOnFailure(t, art, timeline)
			t.Fatalf("cycle %d: tuple drifted\n  baseline=%s\n  observed=%s",
				i, baseline, got)
		}
	}

	// Phase: verify_final
	for i, tup := range timeline {
		if tup != baseline {
			t.Fatalf("verify_final: cycle %d tuple %s differs from baseline %s",
				i, tup, baseline)
		}
	}
	writeTimelineArtifact(t, art, timeline)
}
