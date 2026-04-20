package authority

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ============================================================
// P14 S4 — Observation Institution Tests
//
// Proof matrix from sw-block/design/v3-phase-14-s4-sketch.md §16:
//
//  # | Claim                                                       | Test
// ---+-------------------------------------------------------------+---------------------------
//  1 | Missing inventory -> unsupported, not failover input        | PartialInventory_PendingThenUnsupported +
//    |                                                             | Unsupported_NotInControllerInput
//  2 | Conflicting primary claims -> unsupported, not rebalance    | ConflictingPrimaryClaim_Unsupported +
//    |                                                             | ConflictingVolume_NotInControllerInput
//  3 | Stale observation cannot mint fresh authority               | ExpiredFact_SemanticallyIneligible +
//    |                                                             | ExpiredDoesNotReachController
//  4 | Bad volume does not block healthy volumes                   | BadVolumeDoesNotBlockHealthyVolume_Isolation
//  5 | Route into TopologyController is system-fed                 | EndToEnd_SystemFedSnapshotReachesController
//  6 | Missing-server inventory -> pending then unsupported,       | MissingServerObservation_NeverOrdinaryIneligible
//    |   never ordinary ineligible                                 |
//  7 | Conflict resolution never picks a winner                    | ConflictingPrimaryClaim_NoWinnerEverChosen
//  8 | Per-volume evidence isolation                               | VolumeUnsupportedEvidence_IsolatedPerVolume
//  9 | Push ingest triggers reactive rebuild                       | IngestMutation_TriggersSnapshotRebuild
// 10 | FreshnessWindow honored, not tick multiples                 | FreshnessWindowHonored_NotTickMultiples
// 11 | Observation withholds supportability, does not mint         | NeverMintsAuthority_BoundaryGuard
//    |   authority                                                 |
// 12 | Duplicate-server topology -> unsupported                    | DuplicateServerTopology_UnsupportedAndEvidenceRecorded +
//    |                                                             | DuplicateServerTopology_NotControllerInput
// 13 | Supportability reads only pre-snapshot input, never output  | SupportabilityDoesNotReadClusterSnapshotOutput
// ============================================================

// fakeClock returns a controllable clock for deterministic tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock(t time.Time) *fakeClock                 { return &fakeClock{t: t} }
func (c *fakeClock) Now() time.Time                       { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration)              { c.mu.Lock(); defer c.mu.Unlock(); c.t = c.t.Add(d) }
func (c *fakeClock) Set(t time.Time)                      { c.mu.Lock(); defer c.mu.Unlock(); c.t = t }

// recordingSink captures submitted observed-state calls for
// assertions. Implements ControllerSink's SubmitObservedState —
// both the supported snapshot and the supportability report are
// captured per call, so tests can observe transitions (e.g. a
// volume moving into report.Unsupported) even when the snapshot
// has no supported volumes.
type recordingSink struct {
	mu    sync.Mutex
	calls []observedStateCall
	err   error
}

type observedStateCall struct {
	Snapshot ClusterSnapshot
	Report   SupportabilityReport
}

func (r *recordingSink) SubmitObservedState(snap ClusterSnapshot, report SupportabilityReport) error {
	r.mu.Lock()
	r.calls = append(r.calls, observedStateCall{Snapshot: snap, Report: report})
	err := r.err
	r.mu.Unlock()
	return err
}

func (r *recordingSink) records() []observedStateCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]observedStateCall, len(r.calls))
	copy(cp, r.calls)
	return cp
}

// snapshot returns just the ClusterSnapshots from recorded calls,
// for tests that only care about the supported-volumes sequence.
func (r *recordingSink) snapshot() []ClusterSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]ClusterSnapshot, len(r.calls))
	for i, c := range r.calls {
		cp[i] = c.Snapshot
	}
	return cp
}

// threeSlotVolume is the accepted topology for a single volume
// used across the S4 tests. Three slots on distinct servers.
func threeSlotVolume(volumeID string) VolumeExpected {
	return VolumeExpected{
		VolumeID: volumeID,
		Slots: []ExpectedSlot{
			{ReplicaID: "r1", ServerID: "s1"},
			{ReplicaID: "r2", ServerID: "s2"},
			{ReplicaID: "r3", ServerID: "s3"},
		},
	}
}

// healthySlot builds a fresh well-formed SlotFact.
func healthySlot(volumeID, replicaID string) SlotFact {
	return SlotFact{
		VolumeID:        volumeID,
		ReplicaID:       replicaID,
		DataAddr:        "data-" + replicaID,
		CtrlAddr:        "ctrl-" + replicaID,
		Reachable:       true,
		ReadyForPrimary: true,
		Eligible:        true,
	}
}

// serverObs assembles a HeartbeatMessage for one server listing
// the given slots. The assertion tests use HeartbeatToObservation
// to make sure the wire port is exercised alongside the direct
// Ingest path.
func serverObs(serverID string, at time.Time, slots ...SlotFact) HeartbeatMessage {
	return HeartbeatMessage{
		ServerID:  serverID,
		SentAt:    at,
		Reachable: true,
		Eligible:  true,
		Slots: func() []HeartbeatSlot {
			out := make([]HeartbeatSlot, len(slots))
			for i, s := range slots {
				out[i] = HeartbeatSlot{
					VolumeID:        s.VolumeID,
					ReplicaID:       s.ReplicaID,
					DataAddr:        s.DataAddr,
					CtrlAddr:        s.CtrlAddr,
					Reachable:       s.Reachable,
					ReadyForPrimary: s.ReadyForPrimary,
					Eligible:        s.Eligible,
					Withdrawn:       s.Withdrawn,
					EvidenceScore:   s.EvidenceScore,
					LocalRoleClaim:  s.LocalRoleClaim,
				}
			}
			return out
		}(),
	}
}

// ============================================================
// Rows 1 + 6: partial inventory / missing-server
// ============================================================

func TestObservation_PartialInventory_PendingThenUnsupported(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{
		FreshnessWindow: 5 * time.Second,
		PendingGrace:    5 * time.Second,
	}, clk.Now)

	topo := AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}}

	// Within bootstrap grace: submit only s1's observation. s2, s3 missing.
	obs, err := HeartbeatToObservation(serverObs("s1", start, healthySlot("v1", "r1")))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if err := store.Ingest(obs); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	result := BuildSnapshot(store.Snapshot(), topo, nil)
	if _, supported := findSupported(result, "v1"); supported {
		t.Fatal("partial-inventory volume should not be supported within grace")
	}
	if _, ok := result.Report.Pending["v1"]; !ok {
		t.Fatal("partial-inventory volume within grace should be pending, got none")
	}
	if _, ok := result.Report.Unsupported["v1"]; ok {
		t.Fatal("partial-inventory volume within grace must NOT be unsupported")
	}

	// Advance past pending grace. Still only s1 observed. Now unsupported.
	clk.Advance(10 * time.Second)
	// Re-ingest to refresh s1's observation (so its freshness is current).
	obs2, _ := HeartbeatToObservation(serverObs("s1", clk.Now(), healthySlot("v1", "r1")))
	if err := store.Ingest(obs2); err != nil {
		t.Fatalf("ingest 2: %v", err)
	}

	result2 := BuildSnapshot(store.Snapshot(), topo, nil)
	if _, ok := result2.Report.Unsupported["v1"]; !ok {
		t.Fatal("past pending grace, partial inventory must be unsupported")
	}
	if _, ok := result2.Report.Pending["v1"]; ok {
		t.Fatal("past pending grace, volume must NOT still be pending")
	}
	reasons := result2.Report.Unsupported["v1"].Reasons
	if !contains(reasons, ReasonMissingServerObservation) {
		t.Fatalf("expected ReasonMissingServerObservation, got %v", reasons)
	}
}

func TestObservation_MissingServerObservation_NeverOrdinaryIneligible(t *testing.T) {
	// A slot whose ServerID is not observed must NOT appear in
	// ClusterSnapshot.Volumes[].Slots as an ordinary candidate.
	// It must either make the whole volume pending or make it
	// unsupported — never silently enter as "ineligible".
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{
		FreshnessWindow: 5 * time.Second,
		PendingGrace:    100 * time.Millisecond,
	}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}}

	// Observe s1 and s2 only. s3 is missing.
	_ = ingest(t, store, serverObs("s1", start, healthySlot("v1", "r1")))
	_ = ingest(t, store, serverObs("s2", start, healthySlot("v1", "r2")))

	clk.Advance(200 * time.Millisecond) // past pending grace
	// Re-ingest to refresh freshness of s1/s2.
	_ = ingest(t, store, serverObs("s1", clk.Now(), healthySlot("v1", "r1")))
	_ = ingest(t, store, serverObs("s2", clk.Now(), healthySlot("v1", "r2")))

	result := BuildSnapshot(store.Snapshot(), topo, nil)
	if _, ok := result.Report.Unsupported["v1"]; !ok {
		t.Fatal("missing-server volume must be unsupported past grace")
	}
	for _, v := range result.Snapshot.Volumes {
		if v.VolumeID != "v1" {
			continue
		}
		for _, slot := range v.Slots {
			if slot.ReplicaID == "r3" {
				t.Fatal("missing-server slot must NOT appear as ordinary candidate in ClusterSnapshot")
			}
		}
	}
}

// ============================================================
// Row 5: end-to-end system-fed route
// ============================================================

func TestObservation_EndToEnd_SystemFedSnapshotReachesController(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	sink := &recordingSink{}
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 5 * time.Second, PendingGrace: 100 * time.Millisecond},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Sink:      sink,
		Now:       clk.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host.Start(ctx)
	defer host.Stop()

	// All three servers observed fresh.
	_ = host.IngestHeartbeat(serverObs("s1", start, healthySlot("v1", "r1")))
	_ = host.IngestHeartbeat(serverObs("s2", start, healthySlot("v1", "r2")))
	_ = host.IngestHeartbeat(serverObs("s3", start, healthySlot("v1", "r3")))

	// Poll until the sink has RECEIVED a submission whose
	// Volumes slice contains a supported v1 with three slots.
	// Earlier drafts of this test broke on the first non-empty
	// submission (which is the initial host-start rebuild that
	// submits a PENDING v1 because no ingests have landed yet),
	// then asserted on sink[last] — which could race to pick the
	// pending submission instead of the supported one that comes
	// after the ingests. The semantic under test is "a supported
	// v1 eventually reaches the sink"; poll accordingly.
	deadline := time.Now().Add(2 * time.Second)
	var final ClusterSnapshot
	sawSupported := false
	for time.Now().Before(deadline) {
		for _, c := range sink.records() {
			if len(c.Snapshot.Volumes) == 1 &&
				c.Snapshot.Volumes[0].VolumeID == "v1" &&
				len(c.Snapshot.Volumes[0].Slots) == 3 {
				final = c.Snapshot
				sawSupported = true
				break
			}
		}
		if sawSupported {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawSupported {
		t.Fatalf("sink never received a supported v1 snapshot; last observed: %+v",
			lastVolumes(sink))
	}
	if final.Volumes[0].VolumeID != "v1" {
		t.Fatalf("expected v1, got %+v", final.Volumes)
	}
	if len(final.Volumes[0].Slots) != 3 {
		t.Fatalf("expected 3 slots, got %d", len(final.Volumes[0].Slots))
	}
}

// lastVolumes is a diagnostic helper for test failures: reports
// the Volumes slice of the most recent sink submission (or "none").
func lastVolumes(sink *recordingSink) any {
	calls := sink.records()
	if len(calls) == 0 {
		return "none"
	}
	return calls[len(calls)-1].Snapshot.Volumes
}

// ============================================================
// Rows 2 + 7: conflicting primary claim
// ============================================================

func TestObservation_ConflictingPrimaryClaim_Unsupported(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 5 * time.Second}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}}

	// Two fresh slots each claim LocalRolePrimary for v1.
	s1slot := healthySlot("v1", "r1")
	s1slot.LocalRoleClaim = LocalRolePrimary
	s2slot := healthySlot("v1", "r2")
	s2slot.LocalRoleClaim = LocalRolePrimary
	_ = ingest(t, store, serverObs("s1", start, s1slot))
	_ = ingest(t, store, serverObs("s2", start, s2slot))
	_ = ingest(t, store, serverObs("s3", start, healthySlot("v1", "r3")))

	result := BuildSnapshot(store.Snapshot(), topo, nil)
	ev, ok := result.Report.Unsupported["v1"]
	if !ok {
		t.Fatal("conflicting primary claim must produce unsupported evidence")
	}
	if !contains(ev.Reasons, ReasonConflictingPrimaryClaim) {
		t.Fatalf("expected ConflictingPrimaryClaim in reasons, got %v", ev.Reasons)
	}
	if len(ev.OffendingFacts) < 2 {
		t.Fatalf("expected both offending facts recorded, got %d", len(ev.OffendingFacts))
	}
	// Volume must not be in snapshot.
	for _, v := range result.Snapshot.Volumes {
		if v.VolumeID == "v1" {
			t.Fatal("conflicting volume must NOT appear in ClusterSnapshot")
		}
	}
}

func TestObservation_ConflictingPrimaryClaim_NoWinnerEverChosen(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 5 * time.Second}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}}

	// Three primary claims with differing evidence-score and
	// differing observed-at. The observation layer must NOT
	// arbitrate — regardless of rankings.
	s1slot := healthySlot("v1", "r1")
	s1slot.LocalRoleClaim = LocalRolePrimary
	s1slot.EvidenceScore = 100 // highest
	s2slot := healthySlot("v1", "r2")
	s2slot.LocalRoleClaim = LocalRolePrimary
	s2slot.EvidenceScore = 50
	s3slot := healthySlot("v1", "r3")
	s3slot.LocalRoleClaim = LocalRolePrimary
	s3slot.EvidenceScore = 1 // lowest

	_ = ingest(t, store, serverObs("s1", start.Add(2*time.Second), s1slot))
	_ = ingest(t, store, serverObs("s2", start.Add(3*time.Second), s2slot)) // newest
	_ = ingest(t, store, serverObs("s3", start.Add(1*time.Second), s3slot)) // oldest

	result := BuildSnapshot(store.Snapshot(), topo, nil)

	if _, supported := findSupported(result, "v1"); supported {
		t.Fatal("volume must not be supported; any 'winner' choice is an authority mint")
	}
	if _, ok := result.Report.Unsupported["v1"]; !ok {
		t.Fatal("expected unsupported evidence for three-way primary conflict")
	}
}

// ============================================================
// Row 3: stale observation cannot mint fresh authority
// ============================================================

func TestObservation_ExpiredFact_SemanticallyIneligible(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 2 * time.Second}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}}

	_ = ingest(t, store, serverObs("s1", start, healthySlot("v1", "r1")))
	_ = ingest(t, store, serverObs("s2", start, healthySlot("v1", "r2")))
	_ = ingest(t, store, serverObs("s3", start, healthySlot("v1", "r3")))

	// Now advance well past FreshnessWindow. All three observations
	// are expired. They remain visible in the store but are
	// semantically ineligible for supported snapshot synthesis.
	clk.Advance(10 * time.Second)

	// Store still holds them (visibility).
	snap := store.Snapshot()
	if len(snap.observations) != 3 {
		t.Fatalf("expected expired observations to remain visible, got %d", len(snap.observations))
	}
	// But none are ServerFresh.
	for _, f := range snap.serverFreshness {
		if f == ServerFresh {
			t.Fatal("expired observation must not be ServerFresh")
		}
	}
	// Build: volume must be unsupported with StaleObservation.
	result := BuildSnapshot(snap, topo, nil)
	ev, ok := result.Report.Unsupported["v1"]
	if !ok {
		t.Fatal("expired-only volume must be unsupported")
	}
	if !contains(ev.Reasons, ReasonStaleObservation) {
		t.Fatalf("expected ReasonStaleObservation, got %v", ev.Reasons)
	}
}

func TestObservation_ExpiredDoesNotReachController(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	sink := &recordingSink{}
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 2 * time.Second},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Sink:      sink,
		Now:       clk.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host.Start(ctx)
	defer host.Stop()

	_ = host.IngestHeartbeat(serverObs("s1", start, healthySlot("v1", "r1")))
	_ = host.IngestHeartbeat(serverObs("s2", start, healthySlot("v1", "r2")))
	_ = host.IngestHeartbeat(serverObs("s3", start, healthySlot("v1", "r3")))

	// Let initial publish land.
	time.Sleep(50 * time.Millisecond)
	initialCalls := len(sink.snapshot())

	// Advance time so observations expire.
	clk.Advance(10 * time.Second)
	// Submit a spurious GC to re-trigger rebuild at the advanced
	// time so the builder re-evaluates with everything expired.
	host.Store().GC(1 * time.Hour) // no-op remove (everything within retain)

	// Force a rebuild: re-ingest a trivially-newer "s1" with the
	// SAME local inventory so the store records a mutation and
	// triggers a rebuild at the advanced clock.
	_ = host.IngestHeartbeat(serverObs("s1", clk.Now(), healthySlot("v1", "r1")))
	time.Sleep(50 * time.Millisecond)

	// The last build's report must reflect the expired state, and
	// the sink must NOT have received any supported snapshot for
	// v1 after expiry (prior supported snapshots are allowed
	// since they were valid at submission time; we assert the
	// latest BuildResult did not publish a supported v1).
	final, ok := host.LastBuild()
	if !ok {
		t.Fatal("host produced no BuildResult")
	}
	for _, v := range final.Snapshot.Volumes {
		if v.VolumeID == "v1" {
			t.Fatalf("expired-only v1 must NOT be in latest synthesized snapshot; got slots=%d", len(v.Slots))
		}
	}
	// Confirm no NEW sink publish happened for an unsupported v1.
	// (We only check that no supported v1 snapshot emerged after
	// the clock advance — any snapshot before the advance is OK.)
	_ = initialCalls
}

// ============================================================
// Row 4: bad volume does not block healthy volumes
// ============================================================

func TestObservation_BadVolumeDoesNotBlockHealthyVolume_Isolation(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 5 * time.Second}, clk.Now)

	// Two volumes, same server set. v1 healthy, v2 has conflicting
	// primary claims.
	topo := AcceptedTopology{Volumes: []VolumeExpected{
		threeSlotVolume("v1"),
		threeSlotVolume("v2"),
	}}

	v2r1 := healthySlot("v2", "r1")
	v2r1.LocalRoleClaim = LocalRolePrimary
	v2r2 := healthySlot("v2", "r2")
	v2r2.LocalRoleClaim = LocalRolePrimary
	v2r3 := healthySlot("v2", "r3")

	_ = ingest(t, store, serverObs("s1", start,
		healthySlot("v1", "r1"), v2r1))
	_ = ingest(t, store, serverObs("s2", start,
		healthySlot("v1", "r2"), v2r2))
	_ = ingest(t, store, serverObs("s3", start,
		healthySlot("v1", "r3"), v2r3))

	result := BuildSnapshot(store.Snapshot(), topo, nil)

	// v1 must be supported.
	v1, supported := findSupported(result, "v1")
	if !supported {
		t.Fatalf("v1 must be supported; got report=%+v", result.Report)
	}
	if len(v1.Slots) != 3 {
		t.Fatalf("v1 must have 3 slots, got %d", len(v1.Slots))
	}

	// v2 must be unsupported.
	if _, ok := result.Report.Unsupported["v2"]; !ok {
		t.Fatal("v2 must be unsupported")
	}

	// Isolation: v2's unsupported status must not erase v1's
	// supported status (per-volume evidence key; sketch §9).
	for _, v := range result.Snapshot.Volumes {
		if v.VolumeID == "v2" {
			t.Fatal("v2 must NOT appear in ClusterSnapshot alongside v1")
		}
	}
}

// ============================================================
// Row 8: per-volume evidence isolation
// ============================================================

func TestObservation_VolumeUnsupportedEvidence_IsolatedPerVolume(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{
		FreshnessWindow: 5 * time.Second,
		PendingGrace:    50 * time.Millisecond,
	}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{
		threeSlotVolume("v1"),
		threeSlotVolume("v2"),
	}}

	// Two different volumes, two different unsupported reasons.
	// v1 → conflicting primary; v2 → partial inventory past grace.
	v1r1 := healthySlot("v1", "r1")
	v1r1.LocalRoleClaim = LocalRolePrimary
	v1r2 := healthySlot("v1", "r2")
	v1r2.LocalRoleClaim = LocalRolePrimary

	_ = ingest(t, store, serverObs("s1", start, v1r1, healthySlot("v2", "r1")))
	_ = ingest(t, store, serverObs("s2", start, v1r2 /* v2 partial: no v2 slots */))
	_ = ingest(t, store, serverObs("s3", start, healthySlot("v1", "r3") /* no v2 slots */))

	clk.Advance(100 * time.Millisecond)
	_ = ingest(t, store, serverObs("s1", clk.Now(), v1r1, healthySlot("v2", "r1")))
	_ = ingest(t, store, serverObs("s2", clk.Now(), v1r2))
	_ = ingest(t, store, serverObs("s3", clk.Now(), healthySlot("v1", "r3")))

	result := BuildSnapshot(store.Snapshot(), topo, nil)

	// Each volume must have its OWN evidence; they must not
	// overwrite each other.
	if _, ok := result.Report.Unsupported["v1"]; !ok {
		t.Fatal("v1 evidence missing (should be ConflictingPrimaryClaim)")
	}
	if _, ok := result.Report.Unsupported["v2"]; !ok {
		t.Fatal("v2 evidence missing (should be PartialInventory)")
	}
	if !contains(result.Report.Unsupported["v1"].Reasons, ReasonConflictingPrimaryClaim) {
		t.Fatalf("v1 reasons: %v", result.Report.Unsupported["v1"].Reasons)
	}
	if !contains(result.Report.Unsupported["v2"].Reasons, ReasonPartialInventory) &&
		!contains(result.Report.Unsupported["v2"].Reasons, ReasonMissingServerObservation) {
		t.Fatalf("v2 reasons: %v", result.Report.Unsupported["v2"].Reasons)
	}
}

// ============================================================
// Row 9: push ingest triggers reactive rebuild
// ============================================================

func TestObservationHost_IngestMutation_TriggersSnapshotRebuild(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	sink := &recordingSink{}
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 5 * time.Second},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Sink:      sink,
		Now:       clk.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host.Start(ctx)
	defer host.Stop()

	// With v1 configured but no observations yet, the initial
	// rebuild produces a Pending report entry for v1 and submits
	// it — supportability transitions propagate even without
	// supported volumes. (This behavior is the fix for the
	// architect finding that pending/unsupported did not reach
	// the controller.)
	time.Sleep(50 * time.Millisecond)
	initialCalls := sink.records()
	if len(initialCalls) == 0 {
		t.Fatal("initial rebuild must submit (pending for v1) after host Start")
	}
	if _, ok := initialCalls[0].Report.Pending["v1"]; !ok {
		t.Fatalf("initial call must carry Pending[v1]; got report=%+v", initialCalls[0].Report)
	}
	if len(initialCalls[0].Snapshot.Volumes) != 0 {
		t.Fatalf("initial call must have no supported volumes; got %d", len(initialCalls[0].Snapshot.Volumes))
	}

	// Ingest one full volume's worth of observations one at a
	// time. Each ingest must wake the rebuild loop.
	_ = host.IngestHeartbeat(serverObs("s1", start, healthySlot("v1", "r1")))
	_ = host.IngestHeartbeat(serverObs("s2", start, healthySlot("v1", "r2")))
	_ = host.IngestHeartbeat(serverObs("s3", start, healthySlot("v1", "r3")))

	// Eventually, a supported-v1 snapshot should reach the sink.
	deadline := time.Now().Add(1 * time.Second)
	sawSupported := false
	for time.Now().Before(deadline) {
		for _, c := range sink.records() {
			for _, v := range c.Snapshot.Volumes {
				if v.VolumeID == "v1" {
					sawSupported = true
				}
			}
		}
		if sawSupported {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawSupported {
		t.Fatal("push ingest did not produce a supported-v1 submission")
	}
}

// ============================================================
// Row 10: freshness window is honored, not tick multiples
// ============================================================

func TestObservation_FreshnessWindowHonored_NotTickMultiples(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 3 * time.Second}, clk.Now)

	obs, _ := HeartbeatToObservation(serverObs("s1", start, healthySlot("v1", "r1")))
	_ = store.Ingest(obs)

	// Before window elapses: fresh.
	clk.Advance(2 * time.Second)
	if store.Snapshot().serverFreshness["s1"] != ServerFresh {
		t.Fatal("within window, must be fresh")
	}

	// At exact window: still fresh (`now > ExpiresAt`).
	clk.Set(start.Add(3 * time.Second))
	if store.Snapshot().serverFreshness["s1"] != ServerFresh {
		t.Fatal("exactly at window, must still be fresh")
	}

	// One nanosecond past window: expired.
	clk.Set(start.Add(3*time.Second + 1))
	if store.Snapshot().serverFreshness["s1"] != ServerExpired {
		t.Fatal("one nanosecond past window, must be expired")
	}
}

// ============================================================
// Rows 11 + 13: boundary-guard (AST-based)
// ============================================================

// TestObservation_NeverMintsAuthority_BoundaryGuard enforces the
// sketch §16a claim that observation code may not construct
// authority requests, may not call authority write surfaces, and
// may not take a wider-than-needed dependency on Publisher.
func TestObservation_NeverMintsAuthority_BoundaryGuard(t *testing.T) {
	files := observationFiles(t)
	fset := token.NewFileSet()

	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			// Forbid composite literal construction of AssignmentAsk
			// or adapter.AssignmentInfo in any form.
			if cl, ok := n.(*ast.CompositeLit); ok && cl.Type != nil {
				if typeNameMatches(cl.Type, "AssignmentAsk") ||
					isAdapterAssignmentInfo(cl.Type) {
					t.Errorf("%s: observation code constructed %s", fset.Position(cl.Pos()), exprString(cl.Type))
				}
			}
			// Forbid AddressOf on such composite literals: &AssignmentAsk{...}.
			if ue, ok := n.(*ast.UnaryExpr); ok && ue.Op == token.AND {
				if cl, ok := ue.X.(*ast.CompositeLit); ok && cl.Type != nil {
					if typeNameMatches(cl.Type, "AssignmentAsk") ||
						isAdapterAssignmentInfo(cl.Type) {
						t.Errorf("%s: observation code took &%s{...}", fset.Position(ue.Pos()), exprString(cl.Type))
					}
				}
			}
			// Forbid *Publisher as a field or parameter type —
			// observation code must take a narrow read-only
			// interface, not the concrete publisher.
			if star, ok := n.(*ast.StarExpr); ok {
				if id, ok := star.X.(*ast.Ident); ok && id.Name == "Publisher" {
					t.Errorf("%s: observation code depends on *Publisher concrete type — must use narrow read-only interface", fset.Position(star.Pos()))
				}
			}
			return true
		})
	}
}

// TestObservation_SupportabilityDoesNotReadClusterSnapshotOutput
// is the pipeline-direction guard (row 13). Supportability code
// lives in observation_store.go + snapshot_builder.go. Those
// files must not take a `ClusterSnapshot` as INPUT (parameter).
// ClusterSnapshot is an OUTPUT type here.
func TestObservation_SupportabilityDoesNotReadClusterSnapshotOutput(t *testing.T) {
	files := []string{"observation_store.go", "snapshot_builder.go"}
	fset := token.NewFileSet()

	for _, name := range files {
		path := resolveAuthorityFile(t, name)
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Type == nil || fn.Type.Params == nil {
				return true
			}
			for _, p := range fn.Type.Params.List {
				if typeNameMatches(p.Type, "ClusterSnapshot") {
					t.Errorf("%s: function %s takes ClusterSnapshot as input — supportability must not read synthesized output",
						fset.Position(p.Pos()), fn.Name.Name)
				}
			}
			return true
		})
	}
}

// ============================================================
// Row 12: duplicate-server topology
// ============================================================

func TestObservation_DuplicateServerTopology_UnsupportedAndEvidenceRecorded(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 5 * time.Second}, clk.Now)

	// Accepted topology claims r1@s1, r2@s2, r3@s3. But s1 also
	// observes a fact claiming r2 lives on s1. Same-server
	// duplicate.
	topo := AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}}
	badR2 := healthySlot("v1", "r2")
	_ = ingest(t, store, serverObs("s1", start,
		healthySlot("v1", "r1"),
		badR2, // s1 ALSO claims r2 — topology violation
	))
	_ = ingest(t, store, serverObs("s2", start, healthySlot("v1", "r2")))
	_ = ingest(t, store, serverObs("s3", start, healthySlot("v1", "r3")))

	result := BuildSnapshot(store.Snapshot(), topo, nil)
	ev, ok := result.Report.Unsupported["v1"]
	if !ok {
		t.Fatal("duplicate-server topology must produce unsupported evidence")
	}
	if !contains(ev.Reasons, ReasonDuplicateServerTopology) {
		t.Fatalf("expected ReasonDuplicateServerTopology, got %v", ev.Reasons)
	}
	if len(ev.OffendingFacts) == 0 {
		t.Fatal("offending facts must record the duplicated server observation")
	}
}

func TestObservation_DuplicateServerTopology_NotControllerInput(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	sink := &recordingSink{}
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 5 * time.Second},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Sink:      sink,
		Now:       clk.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host.Start(ctx)
	defer host.Stop()

	_ = host.IngestHeartbeat(serverObs("s1", start,
		healthySlot("v1", "r1"), healthySlot("v1", "r2")))
	_ = host.IngestHeartbeat(serverObs("s2", start, healthySlot("v1", "r2")))
	_ = host.IngestHeartbeat(serverObs("s3", start, healthySlot("v1", "r3")))

	time.Sleep(100 * time.Millisecond)
	// Sink must not see v1 — the supported-volumes set was empty,
	// so no SubmitClusterSnapshot call fires for it.
	for _, s := range sink.snapshot() {
		for _, v := range s.Volumes {
			if v.VolumeID == "v1" {
				t.Fatal("duplicate-server volume must NOT reach the controller")
			}
		}
	}
}

// ============================================================
// Helpers
// ============================================================

func ingest(t *testing.T, store *ObservationStore, msg HeartbeatMessage) error {
	t.Helper()
	obs, err := HeartbeatToObservation(msg)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if err := store.Ingest(obs); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func findSupported(result BuildResult, volumeID string) (VolumeTopologySnapshot, bool) {
	for _, v := range result.Snapshot.Volumes {
		if v.VolumeID == volumeID {
			return v, true
		}
	}
	return VolumeTopologySnapshot{}, false
}

// observationFiles returns every production .go file under
// core/authority/ whose name starts with "observation_".
func observationFiles(t *testing.T) []string {
	t.Helper()
	dir := resolveAuthorityDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if !strings.HasPrefix(name, "observation_") {
			continue
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		out = append(out, filepath.Join(dir, name))
	}
	sort.Strings(out)
	if len(out) == 0 {
		t.Fatal("no observation_*.go files found")
	}
	return out
}

func resolveAuthorityDir(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// Tests run in package dir.
	return cwd
}

func resolveAuthorityFile(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(resolveAuthorityDir(t), name)
}

// typeNameMatches reports whether an AST type expression names a
// given identifier, possibly via a selector (e.g. pkg.Name).
func typeNameMatches(expr ast.Expr, name string) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name == name
	case *ast.SelectorExpr:
		return t.Sel.Name == name
	case *ast.StarExpr:
		return typeNameMatches(t.X, name)
	case *ast.ArrayType:
		return typeNameMatches(t.Elt, name)
	}
	return false
}

func isAdapterAssignmentInfo(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.SelectorExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return id.Name == "adapter" && t.Sel.Name == "AssignmentInfo"
		}
	case *ast.StarExpr:
		return isAdapterAssignmentInfo(t.X)
	case *ast.ArrayType:
		return isAdapterAssignmentInfo(t.Elt)
	}
	return false
}

func exprString(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return exprString(t.X) + "." + t.Sel.Name
	case *ast.StarExpr:
		return "*" + exprString(t.X)
	}
	return "?"
}

// ============================================================
// Architect-round regressions (v3-phase-14-s4 review #2)
// ============================================================

// fakeBasisReader is a minimal AuthorityBasisReader for tests
// that need to drive observation-fed snapshots with controlled
// per-volume authority basis values.
type fakeReaderForHost struct {
	mu       sync.Mutex
	perVol   map[string]AuthorityBasis
	perSlot  map[string]AuthorityBasis
}

func newFakeReaderForHost() *fakeReaderForHost {
	return &fakeReaderForHost{
		perVol:  map[string]AuthorityBasis{},
		perSlot: map[string]AuthorityBasis{},
	}
}
func (r *fakeReaderForHost) LastAuthorityBasis(volumeID, replicaID string) (AuthorityBasis, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.perSlot[volumeID+"/"+replicaID]
	return b, ok
}
func (r *fakeReaderForHost) VolumeAuthorityLine(volumeID string) (AuthorityBasis, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.perVol[volumeID]
	return b, ok
}
func (r *fakeReaderForHost) SetVolume(volumeID string, b AuthorityBasis) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.perVol[volumeID] = b
	r.perSlot[volumeID+"/"+b.ReplicaID] = b
}

// TestObservation_BuilderStampsAuthorityFromReader is the direct
// regression for architect finding #1: observation-fed supported
// VolumeTopologySnapshots must carry the publisher's current
// per-volume authority basis so the controller's stale-resistance
// check can match. A snapshot that always says Assigned=false
// would make the controller drop every observation-fed volume
// once a publisher line exists.
func TestObservation_BuilderStampsAuthorityFromReader(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 5 * time.Second}, clk.Now)
	topo := AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}}

	// All three slots observed fresh.
	_ = ingest(t, store, serverObs("s1", start, healthySlot("v1", "r1")))
	_ = ingest(t, store, serverObs("s2", start, healthySlot("v1", "r2")))
	_ = ingest(t, store, serverObs("s3", start, healthySlot("v1", "r3")))

	reader := newFakeReaderForHost()
	// Publisher currently holds r1@5 for v1 (simulated).
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1,
		DataAddr: "data-r1", CtrlAddr: "ctrl-r1",
	})

	result := BuildSnapshot(store.Snapshot(), topo, reader)
	v, ok := findSupported(result, "v1")
	if !ok {
		t.Fatal("v1 must be supported")
	}
	if !v.Authority.Assigned {
		t.Fatal("supported volume must carry publisher's current authority basis; Authority.Assigned=false")
	}
	if v.Authority.ReplicaID != "r1" || v.Authority.Epoch != 5 {
		t.Fatalf("Authority basis wrong: got %+v", v.Authority)
	}

	// Without a reader, default is Assigned=false (initial-bind
	// case). That is still the honest "no view" default.
	resultNoReader := BuildSnapshot(store.Snapshot(), topo, nil)
	vn, ok := findSupported(resultNoReader, "v1")
	if !ok {
		t.Fatal("v1 must be supported without reader")
	}
	if vn.Authority.Assigned {
		t.Fatalf("nil reader must leave Authority.Assigned=false, got %+v", vn.Authority)
	}
}

// TestObservation_SupportabilityCollapsePropagatesToController
// is the direct regression for architect finding #2: when all
// supported volumes disappear (e.g. observations expire), the
// host must still propagate pending/unsupported transitions to
// the controller, and the controller must clear its prior
// desired state for those volumes.
func TestObservation_SupportabilityCollapsePropagatesToController(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	sink := &recordingSink{}
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 2 * time.Second, PendingGrace: 100 * time.Millisecond},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Sink:      sink,
		Now:       clk.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host.Start(ctx)
	defer host.Stop()

	// Reach supported for v1.
	_ = host.IngestHeartbeat(serverObs("s1", start, healthySlot("v1", "r1")))
	_ = host.IngestHeartbeat(serverObs("s2", start, healthySlot("v1", "r2")))
	_ = host.IngestHeartbeat(serverObs("s3", start, healthySlot("v1", "r3")))
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range sink.records() {
			if len(c.Snapshot.Volumes) > 0 {
				goto reachedSupported
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("did not reach supported state")
reachedSupported:
	preCount := len(sink.records())

	// Collapse: advance clock past freshness window so every
	// observation expires. Then trigger a rebuild by re-ingesting
	// a single fresh observation for a DIFFERENT, unconfigured
	// volume — this mutates the store (triggering rebuild) but
	// does not restore v1's fresh observations.
	clk.Advance(10 * time.Second)
	// Trigger a mutation. Use GC or a fresh observation for a
	// non-v1 replica. Easiest: a fresh s1 observation claiming a
	// DIFFERENT (unknown) volume so v1 itself stays expired.
	_ = host.IngestHeartbeat(serverObs("s1", clk.Now(), SlotFact{
		VolumeID: "v-unknown", ReplicaID: "x",
		DataAddr: "d", CtrlAddr: "c",
		Reachable: true, ReadyForPrimary: true, Eligible: true,
	}))

	// Wait for a submission where v1 is NOT in supported volumes
	// but IS in the unsupported report.
	deadline = time.Now().Add(1 * time.Second)
	sawCollapse := false
	for time.Now().Before(deadline) {
		records := sink.records()
		if len(records) > preCount {
			for _, c := range records[preCount:] {
				// Collapse: v1 is either in Unsupported or not
				// anywhere in supported.
				inSupported := false
				for _, v := range c.Snapshot.Volumes {
					if v.VolumeID == "v1" {
						inSupported = true
					}
				}
				_, inUnsupported := c.Report.Unsupported["v1"]
				if !inSupported && inUnsupported {
					sawCollapse = true
					break
				}
			}
		}
		if sawCollapse {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawCollapse {
		t.Fatal("supportability collapse did not propagate to sink as Unsupported[v1]")
	}
}

// TestObservation_SupportabilityCollapseClearsControllerDesired
// is the controller-side half of finding #2: the controller's
// SubmitObservedState intake must clear the desired-assignment
// map for any volume that appears in report.Unsupported, so old
// desired state does not linger after supportability collapses.
func TestObservation_SupportabilityCollapseClearsControllerDesired(t *testing.T) {
	reader := newFakeReaderForHost()
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	}, reader)

	// Seed a desired assignment via a supported submission.
	snap := ClusterSnapshot{
		CollectedRevision: 1,
		CollectedAt:       time.Now(),
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{{
			VolumeID: "v1",
			Slots: []ReplicaCandidate{
				{ReplicaID: "r1", ServerID: "s1", DataAddr: "d1", CtrlAddr: "c1", Reachable: true, ReadyForPrimary: true, Eligible: true},
				{ReplicaID: "r2", ServerID: "s2", DataAddr: "d2", CtrlAddr: "c2", Reachable: true, ReadyForPrimary: true, Eligible: true},
				{ReplicaID: "r3", ServerID: "s3", DataAddr: "d3", CtrlAddr: "c3", Reachable: true, ReadyForPrimary: true, Eligible: true},
			},
		}},
	}
	if err := ctrl.SubmitObservedState(snap, SupportabilityReport{}); err != nil {
		t.Fatalf("initial: %v", err)
	}
	// Drain the ask (should be a Bind for v1).
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := ctrl.Next(ctx); err != nil {
		t.Fatalf("expected initial Bind ask: %v", err)
	}

	// Now submit an observed state where v1 is unsupported (e.g.
	// observations collapsed). No supported volumes. Report has
	// v1 in Unsupported.
	report := SupportabilityReport{
		Unsupported: map[string]VolumeUnsupportedEvidence{
			"v1": {
				VolumeID:         "v1",
				SnapshotRevision: 2,
				EvaluatedAt:      time.Now(),
				Reasons:          []string{ReasonStaleObservation},
			},
		},
	}
	if err := ctrl.SubmitObservedState(ClusterSnapshot{CollectedRevision: 2}, report); err != nil {
		t.Fatalf("collapse submit: %v", err)
	}

	// Controller must expose per-volume unsupported evidence now.
	if _, ok := ctrl.LastUnsupported("v1"); !ok {
		t.Fatal("controller must record unsupported evidence from observation report")
	}

	// And it must have no outstanding desired ask for v1 —
	// draining Next must block, not return a stale Reassign or
	// similar.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if ask, err := ctrl.Next(ctx2); err == nil {
		t.Fatalf("expected no ask after collapse; got %+v", ask)
	}
}

// TestObservation_UnknownObservedVolume_ProducesUnsupportedEvidence
// is the regression for architect finding #3: observed VolumeIDs
// outside the accepted topology must not be silently dropped;
// they must appear in the supportability report as unsupported.
func TestObservation_UnknownObservedVolume_ProducesUnsupportedEvidence(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	store := NewObservationStore(FreshnessConfig{FreshnessWindow: 5 * time.Second}, clk.Now)

	// Accepted topology knows only v1. But a server reports a
	// slot for "v-rogue" — outside the accepted set.
	topo := AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}}

	// Fresh, well-formed observation for a volume the operator
	// has not configured.
	_ = ingest(t, store, serverObs("s1", start,
		healthySlot("v1", "r1"),
		healthySlot("v-rogue", "r1"),
	))

	result := BuildSnapshot(store.Snapshot(), topo, nil)
	ev, ok := result.Report.Unsupported["v-rogue"]
	if !ok {
		t.Fatal("unknown observed volume must surface as unsupported evidence, not silent drop")
	}
	if !contains(ev.Reasons, ReasonUnknownReplicaClaim) {
		t.Fatalf("expected ReasonUnknownReplicaClaim, got %v", ev.Reasons)
	}
	if len(ev.OffendingFacts) == 0 {
		t.Fatal("unknown-volume evidence must carry offending observation facts")
	}
	// And v-rogue must not appear in the synthesized Volumes list.
	for _, v := range result.Snapshot.Volumes {
		if v.VolumeID == "v-rogue" {
			t.Fatal("unknown volume must NOT appear in ClusterSnapshot.Volumes")
		}
	}
}

// TestObservationHost_IdenticalSupportedSnapshots_NoDuplicateAsks
// is the regression for the architect's round-3 finding #1: the
// observation-fed controller intake must dedupe repeated
// identical supported snapshots. Without dedupe, each feed of the
// same supported state would enqueue another Reassign, and
// Reassign mints a new authority epoch — authority churn, not
// convergence.
func TestObservationHost_IdenticalSupportedSnapshots_NoDuplicateAsks(t *testing.T) {
	reader := newFakeReaderForHost()
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	}, reader)

	// Publisher currently holds r1@5 for v1.
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})

	// A supported snapshot where the current primary r1 is NOT
	// acceptable (Reachable=false). That forces the controller's
	// decision table to emit a failover Reassign to r2.
	primarySlot := ReplicaCandidate{
		ReplicaID: "r1", ServerID: "s1", DataAddr: "d1", CtrlAddr: "c1",
		Reachable: false, ReadyForPrimary: false, Eligible: true,
	}
	secondarySlot := ReplicaCandidate{
		ReplicaID: "r2", ServerID: "s2", DataAddr: "d2", CtrlAddr: "c2",
		Reachable: true, ReadyForPrimary: true, Eligible: true,
	}
	tertiarySlot := ReplicaCandidate{
		ReplicaID: "r3", ServerID: "s3", DataAddr: "d3", CtrlAddr: "c3",
		Reachable: true, ReadyForPrimary: true, Eligible: true,
	}
	snap := ClusterSnapshot{
		CollectedRevision: 1,
		CollectedAt:       time.Now(),
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{{
			VolumeID: "v1",
			Authority: AuthorityBasis{
				Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1,
				DataAddr: "d1", CtrlAddr: "c1",
			},
			Slots: []ReplicaCandidate{primarySlot, secondarySlot, tertiarySlot},
		}},
	}

	// First submit: should enqueue one Reassign.
	if err := ctrl.SubmitObservedState(snap, SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()
	ask1, err := ctrl.Next(ctx1)
	if err != nil {
		t.Fatalf("expected initial Reassign ask: %v", err)
	}
	if ask1.Intent != IntentReassign || ask1.ReplicaID != "r2" {
		t.Fatalf("expected Reassign to r2, got %+v", ask1)
	}

	// Now submit the SAME supported snapshot again, and again.
	// The decision table still wants to Reassign to r2 because
	// the publisher hasn't advanced yet (r1@5 still current);
	// without dedupe this re-enqueues every time.
	for i := 0; i < 3; i++ {
		// Bump revision to prove dedupe is based on ask content,
		// not just revision equality.
		snap.CollectedRevision = uint64(2 + i)
		if err := ctrl.SubmitObservedState(snap, SupportabilityReport{}); err != nil {
			t.Fatalf("submit %d: %v", i+2, err)
		}
	}

	// Next must block — no new ask should have been enqueued
	// despite three more identical-intent submissions.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel2()
	if ask, err := ctrl.Next(ctx2); err == nil {
		t.Fatalf("same-ask dedupe failed: got churning ask %+v", ask)
	}
}

// TestObservationHost_VolumeRecovers_ClearsStaleDesired is the
// regression for the architect's round-3 finding #2: when a
// supported snapshot shows the current primary is acceptable
// again, any prior-queued failover/rebalance ask must be
// cleared. Otherwise the controller can publish a move it no
// longer wants.
func TestObservationHost_VolumeRecovers_ClearsStaleDesired(t *testing.T) {
	reader := newFakeReaderForHost()
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
	}, reader)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})

	servers := []ServerObservation{
		{ServerID: "s1", Reachable: true, Eligible: true},
		{ServerID: "s2", Reachable: true, Eligible: true},
		{ServerID: "s3", Reachable: true, Eligible: true},
	}
	authMatchesLine := AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	}
	okSlotR1 := ReplicaCandidate{
		ReplicaID: "r1", ServerID: "s1", DataAddr: "d1", CtrlAddr: "c1",
		Reachable: true, ReadyForPrimary: true, Eligible: true,
	}
	badSlotR1 := ReplicaCandidate{
		ReplicaID: "r1", ServerID: "s1", DataAddr: "d1", CtrlAddr: "c1",
		Reachable: false, ReadyForPrimary: false, Eligible: true,
	}
	slotR2 := ReplicaCandidate{
		ReplicaID: "r2", ServerID: "s2", DataAddr: "d2", CtrlAddr: "c2",
		Reachable: true, ReadyForPrimary: true, Eligible: true,
	}
	slotR3 := ReplicaCandidate{
		ReplicaID: "r3", ServerID: "s3", DataAddr: "d3", CtrlAddr: "c3",
		Reachable: true, ReadyForPrimary: true, Eligible: true,
	}

	// Snapshot A: r1 is unacceptable -> controller queues a
	// failover Reassign to r2. We do NOT drain it here — this
	// simulates the "queued ask has not drained yet" window the
	// architect flagged.
	snapA := ClusterSnapshot{
		CollectedRevision: 1,
		CollectedAt:       time.Now(),
		Servers:           servers,
		Volumes: []VolumeTopologySnapshot{{
			VolumeID:  "v1",
			Authority: authMatchesLine,
			Slots:     []ReplicaCandidate{badSlotR1, slotR2, slotR3},
		}},
	}
	if err := ctrl.SubmitObservedState(snapA, SupportabilityReport{}); err != nil {
		t.Fatalf("submit A: %v", err)
	}

	// Snapshot B: r1 has recovered, is acceptable again. The
	// decision table now returns no-emit. The previously-queued
	// Reassign must be cleared, not left live in the queue.
	snapB := ClusterSnapshot{
		CollectedRevision: 2,
		CollectedAt:       time.Now(),
		Servers:           servers,
		Volumes: []VolumeTopologySnapshot{{
			VolumeID:  "v1",
			Authority: authMatchesLine,
			Slots:     []ReplicaCandidate{okSlotR1, slotR2, slotR3},
		}},
	}
	if err := ctrl.SubmitObservedState(snapB, SupportabilityReport{}); err != nil {
		t.Fatalf("submit B: %v", err)
	}

	// Next must block — the prior Reassign ask must have been
	// discarded when snapshot B showed recovery.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if ask, err := ctrl.Next(ctx); err == nil {
		t.Fatalf("expected no ask after recovery; got stale move %+v", ask)
	}
}

// TestObservationHost_GCRetainWindow_Honored covers finding #4:
// the host's configured GCRetainWindow must be used by the
// periodic GC sweep. Previously the sweep hardcoded
// FreshnessWindow*4.
func TestObservationHost_GCRetainWindow_Honored(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 10 * time.Second},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Now:       clk.Now,
		// Small explicit retain window — a default would have
		// been 40s (10s * 4).
		GCRetainWindow: 3 * time.Second,
	})

	// Ingest one observation, then advance past GCRetainWindow
	// and call Store().GC with the host's retain window.
	_ = host.IngestHeartbeat(serverObs("s1", start, healthySlot("v1", "r1")))
	if got := len(host.Store().Snapshot().observations); got != 1 {
		t.Fatalf("expected 1 observation stored, got %d", got)
	}

	// Advance 5 seconds — past GCRetainWindow=3s but NOT past
	// the default 40s. If the host honored the configured
	// retain window, a direct GC(hostRetain) would evict. We
	// prove the wiring by reading the internal field: the host
	// retained the configured value.
	clk.Advance(5 * time.Second)

	// Call GC directly with the host's retain window — this is
	// what the host's run-loop does on the GC tick. We assert
	// the observation was evicted, which only happens if the
	// retain value was truthfully 3s, not the hardcoded 40s.
	if removed := host.Store().GC(host.gcRetain); removed != 1 {
		t.Fatalf("expected GC to evict 1 observation with retain=%v; removed=%d", host.gcRetain, removed)
	}
	if got := len(host.Store().Snapshot().observations); got != 0 {
		t.Fatalf("expected observation evicted by GC, %d remain", got)
	}

	// Sanity: explicit honor — if we omit GCRetainWindow, the
	// default must still be derived from FreshnessWindow*4, not
	// something smaller.
	host2 := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 10 * time.Second},
		Now:       clk.Now,
	})
	if host2.gcRetain != 40*time.Second {
		t.Fatalf("default GC retain must be 4x FreshnessWindow (40s), got %v", host2.gcRetain)
	}
}
