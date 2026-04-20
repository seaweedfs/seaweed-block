package authority

import (
	"context"
	"testing"
	"time"
)

// ============================================================
// P14 S6 — Convergence Institution proof tests
//
// Each test maps to a row in sw-block/design/v3-phase-14-s6-
// sketch.md §13 (Proof Matrix). The tests exercise the
// controller's convergence surface directly (no observation
// host) so the bounded-wait + stuck + supersede contracts can
// be verified independently of S4 plumbing.
// ============================================================

// convTestCtrl builds a controller wired to a fake clock and
// fake reader with an explicit retry window (explicit to keep
// the tests insensitive to default changes). Returns controller,
// reader, and clock.
func convTestCtrl(t *testing.T, retryWindow time.Duration) (*TopologyController, *fakeReaderForHost, *fakeClock) {
	t.Helper()
	reader := newFakeReaderForHost()
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
		RetryWindow:            retryWindow,
	}, reader)
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))
	ctrl.SetNowForTest(clk.Now)
	return ctrl, reader, clk
}

// failoverSnapForV1 builds a v1 snapshot where r1 is
// unreachable (forcing a failover Reassign to r2) and r2/r3 are
// healthy. authority is the Authority field on the snapshot
// (what the observation reports the system thinks is current).
func failoverSnapForV1(authority AuthorityBasis, rev uint64) ClusterSnapshot {
	return ClusterSnapshot{
		CollectedRevision: rev,
		CollectedAt:       time.Now(),
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{{
			VolumeID:  "v1",
			Authority: authority,
			Slots: []ReplicaCandidate{
				{ReplicaID: "r1", ServerID: "s1", DataAddr: "d1", CtrlAddr: "c1", Reachable: false, ReadyForPrimary: false, Eligible: true},
				{ReplicaID: "r2", ServerID: "s2", DataAddr: "d2", CtrlAddr: "c2", Reachable: true, ReadyForPrimary: true, Eligible: true},
				{ReplicaID: "r3", ServerID: "s3", DataAddr: "d3", CtrlAddr: "c3", Reachable: true, ReadyForPrimary: true, Eligible: true},
			},
		}},
	}
}

// healthySnapForV1 builds a v1 snapshot where ALL three slots
// are healthy. Used after a successful move to let the decision
// table settle (no emit).
func healthySnapForV1(authority AuthorityBasis, rev uint64) ClusterSnapshot {
	snap := failoverSnapForV1(authority, rev)
	snap.Volumes[0].Slots[0].Reachable = true
	snap.Volumes[0].Slots[0].ReadyForPrimary = true
	return snap
}

// drainOneAsk returns the single ask currently queued or fails.
func drainOneAsk(t *testing.T, ctrl *TopologyController) AssignmentAsk {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ask, err := ctrl.Next(ctx)
	if err != nil {
		t.Fatalf("expected one ask, got err: %v", err)
	}
	return ask
}

// expectNoQueuedAsk asserts that no ask is currently queued.
func expectNoQueuedAsk(t *testing.T, ctrl *TopologyController) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if ask, err := ctrl.Next(ctx); err == nil {
		t.Fatalf("expected no queued ask, got %+v", ask)
	}
}

// Row 1: observed confirmation clears desired (Active → None).
func TestConvergence_Confirmation_ClearsDesired(t *testing.T) {
	ctrl, reader, _ := convTestCtrl(t, 5*time.Second)

	// Publisher holds r1@5. First submit produces Reassign to r2.
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	if err := ctrl.SubmitObservedState(failoverSnapForV1(AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	}, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)
	if _, ok := ctrl.DesiredFor("v1"); !ok {
		t.Fatal("expected desired entry after emit")
	}

	// Publisher applies: r2@6. Observation catches up to r2@6.
	applied := AuthorityBasis{Assigned: true, ReplicaID: "r2", Epoch: 6, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2"}
	reader.SetVolume("v1", applied)
	if err := ctrl.SubmitObservedState(healthySnapForV1(applied, 2), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if _, ok := ctrl.DesiredFor("v1"); ok {
		t.Fatal("expected desired cleared after confirmation")
	}
	if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatal("expected stuck evidence cleared on confirmation")
	}
}

// Row 2: confirmation requires BOTH observation and publisher
// to agree. Publisher applied, observation still stale → NOT
// confirmed.
func TestConvergence_Confirmation_RequiresBothSources(t *testing.T) {
	ctrl, reader, _ := convTestCtrl(t, 5*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1,
		DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)

	// Publisher advances to r2@6 but observation still reports r1@5.
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r2", Epoch: 6, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2",
	})
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 2), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if _, ok := ctrl.DesiredFor("v1"); !ok {
		t.Fatal("desired must remain until observation confirms (not publisher alone)")
	}
}

// Row 3: stale observation cannot confirm.
func TestConvergence_StaleObservation_NoConfirmation(t *testing.T) {
	ctrl, reader, _ := convTestCtrl(t, 5*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	if err := ctrl.SubmitObservedState(failoverSnapForV1(AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	}, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)

	// Publisher applied r2@6. Observation reports r2@5 (wrong
	// epoch) — mismatch, not fresh.
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r2", Epoch: 6, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2",
	})
	obs := AuthorityBasis{Assigned: true, ReplicaID: "r2", Epoch: 5, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2"}
	if err := ctrl.SubmitObservedState(healthySnapForV1(obs, 2), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if _, ok := ctrl.DesiredFor("v1"); !ok {
		t.Fatal("desired must remain when observation epoch disagrees with publisher")
	}
}

// Row 4: publish-not-observed marks Stuck after RetryWindow.
func TestConvergence_PublishNotObserved_StuckAfterWindow(t *testing.T) {
	ctrl, reader, clk := convTestCtrl(t, 5*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)
	if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatal("must not be Stuck before RetryWindow elapses")
	}

	// Advance within the window — still not stuck.
	clk.Advance(3 * time.Second)
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r2", Epoch: 6, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2",
	})
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 2), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatal("must not be Stuck within RetryWindow")
	}

	// Advance past the window.
	clk.Advance(3 * time.Second)
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 3), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 3: %v", err)
	}
	ev, ok := ctrl.LastConvergenceStuck("v1")
	if !ok {
		t.Fatal("expected Stuck evidence after RetryWindow")
	}
	if ev.Reason != ReasonConvergenceStuck {
		t.Fatalf("reason: got %q want %q", ev.Reason, ReasonConvergenceStuck)
	}
	if ev.Ask.ReplicaID != "r2" {
		t.Fatalf("evidence must carry the outstanding ask, got %+v", ev.Ask)
	}
	if ev.ProposedBasis.Epoch != 5 || ev.ProposedBasis.ReplicaID != "r1" {
		t.Fatalf("evidence must carry ProposedBasis (r1@5), got %+v", ev.ProposedBasis)
	}
}

// Row 5: Stuck does NOT re-emit. After the stuck transition, no
// new ask is enqueued and no new IntentReassign epoch would be
// minted from the same unchanged observed truth.
func TestConvergence_Stuck_DoesNotReMintReassign(t *testing.T) {
	ctrl, reader, clk := convTestCtrl(t, 2*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)

	// Push many identical snapshots past the retry window.
	for i := 0; i < 5; i++ {
		clk.Advance(1 * time.Second)
		if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, uint64(2+i)), SupportabilityReport{}); err != nil {
			t.Fatalf("submit %d: %v", i+2, err)
		}
	}
	// Must be Stuck now.
	if _, ok := ctrl.LastConvergenceStuck("v1"); !ok {
		t.Fatal("expected Stuck after multiple post-window submits")
	}
	// And no new ask in the queue — the passive-retry rule
	// means Stuck keeps the entry but never re-enqueues.
	expectNoQueuedAsk(t, ctrl)
}

// Row 9 (main): superseded desired — publisher itself advanced
// past ProposedBasis to a replica other than our target.
func TestConvergence_Supersede_PublisherAdvancedToOtherReplica(t *testing.T) {
	ctrl, reader, _ := convTestCtrl(t, 5*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl) // Reassign → r2

	// Publisher independently advanced to r3@6 (not our target).
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r3", Epoch: 6, EndpointVersion: 1, DataAddr: "d3", CtrlAddr: "c3",
	})
	// Observation still shows old r1@5.
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 2), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if _, ok := ctrl.DesiredFor("v1"); ok {
		t.Fatal("desired must be superseded when publisher advanced to a different target")
	}
}

// Row 9b: NORMAL LAG — Reassign published, observation still
// reports old line. Desired must remain Active, with no
// duplicate ask and no supersede. Directly guards the round-2
// architect finding.
func TestConvergence_NormalLag_OldObservationDoesNotSupersede(t *testing.T) {
	ctrl, reader, _ := convTestCtrl(t, 5*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	first := drainOneAsk(t, ctrl)
	if first.ReplicaID != "r2" || first.Intent != IntentReassign {
		t.Fatalf("expected Reassign to r2, got %+v", first)
	}

	// Publisher has applied r2@6, but observation still reports
	// the old r1@5 line. Desired target (r2) == publisher target.
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r2", Epoch: 6, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2",
	})
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 2), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if _, ok := ctrl.DesiredFor("v1"); !ok {
		t.Fatal("desired MUST remain Active during normal publisher-vs-observation lag")
	}
	expectNoQueuedAsk(t, ctrl)
}

// Row 10: supersede — decision table on fresh snapshot produces
// a DIFFERENT ask. The old desired (r2) is replaced by the new
// one (r3), and stuck evidence (if any) is dropped.
func TestConvergence_Supersede_DecisionTableProducesDifferentAsk(t *testing.T) {
	ctrl, reader, clk := convTestCtrl(t, 2*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	snap1 := failoverSnapForV1(stale, 1)
	// Force decision toward r2 first by making r3 unreachable.
	snap1.Volumes[0].Slots[2].Reachable = false
	snap1.Volumes[0].Slots[2].ReadyForPrimary = false
	if err := ctrl.SubmitObservedState(snap1, SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	first := drainOneAsk(t, ctrl)
	if first.ReplicaID != "r2" {
		t.Fatalf("expected Reassign to r2, got %+v", first)
	}

	// Drive to stuck so we can verify evidence is dropped when
	// the decision differs.
	clk.Advance(3 * time.Second)
	// Second snapshot: now r2 is unreachable and r3 is back;
	// decision must pick r3.
	snap2 := failoverSnapForV1(stale, 2)
	snap2.Volumes[0].Slots[1].Reachable = false
	snap2.Volumes[0].Slots[1].ReadyForPrimary = false
	if err := ctrl.SubmitObservedState(snap2, SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	second := drainOneAsk(t, ctrl)
	if second.ReplicaID != "r3" {
		t.Fatalf("expected supersede Reassign to r3, got %+v", second)
	}
	d, ok := ctrl.DesiredFor("v1")
	if !ok || d.Ask.ReplicaID != "r3" {
		t.Fatalf("desired must now target r3, got %+v ok=%v", d, ok)
	}
	if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatal("stuck evidence must be cleared when the desired is replaced by a different ask")
	}
}

// Row 7: pending observation clears desired.
func TestConvergence_PendingClearsDesired(t *testing.T) {
	ctrl, reader, _ := convTestCtrl(t, 5*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)

	// v1 transitions to Pending.
	if err := ctrl.SubmitObservedState(ClusterSnapshot{CollectedRevision: 2},
		SupportabilityReport{
			Pending: map[string]VolumePendingStatus{"v1": {
				VolumeID:    "v1",
				EvaluatedAt: time.Now(),
			}},
		}); err != nil {
		t.Fatalf("submit pending: %v", err)
	}
	if _, ok := ctrl.DesiredFor("v1"); ok {
		t.Fatal("desired must clear on Pending transition")
	}
	if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatal("stuck evidence must clear with desired on Pending")
	}
}

// Row 8: unsupported observation clears desired AND records
// UnsupportedEvidence (distinct from ConvergenceStuckEvidence).
func TestConvergence_UnsupportedClearsDesiredWithEvidence(t *testing.T) {
	ctrl, reader, _ := convTestCtrl(t, 5*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)

	// v1 transitions to Unsupported.
	unsupportedAt := time.Now()
	if err := ctrl.SubmitObservedState(ClusterSnapshot{CollectedRevision: 2},
		SupportabilityReport{
			Unsupported: map[string]VolumeUnsupportedEvidence{"v1": {
				VolumeID:         "v1",
				SnapshotRevision: 2,
				EvaluatedAt:      unsupportedAt,
				Reasons:          []string{ReasonConflictingPrimaryClaim},
			}},
		}); err != nil {
		t.Fatalf("submit unsupported: %v", err)
	}
	if _, ok := ctrl.DesiredFor("v1"); ok {
		t.Fatal("desired must clear on Unsupported")
	}
	if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatal("stuck evidence must clear with desired on Unsupported")
	}
	ev, ok := ctrl.LastUnsupported("v1")
	if !ok {
		t.Fatal("UnsupportedEvidence must be recorded (distinct class from ConvergenceStuck)")
	}
	if ev.Reason != ReasonConflictingPrimaryClaim {
		t.Fatalf("unsupported reason: got %q want %q", ev.Reason, ReasonConflictingPrimaryClaim)
	}
}

// Row 11: one stuck volume does not block unrelated volume
// convergence. Both v1 and v2 decide Reassigns on the same
// snapshot; v1 cannot observe (stays stuck) but v2 confirms
// normally.
func TestConvergence_OneStuckVolumeDoesNotBlockOthers(t *testing.T) {
	reader := newFakeReaderForHost()
	// High RebalanceSkew so the decision table does not fire a
	// post-confirmation rebalance after v1/v2 end up co-located
	// on the same server — we are testing convergence isolation,
	// not rebalance.
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
		RetryWindow:            2 * time.Second,
		RebalanceSkew:          100,
	}, reader)
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))
	ctrl.SetNowForTest(clk.Now)

	reader.SetVolume("v1", AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"})
	reader.SetVolume("v2", AuthorityBasis{Assigned: true, ReplicaID: "s1", Epoch: 7, EndpointVersion: 1, DataAddr: "e1", CtrlAddr: "f1"})

	v1Stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	v2Stale := AuthorityBasis{Assigned: true, ReplicaID: "s1", Epoch: 7, EndpointVersion: 1, DataAddr: "e1", CtrlAddr: "f1"}
	snap := ClusterSnapshot{
		CollectedRevision: 1,
		Servers: []ServerObservation{
			{ServerID: "sA", Reachable: true, Eligible: true},
			{ServerID: "sB", Reachable: true, Eligible: true},
			{ServerID: "sC", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{
			{
				VolumeID:  "v1",
				Authority: v1Stale,
				Slots: []ReplicaCandidate{
					{ReplicaID: "r1", ServerID: "sA", DataAddr: "d1", CtrlAddr: "c1", Reachable: false, ReadyForPrimary: false, Eligible: true},
					{ReplicaID: "r2", ServerID: "sB", DataAddr: "d2", CtrlAddr: "c2", Reachable: true, ReadyForPrimary: true, Eligible: true},
					{ReplicaID: "r3", ServerID: "sC", DataAddr: "d3", CtrlAddr: "c3", Reachable: true, ReadyForPrimary: true, Eligible: true},
				},
			},
			{
				VolumeID:  "v2",
				Authority: v2Stale,
				Slots: []ReplicaCandidate{
					{ReplicaID: "s1", ServerID: "sA", DataAddr: "e1", CtrlAddr: "f1", Reachable: false, ReadyForPrimary: false, Eligible: true},
					{ReplicaID: "s2", ServerID: "sB", DataAddr: "e2", CtrlAddr: "f2", Reachable: true, ReadyForPrimary: true, Eligible: true},
					{ReplicaID: "s3", ServerID: "sC", DataAddr: "e3", CtrlAddr: "f3", Reachable: true, ReadyForPrimary: true, Eligible: true},
				},
			},
		},
	}
	if err := ctrl.SubmitObservedState(snap, SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)
	_ = drainOneAsk(t, ctrl)

	// Advance past retry window. v2 confirms (publisher+observation
	// catch up to s2@8). v1 only has publisher advanced; observation
	// still stale.
	clk.Advance(3 * time.Second)
	reader.SetVolume("v1", AuthorityBasis{Assigned: true, ReplicaID: "r2", Epoch: 6, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2"})
	reader.SetVolume("v2", AuthorityBasis{Assigned: true, ReplicaID: "s2", Epoch: 8, EndpointVersion: 1, DataAddr: "e2", CtrlAddr: "f2"})

	snap.CollectedRevision = 2
	// v1 observation still stale; v2 observation updated to s2@8 and healthy.
	snap.Volumes[1].Authority = AuthorityBasis{Assigned: true, ReplicaID: "s2", Epoch: 8, EndpointVersion: 1, DataAddr: "e2", CtrlAddr: "f2"}
	snap.Volumes[1].Slots[0].Reachable = true
	snap.Volumes[1].Slots[0].ReadyForPrimary = true

	if err := ctrl.SubmitObservedState(snap, SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}

	// v1 must be Stuck with desired still Active.
	if _, ok := ctrl.LastConvergenceStuck("v1"); !ok {
		t.Fatal("v1 expected Stuck")
	}
	if _, ok := ctrl.DesiredFor("v1"); !ok {
		t.Fatal("v1 desired must remain Active while Stuck")
	}
	// v2 must be Confirmed (desired cleared, no stuck evidence).
	if _, ok := ctrl.DesiredFor("v2"); ok {
		t.Fatal("v2 desired must be cleared on confirmation")
	}
	if _, ok := ctrl.LastConvergenceStuck("v2"); ok {
		t.Fatal("v2 stuck evidence must be absent (never stuck, confirmed)")
	}
}

// Row 12: retry state is per-volume. Two volumes with different
// DesiredAt timestamps reach Stuck at different wall-clock times.
func TestConvergence_RetryStateIsPerVolume(t *testing.T) {
	reader := newFakeReaderForHost()
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
		RetryWindow:            2 * time.Second,
	}, reader)
	clk := newFakeClock(time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC))
	ctrl.SetNowForTest(clk.Now)

	// Submit v1 first.
	reader.SetVolume("v1", AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"})
	v1Stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(v1Stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit v1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)

	// Advance 1.5s. v1 still within window.
	clk.Advance(1500 * time.Millisecond)

	// Now propose a v2 failover. v2's DesiredAt is 1.5s later
	// than v1's.
	reader.SetVolume("v2", AuthorityBasis{Assigned: true, ReplicaID: "s1", Epoch: 7, EndpointVersion: 1, DataAddr: "e1", CtrlAddr: "f1"})
	v2Stale := AuthorityBasis{Assigned: true, ReplicaID: "s1", Epoch: 7, EndpointVersion: 1, DataAddr: "e1", CtrlAddr: "f1"}
	snap2 := ClusterSnapshot{
		CollectedRevision: 2,
		Servers: []ServerObservation{
			{ServerID: "sA", Reachable: true, Eligible: true},
			{ServerID: "sB", Reachable: true, Eligible: true},
			{ServerID: "sC", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{{
			VolumeID:  "v2",
			Authority: v2Stale,
			Slots: []ReplicaCandidate{
				{ReplicaID: "s1", ServerID: "sA", DataAddr: "e1", CtrlAddr: "f1", Reachable: false, ReadyForPrimary: false, Eligible: true},
				{ReplicaID: "s2", ServerID: "sB", DataAddr: "e2", CtrlAddr: "f2", Reachable: true, ReadyForPrimary: true, Eligible: true},
				{ReplicaID: "s3", ServerID: "sC", DataAddr: "e3", CtrlAddr: "f3", Reachable: true, ReadyForPrimary: true, Eligible: true},
			},
		}},
	}
	if err := ctrl.SubmitObservedState(snap2, SupportabilityReport{}); err != nil {
		t.Fatalf("submit v2: %v", err)
	}
	_ = drainOneAsk(t, ctrl)

	// Advance +1s → total 2.5s since v1 DesiredAt, 1s since v2
	// DesiredAt. v1 Stuck, v2 NOT Stuck.
	clk.Advance(1 * time.Second)
	if err := ctrl.SubmitObservedState(failoverSnapForV1(v1Stale, 3), SupportabilityReport{}); err != nil {
		t.Fatalf("re-submit v1: %v", err)
	}
	if err := ctrl.SubmitObservedState(snap2, SupportabilityReport{}); err != nil {
		t.Fatalf("re-submit v2: %v", err)
	}
	if _, ok := ctrl.LastConvergenceStuck("v1"); !ok {
		t.Fatal("v1 expected Stuck (2.5s > 2s window)")
	}
	if _, ok := ctrl.LastConvergenceStuck("v2"); ok {
		t.Fatal("v2 must NOT be Stuck (1s < 2s window) — per-volume clock")
	}
}

// Row 1b: late confirmation after Stuck must clear BOTH the
// desired entry AND the stuck evidence. Without this, a volume
// that timed out into Stuck and later genuinely converged would
// keep reporting as stuck on LastConvergenceStuck() even though
// convergence had completed — a bounded-fate surface regression.
func TestConvergence_StuckThenConfirm_ClearsStuckEvidence(t *testing.T) {
	ctrl, reader, clk := convTestCtrl(t, 1*time.Second)

	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	_ = drainOneAsk(t, ctrl)

	// Advance past the retry window to force Stuck.
	clk.Advance(2 * time.Second)
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 2), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 2: %v", err)
	}
	if _, ok := ctrl.LastConvergenceStuck("v1"); !ok {
		t.Fatal("precondition: expected Stuck after retry window")
	}

	// Publisher applies, observation catches up — late
	// confirmation.
	applied := AuthorityBasis{Assigned: true, ReplicaID: "r2", Epoch: 6, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2"}
	reader.SetVolume("v1", applied)
	if err := ctrl.SubmitObservedState(healthySnapForV1(applied, 3), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 3: %v", err)
	}
	if _, ok := ctrl.DesiredFor("v1"); ok {
		t.Fatal("desired must clear on late confirmation")
	}
	if ev, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatalf("stuck evidence must be cleared after late confirmation, got %+v", ev)
	}
}

// Supported-recovery regression: once a volume re-enters the
// supported set, any prior UnsupportedEvidence from an earlier
// build must drop. Without this, LastUnsupported() keeps
// reporting a stale fault after the volume is healthy.
func TestConvergence_SupportedRecovery_ClearsPriorUnsupportedEvidence(t *testing.T) {
	ctrl, reader, _ := convTestCtrl(t, 5*time.Second)

	// First build: v1 goes Unsupported.
	if err := ctrl.SubmitObservedState(ClusterSnapshot{CollectedRevision: 1},
		SupportabilityReport{
			Unsupported: map[string]VolumeUnsupportedEvidence{"v1": {
				VolumeID:         "v1",
				SnapshotRevision: 1,
				EvaluatedAt:      time.Now(),
				Reasons:          []string{ReasonConflictingPrimaryClaim},
			}},
		}); err != nil {
		t.Fatalf("submit unsupported: %v", err)
	}
	if _, ok := ctrl.LastUnsupported("v1"); !ok {
		t.Fatal("precondition: unsupported evidence must be recorded")
	}

	// Second build: v1 is now supported (healthy topology,
	// publisher currently holds r1@5).
	reader.SetVolume("v1", AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1",
	})
	healthy := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(healthySnapForV1(healthy, 2), SupportabilityReport{}); err != nil {
		t.Fatalf("submit supported: %v", err)
	}
	if ev, ok := ctrl.LastUnsupported("v1"); ok {
		t.Fatalf("unsupported evidence must clear on supported recovery, got %+v", ev)
	}
}

// Row 13: IntentReassign is NEVER called twice for the same
// outstanding desired. Even across many submits past the retry
// window, only the first enqueue is observed in the queue.
// This is the direct guard against authority-churn.
func TestConvergence_Reassign_CalledAtMostOncePerDesired(t *testing.T) {
	ctrl, reader, clk := convTestCtrl(t, 1*time.Second)

	reader.SetVolume("v1", AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"})
	stale := AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 5, EndpointVersion: 1, DataAddr: "d1", CtrlAddr: "c1"}
	if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, 1), SupportabilityReport{}); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	ask := drainOneAsk(t, ctrl)
	if ask.Intent != IntentReassign || ask.ReplicaID != "r2" {
		t.Fatalf("expected Reassign r2, got %+v", ask)
	}

	// Hammer identical snapshots past the retry window.
	for i := 0; i < 10; i++ {
		clk.Advance(500 * time.Millisecond)
		if err := ctrl.SubmitObservedState(failoverSnapForV1(stale, uint64(2+i)), SupportabilityReport{}); err != nil {
			t.Fatalf("submit %d: %v", i+2, err)
		}
	}
	expectNoQueuedAsk(t, ctrl)
}
