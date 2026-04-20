package authority

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// ============================================================
// P14 S6 — L1 Component Route Tests
//
// These tests exercise the full observation-fed convergence
// pipeline end-to-end:
//
//   ObservationHost → TopologyController → Publisher
//                                       → VolumeBridge → Consumer
//
// L0 unit tests in convergence_test.go cover the controller's
// lifecycle / retry / supersede / clearing rules in isolation
// with direct SubmitObservedState calls. These L1 tests verify
// that the SAME rules hold when driven through the real
// observation host and real publisher mint path — i.e. that
// confirmation clearing, Stuck evidence, and supersede dropping
// survive the routing layer.
//
// Architect requested three routes (plus an optional fourth):
//
//   1. Observation-fed failover is confirmed end-to-end and
//      clears desired.
//   2. Publish-but-not-observed: publisher advances, observation
//      stays stale → desired Active, RetryWindow expiry produces
//      ConvergenceStuckEvidence, no duplicate Reassign emitted.
//   3. Supersede: publisher advances to a replica other than
//      desired target → old desired cleared, no stale ask
//      published.
// ============================================================

// ============================================================
// Shared helpers for L1 route tests
// ============================================================

// snapshottingReader wraps an AuthorityBasisReader but can be
// frozen at a point in time. While frozen it returns the view
// captured at Freeze() time; while thawed it delegates to the
// underlying reader (live publisher state).
//
// Used by the publish-but-not-observed test to simulate an
// observation layer that has not yet caught up to the publisher.
// The controller still holds the live publisher as its own
// reader — only the observation-host builder is stalled, which
// is the production-realistic shape.
type snapshottingReader struct {
	underlying AuthorityBasisReader

	mu         sync.Mutex
	frozen     bool
	perVol     map[string]AuthorityBasis
	perSlot    map[string]AuthorityBasis
}

func newSnapshottingReader(u AuthorityBasisReader) *snapshottingReader {
	return &snapshottingReader{
		underlying: u,
		perVol:     map[string]AuthorityBasis{},
		perSlot:    map[string]AuthorityBasis{},
	}
}

func (s *snapshottingReader) Freeze(volumeIDs ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frozen = true
	for _, vid := range volumeIDs {
		if b, ok := s.underlying.VolumeAuthorityLine(vid); ok {
			s.perVol[vid] = b
			s.perSlot[vid+"/"+b.ReplicaID] = b
		}
	}
}

func (s *snapshottingReader) Thaw() {
	s.mu.Lock()
	s.frozen = false
	s.perVol = map[string]AuthorityBasis{}
	s.perSlot = map[string]AuthorityBasis{}
	s.mu.Unlock()
}

func (s *snapshottingReader) LastAuthorityBasis(volumeID, replicaID string) (AuthorityBasis, bool) {
	s.mu.Lock()
	if s.frozen {
		b, ok := s.perSlot[volumeID+"/"+replicaID]
		s.mu.Unlock()
		return b, ok
	}
	s.mu.Unlock()
	return s.underlying.LastAuthorityBasis(volumeID, replicaID)
}

func (s *snapshottingReader) VolumeAuthorityLine(volumeID string) (AuthorityBasis, bool) {
	s.mu.Lock()
	if s.frozen {
		b, ok := s.perVol[volumeID]
		s.mu.Unlock()
		return b, ok
	}
	s.mu.Unlock()
	return s.underlying.VolumeAuthorityLine(volumeID)
}

// buildV1Heartbeats returns the three-server heartbeat set for
// a volume "v1" with r1/r2/r3 slots. The caller picks which
// slot(s) are unreachable via the unreachable map.
func buildV1Heartbeats(at time.Time, unreachable map[string]bool) []HeartbeatMessage {
	replicas := []struct{ server, replica string }{
		{"s1", "r1"},
		{"s2", "r2"},
		{"s3", "r3"},
	}
	out := make([]HeartbeatMessage, 0, len(replicas))
	for _, r := range replicas {
		slot := healthySlot("v1", r.replica)
		if unreachable[r.replica] {
			slot.Reachable = false
			slot.ReadyForPrimary = false
		}
		out = append(out, serverObs(r.server, at, slot))
	}
	return out
}

// waitForConsumerMatch polls consumer until pred returns true on
// the most recent delivery, or deadlines out.
func waitForConsumerMatch(t *testing.T, cons *recordingConsumer, pred func(adapter.AssignmentInfo) bool) adapter.AssignmentInfo {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		seen := cons.snapshot()
		if len(seen) > 0 && pred(seen[len(seen)-1]) {
			return seen[len(seen)-1]
		}
		time.Sleep(10 * time.Millisecond)
	}
	seen := cons.snapshot()
	t.Fatalf("consumer did not see matching assignment within deadline, last=%+v", seen)
	return adapter.AssignmentInfo{}
}

// waitForDesiredCleared polls until ctrl.DesiredFor(vid) is
// empty, or deadlines out.
func waitForDesiredCleared(t *testing.T, ctrl *TopologyController, vid string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := ctrl.DesiredFor(vid); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	d, _ := ctrl.DesiredFor(vid)
	t.Fatalf("desired for %s not cleared within deadline, got %+v", vid, d)
}

// ============================================================
// Route 1: ObservationHost → Controller → Publisher →
// VolumeBridge → Consumer. Happy path: failover confirms and
// desired clears.
// ============================================================

func TestConvergenceRoute_ObservationFedFailover_ConfirmsAndClearsDesired(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)

	holder := &basisReaderHolder{}
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
		RebalanceSkew:          100, // keep rebalance out of the picture
		RetryWindow:            5 * time.Second,
	}, holder)
	ctrl.SetNowForTest(clk.Now)

	pub := NewPublisher(ctrl)
	holder.set(pub)

	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 30 * time.Second, PendingGrace: 10 * time.Millisecond},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Sink:      ctrl,
		Reader:    holder,
		Now:       clk.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons := &recordingConsumer{}
	go pub.Run(ctx)
	go VolumeBridge(ctx, pub, cons, "v1", "r1", "r2", "r3")
	host.Start(ctx)
	defer host.Stop()

	// Phase 1: all three slots healthy. Controller must bind v1
	// to r1 (tiebreak by replica ID), publisher mints r1@1, bridge
	// delivers to consumer.
	for _, hb := range buildV1Heartbeats(start, nil) {
		if err := host.IngestHeartbeat(hb); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	r1Line := waitForConsumerMatch(t, cons, func(info adapter.AssignmentInfo) bool {
		return info.ReplicaID == "r1" && info.Epoch == 1
	})
	// Note: Bind(r1) desired does NOT clear between phases.
	// Confirmation requires a subsequent host rebuild (next
	// ingest) to observe Authority=r1@1 and run the confirmation
	// contract. Phase 2's first ingest will both confirm Bind
	// AND emit Reassign(r2) in the same SubmitObservedState call
	// — that's the correct S6 semantic.

	// Phase 2: r1 becomes unreachable. Controller decides failover
	// to r2, publisher mints r2@2 (per-volume monotonic), bridge
	// delivers. Desired remains Active until observation catches up.
	clk.Advance(1 * time.Second)
	for _, hb := range buildV1Heartbeats(clk.Now(), map[string]bool{"r1": true}) {
		if err := host.IngestHeartbeat(hb); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	_ = waitForConsumerMatch(t, cons, func(info adapter.AssignmentInfo) bool {
		return info.ReplicaID == "r2" && info.Epoch == r1Line.Epoch+1
	})

	// Phase 3: observation catches up — next heartbeat with r2
	// healthy AND publisher now at r2@2, so the next rebuild
	// stamps Authority=r2@2 on the supported snapshot. The
	// controller's confirmation contract matches and clears
	// desired.
	clk.Advance(1 * time.Second)
	for _, hb := range buildV1Heartbeats(clk.Now(), map[string]bool{"r1": true}) {
		if err := host.IngestHeartbeat(hb); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}
	waitForDesiredCleared(t, ctrl, "v1")
	if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatal("stuck evidence must not be present after confirmation")
	}
}

// ============================================================
// Route 2: publish-but-not-observed. Publisher advances to the
// new line (consumer sees it), but observation stays frozen on
// the old line. Desired must remain Active; RetryWindow expiry
// must produce ConvergenceStuckEvidence; no duplicate Reassign
// is ever emitted.
// ============================================================

func TestConvergenceRoute_PublishNotObserved_StuckWithoutChurn(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)

	holder := &basisReaderHolder{}
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
		RebalanceSkew:          100,
		RetryWindow:            2 * time.Second,
	}, holder)
	ctrl.SetNowForTest(clk.Now)

	pub := NewPublisher(ctrl)
	holder.set(pub)

	// Host's builder reads Authority through a snapshotting
	// reader. When frozen, it reports the captured pre-failover
	// line even though the underlying publisher has already
	// advanced.
	hostReader := newSnapshottingReader(holder)
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 30 * time.Second, PendingGrace: 10 * time.Millisecond},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Sink:      ctrl,
		Reader:    hostReader,
		Now:       clk.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons := &recordingConsumer{}
	go pub.Run(ctx)
	go VolumeBridge(ctx, pub, cons, "v1", "r1", "r2", "r3")
	host.Start(ctx)
	defer host.Stop()

	// Phase 1: initial bind to r1.
	for _, hb := range buildV1Heartbeats(start, nil) {
		_ = host.IngestHeartbeat(hb)
	}
	_ = waitForConsumerMatch(t, cons, func(info adapter.AssignmentInfo) bool {
		return info.ReplicaID == "r1" && info.Epoch == 1
	})

	// Freeze host.Reader view at r1@1 BEFORE any further phase.
	// Every subsequent rebuild will stamp supported snapshots with
	// Authority=r1@1 regardless of what the publisher actually
	// holds. The controller still uses the live publisher as its
	// own reader, so its currentLine reflects reality — only the
	// observation-fed Authority stays stale, which is the
	// production-realistic "observation layer lags publisher" shape.
	hostReader.Freeze("v1")

	// Phase 2: r1 unreachable → failover to r2. Publisher mints
	// r2@2, consumer sees it.
	clk.Advance(500 * time.Millisecond)
	for _, hb := range buildV1Heartbeats(clk.Now(), map[string]bool{"r1": true}) {
		_ = host.IngestHeartbeat(hb)
	}
	_ = waitForConsumerMatch(t, cons, func(info adapter.AssignmentInfo) bool {
		return info.ReplicaID == "r2" && info.Epoch == 2
	})

	// Record the current count of consumer deliveries; no new
	// delivery should appear while we wait for Stuck.
	baseline := len(cons.snapshot())

	// Phase 3: drive ingests past RetryWindow while observation
	// stays frozen. Stuck must fire; no duplicate Reassign may be
	// emitted (the bridge would deliver one if it were).
	for i := 0; i < 6; i++ {
		clk.Advance(500 * time.Millisecond)
		for _, hb := range buildV1Heartbeats(clk.Now(), map[string]bool{"r1": true}) {
			_ = host.IngestHeartbeat(hb)
		}
	}

	// Poll for Stuck. Desired must still be present (bounded
	// wait + evidence, not re-drive).
	deadline := time.Now().Add(2 * time.Second)
	stuck := false
	for time.Now().Before(deadline) {
		if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
			stuck = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !stuck {
		t.Fatal("expected ConvergenceStuckEvidence after RetryWindow with frozen observation")
	}
	if _, ok := ctrl.DesiredFor("v1"); !ok {
		t.Fatal("desired must REMAIN Active while Stuck — passive retry only records evidence")
	}

	// No new consumer delivery since baseline. If the controller
	// had actively re-emitted, publisher would have minted a new
	// epoch and bridge would have delivered it.
	if delta := len(cons.snapshot()) - baseline; delta != 0 {
		t.Fatalf("consumer saw %d additional deliveries during Stuck — indicates re-emission churn", delta)
	}
}

// ============================================================
// Route 3: supersede. Controller has a desired Reassign(r2)
// outstanding. Publisher is independently advanced to r3 (as a
// second authority author would) past ProposedBasis. On the next
// observation, supersede must drop the old desired; no stale
// Reassign(r2) may leak to the consumer afterward.
// ============================================================

func TestConvergenceRoute_Supersede_PublisherMovedElsewhere_DropsStaleDesired(t *testing.T) {
	start := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	clk := newFakeClock(start)

	holder := &basisReaderHolder{}
	ctrl := NewTopologyController(TopologyControllerConfig{
		ExpectedSlotsPerVolume: 3,
		RebalanceSkew:          100,
		RetryWindow:            30 * time.Second, // keep Stuck out of this test
	}, holder)
	ctrl.SetNowForTest(clk.Now)

	pub := NewPublisher(ctrl)
	holder.set(pub)

	hostReader := newSnapshottingReader(holder)
	host := NewObservationHost(ObservationHostConfig{
		Freshness: FreshnessConfig{FreshnessWindow: 30 * time.Second, PendingGrace: 10 * time.Millisecond},
		Topology:  AcceptedTopology{Volumes: []VolumeExpected{threeSlotVolume("v1")}},
		Sink:      ctrl,
		Reader:    hostReader,
		Now:       clk.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons := &recordingConsumer{}
	go pub.Run(ctx)
	go VolumeBridge(ctx, pub, cons, "v1", "r1", "r2", "r3")
	host.Start(ctx)
	defer host.Stop()

	// Phase 1: initial bind to r1.
	for _, hb := range buildV1Heartbeats(start, nil) {
		_ = host.IngestHeartbeat(hb)
	}
	_ = waitForConsumerMatch(t, cons, func(info adapter.AssignmentInfo) bool {
		return info.ReplicaID == "r1" && info.Epoch == 1
	})

	// Freeze host reader at r1@1 — same rationale as the publish-
	// but-not-observed route.
	hostReader.Freeze("v1")

	// Phase 2: r1 unreachable → controller emits Reassign(r2).
	// Publisher mints r2@2, consumer sees it. ProposedBasis
	// captured by the controller is r1@1.
	clk.Advance(500 * time.Millisecond)
	for _, hb := range buildV1Heartbeats(clk.Now(), map[string]bool{"r1": true}) {
		_ = host.IngestHeartbeat(hb)
	}
	_ = waitForConsumerMatch(t, cons, func(info adapter.AssignmentInfo) bool {
		return info.ReplicaID == "r2" && info.Epoch == 2
	})

	// Wait for desired to reflect the outstanding Reassign(r2).
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if d, ok := ctrl.DesiredFor("v1"); ok && d.Ask.ReplicaID == "r2" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if d, ok := ctrl.DesiredFor("v1"); !ok || d.Ask.ReplicaID != "r2" {
		t.Fatalf("precondition: desired must be Reassign(r2), got ok=%v d=%+v", ok, d)
	}

	// Phase 3: another authority author advances the publisher to
	// r3@3 (simulated via pub.apply — the publisher is still the
	// sole minter; we're simulating a second in-process decision
	// path writing through the same publisher, which is the
	// supersede-relevant shape).
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r3", DataAddr: "data-r3", CtrlAddr: "ctrl-r3",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("pub.apply r3 Reassign: %v", err)
	}
	_ = waitForConsumerMatch(t, cons, func(info adapter.AssignmentInfo) bool {
		return info.ReplicaID == "r3" && info.Epoch == 3
	})

	// Record consumer count — no further Reassign(r2) may appear
	// after supersede.
	consBefore := len(cons.snapshot())

	// Phase 4: next ingest → host rebuild. Controller's reader
	// (the live publisher) now returns r3@3, which is strictly
	// newer than ProposedBasis (r1@1) and targets a different
	// replica than the desired target r2 → supersede fires. The
	// host's frozen view still reports r2@2 as Authority so the
	// stale-basis path doesn't short-circuit supersede (supersede
	// is evaluated BEFORE basisMatchesLine per §9 / §7.2).
	clk.Advance(500 * time.Millisecond)
	for _, hb := range buildV1Heartbeats(clk.Now(), map[string]bool{"r1": true}) {
		_ = host.IngestHeartbeat(hb)
	}
	waitForDesiredCleared(t, ctrl, "v1")

	// Sanity: stuck evidence was never recorded (RetryWindow is
	// long) AND no stale Reassign(r2) leaked to the consumer
	// after supersede.
	if _, ok := ctrl.LastConvergenceStuck("v1"); ok {
		t.Fatal("stuck evidence must not be present — supersede cleared desired before window elapsed")
	}
	// Give the publisher/bridge a moment to deliver anything
	// queued before we read the final consumer count.
	time.Sleep(60 * time.Millisecond)
	for _, info := range cons.snapshot()[consBefore:] {
		if info.ReplicaID == "r2" {
			t.Fatalf("stale Reassign(r2) leaked to consumer after supersede: %+v", info)
		}
	}
}
