package authority

import (
	"testing"
	"time"
)

// Primary boot grace (#128): a restarted current primary that is placeable
// but not ready yet keeps its authority for one bounded grace.

type bootGraceRig struct {
	t      *testing.T
	ctrl   *TopologyController
	reader *fakeBasisReader
	now    time.Time
	rev    uint64
	// reissued models the publisher having re-issued r1's line for the
	// process that reports it (the restart refresh already applied), so a
	// test of the grace sees only the grace. Off, the line stays with the
	// process it was first issued for (r1-a).
	reissued bool
	line     AuthorityBasis
}

const testBootGrace = 30 * time.Second

func newBootGraceRig(t *testing.T) *bootGraceRig {
	reader := newFakeBasisReader()
	line := AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
		DataAddr: "d1-old", CtrlAddr: "c1-old", Incarnation: "r1-a",
	}
	reader.Set("v1", "r1", line)
	g := &bootGraceRig{t: t, now: time.Unix(1000, 0), reader: reader, reissued: true, line: line}
	g.ctrl = NewTopologyController(TopologyControllerConfig{PrimaryBootGrace: testBootGrace}, reader)
	g.ctrl.SetNowForTest(func() time.Time { return g.now })
	return g
}

// r1 is the current primary; r2 and r3 are ready peers. A slot without
// an observation time is stamped with the rig's clock.
func (g *bootGraceRig) submit(r1 ReplicaCandidate) {
	g.t.Helper()
	g.rev++
	if r1.ObservedAt.IsZero() {
		r1.ObservedAt = g.now
	}
	if g.reissued && r1.Incarnation != "" && r1.Incarnation != g.line.Incarnation {
		g.line.Incarnation = r1.Incarnation
		g.reader.Set("v1", "r1", g.line)
	}
	r2 := candidate("r2", "s2", "d2", "c2", 20)
	r2.Incarnation = "r2-a"
	r3 := candidate("r3", "s3", "d3", "c3", 10)
	r3.Incarnation = "r3-a"
	snap := ClusterSnapshot{
		CollectedRevision: g.rev,
		Servers: []ServerObservation{
			{ServerID: "s1", Reachable: true, Eligible: true},
			{ServerID: "s2", Reachable: true, Eligible: true},
			{ServerID: "s3", Reachable: true, Eligible: true},
		},
		Volumes: []VolumeTopologySnapshot{clusterVolume("v1", g.line, r1, r2, r3)},
	}
	if err := g.ctrl.SubmitClusterSnapshot(snap); err != nil {
		g.t.Fatalf("SubmitClusterSnapshot: %v", err)
	}
}

func r1Slot(incarnation, dataAddr, ctrlAddr string, ready bool) ReplicaCandidate {
	c := candidate("r1", "s1", dataAddr, ctrlAddr, 30)
	c.Incarnation = incarnation
	c.ReadyForPrimary = ready
	return c
}

// The running primary, ready at its old endpoint: nothing to do.
func (g *bootGraceRig) steadyPrimary() {
	g.t.Helper()
	g.submit(r1Slot("r1-a", "d1-old", "c1-old", true))
	expectNoNext(g.t, g.ctrl)
	g.now = g.now.Add(time.Second) // later facts are strictly newer
}

func expectFailoverToR2(t *testing.T, ctrl *TopologyController, why string) {
	t.Helper()
	ask := nextAskOrFail(t, ctrl)
	if ask.Intent != IntentReassign || ask.ReplicaID != "r2" {
		t.Fatalf("%s: want failover to r2, got %+v", why, ask)
	}
}

// RED today: a restart with new addresses fails over at once. Within the
// grace the primary keeps its authority and only its endpoint refreshes.
func TestPrimaryBootGrace_RestartWithReadyPeerRefreshesEndpoint(t *testing.T) {
	g := newBootGraceRig(t)
	g.steadyPrimary()
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-new", "c1-new", false))
	ask := nextAskOrFail(t, g.ctrl)
	if ask.Intent != IntentRefreshEndpoint || ask.ReplicaID != "r1" || ask.DataAddr != "d1-new" || ask.CtrlAddr != "c1-new" {
		t.Fatalf("restarted primary inside its boot grace: want endpoint refresh on r1, got %+v", ask)
	}
}

// Same endpoint, not ready: no ask during the grace; still unready after
// it expires -> failover to the ready peer.
func TestPrimaryBootGrace_StaysUnreadyPastGraceFailsOver(t *testing.T) {
	g := newBootGraceRig(t)
	g.steadyPrimary()
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
	expectNoNext(t, g.ctrl)
	g.now = g.now.Add(testBootGrace - time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
	expectNoNext(t, g.ctrl)
	g.now = g.now.Add(2 * time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
	expectFailoverToR2(t, g.ctrl, "unready past the boot grace")
}

// An explicit unhealthy report gets no grace: immediate failover.
func TestPrimaryBootGrace_ExplicitUnhealthyFailsOverAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*ReplicaCandidate)
	}{
		{"ineligible", func(c *ReplicaCandidate) { c.Eligible = false }},
		{"unreachable", func(c *ReplicaCandidate) { c.Reachable = false }},
		{"withdrawn", func(c *ReplicaCandidate) { c.Withdrawn = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newBootGraceRig(t)
			g.steadyPrimary()
			s := r1Slot("r1-b", "d1-old", "c1-old", false)
			tc.mut(&s)
			g.submit(s)
			expectFailoverToR2(t, g.ctrl, tc.name+" restarted primary")
		})
	}
}

// Ready ends the grace for good: the same incarnation going unready
// again fails over at once.
func TestPrimaryBootGrace_ReadyEndsGrace(t *testing.T) {
	g := newBootGraceRig(t)
	g.steadyPrimary()
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
	expectNoNext(t, g.ctrl)
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", true))
	expectNoNext(t, g.ctrl)
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
	expectFailoverToR2(t, g.ctrl, "unready again after ready in the same incarnation")
}

// A flapping primary (3 process starts within the flap window) gets no
// grace.
func TestPrimaryBootGrace_FlappingPrimaryGetsNoGrace(t *testing.T) {
	g := newBootGraceRig(t)
	g.steadyPrimary() // start 1
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false)) // start 2: grace
	expectNoNext(t, g.ctrl)
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", true))
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-c", "d1-old", "c1-old", false)) // start 3: flapping
	expectFailoverToR2(t, g.ctrl, "third restart within the flap window")
}

// A primary that reports no incarnation (older binary) keeps today's
// rule: not ready with a ready peer fails over.
func TestPrimaryBootGrace_NoIncarnationNoGrace(t *testing.T) {
	g := newBootGraceRig(t)
	g.submit(r1Slot("", "d1-old", "c1-old", false))
	expectFailoverToR2(t, g.ctrl, "no incarnation")
}

// A delayed snapshot carrying the previous incarnation's older fact
// arrives after the restart: it must neither count a process start nor
// reset or end the restarted primary's grace (review of #130).
func TestPrimaryBootGrace_OutOfOrderSnapshotDoesNotTouchHistory(t *testing.T) {
	g := newBootGraceRig(t)
	t1 := g.now
	g.steadyPrimary() // A, observed at t1
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false)) // B restarted, observed at t1+1s
	expectNoNext(t, g.ctrl)

	delayedA := r1Slot("r1-a", "d1-old", "c1-old", true)
	delayedA.ObservedAt = t1 // older than B's fact
	g.now = g.now.Add(time.Second)
	g.submit(delayedA)
	expectNoNext(t, g.ctrl) // A is ready in this stale view: nothing to do

	for i := 0; i < 2; i++ {
		g.now = g.now.Add(time.Second)
		g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
		expectNoNext(t, g.ctrl) // B keeps its grace: no extra starts, not flapping
	}
	g.now = g.now.Add(testBootGrace)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
	expectFailoverToR2(t, g.ctrl, "B unready past its own grace")
}

// Two facts with the same observation time: the second never changes
// history (no new start, no ready latch).
func TestPrimaryBootGrace_EqualTimestampFactIgnored(t *testing.T) {
	g := newBootGraceRig(t)
	g.steadyPrimary()
	g.now = g.now.Add(time.Second)
	b := r1Slot("r1-b", "d1-old", "c1-old", false)
	g.submit(b)
	expectNoNext(t, g.ctrl)
	sameTimeReady := b
	sameTimeReady.ObservedAt = g.now
	sameTimeReady.ReadyForPrimary = true
	g.submit(sameTimeReady)
	expectNoNext(t, g.ctrl)
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
	expectNoNext(t, g.ctrl) // the equal-time ready fact did not end the grace
}

// A delayed UNREADY fact from the previous incarnation arrives while the
// restarted primary is in its grace: the decision follows the newest
// recorded fact, so it must not fail over (review of #130 r2).
func TestPrimaryBootGrace_DelayedUnreadyPreviousIncarnationKeepsGrace(t *testing.T) {
	g := newBootGraceRig(t)
	t1 := g.now
	g.steadyPrimary() // r1-a observed at t1
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false)) // r1-b, in grace
	expectNoNext(t, g.ctrl)

	delayedA := r1Slot("r1-a", "d1-old", "c1-old", false) // unready, older
	delayedA.ObservedAt = t1
	g.now = g.now.Add(time.Second)
	g.submit(delayedA)
	expectNoNext(t, g.ctrl)
}

// A fact with the same observation time as the recorded one but a
// different incarnation is superseded, never a decision input.
func TestPrimaryBootGrace_EqualTimeChangedIncarnationKeepsGrace(t *testing.T) {
	g := newBootGraceRig(t)
	g.steadyPrimary()
	g.now = g.now.Add(time.Second)
	b := r1Slot("r1-b", "d1-old", "c1-old", false)
	g.submit(b)
	expectNoNext(t, g.ctrl)

	other := r1Slot("r1-x", "d1-old", "c1-old", false)
	other.ObservedAt = g.now // same time as r1-b's fact
	g.submit(other)
	expectNoNext(t, g.ctrl)
}

// A snapshot carrying a superseded fact is a stale snapshot: no
// decision branch may use it (review of #130 r3). Here a delayed READY
// fact of the previous incarnation carries an obsolete endpoint; today
// it would drive an endpoint refresh to that endpoint.
func TestPrimaryBootGrace_DelayedReadyObsoleteEndpointDecidesNothing(t *testing.T) {
	g := newBootGraceRig(t)
	t1 := g.now
	g.steadyPrimary() // r1-a ready at d1-old (the line), observed at t1
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false)) // r1-b, in grace
	expectNoNext(t, g.ctrl)

	delayedA := r1Slot("r1-a", "d1-ancient", "c1-ancient", true)
	delayedA.ObservedAt = t1
	g.now = g.now.Add(time.Second)
	g.submit(delayedA)
	expectNoNext(t, g.ctrl)
}

// Newest B already reported ready (its grace ended); a delayed UNREADY
// fact of the previous incarnation must not cause a failover.
func TestPrimaryBootGrace_NewerReadyThenOlderUnreadyDecidesNothing(t *testing.T) {
	g := newBootGraceRig(t)
	t1 := g.now
	g.steadyPrimary()
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", true)) // r1-b ready
	expectNoNext(t, g.ctrl)

	delayedA := r1Slot("r1-a", "d1-old", "c1-old", false)
	delayedA.ObservedAt = t1
	g.now = g.now.Add(time.Second)
	g.submit(delayedA)
	expectNoNext(t, g.ctrl)

	// A fresh unready fact of B is still a valid decision input.
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", false))
	expectFailoverToR2(t, g.ctrl, "fresh unready B after its grace ended")
}
