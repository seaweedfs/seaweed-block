package authority

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// A restarted primary process (a new incarnation at the same addresses)
// is re-issued its line at a new endpoint version: every session the new
// process opens then outranks the dead process's on each replica. The same
// process again changes nothing.
func TestPublisher_RestartedPrimaryGetsANewEndpointVersion(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	bind := AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind, Incarnation: "p1"}
	if err := pub.apply(bind); err != nil {
		t.Fatal(err)
	}
	if got, _ := pub.LastPublished("v1", "r1"); got.Incarnation != "p1" || got.EndpointVersion != 1 {
		t.Fatalf("bind: %+v; want incarnation p1 at EV 1", got)
	}
	refresh := AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentRefreshEndpoint}
	for _, c := range []struct {
		incarnation string
		ev          uint64
		line        string
		why         string
	}{
		{"p1", 1, "p1", "the same process at the same addresses"},
		{"", 1, "p1", "a refresh that names no process"},
		{"p2", 2, "p2", "a restarted process at the same addresses"},
		{"p2", 2, "p2", "the restarted process again"},
	} {
		refresh.Incarnation = c.incarnation
		if err := pub.apply(refresh); err != nil {
			t.Fatal(err)
		}
		got, _ := pub.LastPublished("v1", "r1")
		if got.Epoch != 1 || got.EndpointVersion != c.ev || got.Incarnation != c.line {
			t.Fatalf("%s: %+v; want epoch 1, EV %d, issued for %s", c.why, got, c.ev, c.line)
		}
	}
}

// The line remembers the process it was issued for across a master restart
// (the durable record), so a restarted master neither re-issues the line of
// a primary that did not restart nor misses one that restarted while the
// master was down.
func TestDurableAuthority_ReloadKeepsTheLinesIncarnation(t *testing.T) {
	dir := t.TempDir()
	storeA, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	pubA := NewPublisher(NewStaticDirective(nil), WithStore(storeA))
	if err := pubA.apply(AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind, Incarnation: "p1"}); err != nil {
		t.Fatal(err)
	}
	_ = storeA.Close()
	storeB, err := NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer storeB.Close()
	pubB := NewPublisher(NewStaticDirective(nil), WithStore(storeB))
	line, ok := pubB.VolumeAuthorityLine("v1")
	if !ok || line.Incarnation != "p1" || line.EndpointVersion != 1 {
		t.Fatalf("reloaded line %+v; want incarnation p1 at EV 1", line)
	}
	same := AssignmentAsk{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentRefreshEndpoint, Incarnation: "p1"}
	if err := pubB.apply(same); err != nil {
		t.Fatal(err)
	}
	if got, _ := pubB.LastPublished("v1", "r1"); got.EndpointVersion != 1 {
		t.Fatalf("after a master restart the same process was re-issued: EV %d", got.EndpointVersion)
	}
	restarted := same
	restarted.Incarnation = "p2" // the primary restarted while the master was down
	if err := pubB.apply(restarted); err != nil {
		t.Fatal(err)
	}
	if got, _ := pubB.LastPublished("v1", "r1"); got.EndpointVersion != 2 || got.Incarnation != "p2" {
		t.Fatalf("a primary restarted during the master's downtime: %+v; want EV 2 for p2", got)
	}
}

// The controller asks to re-issue the current primary's line when the
// process reporting it is not the one the line was issued for, ready or
// inside its boot grace; the running process asks nothing.
func TestTopologyController_RestartedPrimaryAtTheSameAddressIsReissued(t *testing.T) {
	for _, ready := range []bool{true, false} {
		g := newBootGraceRig(t)
		g.reissued = false
		g.steadyPrimary() // r1-a, the line's process: no ask
		g.submit(r1Slot("r1-b", "d1-old", "c1-old", ready))
		ask := nextAskOrFail(t, g.ctrl)
		if ask.Intent != IntentRefreshEndpoint || ask.ReplicaID != "r1" || ask.Incarnation != "r1-b" || ask.DataAddr != "d1-old" {
			t.Fatalf("restarted primary (ready=%v) at the same address: %+v; want a refresh of r1 for r1-b", ready, ask)
		}
	}
}

// Cell: a late report of the dead process, after the line was re-issued
// for the new one, re-issues nothing: exactly one new endpoint version.
func TestTopologyController_LateReportOfTheDeadProcessReissuesNothing(t *testing.T) {
	g := newBootGraceRig(t)
	g.reissued = false
	g.steadyPrimary()
	restartedAt := g.now
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", true))
	if ask := nextAskOrFail(t, g.ctrl); ask.Incarnation != "r1-b" {
		t.Fatalf("restart: %+v", ask)
	}
	g.line.Incarnation, g.line.EndpointVersion = "r1-b", 2 // the publisher re-issued it
	g.reader.Set("v1", "r1", g.line)
	g.now = g.now.Add(time.Second)
	g.submit(r1Slot("r1-b", "d1-old", "c1-old", true))
	expectNoNext(t, g.ctrl)
	late := r1Slot("r1-a", "d1-old", "c1-old", true)
	late.ObservedAt = restartedAt.Add(-time.Millisecond) // sent before the restart, delivered after
	g.now = g.now.Add(time.Second)
	g.submit(late)
	expectNoNext(t, g.ctrl)
}

// Cell: a failover records the incarnation the decision saw for its
// target. A target that restarts between the decision and its first report
// as primary is re-issued once (a new endpoint version), never promoted
// again.
func TestTopologyController_TargetRestartedAfterThePromotionIsReissuedNotPromotedAgain(t *testing.T) {
	g := newBootGraceRig(t)
	g.steadyPrimary()
	g.now = g.now.Add(testBootGrace + time.Second)
	dead := r1Slot("r1-a", "d1-old", "c1-old", false)
	dead.Reachable, dead.ReadyForPrimary = false, false
	g.submit(dead)
	ask := nextAskOrFail(t, g.ctrl)
	if ask.Intent != IntentReassign || ask.ReplicaID != "r2" || ask.Incarnation != "r2-a" {
		t.Fatalf("failover: %+v; want a reassign to r2 recording r2-a", ask)
	}
	// Published: r2 holds the line, issued for r2-a. r2 restarts as r2-b
	// before it reports as primary.
	line := AuthorityBasis{Assigned: true, ReplicaID: "r2", Epoch: 2, EndpointVersion: 1, DataAddr: "d2", CtrlAddr: "c2", Incarnation: "r2-a"}
	reader := newFakeBasisReader()
	reader.Set("v1", "r2", line)
	ctrl := NewTopologyController(TopologyControllerConfig{PrimaryBootGrace: testBootGrace}, reader)
	r2 := candidate("r2", "s2", "d2", "c2", 20)
	r2.Incarnation, r2.ObservedAt = "r2-b", g.now
	r3 := candidate("r3", "s3", "d3", "c3", 10)
	r3.Incarnation, r3.ObservedAt = "r3-a", g.now
	snap := ClusterSnapshot{CollectedRevision: 1,
		Servers: []ServerObservation{{ServerID: "s1", Reachable: false}, {ServerID: "s2", Reachable: true, Eligible: true}, {ServerID: "s3", Reachable: true, Eligible: true}},
		Volumes: []VolumeTopologySnapshot{clusterVolume("v1", line, dead, r2, r3)}}
	if err := ctrl.SubmitClusterSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	ask = nextAskOrFail(t, ctrl)
	if ask.Intent != IntentRefreshEndpoint || ask.ReplicaID != "r2" || ask.Incarnation != "r2-b" {
		t.Fatalf("the promoted target restarted: %+v; want a refresh of r2 for r2-b, not another promotion", ask)
	}
}

// Cell (LOCAL mode): re-issuing a workspace volume's line for a restarted
// primary moves only its endpoint version. The workspace record (moves,
// lease, revocation) is the one the line already had, and the holder keeps
// renewing at its epoch: neither the lease nor the layout's HEAD and claim
// (which name the epoch, never the endpoint version) are touched.
func TestWorkspaceRestartReissueTouchesOnlyTheEndpointVersion(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	now := time.Unix(1000, 0)
	p := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return now }))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind, Incarnation: "p1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	before := cloneWorkspace(p.workspace["v"])
	p.mu.Unlock()
	if before == nil {
		t.Fatal("fixture: the volume is not enrolled")
	}
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentRefreshEndpoint, Incarnation: "p2"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.LastPublished("v", "r"); got.Epoch != 1 || got.EndpointVersion != 2 || got.Incarnation != "p2" {
		t.Fatalf("re-issued workspace line %+v; want epoch 1, EV 2, p2", got)
	}
	p.mu.Lock()
	after := cloneWorkspace(p.workspace["v"])
	p.mu.Unlock()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("the re-issue changed the workspace record:\nbefore %+v\nafter  %+v", before, after)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatalf("the holder no longer renews at its epoch: %v", err)
	}
}
