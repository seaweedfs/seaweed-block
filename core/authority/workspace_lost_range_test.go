package authority

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A forced move records the abandoned range: the kept HEAD frontier
// (RemoteLSN) and the forced barrier (lease horizon plus skew) as the
// latest master time an old-holder write can have been acknowledged. The
// record is journaled: a reload returns the same bound.
//
// After a restart the conservative restart deadline, not the lease horizon,
// is the later term of the barrier.
func TestWorkspaceForcedMoveRecordsTheLostRange(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "running", true: "after restart"}[restart], func(t *testing.T) { forcedLostRange(t, restart) })
	}
}

func forcedLostRange(t *testing.T, restart bool) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	p := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, clock))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	if restart {
		now = now.Add(time.Second)
		p = NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, clock))
	}
	lease := p.WorkspaceState("v").Lease
	if restart && !p.leaseDeadlineLockedForTest("v").After(time.Unix(0, lease.ExpiresAtNS)) {
		t.Fatal("fixture: the restart deadline is not the later barrier term")
	}
	barrier := time.Unix(0, lease.ExpiresAtNS)
	if d := p.leaseDeadlineLockedForTest("v"); d.After(barrier) {
		barrier = d
	}
	barrier = barrier.Add(p.workspaceSkew(lease))
	req := WorkspaceMoveRequest{RequestID: "f", VolumeID: "v", ExpectedReplicaID: "old", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1,
		TargetReplicaID: "old", TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
	head := WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42}
	now = barrier.Add(-time.Nanosecond)
	if _, err := p.MoveWorkspace(ctx, req, head); err == nil {
		t.Fatal("forced move admitted before its barrier")
	}
	now = barrier.Add(5 * time.Second) // the bound is the barrier, not the time of the move
	r, err := p.MoveWorkspace(ctx, req, head)
	if err != nil {
		t.Fatal(err)
	}
	if r.Kind != "forced" || r.RemoteLSN != 42 || r.LostUntilNS != barrier.UnixNano() {
		t.Fatalf("forced record RemoteLSN=%d LostUntilNS=%d; want 42 and the barrier %d", r.RemoteLSN, r.LostUntilNS, barrier.UnixNano())
	}
	reloaded := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, clock)).WorkspaceState("v")
	if reloaded == nil || len(reloaded.Moves) != 1 || reloaded.Moves[0].LostUntilNS != barrier.UnixNano() || reloaded.Moves[0].RemoteLSN != 42 {
		t.Fatalf("reloaded record lost the range: %+v", reloaded)
	}
}

// A clean move abandons nothing: its record has no lost-range bound, and
// its persisted encoding is unchanged (no LostUntilNS key).
func TestWorkspaceCleanMoveRecordsNoLostRange(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	p := NewPublisher(nil, WithStore(disk))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.BeginWorkspaceRevocation(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "old", Epoch: 1, HeadGen: 7}); err != nil {
		t.Fatal(err)
	}
	req := WorkspaceMoveRequest{RequestID: "c", VolumeID: "v", ExpectedReplicaID: "old", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1,
		TargetReplicaID: "old", TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "clean"}
	r, err := p.MoveWorkspace(ctx, req, WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42, Detached: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.LostUntilNS != 0 {
		t.Fatalf("clean record LostUntilNS=%d; want none", r.LostUntilNS)
	}
	raw, err := json.Marshal(NewPublisher(nil, WithStore(disk)).WorkspaceState("v").Moves[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "LostUntilNS") {
		t.Fatalf("clean record encoding changed: %s", raw)
	}
}
