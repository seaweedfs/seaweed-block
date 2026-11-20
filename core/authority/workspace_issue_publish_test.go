package authority

import (
	"context"
	"testing"
	"time"
)

// A renewal of the new epoch that lands right after an issued move is
// published (the seam runs under the publisher lock, where only the leaf
// renewal path can run) raises the deadline; the issue must not lower it
// afterwards, or a later renewal inside that outstanding grant is refused.
func TestWorkspaceIssuePublishNeverLowersARenewedDeadline(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	now := time.Unix(1000, 0)
	p := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(grantTTL, func() time.Time { return now }))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: predecessorHead.Gen}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(100 * time.Millisecond)
	issued := now
	var grant WorkspaceLease
	var hooked int
	p.testAfterIssuePublish = func(volume string) {
		hooked++
		now = issued.Add(200 * time.Millisecond)
		l, err := p.RenewWorkspaceLease(ctx, volume, "r", 2)
		if err != nil {
			t.Errorf("renewal of the new epoch at the issue publication: %v", err)
			return
		}
		grant = l
	}
	req := WorkspaceMoveRequest{RequestID: "clean", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "clean"}
	if r, err := p.MoveWorkspace(ctx, req, predecessorHead); err != nil || r.NewEpoch != 2 {
		t.Fatal("clean move:", r, err)
	}
	p.testAfterIssuePublish = nil
	if hooked != 1 || grant.Epoch != 2 || grant.ExpiresAtNS != issued.Add(200*time.Millisecond+grantTTL).UnixNano() {
		t.Fatalf("seam ran %d times, grant %+v", hooked, grant)
	}
	if got := p.leaseDeadlineLockedForTest("v"); got.UnixNano() < grant.ExpiresAtNS {
		t.Fatalf("the issue lowered the deadline to %s below the outstanding grant %s", got, time.Unix(0, grant.ExpiresAtNS))
	}
	// Past issue+TTL but inside the outstanding grant: the holder still renews.
	now = issued.Add(grantTTL + 50*time.Millisecond)
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 2); err != nil {
		t.Fatal("renewal inside the outstanding grant:", err)
	}
	assertLeaseViewMatches(t, p, "after issue and renewal")
	p.CloseWorkspaceExtenders()
}
