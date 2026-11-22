package authority

import (
	"context"
	"testing"
	"time"
)

// INV-WS-LEASE-REGRANT-001: expiry alone does not abandon an unchanged owner.
// A new guard may use a durable re-grant; existing fenced guards stay terminal.
func TestWorkspaceLeaseExpiredSameHolderRegrantIsDurable(t *testing.T) {
	disk, _ := newTempStore(t)
	clock := newTestClock()
	store := &hookStore{AuthorityStore: disk}
	p := newLeasePublisher(t, store, time.Second, clock)
	original := bindAndGrant(t, p, "v", "r")
	before := p.WorkspaceState("v").Lease
	clock.at(3)
	writes := 0
	store.setBefore(func(r DurableRecord) error { writes++; return nil })
	renewed, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1)
	if err != nil {
		t.Fatalf("unchanged expired holder cannot obtain a durable re-grant: %v", err)
	}
	if renewed.ReplicaID != "r" || renewed.Epoch != 1 || renewed.Revoking || renewed.ExpiresAtNS <= clock.now().UnixNano() || renewed.ExpiresAtNS <= original.ExpiresAtNS || writes != 1 {
		t.Fatalf("bad re-grant %+v writes%d", renewed, writes)
	}
	records, err := disk.Load()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range records {
		if r.VolumeID == "v" && r.Workspace != nil && r.Workspace.Lease != nil {
			l := r.Workspace.Lease
			found = true
			if l.ExpiresAtNS < renewed.ExpiresAtNS || l.ExpiresAtNS < before.ExpiresAtNS || l.MaxClockSkewNS < before.MaxClockSkewNS {
				t.Fatalf("grant ahead of durable horizon/skew: disk%+v reply%+v", l, renewed)
			}
		}
	}
	if !found {
		t.Fatal("re-grant used without durable lease")
	}
	line, _ := p.VolumeAuthorityLine("v")
	if line.Epoch != 1 || line.ReplicaID != "r" {
		t.Fatal("re-grant minted authority", line)
	}
	// Immediate flapping/repeated requests take the memory path: one durable
	// re-grant, no extra writes while its positive grant is outstanding.
	for i := 0; i < 10; i++ {
		if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
			t.Fatal(err)
		}
	}
	if writes != 1 {
		t.Fatalf("repeat requests amplified durable writes: %d", writes)
	}
}

func TestWorkspaceLeaseRegrantDefeatsAlreadyPassedForcedPreflight(t *testing.T) {
	disk, _ := newTempStore(t)
	clock := newTestClock()
	p := newLeasePublisher(t, disk, time.Second, clock)
	bindAndGrant(t, p, "v", "r")
	clock.at(3)
	req := WorkspaceMoveRequest{RequestID: "force", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old", TargetHost: "new", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
	if err := p.WorkspaceForcedMoveAdmitted(context.Background(), "v", req.RequestID, "r", 1); err != nil {
		t.Fatal("forced preflight was not actually admitted", err)
	}
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
		t.Fatal("same-owner re-grant failed", err)
	}
	if _, err := p.MoveWorkspace(context.Background(), req, WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old/r", RemoteLSN: 42}); err == nil {
		t.Fatal("force ignored the grant committed after its HEAD preflight")
	}
	line, _ := p.VolumeAuthorityLine("v")
	if line.Epoch != 1 {
		t.Fatal("two winning successors", line)
	}
	if len(p.WorkspaceState("v").Moves) != 0 {
		t.Fatal("refused forced move reserved an epoch")
	}
}

func TestWorkspaceLeaseSealedAckNeverRegrants(t *testing.T) {
	disk, _ := newTempStore(t)
	clock := newTestClock()
	store := &hookStore{AuthorityStore: disk}
	p := newLeasePublisher(t, store, time.Second, clock)
	bindAndGrant(t, p, "v", "r")
	ctx := context.Background()
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7}); err != nil {
		t.Fatal(err)
	}
	// ACK is the master's durable evidence for this holder's sealed generation.
	clock.at(10)
	writes := 0
	store.setBefore(func(DurableRecord) error { writes++; return nil })
	got, err := p.RenewWorkspaceLease(ctx, "v", "r", 1)
	if err == nil && (!got.Revoking || got.ExpiresAtNS > clock.now().UnixNano()) {
		t.Fatalf("sealed/ACKed owner got usable re-grant: %+v", got)
	}
	if writes != 0 || p.WorkspaceState("v").Ack.HeadGen != 7 {
		t.Fatal("sealed proof mutated by re-grant")
	}
}
