package authority

import (
	"context"
	"errors"
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

func TestWorkspaceLeaseRegrantWaitsForDurabilityAndLatchesFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "uncertain"}[fail], func(t *testing.T) {
			disk, _ := newTempStore(t)
			clock := newTestClock()
			store := &hookStore{AuthorityStore: disk}
			p := newLeasePublisher(t, store, time.Second, clock)
			before := bindAndGrant(t, p, "v", "r")
			clock.at(3)
			entered, release := make(chan struct{}), make(chan struct{})
			// Release even on a failed assertion, before publisher cleanup joins.
			defer close(release)
			store.setBefore(func(DurableRecord) error {
				close(entered)
				<-release
				return nil
			})
			store.setAfter(func(DurableRecord) error {
				if fail {
					return errors.New("write landed but reply lost")
				}
				return nil
			})
			type answer struct {
				lease WorkspaceLease
				err   error
			}
			done := make(chan answer, 1)
			go func() {
				l, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1)
				done <- answer{l, err}
			}()
			select {
			case <-entered:
			case got := <-done:
				t.Fatal("re-grant returned before the durable write witness", got)
			}
			if visible, ok := p.WorkspaceLeaseReply("v"); !ok || visible.ExpiresAtNS != before.ExpiresAtNS {
				t.Error("grant published ahead of durable store", visible)
			}
			// An unbuffered release lets this test join the writer without racing
			// a wall-clock delay; its deferred close is harmless after the send.
			release <- struct{}{}
			got := <-done
			if fail {
				if got.err == nil || got.lease.ExpiresAtNS != 0 {
					t.Fatal("uncertain write produced a usable grant", got)
				}
				if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err == nil {
					t.Fatal("renewal bypassed uncertain re-grant")
				}
			} else if got.err != nil || got.lease.ExpiresAtNS != clock.sec(4).UnixNano() {
				t.Fatal("durable re-grant not returned", got)
			}
		})
	}
}

func TestWorkspaceLeaseForcedIssueDefeatsOldHolderRegrant(t *testing.T) {
	disk, _ := newTempStore(t)
	clock := newTestClock()
	p := newLeasePublisher(t, disk, time.Second, clock)
	bindAndGrant(t, p, "v", "r")
	clock.at(3)
	req := WorkspaceMoveRequest{RequestID: "force", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old", TargetHost: "new", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
	grant, err := p.MoveWorkspace(context.Background(), req, WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old/r", RemoteLSN: 42})
	if err != nil || !grant.Issued || grant.NewEpoch != 2 {
		t.Fatal("fixture did not issue forced successor", grant, err)
	}
	clock.at(7)
	if got, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err == nil {
		t.Fatal("old epoch re-granted after forced issue", got)
	}
	line, _ := p.VolumeAuthorityLine("v")
	if line.Epoch != 2 || p.WorkspaceState("v").Lease.Epoch != 2 {
		t.Fatal("re-grant undid forced issue", line)
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

// INV-WS-LEASE-REGRANT-001: expiry alone cannot fence a previous process
// whose own clock may lag. The full skew bound must pass before re-grant.
func TestWorkspaceLeaseRegrantWaitsForFullSkew(t *testing.T) {
	disk, _ := newTempStore(t)
	clock := newTestClock()
	store := &hookStore{AuthorityStore: disk}
	p := NewPublisher(nil, WithStore(store), WithWorkspaceLeaseConfig(10*time.Second, clock.now), WithWorkspaceMaxClockSkew(4*time.Second))
	t.Cleanup(p.CloseWorkspaceExtenders)
	grant := bindAndGrant(t, p, "v", "r")
	if grant.ExpiresAtNS != clock.sec(10).UnixNano() || p.WorkspaceState("v").Lease.ExpiresAtNS != clock.sec(15).UnixNano() {
		t.Fatal("fixture did not create the declared grant/horizon", grant)
	}
	writes := 0
	store.setBefore(func(DurableRecord) error { writes++; return nil })
	// The deadline+skew boundary is 14 s; horizon+skew is 19 s.
	// Re-grant uses the same MAX barrier as a forced move, including the
	// interval after the shorter deadline has passed and its final ns.
	for _, now := range []time.Time{clock.sec(13), clock.sec(14), clock.sec(19).Add(-time.Nanosecond)} {
		clock.ns.Store(now.UnixNano())
		if got, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err == nil {
			t.Fatal("re-grant crossed the later expiry/skew boundary early", now, got)
		}
		if err := p.WorkspaceForcedMoveAdmitted(context.Background(), "v", "force", "r", 1); err == nil {
			t.Fatal("forced move crossed the shared boundary early", now)
		}
		if writes != 0 {
			t.Fatal("unsafe early re-grant attempted persistence", writes)
		}
	}
	clock.at(19)
	if err := p.WorkspaceForcedMoveAdmitted(context.Background(), "v", "force", "r", 1); err != nil {
		t.Fatal("fixture did not reach the shared forced-move barrier", err)
	}
	got, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1)
	if err != nil || got.ExpiresAtNS <= clock.now().UnixNano() || writes != 1 {
		t.Fatalf("safe unchanged owner cannot return: grant=%+v writes=%d err=%v", got, writes, err)
	}
}
