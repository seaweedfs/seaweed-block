package authority

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countPutStore counts authority writes.
type countPutStore struct {
	AuthorityStore
	puts atomic.Int64
}

func (s *countPutStore) Put(r DurableRecord) error {
	s.puts.Add(1)
	return s.AuthorityStore.Put(r)
}

const grantTTL = time.Second

// The predecessor's detached HEAD: the grantee of the clean move below never
// claims it, so HEAD keeps naming the predecessor.
var predecessorHead = WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/r", RemoteLSN: 42, Detached: true}

// cleanMoveToGrantee binds v to r at epoch 1, then moves it cleanly from
// old-node to new-node (epoch 2) at the returned issue time.
func cleanMoveToGrantee(t *testing.T, store AuthorityStore, now *time.Time) (*Publisher, time.Time) {
	t.Helper()
	ctx := context.Background()
	p := NewPublisher(nil, WithStore(store), WithWorkspaceLeaseConfig(grantTTL, func() time.Time { return *now }))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: predecessorHead.Gen, Holder: predecessorHead.Holder, RemoteLSN: predecessorHead.RemoteLSN}, WorkspaceHead{Detached: true, Epoch: 1, Holder: predecessorHead.Holder, RemoteLSN: predecessorHead.RemoteLSN}); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(100 * time.Millisecond)
	req := WorkspaceMoveRequest{RequestID: "clean", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "clean"}
	r, err := p.MoveWorkspace(ctx, req, predecessorHead)
	if err != nil || !r.Issued || r.NewEpoch != 2 {
		t.Fatal("clean move to the grantee:", r, err)
	}
	return p, *now
}

// assertGranteeLease checks the lease the move issue started: the grantee's
// identity at the new epoch, horizon issue+1.5*TTL, not revoking, no ACK.
func assertGranteeLease(t *testing.T, p *Publisher, issued time.Time) {
	t.Helper()
	st := p.WorkspaceState("v")
	want := WorkspaceLease{ReplicaID: "r", Epoch: 2, ExpiresAtNS: issued.Add(grantTTL + grantTTL/2).UnixNano(), MaxClockSkewNS: int64(p.workspaceSkew(nil))}
	if st == nil || st.Lease == nil || *st.Lease != want || st.Ack != nil {
		t.Fatalf("move issue must start the grantee's lease %+v; state %+v", want, st)
	}
}

func forcedFromGrantee(id string) WorkspaceMoveRequest {
	return WorkspaceMoveRequest{RequestID: id, VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "new-node", TargetHost: "third-node", ExpectedEpoch: 2, TargetDataAddr: "td", TargetCtrlAddr: "tc", Kind: "forced", Force: true}
}

// A grantee that dies between its grant and its HEAD claim never renews:
// the lease the move issue started expires, and a forced move to a third
// host is admitted after its expiry plus the skew margin, never before. The
// same holds when blockmaster restarts between the issue and the forced move.
func TestWorkspaceUnclaimedGrantForcedReassignAfterExpiry(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same-master"
		if restart {
			name = "restart"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			disk, _ := newTempStore(t)
			now := time.Unix(1000, 0)
			p, issued := cleanMoveToGrantee(t, disk, &now)
			assertGranteeLease(t, p, issued)
			assertLeaseViewMatches(t, p, "after issue")
			if lease, ok := p.WorkspaceLeaseReply("v"); !ok || lease.Epoch != 2 || lease.ExpiresAtNS != issued.Add(grantTTL).UnixNano() {
				t.Fatalf("reported lease after issue: %+v %v; want the issue+TTL grant", lease, ok)
			}
			if restart {
				now = now.Add(200 * time.Millisecond)
				p = NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(grantTTL, func() time.Time { return now }))
				assertGranteeLease(t, p, issued)
				assertLeaseViewMatches(t, p, "after restart")
			}
			skew := p.workspaceSkew(p.WorkspaceState("v").Lease)
			// The horizon (issue+1.5*TTL) is the later bound: the deadline is
			// issue+TTL, or restart+TTL after a restart 0.2*TTL later.
			barrier := issued.Add(grantTTL + grantTTL/2 + skew)
			// The predecessor's lease (renewed 0.1 s before the issue) has
			// expired plus skew by then: only the grant's lease may admit.
			if old := issued.Add(-100*time.Millisecond + grantTTL + grantTTL/2 + skew); !barrier.Add(-time.Nanosecond).After(old) {
				t.Fatal("test clock does not separate the predecessor's barrier from the grant's")
			}
			now = barrier.Add(-time.Nanosecond)
			if _, err := p.MoveWorkspace(ctx, forcedFromGrantee("forced"), predecessorHead); err == nil || !strings.Contains(err.Error(), "expiry plus clock-skew margin has not elapsed") {
				t.Fatalf("forced move before expiry+skew: %v", err)
			}
			if line, _ := p.VolumeAuthorityLine("v"); line.Epoch != 2 || len(p.WorkspaceState("v").Moves) != 1 {
				t.Fatal("early refusal changed authority or journal", line)
			}
			now = barrier
			r, err := p.MoveWorkspace(ctx, forcedFromGrantee("forced"), predecessorHead)
			if err != nil {
				t.Fatal("forced move after expiry+skew:", err)
			}
			if !r.Issued || r.NewEpoch != 3 || r.NewHost != "third-node" || r.HeadGen != predecessorHead.Gen || r.RemoteLSN != predecessorHead.RemoteLSN {
				t.Fatalf("forced grant %+v", r)
			}
			lost := p.WorkspaceLostRanges("v")
			if len(lost) != 1 || lost[0].NewEpoch != 3 || lost[0].OldEpoch != 2 || lost[0].LostAfterLSN != predecessorHead.RemoteLSN {
				t.Fatalf("lost range %+v", lost)
			}
			if st := p.WorkspaceState("v"); st.Lease == nil || st.Lease.Epoch != 3 || st.Lease.ExpiresAtNS != now.Add(grantTTL+grantTTL/2).UnixNano() {
				t.Fatalf("forced issue must start the next holder's lease: %+v", st.Lease)
			}
			assertLeaseViewMatches(t, p, "after forced move")
		})
	}
}

// The HEAD rule: after admission, a forced move from a grantee that never
// claimed HEAD accepts any HEAD at an older epoch (its Gen and RemoteLSN may
// have moved, e.g. by an earlier holder's late claim); the lost range starts
// at that HEAD's RemoteLSN. A HEAD at the expected or a higher epoch that is
// not the grantee's, or no HEAD, is refused. Clean moves never accept it.
func TestWorkspaceUnclaimedGrantHEADRule(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	now := time.Unix(1000, 0)
	p, issued := cleanMoveToGrantee(t, disk, &now)

	// Clean: even with the grantee's revocation ACK naming that HEAD
	// generation, the predecessor's HEAD is not the grantee's.
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 2); err != nil {
		t.Fatal("revoke the grantee's issued lease:", err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 2, HeadGen: predecessorHead.Gen, Holder: predecessorHead.Holder, RemoteLSN: predecessorHead.RemoteLSN}, WorkspaceHead{Detached: true, Epoch: 2, Holder: predecessorHead.Holder, RemoteLSN: predecessorHead.RemoteLSN}); err != nil {
		t.Fatal(err)
	}
	clean := forcedFromGrantee("clean-2")
	clean.Kind, clean.Force = "clean", false
	if _, err := p.MoveWorkspace(ctx, clean, predecessorHead); err == nil || !strings.Contains(err.Error(), "HEAD not owned by expected holder") {
		t.Fatalf("clean move accepted the predecessor's HEAD: %v", err)
	}

	now = issued.Add(10 * grantTTL)
	refused := map[string]WorkspaceHead{}
	h := predecessorHead
	h.Epoch = 2
	refused["expected epoch, another holder"] = h
	h = WorkspaceHead{Gen: 9, Epoch: 3, Holder: "third-node/r", RemoteLSN: 50}
	refused["epoch above expected"] = h
	h = predecessorHead
	h.Gen = 0
	refused["no HEAD"] = h
	for what, h := range refused {
		if _, err := p.MoveWorkspace(ctx, forcedFromGrantee("forced-"+what), h); err == nil || !strings.Contains(err.Error(), "HEAD not owned by expected holder") {
			t.Fatalf("%s: forced move accepted %+v: %v", what, h, err)
		}
	}
	if line, _ := p.VolumeAuthorityLine("v"); line.Epoch != 2 || len(p.WorkspaceState("v").Moves) != 1 {
		t.Fatal("a refused HEAD changed authority or journal", line)
	}
	// An older-epoch HEAD whose Gen and RemoteLSN moved since the issue.
	moved := predecessorHead
	moved.Gen, moved.RemoteLSN = predecessorHead.Gen+1, predecessorHead.RemoteLSN+8
	r, err := p.MoveWorkspace(ctx, forcedFromGrantee("forced"), moved)
	if err != nil || r.NewEpoch != 3 || r.HeadGen != moved.Gen || r.RemoteLSN != moved.RemoteLSN {
		t.Fatal("forced move with an older-epoch HEAD:", r, err)
	}
	if lost := p.WorkspaceLostRanges("v"); len(lost) != 1 || lost[0].LostAfterLSN != moved.RemoteLSN {
		t.Fatalf("lost range must start at the fresh HEAD's RemoteLSN: %+v", lost)
	}
	// A chain: the epoch-3 grantee dies unclaimed too.
	now = now.Add(10 * grantTTL)
	next := forcedFromGrantee("forced-again")
	next.ExpectedHost, next.TargetHost, next.ExpectedEpoch = "third-node", "fourth-node", 3
	if r, err := p.MoveWorkspace(ctx, next, moved); err != nil || r.NewEpoch != 4 {
		t.Fatal("second unclaimed grant:", r, err)
	}
}

// The grantee's first renewal is an ordinary renewal of the lease the issue
// started: no authority write, no new epoch; the old epoch stays refused.
func TestWorkspaceUnclaimedGrantGranteeRenewsNormally(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	store := &countPutStore{AuthorityStore: disk}
	now := time.Unix(1000, 0)
	p, issued := cleanMoveToGrantee(t, store, &now)
	writes := store.puts.Load()
	now = issued.Add(100 * time.Millisecond)
	l, err := p.RenewWorkspaceLease(ctx, "v", "r", 2)
	if err != nil {
		t.Fatal("grantee's first renewal:", err)
	}
	if l.Epoch != 2 || l.ReplicaID != "r" || l.ExpiresAtNS != now.Add(grantTTL).UnixNano() {
		t.Fatalf("first renewal %+v", l)
	}
	if store.puts.Load() != writes {
		t.Fatalf("first renewal wrote %d authority records; the issue already persisted the lease", store.puts.Load()-writes)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err == nil {
		t.Fatal("old epoch renewed after the move")
	}
	// Past 0.25*TTL the renewal starts an extension of the issued lease.
	now = issued.Add(grantTTL / 2)
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 2); err != nil {
		t.Fatal(err)
	}
	st := p.WorkspaceState("v")
	for wait := time.Now().Add(5 * time.Second); time.Now().Before(wait) && (st.Lease == nil || st.Lease.ExpiresAtNS == issued.Add(grantTTL+grantTTL/2).UnixNano()); st = p.WorkspaceState("v") {
		time.Sleep(5 * time.Millisecond)
	}
	p.CloseWorkspaceExtenders()
	if st.Lease == nil || st.Lease.Epoch != 2 || st.Lease.ExpiresAtNS != now.Add(grantTTL+grantTTL/2).UnixNano() {
		t.Fatalf("issued lease not extended: %+v", st.Lease)
	}
	if line, _ := p.VolumeAuthorityLine("v"); line.Epoch != 2 || len(st.Moves) != 1 {
		t.Fatal("renewals changed authority", line)
	}
	assertLeaseViewMatches(t, p, "after renewals")
}

// A forced reservation resumed after a crash issues through the replay path,
// which starts the new holder's lease the same way.
func TestWorkspaceReplayedIssueStartsGranteeLease(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	fault := &workspaceFailStore{AuthorityStore: disk}
	now := time.Unix(1000, 0)
	p := NewPublisher(nil, WithStore(fault), WithWorkspaceLeaseConfig(grantTTL, func() time.Time { return now }))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	now = now.Add(3 * grantTTL)
	req := WorkspaceMoveRequest{RequestID: "m", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
	head := WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/r", RemoteLSN: 42}
	fault.failIssue = true
	if _, err := p.MoveWorkspace(ctx, req, head); err == nil {
		t.Fatal("injected crash ignored")
	}
	loaded := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(grantTTL, func() time.Time { return now }))
	now = now.Add(2 * grantTTL) // past the restart deadline plus skew
	r, err := loaded.MoveWorkspace(ctx, req, WorkspaceHead{})
	if err != nil || !r.Issued || r.NewEpoch != 2 {
		t.Fatal("replayed issue:", r, err)
	}
	assertGranteeLease(t, loaded, now)
	assertLeaseViewMatches(t, loaded, "after replayed issue")
	if _, err := loaded.RenewWorkspaceLease(ctx, "v", "r", 2); err != nil {
		t.Fatal("grantee renewal after the replayed issue:", err)
	}
	// Unrenewed after this, the replayed grant expires like any other.
	now = now.Add(grantTTL + grantTTL/2 + loaded.workspaceSkew(nil))
	next := WorkspaceMoveRequest{RequestID: "m2", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "new-node", TargetHost: "third-node", ExpectedEpoch: 2, TargetDataAddr: "td", TargetCtrlAddr: "tc", Kind: "forced", Force: true}
	if r, err := loaded.MoveWorkspace(ctx, next, head); err != nil || r.NewEpoch != 3 {
		t.Fatal("forced move after the replayed grant expired:", r, err)
	}
}

// The double fault: grantee 1 renews and sends its HEAD claim, then stalls;
// its lease expires and a forced move grants grantee 2 against the
// predecessor's HEAD; grantee 1's in-flight claim lands afterwards (HEAD
// moves to its epoch and the next generation); grantee 2 dies unclaimed.
// The next forced move must still be admitted on that older-epoch HEAD, with
// the lost range from its RemoteLSN; never refused for good.
func TestWorkspaceLateClaimThenUnclaimedGrantForcedReassign(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	now := time.Unix(1000, 0)
	p, issued := cleanMoveToGrantee(t, disk, &now)
	now = issued.Add(100 * time.Millisecond)
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 2); err != nil {
		t.Fatal("grantee 1 renewal:", err)
	}
	// Grantee 1's claim is in flight; it renews no more.
	now = now.Add(10 * grantTTL)
	r, err := p.MoveWorkspace(ctx, forcedFromGrantee("forced-1"), predecessorHead)
	if err != nil || r.NewEpoch != 3 {
		t.Fatal("forced move to grantee 2:", r, err)
	}
	late := WorkspaceHead{Gen: predecessorHead.Gen + 1, Epoch: 2, Holder: "new-node/r", RemoteLSN: predecessorHead.RemoteLSN}
	// Grantee 2 dies unclaimed.
	now = now.Add(10 * grantTTL)
	next := forcedFromGrantee("forced-2")
	next.ExpectedHost, next.TargetHost, next.ExpectedEpoch = "third-node", "fourth-node", 3
	r, err = p.MoveWorkspace(ctx, next, late)
	if err != nil || r.NewEpoch != 4 || r.HeadGen != late.Gen || r.RemoteLSN != late.RemoteLSN {
		t.Fatal("forced move after grantee 1's late claim and grantee 2's death:", r, err)
	}
	lost := p.WorkspaceLostRanges("v")
	if len(lost) != 2 || lost[1].NewEpoch != 4 || lost[1].LostAfterLSN != late.RemoteLSN {
		t.Fatalf("lost ranges %+v", lost)
	}
	if line, _ := p.VolumeAuthorityLine("v"); line.Epoch != 4 {
		t.Fatal("authority line", line)
	}
}
