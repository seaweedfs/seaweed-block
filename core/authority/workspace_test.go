package authority

import (
	"context"
	"errors"
	"testing"
	"time"
)

type workspaceFailStore struct {
	AuthorityStore
	failJournal bool
	failIssue   bool
}

func (s *workspaceFailStore) Put(r DurableRecord) error {
	if s.failIssue && r.Workspace != nil && len(r.Workspace.Moves) > 0 && r.Workspace.Moves[len(r.Workspace.Moves)-1].Issued {
		return errors.New("crash after journal before epoch")
	}
	if s.failJournal && r.Workspace != nil && len(r.Workspace.Moves) > 0 {
		return errors.New("injected journal persistence failure")
	}
	return s.AuthorityStore.Put(r)
}

func TestWorkspaceCrashAfterJournalBeforeEpochResumesSameGrant(t *testing.T) {
	disk, _ := newTempStore(t)
	fault := &workspaceFailStore{AuthorityStore: disk}
	now := time.Unix(1000, 0)
	opts := []PublisherOption{WithStore(fault), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return now })}
	p := NewPublisher(nil, opts...)
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	// Past the durable horizon (1.5*TTL) plus skew, not only the grant (#205).
	now = now.Add(3 * time.Second)
	req := WorkspaceMoveRequest{RequestID: "m", VolumeID: "v", ExpectedReplicaID: "old", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1, TargetReplicaID: "old", TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
	head := WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42}
	fault.failIssue = true
	ch, unsubscribe := p.Subscribe("v", "old")
	defer unsubscribe()
	select {
	case fact := <-ch:
		if fact.Epoch != 1 {
			t.Fatalf("initial authority: %+v", fact)
		}
	case <-time.After(time.Second):
		t.Fatal("missing initial authority catch-up")
	}
	if _, err := p.MoveWorkspace(context.Background(), req, head); err == nil {
		t.Fatal("injected crash ignored")
	}
	line, _ := p.VolumeAuthorityLine("v")
	if line.Epoch != 1 {
		t.Fatal("epoch delivered before durable issue", line)
	}
	select {
	case fact := <-ch:
		t.Fatalf("epoch notified before durable issue: %+v", fact)
	default:
	}
	loaded := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return now }))
	line, _ = loaded.VolumeAuthorityLine("v")
	if line.Epoch != 1 {
		t.Fatal(line)
	}
	state := loaded.WorkspaceState("v")
	if len(state.Moves) != 1 || state.Moves[0].Issued || state.Moves[0].NewEpoch != 2 {
		t.Fatalf("grant not durable before epoch: %+v", state)
	}
	if _, err := loaded.MoveWorkspace(context.Background(), req, head); err == nil {
		t.Fatal("restart resumed forced reservation before conservative skew barrier")
	}
	now = now.Add(2 * time.Second)
	r, err := loaded.MoveWorkspace(context.Background(), req, head)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Issued || r.NewEpoch != 2 || r.HeadGen != 7 || r.RemoteLSN != 42 {
		t.Fatal(r)
	}
	again, err := loaded.MoveWorkspace(context.Background(), req, WorkspaceHead{})
	if err != nil || again != r {
		t.Fatal("grant replay changed", again, err)
	}
	finalPublisher := NewPublisher(nil, WithStore(disk))
	for name, publisher := range map[string]*Publisher{"issued": loaded, "reloaded": finalPublisher} {
		line, ok := publisher.LastPublished("v", "old")
		if !ok || line.ServerID != "new-node" || !line.OwnerFromMove {
			t.Fatalf("%s lost captured moved owner: %+v", name, line)
		}
		published, _ := publisher.PublishedAuthorityLine("v")
		if published.IssuedHost != line.ServerID || !published.OwnerFromMove {
			t.Fatalf("%s lease and stream owners differ: %+v / %+v", name, published, line)
		}
	}
	final := finalPublisher.WorkspaceState("v")
	if final == nil || len(final.Moves) != 1 || !final.Moves[0].Issued {
		t.Fatal("issued epoch lost journal", final)
	}
}

func TestWorkspaceCleanMoveNeedsDetachedHEADAndRevocationAck(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	p := NewPublisher(nil, WithStore(disk))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	req := WorkspaceMoveRequest{RequestID: "clean", VolumeID: "v", ExpectedReplicaID: "old", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1, TargetReplicaID: "old", TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "clean"}
	head := WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42, Detached: true}
	if _, err := p.MoveWorkspace(ctx, req, head); err == nil {
		t.Fatal("clean without ack")
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "old", Epoch: 1, HeadGen: 7}, WorkspaceHead{Detached: true, Epoch: 1, Holder: "", RemoteLSN: 0}); err == nil {
		t.Fatal("unsolicited ack accepted")
	}
	if err := p.BeginWorkspaceRevocation(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	lease, err := p.RenewWorkspaceLease(ctx, "v", "old", 1)
	if err != nil || !lease.Revoking {
		t.Fatal("revocation not delivered", lease, err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "old", Epoch: 1, HeadGen: 7, Holder: "old-node/old", RemoteLSN: 42}, WorkspaceHead{Detached: true, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42}); err != nil {
		t.Fatal(err)
	}
	bad := head
	bad.Detached = false
	if _, err := p.MoveWorkspace(ctx, req, bad); err == nil {
		t.Fatal("attached head accepted")
	}
	bad = head
	bad.RemoteLSN++
	if _, err := p.MoveWorkspace(ctx, req, bad); err == nil {
		t.Fatal("different sealed position accepted")
	}
	r, err := p.MoveWorkspace(ctx, req, head)
	if err != nil || r.NewEpoch != 2 || !r.Issued {
		t.Fatal(r, err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err == nil {
		t.Fatal("old host renewed after move")
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 2); err != nil {
		t.Fatal(err)
	}
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "retired", DataAddr: "old-d", CtrlAddr: "old-c", Intent: IntentRefreshEndpoint}); err == nil {
		t.Fatal("retired slot overwrote current durable authority")
	}
}

func TestWorkspaceForcedMoveRequiresExplicitForceAndLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	disk, _ := newTempStore(t)
	now := time.Unix(1000, 0)
	p := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return now }))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	req := WorkspaceMoveRequest{RequestID: "f", VolumeID: "v", ExpectedReplicaID: "old", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1, TargetReplicaID: "old", TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
	head := WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42}
	if _, err := p.MoveWorkspace(ctx, req, head); err == nil {
		t.Fatal("unexpired lease moved")
	}
	now = now.Add(2 * time.Second)
	if _, err := p.RenewWorkspaceLease(ctx, "v", "old", 1); err == nil {
		t.Fatal("expired host renewed")
	}
	req.Force = false
	if _, err := p.MoveWorkspace(ctx, req, head); err == nil {
		t.Fatal("implicit forced move")
	}
	req.Force = true
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "new", DataAddr: "nd", CtrlAddr: "nc", Intent: IntentReassign}); err == nil {
		t.Fatal("ordinary failover bypassed move")
	}
	// Reload uses a full-TTL conservative barrier before admitting forced moves.
	p = NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return now }))
	if _, err := p.MoveWorkspace(ctx, req, head); err == nil {
		t.Fatal("restart shortened old host lease")
	}
	now = now.Add(2 * time.Second)
	if _, err := p.MoveWorkspace(ctx, req, head); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceMoveCannotIssueEpochWithoutJournal(t *testing.T) {
	disk, _ := newTempStore(t)
	fault := &workspaceFailStore{AuthorityStore: disk}
	now := time.Unix(1000, 0)
	p := NewPublisher(nil, WithStore(fault), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return now }))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "old", 1); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	fault.failJournal = true
	_, err := p.MoveWorkspace(context.Background(), WorkspaceMoveRequest{RequestID: "move-1", VolumeID: "v", ExpectedReplicaID: "old", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1,
		TargetReplicaID: "old", TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}, WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/old", RemoteLSN: 42})
	if err == nil {
		t.Error("journal failure did not stop move")
	}
	line, _ := p.VolumeAuthorityLine("v")
	if line.Epoch != 1 || line.ReplicaID != "old" {
		t.Errorf("epoch issued without durable journal: %+v", line)
	}
	// Reload from actual files, not a mock's retained Go object.
	reloaded := NewPublisher(nil, WithStore(disk))
	line, _ = reloaded.VolumeAuthorityLine("v")
	if line.Epoch != 1 || line.ReplicaID != "old" {
		t.Errorf("restart found epoch without journal: %+v", line)
	}
}

func TestWorkspaceMoveUsesQualifiedHEADHolder(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	disk, _ := newTempStore(t)
	p := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return now }))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "same", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RenewWorkspaceLease(ctx, "v", "same", 1); err != nil {
		t.Fatal(err)
	}
	// Past the durable horizon (1.5*TTL) plus skew, not only the grant (#205).
	now = now.Add(3 * time.Second)
	q := WorkspaceMoveRequest{RequestID: "qualified", VolumeID: "v", ExpectedReplicaID: "same", TargetReplicaID: "same", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
	for _, holder := range []string{"same", "other-node/same", "old-node/other"} {
		if _, err := p.MoveWorkspace(ctx, q, WorkspaceHead{Gen: 7, Epoch: 1, Holder: holder, RemoteLSN: 42}); err == nil {
			t.Fatalf("accepted mismatched qualified holder %q", holder)
		}
		if state := p.WorkspaceState("v"); len(state.Moves) != 0 {
			t.Fatal("refusal mutated move journal", state)
		}
	}
	g, err := p.MoveWorkspace(ctx, q, WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old-node/same", RemoteLSN: 42})
	if err != nil {
		t.Fatal(err)
	}
	if g.OldReplicaID != "same" || g.NewReplicaID != "same" || g.OldHost != "old-node" || g.NewHost != "new-node" || g.NewEpoch <= 1 {
		t.Fatal(g)
	}
}
