package master

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The T08 shape: two volumes without per-volume bindings, nodes registered
// at slot 0 of their host. "a1" holds slot 0 on node-a; "a2" lives on
// node-b and moves back to node-a. Its target lands on slot 1 (slot 0 is
// held) and is granted; a target on a1's live slot-0 endpoints is refused by
// the collision check, and one on another host's address by the ownership
// check. The grant routes a2 to the slot-1 endpoints it asked for.
func TestWorkspaceMoveToAnotherSlotOnTheTargetHost(t *testing.T) {
	heads := map[string]authority.WorkspaceHead{"a2": {Gen: 9, Epoch: 1, Holder: "node-b/r1", RemoteLSN: 42, Detached: true}}
	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		WorkspaceHeadReader: func(_ context.Context, v string) (authority.WorkspaceHead, error) { return heads[v], nil }})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	defer closeTestMaster(t, m)
	conn, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ws, lc := control.NewWorkspaceServiceClient(conn), control.NewLifecycleServiceClient(conn)
	slot := func(base, n int) (string, string) {
		return "127.0.0.1:" + strconv.Itoa(base+10*n+1), "127.0.0.1:" + strconv.Itoa(base+10*n+2)
	}
	for _, n := range []struct {
		id   string
		base int
	}{{"node-a", 19100}, {"node-b", 19300}} {
		d, c := slot(n.base, 0)
		if _, err := m.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: n.id, DataAddr: d, CtrlAddr: c}); err != nil {
			t.Fatal(err)
		}
	}
	place := func(vol, server string, base int) authority.AuthorityBasis {
		t.Helper()
		if _, err := lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: vol, SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: vol, DesiredRF: 1,
			Candidates: []lifecycle.PlacementCandidate{{VolumeID: vol, ServerID: server, ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
			t.Fatal(err)
		}
		d, c := slot(base, 0)
		if _, err := control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: server, SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
			Slots: []*control.HeartbeatSlot{{VolumeId: vol, ReplicaId: "r1", DataAddr: d, CtrlAddr: c}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := m.RunLifecycleProductTick(); err != nil {
			t.Fatal(err)
		}
		return waitAuthorityLine(t, m.Publisher(), vol)
	}
	a1 := place("a1", "node-a", 19100) // holds node-a slot 0
	a2 := place("a2", "node-b", 19300)
	if a1.DataAddr != "127.0.0.1:19101" || a2.DataAddr != "127.0.0.1:19301" {
		t.Fatalf("fixture lines a1=%+v a2=%+v", a1, a2)
	}
	// a2 stops cleanly on node-b: lease, revocation, ACK of its detached HEAD.
	id := &control.WorkspaceIdentity{ServerId: "node-b", VolumeId: "a2", ReplicaId: "r1", Epoch: a2.Epoch}
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
		t.Fatal(err)
	}
	move := func(req, data, ctrl string) *control.WorkspaceMove {
		return &control.WorkspaceMove{RequestId: req, VolumeId: "a2", ExpectedReplicaId: "r1", ExpectedEpoch: a2.Epoch, TargetReplicaId: "r1",
			TargetServerId: "node-a", TargetDataAddr: data, TargetCtrlAddr: ctrl, Kind: "clean"}
	}
	d0, c0 := slot(19100, 0)
	if _, err := ws.MoveWorkspace(ctx, move("onto-a1", d0, c0)); err == nil || !strings.Contains(err.Error(), "endpoint belongs to live assignment") {
		t.Fatalf("move onto a1's live slot-0 endpoints: %v; want the collision refusal", err)
	}
	if _, err := ws.MoveWorkspace(ctx, move("other-host", "10.9.9.9:19111", "10.9.9.9:19112")); err == nil || !strings.Contains(err.Error(), `endpoints 10.9.9.9:19111/10.9.9.9:19112 on "node-a" are not registered`) {
		t.Fatalf("move to another host's address: %v; want the ownership refusal", err)
	}
	d1, c1 := slot(19100, 1)
	if _, err := ws.MoveWorkspace(ctx, move("ctrl-other-host", d1, "10.9.9.9:19112")); err == nil || !strings.Contains(err.Error(), `on "node-a" are not registered`) {
		t.Fatalf("move with its ctrl endpoint on another host: %v; want the ownership refusal", err)
	}
	// Invalid endpoints on the right host are refused before anything is
	// written: the epoch, the journal, the lease and the ACK stay as they were.
	before := m.Publisher().WorkspaceState("a2")
	for _, bad := range []struct{ name, data, ctrl string }{
		{"non-numeric port", "127.0.0.1:bad", c1},
		{"port 0", "127.0.0.1:0", c1},
		{"port 65536", d1, "127.0.0.1:65536"},
		{"empty port", "127.0.0.1:", c1},
		{"data equals ctrl", d1, d1},
	} {
		if _, err := ws.MoveWorkspace(ctx, move("bad-"+bad.name, bad.data, bad.ctrl)); err == nil || !strings.Contains(err.Error(), "move target endpoints invalid") {
			t.Fatalf("%s: %v; want the invalid-endpoint refusal", bad.name, err)
		}
		after := m.Publisher().WorkspaceState("a2")
		line, _ := m.Publisher().VolumeAuthorityLine("a2")
		if line.Epoch != a2.Epoch || after == nil || len(after.Moves) != 0 || after.Ack == nil || before.Ack == nil || *after.Ack != *before.Ack || after.Lease == nil || !after.Lease.Revoking {
			t.Fatalf("%s: state changed by a refused move: line %+v state %+v", bad.name, line, after)
		}
	}
	g, err := ws.MoveWorkspace(ctx, move("slot-1", d1, c1))
	if err != nil {
		t.Fatalf("move to node-a slot 1 while slot 0 is held: %v", err)
	}
	line, ok := m.Publisher().VolumeAuthorityLine("a2")
	if !g.Issued || g.NewHost != "node-a" || g.OldHost != "node-b" || !ok || line.Epoch != g.NewEpoch || line.DataAddr != d1 || line.CtrlAddr != c1 {
		t.Fatalf("grant %+v line %+v; want a2 issued on node-a at %s/%s", g, line, d1, c1)
	}
	if l1, _ := m.Publisher().VolumeAuthorityLine("a1"); l1.DataAddr != d0 || l1.Epoch != a1.Epoch {
		t.Fatalf("a1 changed: %+v", l1)
	}
}
