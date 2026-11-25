package master

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type workspaceRPCView struct{ p frontend.Projection }

func (v workspaceRPCView) Projection() frontend.Projection { return v.p }

// lostAckReply returns an unknown outcome only after the real master has
// committed the ACK. The caller must resolve it from a recorded witness.
type lostAckReply struct{ control.WorkspaceServiceClient }

func (c lostAckReply) AckWorkspaceRevoked(ctx context.Context, q *control.WorkspaceRevocationAck, opts ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	reply, err := c.WorkspaceServiceClient.AckWorkspaceRevoked(ctx, q, opts...)
	if err != nil {
		return reply, err
	}
	return nil, status.Error(codes.DeadlineExceeded, "injected ACK reply loss after commit")
}

func TestWorkspaceRPCCleanMovePreservesReplicaIdentity(t *testing.T) {
	// The HEAD the master reads for vol-a: held by node-a/r2 at epoch 1,
	// detached once the holder's final cut is in.
	var detached atomic.Bool
	reader := func(_ context.Context, vol string) (authority.WorkspaceHead, error) {
		if vol != "vol-a" {
			return authority.WorkspaceHead{}, fmt.Errorf("no HEAD for %s", vol)
		}
		return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r2", RemoteLSN: 42, Detached: detached.Load()}, nil
	}
	authDir, lifeDir := t.TempDir(), t.TempDir()
	cfg := Config{AuthorityStoreDir: authDir, LifecycleStoreDir: lifeDir, Listen: "127.0.0.1:0", WorkspaceRPC: true, WorkspaceHeadReader: reader}
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	defer closeTestMaster(t, h)
	if _, err := h.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node-b", DataAddr: "127.0.0.1:9201", CtrlAddr: "127.0.0.1:9101"}); err != nil {
		t.Fatal(err)
	}
	registerWorkspaceHolder(t, h, "node-a") // a workspace holder's server id must be registered (INV-AUTH-HOLDER-IDENTITY-001)
	seedVerifiedExistingReplicaPlacement(t, h)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-a")
	if line.Epoch != 1 || line.ReplicaID != "r2" {
		t.Fatal(line)
	}
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := control.NewWorkspaceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	id := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "vol-a", ReplicaId: "r2", Epoch: 1}
	stops := 0
	g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "vol-a", ReplicaID: "r2", Epoch: 1, Healthy: true}}, lostAckReply{client}, id,
		func(context.Context) (uint64, error) { stops++; return 9, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	q := &control.WorkspaceMove{RequestId: "clean-1", VolumeId: "vol-a", ExpectedReplicaId: "r2", ExpectedEpoch: 1, TargetReplicaId: "r2", TargetServerId: "node-b",
		TargetDataAddr: "127.0.0.1:9201", TargetCtrlAddr: "127.0.0.1:9101", Kind: "clean"}
	if _, err := client.MoveWorkspace(ctx, q); err == nil {
		t.Fatal("move without detached/ACK accepted")
	}
	if _, err := client.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	// The stop/ACK path keeps its production timeout. Resolve a lost reply
	// from the master's exact ACK, never from elapsed time or the stop alone.
	if err := g.Refresh(ctx); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected unknown ACK reply, got %v", err)
	}
	recorded, err := client.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-a"})
	if err != nil {
		t.Fatal("ACK witness unreadable", err)
	}
	ack := recorded.GetRevocationAck()
	if ack.GetIdentity().GetVolumeId() != "vol-a" || ack.GetIdentity().GetReplicaId() != "r2" || ack.GetIdentity().GetEpoch() != 1 || ack.GetHeadGen() != 9 {
		t.Fatalf("unknown ACK has no exact recorded witness: %+v", ack)
	}
	if g.Projection().Healthy || stops != 1 {
		t.Fatal("revoked frontend remained admitted")
	}
	if _, err := client.MoveWorkspace(ctx, q); err == nil {
		t.Fatal("ACK without detached HEAD accepted")
	}
	detached.Store(true)
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, err := control.NewAssignmentServiceClient(conn).SubscribeAssignments(streamCtx, &control.SubscribeRequest{VolumeId: "vol-a", ReplicaId: "r2", ServerId: "node-a"})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := stream.Recv()
	if err != nil || initial.ServerId != "node-a" || initial.ReplicaId != "r2" || initial.Epoch != 1 {
		t.Fatal("initial assignment", initial, err)
	}
	beforeLine, _ := h.Publisher().VolumeAuthorityLine("vol-a")
	beforeState := h.Publisher().WorkspaceState("vol-a")
	q.TargetReplicaId = "r1" // Only this fully prepared negative changes logical identity.
	if grant, err := client.MoveWorkspace(ctx, q); err == nil {
		t.Fatalf("v1 accepted logical replica change r2 -> r1: %v", grant)
	}
	afterLine, _ := h.Publisher().VolumeAuthorityLine("vol-a")
	if !reflect.DeepEqual(beforeLine, afterLine) || !reflect.DeepEqual(beforeState, h.Publisher().WorkspaceState("vol-a")) {
		t.Fatal("refused replica change altered authority or journal")
	}
	unchanged, err := client.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-a"})
	if err != nil || len(unchanged.GetMoves()) != 0 || unchanged.GetRevocationAck().GetHeadGen() != 9 {
		t.Fatal("refusal consumed ACK or wrote a move", unchanged, err)
	}
	q.TargetReplicaId = "r2"
	grant, err := client.MoveWorkspace(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if !grant.Issued || grant.NewEpoch != 2 || grant.HeadGen != 9 || grant.RemoteLsn != 42 || grant.OldHost != "node-a" || grant.NewHost != "node-b" {
		t.Fatal(grant)
	}
	delivered, err := stream.Recv()
	if err != nil || delivered.ServerId != "node-b" || delivered.ReplicaId != "r2" || delivered.Epoch != 2 || delivered.DataAddr != q.TargetDataAddr || delivered.CtrlAddr != q.TargetCtrlAddr || len(delivered.Peers) != 0 {
		t.Fatal("same-ID move was not delivered on the existing stream with no retired peer", delivered, err)
	}
	cancelStream()
	state, err := client.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-a"})
	if err != nil || len(state.GetMoves()) != 1 {
		t.Fatal(state, err)
	}
	if _, err := client.RenewWorkspaceLease(ctx, id); err == nil {
		t.Fatal("old authority renewed")
	}
	if _, err := client.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{ServerId: "node-b", VolumeId: "vol-a", ReplicaId: "r2", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Start()
	defer closeTestMaster(t, restarted)
	if line, ok := restarted.Publisher().VolumeAuthorityLine("vol-a"); !ok || line.Epoch != 2 || line.ReplicaID != "r2" {
		t.Fatal(line)
	}
	if state := restarted.Publisher().WorkspaceState("vol-a"); state == nil || len(state.Moves) != 1 || !state.Moves[0].Issued {
		t.Fatal(state)
	}
	// Journal isolation reads "serves" from durable state: the issued move's
	// target serves vol-a through its authority line alone (the placement
	// still names the old node), also on a restarted master.
	if p, ok := restarted.Lifecycle().Placements.GetPlacement("vol-a"); ok {
		for _, slot := range p.Slots {
			if slot.ServerID == "node-b" {
				t.Fatalf("placement already names the move target: %+v", p)
			}
		}
	}
	if !restarted.nodeServesVolumes("node-b") {
		t.Fatal("the issued move's target does not count as serving vol-a")
	}
}
