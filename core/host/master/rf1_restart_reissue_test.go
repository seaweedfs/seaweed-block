package master

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A runtime-created RF1 workspace volume (CreateVolume, no static topology)
// is served only by the product loop's direct path. When its holder
// restarts at the same addresses (a new process, a new incarnation), the
// line it holds names the dead process, which the new one refuses to adopt;
// the next tick re-issues the line for the new process (a new endpoint
// version naming its incarnation), as the topology controller does for a
// static volume. A tick with the same process re-issues nothing.
func TestRF1RuntimeVolumeIsReissuedToARestartedHolder(t *testing.T) {
	rf1ReissueCase(t, "proc-a")
}

// A move mints its target's line at issue, before the target starts (the
// documented order), so the line names no process. On the product loop's
// direct path the started target is re-issued the line for its process
// (a line that names none differs from any process), and serves.
func TestRF1RuntimeLineNamingNoProcessIsReissued(t *testing.T) {
	rf1ReissueCase(t, "")
}

// rf1ReissueCase: the line is first issued for incarnation first ("" = the
// slot reports no process, so the line names none); then proc-b reports at
// the same addresses and must be re-issued the line.
func rf1ReissueCase(t *testing.T, first string) {
	t.Helper()
	h, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, LayoutCreateVolume: testLayoutCreateVolume, WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	defer closeTestMaster(t, h)
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := control.NewWorkspaceServiceClient(conn)
	if _, err = control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "rt", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node-a", DataAddr: "127.0.0.1:9201", CtrlAddr: "127.0.0.1:9101"}); err != nil {
		t.Fatal(err)
	}
	b := &control.WorkspaceBinding{VolumeId: "rt", ServerId: "node-a", DataAddr: "127.0.0.1:19201", CtrlAddr: "127.0.0.1:19101"}
	if _, err = client.RegisterWorkspaceBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err = client.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "rt", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal(err)
	}
	report := func(incarnation string) {
		t.Helper()
		if _, err := control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
			Slots: []*control.HeartbeatSlot{{VolumeId: "rt", ReplicaId: "r1", DataAddr: b.DataAddr, CtrlAddr: b.CtrlAddr, Incarnation: incarnation}}}); err != nil {
			t.Fatal(err)
		}
	}
	tick := func() LifecycleProductTickResult {
		t.Helper()
		r, err := h.RunLifecycleProductTick()
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	report(first)
	tick()
	issued := waitAuthorityLine(t, h.Publisher(), "rt")
	if issued.ReplicaID != "r1" || issued.Incarnation != first {
		t.Fatalf("first line %+v; want r1 issued for %q", issued, first)
	}

	// The same report again: nothing is re-issued.
	report(first)
	tick()
	if line, _ := h.Publisher().VolumeAuthorityLine("rt"); line.EndpointVersion != issued.EndpointVersion || line.Incarnation != first {
		t.Fatalf("line %+v re-issued for the same report; want %+v", line, issued)
	}

	// The holder (restarted, or the move's target started) reports proc-b.
	report("proc-b")
	tick()
	deadline := time.Now().Add(5 * time.Second)
	for {
		line, _ := h.Publisher().VolumeAuthorityLine("rt")
		if line.Incarnation == "proc-b" {
			if line.ReplicaID != "r1" || line.Epoch != issued.Epoch || line.EndpointVersion != issued.EndpointVersion+1 || line.DataAddr != b.DataAddr || line.CtrlAddr != b.CtrlAddr {
				t.Fatalf("re-issued line %+v; want r1 at epoch %d, EV %d, same addresses", line, issued.Epoch, issued.EndpointVersion+1)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("line %+v still names %q after the holder restarted as proc-b: the restarted holder never adopts it", line, line.Incarnation)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A refresh decided before a move (a holder restarted at the source after a
// lost ACK answer, the tick building its refresh) and applied after the
// operator's clean move to another host never rewrites the moved line: it is
// submitted through the normal publication path and refused there, so the
// line stays the target's, at the move's epoch.
func TestRF1RefreshDecidedBeforeACrossHostMoveIsNotApplied(t *testing.T) {
	head := authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 1, Detached: true}
	h, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, LayoutCreateVolume: testLayoutCreateVolume,
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return head, nil }})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	defer closeTestMaster(t, h)
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ws := control.NewWorkspaceServiceClient(conn)
	if _, err = control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "rt", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	for _, n := range []lifecycle.NodeRegistration{{ServerID: "node-a", DataAddr: "127.0.0.1:9201", CtrlAddr: "127.0.0.1:9101"}, {ServerID: "node-b", DataAddr: "127.0.0.1:9202", CtrlAddr: "127.0.0.1:9102"}} {
		if _, err = h.Lifecycle().Nodes.RegisterNode(n); err != nil {
			t.Fatal(err)
		}
	}
	src := &control.WorkspaceBinding{VolumeId: "rt", ServerId: "node-a", DataAddr: "127.0.0.1:19201", CtrlAddr: "127.0.0.1:19101"}
	dst := &control.WorkspaceBinding{VolumeId: "rt", ServerId: "node-b", DataAddr: "127.0.0.1:19202", CtrlAddr: "127.0.0.1:19102"}
	if _, err = ws.RegisterWorkspaceBinding(ctx, src); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "rt", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal(err)
	}
	report := func(server string, b *control.WorkspaceBinding, incarnation string) {
		t.Helper()
		if _, err := control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: server, SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
			Slots: []*control.HeartbeatSlot{{VolumeId: "rt", ReplicaId: "r1", DataAddr: b.DataAddr, CtrlAddr: b.CtrlAddr, Incarnation: incarnation}}}); err != nil {
			t.Fatal(err)
		}
	}
	report("node-a", src, "proc-a")
	if _, err = h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "rt")
	id := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "rt", ReplicaId: "r1", Epoch: line.Epoch}
	if _, err = ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
		t.Fatal(err)
	}

	// The source holder restarts (a lost ACK answer, exit 75) and reports a
	// new process at the source; the tick decides its refresh now.
	report("node-a", src, "proc-b")
	stale, restarted := h.restartedHolderRefresh(authority.AssignmentAsk{VolumeID: "rt", ReplicaID: "r1", DataAddr: src.DataAddr, CtrlAddr: src.CtrlAddr, Intent: authority.IntentBind})
	if !restarted {
		t.Fatal("fixture: no refresh decided for the restarted source")
	}

	// The operator's clean move lands first.
	if _, err = ws.RegisterWorkspaceBinding(ctx, dst); err != nil {
		t.Fatal(err)
	}
	grant, err := ws.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: "clean-1", VolumeId: "rt", ExpectedReplicaId: "r1", ExpectedEpoch: line.Epoch,
		TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: dst.DataAddr, TargetCtrlAddr: dst.CtrlAddr, Kind: "clean"})
	if err != nil || !grant.Issued {
		t.Fatalf("clean move: %v %v", grant, err)
	}
	moved := waitAuthorityReplica(t, h.Publisher(), "rt", "r1")
	for deadline := time.Now().Add(5 * time.Second); moved.Epoch != grant.NewEpoch && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		moved, _ = h.Publisher().VolumeAuthorityLine("rt")
	}
	if moved.Epoch != grant.NewEpoch || moved.DataAddr != dst.DataAddr {
		t.Fatalf("moved line %+v; want r1@%d at the target", moved, grant.NewEpoch)
	}

	// Then the stale refresh is submitted.
	if err := h.ctrl.SubmitAssignmentAsk(stale); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if after, _ := h.Publisher().VolumeAuthorityLine("rt"); after.Epoch != moved.Epoch || after.EndpointVersion != moved.EndpointVersion || after.DataAddr != dst.DataAddr || after.CtrlAddr != dst.CtrlAddr {
		t.Fatalf("line %+v after the stale refresh; want the moved line %+v (the target's addresses)", after, moved)
	}
}
