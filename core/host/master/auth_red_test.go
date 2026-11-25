package master

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// authMaster is a started blockmaster with mutual TLS and a role file:
// node-a and node-b (node), alice (operator), root (admin). dial opens a
// connection as one of them.
type authMaster struct {
	h    *Host
	dial func(who string) *grpc.ClientConn
}

func startAuthMaster(t *testing.T) authMaster {
	t.Helper()
	ca := authtest.NewCA(t, "test-ca")
	dir := t.TempDir()
	caPath := ca.WriteCA(t, dir)
	expiry := time.Now().Add(time.Hour)
	srvCert, srvKey := ca.Issue(t, "blockmaster", expiry).WriteFiles(t, dir, "master")
	leaves := map[string]*authtest.Leaf{}
	var lines []string
	for who, role := range map[string]string{"node-a": "node", "node-b": "node", "alice": "operator", "root": "admin"} {
		leaves[who] = ca.Issue(t, who, expiry)
		lines = append(lines, leaves[who].Fingerprint+" "+role+" "+who)
	}
	table, err := auth.ParseRoleFile(strings.NewReader(strings.Join(lines, "\n")))
	if err != nil {
		t.Fatal(err)
	}
	creds, err := link.ServerCredentials(link.TLSFiles{Cert: srvCert, Key: srvKey, CA: caPath})
	if err != nil {
		t.Fatal(err)
	}
	authn := auth.NewAuthenticator(table, nil)
	authn.SetAudit(auth.NewAuditLog(io.Discard, nil)) // strict calls need an audit log
	h, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
			return authority.WorkspaceHead{Gen: 2, Epoch: 1, Holder: "node-a/r1"}, nil
		},
		Auth: authn, ServerOptions: []grpc.ServerOption{creds}})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() { closeTestMaster(t, h) })
	for _, server := range []string{"node-a", "node-b"} {
		if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); err != nil {
			t.Fatal(err)
		}
	}
	return authMaster{h: h, dial: func(who string) *grpc.ClientConn {
		t.Helper()
		cert, key := leaves[who].WriteFiles(t, t.TempDir(), who)
		d, err := link.ClientCredentials(link.TLSFiles{Cert: cert, Key: key, CA: caPath}, "")
		if err != nil {
			t.Fatal(err)
		}
		c, err := link.DialMaster(h.Addr(), d)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}}
}

// lineOnNodeA creates v placed on node-a, publishes its line and grants
// node-a its lease, all through authenticated callers.
func (m authMaster) lineOnNodeA(t *testing.T, ctx context.Context) authority.AuthorityBasis {
	t.Helper()
	root, alice, nodeA := m.dial("root"), m.dial("alice"), m.dial("node-a")
	if _, err := control.NewLifecycleServiceClient(alice).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "v", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal("operator CreateVolume:", err)
	}
	b := &control.WorkspaceBinding{VolumeId: "v", ServerId: "node-a", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
	if _, err := control.NewWorkspaceServiceClient(root).RegisterWorkspaceBinding(ctx, b); err != nil {
		t.Fatal("admin RegisterWorkspaceBinding:", err)
	}
	if _, err := control.NewWorkspaceServiceClient(alice).PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "v", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal("operator PlaceWorkspace:", err)
	}
	if _, err := control.NewObservationServiceClient(nodeA).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "v", ReplicaId: "r1", DataAddr: b.DataAddr, CtrlAddr: b.CtrlAddr}}}); err != nil {
		t.Fatal("node-a heartbeat:", err)
	}
	if _, err := m.h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, m.h.Publisher(), "v")
	if _, err := control.NewWorkspaceServiceClient(nodeA).RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: line.Epoch, ServerId: "node-a"}); err != nil {
		t.Fatal("node-a renewal:", err)
	}
	return line
}

// T5 (phase 0 red): an operator's forced move is admitted today; it needs an
// admin (PERMISSION_DENIED before any barrier or lease check). An admin's
// forced move passes authorization (and then meets the barrier).
func TestForcedMoveNeedsAnAdmin(t *testing.T) {
	m := startAuthMaster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	line := m.lineOnNodeA(t, ctx)
	move := &control.WorkspaceMove{RequestId: "f1", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: line.Epoch, TargetReplicaId: "r1",
		TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:20000", TargetCtrlAddr: "127.0.0.1:20001", Kind: "forced", Force: true}
	_, err := control.NewWorkspaceServiceClient(m.dial("alice")).MoveWorkspace(ctx, move)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("an operator's forced move: %v; want PERMISSION_DENIED", err)
	}
	move.RequestId = "f2"
	_, err = control.NewWorkspaceServiceClient(m.dial("root")).MoveWorkspace(ctx, move)
	if code := status.Code(err); code == codes.PermissionDenied || code == codes.Unauthenticated {
		t.Fatalf("an admin's forced move refused by authorization: %v", err)
	}
}

// T5 / T8 (phase 0 red): a node acts only for its own server id. Today a
// heartbeat that names node-a but comes from node-b's certificate is
// accepted (only the addresses inside the report are compared), and node-b
// can renew node-a's lease (the address check passes on one host).
func TestNodeActsOnlyForItsOwnServerID(t *testing.T) {
	m := startAuthMaster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	line := m.lineOnNodeA(t, ctx)
	nodeB := m.dial("node-b")
	_, err := control.NewObservationServiceClient(nodeB).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "v", ReplicaId: "r1", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("node-b's heartbeat naming node-a: %v; want PERMISSION_DENIED", err)
	}
	_, err = control.NewWorkspaceServiceClient(nodeB).RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: line.Epoch, ServerId: "node-a"})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("node-b renewing node-a's lease: %v; want PERMISSION_DENIED", err)
	}
	_, err = control.NewWorkspaceServiceClient(nodeB).AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: line.Epoch, ServerId: "node-a"}, HeadGen: 2})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("node-b ACKing for node-a: %v; want PERMISSION_DENIED", err)
	}
	stream, err := control.NewAssignmentServiceClient(nodeB).SubscribeAssignments(ctx, &control.SubscribeRequest{VolumeId: "v", ReplicaId: "r1", ServerId: "node-a"})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("node-b subscribing as node-a: %v; want PERMISSION_DENIED", err)
	}
}
