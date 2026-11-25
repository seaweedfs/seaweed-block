package master

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkspaceBindingPlacesRF1OnChosenNode(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := control.NewWorkspaceServiceClient(conn)
	if _, err = control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "chosen", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node-chosen", DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); err != nil {
		t.Fatal(err)
	}
	b := &control.WorkspaceBinding{VolumeId: "chosen", ServerId: "node-chosen", DataAddr: "127.0.0.1:19200", CtrlAddr: "127.0.0.1:19100"}
	if _, err = client.RegisterWorkspaceBinding(ctx, b); err != nil {
		t.Fatalf("register chosen per-volume binding: %v", err)
	}
	if _, err = client.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "chosen", ServerId: "node-chosen", ReplicaId: "r1"}); err != nil {
		t.Fatal("place chosen RF1", err)
	}
	p, ok := h.Lifecycle().Placements.GetPlacement("chosen")
	if !ok || len(p.Slots) != 1 || p.Slots[0].ServerID != "node-chosen" || p.Slots[0].DataAddr != b.DataAddr || p.Slots[0].CtrlAddr != b.CtrlAddr {
		t.Fatal("wrong chosen intent", p)
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("chosen"); ok {
		t.Fatal("placement minted authority without observation")
	}
	if _, err = control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-chosen", SentAt: timestamppb.Now(), Reachable: true, Eligible: true, Slots: []*control.HeartbeatSlot{{VolumeId: "chosen", ReplicaId: "r1", DataAddr: b.DataAddr, CtrlAddr: b.CtrlAddr}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "chosen")
	if line.ReplicaID != "r1" || line.DataAddr != b.DataAddr || line.CtrlAddr != b.CtrlAddr {
		t.Fatal("wrong chosen assignment", line)
	}
	if _, err = client.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "chosen", ServerId: "node-chosen", ReplicaId: "different"}); err == nil {
		t.Fatal("assigned volume accepted new placement")
	}
}

func TestWorkspaceBindingsMoveTwoVolumesToOneHost(t *testing.T) {
	workspaceBindingsMove(t, false)
}

func TestWorkspaceBindingsForcedRetiresOnlyAfterIssue(t *testing.T) {
	workspaceBindingsMove(t, true)
}

func workspaceBindingsMove(t *testing.T, forced bool) {
	cfg := Config{WorkspaceMaxClockSkew: 100 * time.Millisecond, Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, LayoutCreateVolume: testLayoutCreateVolume, WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}, nil
	}}
	h, err := New(cfg)
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
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := control.NewWorkspaceServiceClient(conn)
	for _, server := range []string{"node-a", "node-b"} {
		if _, err = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); err != nil {
			t.Fatal(err)
		}
	}
	volumes := []string{"vol-one", "vol-two"}
	if forced {
		volumes = volumes[:1]
	}
	for i, vol := range volumes {
		if _, err = control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: vol, SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
			t.Fatal(err)
		}
		src := &control.WorkspaceBinding{VolumeId: vol, ServerId: "node-a", DataAddr: fmt.Sprintf("127.0.0.1:%d", 19200+i), CtrlAddr: fmt.Sprintf("127.0.0.1:%d", 19100+i)}
		target := &control.WorkspaceBinding{VolumeId: vol, ServerId: "node-b", DataAddr: fmt.Sprintf("127.0.0.1:%d", 29200+i), CtrlAddr: fmt.Sprintf("127.0.0.1:%d", 29100+i)}
		for _, b := range []*control.WorkspaceBinding{src, target} {
			if _, err = client.RegisterWorkspaceBinding(ctx, b); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = client.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: vol, ServerId: "node-a", ReplicaId: "r1"}); err != nil {
			t.Fatal(err)
		}
		if _, err = control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true, Slots: []*control.HeartbeatSlot{{VolumeId: vol, ReplicaId: "r1", DataAddr: src.DataAddr, CtrlAddr: src.CtrlAddr}}}); err != nil {
			t.Fatal(err)
		}
		if _, err = h.RunLifecycleProductTick(); err != nil {
			t.Fatal(err)
		}
		line := waitAuthorityLine(t, h.Publisher(), vol)
		id := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: vol, ReplicaId: "r1", Epoch: line.Epoch}
		lease, e := client.RenewWorkspaceLease(ctx, id)
		if e != nil {
			t.Fatal(e)
		}
		if forced {
			// The forced barrier is the durable horizon (>= every grant) plus
			// skew, not the reply's grant (#205).
			horizon := lease.ExpiresAtNs
			if st := h.Publisher().WorkspaceState(vol); st != nil && st.Lease != nil && st.Lease.ExpiresAtNS > horizon {
				horizon = st.Lease.ExpiresAtNS
			}
			select {
			case <-time.After(time.Until(time.Unix(0, horizon)) + 100*time.Millisecond + 20*time.Millisecond):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		} else {
			if _, err = client.RevokeWorkspace(ctx, id); err != nil {
				t.Fatal(err)
			}
			if _, err = client.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
				t.Fatal(err)
			}
		}
		before, e := client.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: vol})
		if e != nil {
			t.Fatal(e)
		}
		for _, b := range before.Bindings {
			if b.ServerId == "node-a" && b.Retired == forced {
				t.Fatal("source retirement occurred at wrong boundary", b)
			}
		}
		q := &control.WorkspaceMove{RequestId: vol + "-bad", VolumeId: vol, ExpectedReplicaId: "r1", ExpectedEpoch: line.Epoch, TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:9200", TargetCtrlAddr: "127.0.0.1:9100", Kind: "clean"}
		if forced {
			q.Kind = "forced"
			q.Force = true
		}
		if _, err = client.MoveWorkspace(ctx, q); err == nil {
			t.Fatal("explicit volume fell back to legacy node pair")
		}
		q.RequestId = vol + "-move"
		q.TargetDataAddr = target.DataAddr
		q.TargetCtrlAddr = target.CtrlAddr
		if _, err = client.MoveWorkspace(ctx, q); err != nil {
			t.Fatal("prepared per-volume target refused", err)
		}
		got, ok := h.Publisher().VolumeAuthorityLine(vol)
		if !ok || got.Epoch != 2 || got.DataAddr != target.DataAddr || got.CtrlAddr != target.CtrlAddr {
			t.Fatal("wrong moved authority", got)
		}
		placement, _ := h.Lifecycle().Placements.GetPlacement(vol)
		if placement.Slots[0].ServerID != "node-a" {
			t.Fatal("move rewrote source intent", placement)
		}
		state, err := client.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: vol})
		if err != nil {
			t.Fatal(err)
		}
		if len(state.Bindings) != 2 {
			t.Fatal("missing bindings", state)
		}
		for _, b := range state.Bindings {
			if (b.ServerId == "node-a") != b.Retired {
				t.Fatal("wrong binding lifecycle", b)
			}
		}
	}
	closeTestMaster(t, h)
	h2, err := New(cfg)
	if err != nil {
		t.Fatal("restart", err)
	}
	h2.Start()
	defer closeTestMaster(t, h2)
	c2, err := grpc.NewClient(h2.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for _, v := range volumes {
		state, e := control.NewWorkspaceServiceClient(c2).QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: v})
		if e != nil {
			t.Fatal(e)
		}
		if len(state.Bindings) != 2 || len(state.Moves) != 1 || state.Moves[0].NewEpoch != 2 {
			t.Fatal("restart lost binding/grant", state)
		}
		for _, b := range state.Bindings {
			if (b.ServerId == "node-a") != b.Retired {
				t.Fatal("restart changed retirement", b)
			}
		}
	}
}

func TestWorkspaceBindingsRejectCollisionsAndLiveChangesAndDeleteCleans(t *testing.T) {
	h, e := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, LayoutCreateVolume: testLayoutCreateVolume, WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }})
	if e != nil {
		t.Fatal(e)
	}
	h.Start()
	defer closeTestMaster(t, h)
	c, e := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, life := control.NewWorkspaceServiceClient(c), control.NewLifecycleServiceClient(c)
	for _, v := range []string{"a", "b", "c"} {
		if _, e = life.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: v, SizeBytes: 1 << 20, ReplicationFactor: 1}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node", DataAddr: "127.0.0.1:9000", CtrlAddr: "127.0.0.1:9001"}); e != nil {
		t.Fatal(e)
	}
	a := &control.WorkspaceBinding{VolumeId: "a", ServerId: "unknown", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
	if _, e = client.RegisterWorkspaceBinding(ctx, a); e == nil {
		t.Fatal("unknown server accepted")
	}
	a.ServerId = "node"
	for i := 0; i < 2; i++ {
		if _, e = client.RegisterWorkspaceBinding(ctx, a); e != nil {
			t.Fatal("idempotent registration", e)
		}
	}
	changed := &control.WorkspaceBinding{VolumeId: a.VolumeId, ServerId: a.ServerId, DataAddr: "127.0.0.1:10002", CtrlAddr: a.CtrlAddr}
	if _, e = client.RegisterWorkspaceBinding(ctx, changed); e == nil {
		t.Fatal("live binding changed")
	}
	for _, pair := range [][2]string{{a.DataAddr, a.CtrlAddr}, {a.CtrlAddr, "127.0.0.1:10003"}, {"127.0.0.1:010000", "127.0.0.1:10003"}, {"[::ffff:127.0.0.1]:10000", "127.0.0.1:10003"}} {
		if _, e = client.RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: "b", ServerId: "node", DataAddr: pair[0], CtrlAddr: pair[1]}); e == nil {
			t.Fatal("live endpoint collision accepted")
		}
	}
	if _, e = life.DeleteVolume(ctx, &control.DeleteVolumeRequest{VolumeId: "a"}); e != nil {
		t.Fatal(e)
	}
	if _, bound, e := h.Lifecycle().WorkspaceBindings.Get("a"); e != nil || bound {
		t.Fatal("deleted volume retained binding", bound, e)
	}
	a.VolumeId = "c"
	if _, e = client.RegisterWorkspaceBinding(ctx, a); e != nil {
		t.Fatal("deleted volume did not release endpoints", e)
	}
}
