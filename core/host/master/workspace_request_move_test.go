package master

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"
)

func TestWorkspaceMoveRequestReplayAfterMasterRestart(t *testing.T) {
	forced := false
	cfg := Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
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
		src := &control.WorkspaceBinding{RequestId: vol + "-src", VolumeId: vol, ServerId: "node-a", DataAddr: fmt.Sprintf("127.0.0.1:%d", 19200+i), CtrlAddr: fmt.Sprintf("127.0.0.1:%d", 19100+i)}
		target := &control.WorkspaceBinding{RequestId: vol + "-target", VolumeId: vol, ServerId: "node-b", DataAddr: fmt.Sprintf("127.0.0.1:%d", 29200+i), CtrlAddr: fmt.Sprintf("127.0.0.1:%d", 29100+i)}
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
		id := &control.WorkspaceIdentity{VolumeId: vol, ReplicaId: "r1", Epoch: line.Epoch}
		lease, e := client.RenewWorkspaceLease(ctx, id)
		if e != nil {
			t.Fatal(e)
		}
		if forced {
			select {
			case <-time.After(time.Until(time.Unix(0, lease.ExpiresAtNs)) + 20*time.Millisecond):
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
	cfg.WorkspaceHeadReader = func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{}, fmt.Errorf("HEAD unavailable after completed move")
	}
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
		q := &control.WorkspaceMove{RequestId: v + "-move", VolumeId: v, ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: state.Moves[0].NewHost, Kind: "clean"}
		for _, b := range state.Bindings {
			if b.ServerId == "node-b" {
				q.TargetDataAddr = b.DataAddr
				q.TargetCtrlAddr = b.CtrlAddr
			}
		}
		replay, e := control.NewWorkspaceServiceClient(c2).MoveWorkspace(ctx, q)
		if e != nil || !proto.Equal(replay, state.Moves[0]) {
			t.Fatalf("lost response replay after restart: %v %v", replay, e)
		}
		if replay.NewHolder != "node-b/r1" || replay.JournalRecordId != v+"-move" || replay.HeadGen != 9 || replay.RemoteLsn != 42 {
			t.Fatalf("incomplete move result: %v", replay)
		}
		q.TargetCtrlAddr = "127.0.0.1:29999"
		if _, e = control.NewWorkspaceServiceClient(c2).MoveWorkspace(ctx, q); status.Code(e) != codes.AlreadyExists {
			t.Fatalf("changed move request_id accepted: %v", e)
		}
		for _, b := range state.Bindings {
			if b.ServerId == "node-a" {
				q := proto.Clone(b).(*control.WorkspaceBinding)
				q.RequestId = v + "-src"
				q.Retired = false
				original, e := control.NewWorkspaceServiceClient(c2).RegisterWorkspaceBinding(ctx, q)
				if e != nil || original.Retired || original.DataAddr != b.DataAddr || original.CtrlAddr != b.CtrlAddr {
					t.Fatalf("original bind result lost after retirement: %v %v", original, e)
				}
			}
		}
		verify, e := control.NewWorkspaceServiceClient(c2).QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: v})
		if e != nil {
			t.Fatal(e)
		}
		for _, b := range verify.Bindings {
			if b.ServerId == "node-a" && !b.Retired {
				t.Fatal("replay reactivated retired source", b)
			}
		}
		filtered, e := control.NewWorkspaceServiceClient(c2).QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: v, RequestId: v + "-move"})
		if e != nil || len(filtered.Moves) != 1 || !proto.Equal(filtered.Moves[0], replay) {
			t.Fatalf("journal lookup: %v %v", filtered, e)
		}
		missing, e := control.NewWorkspaceServiceClient(c2).QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: v, RequestId: "not-this-request"})
		if e != nil || len(missing.Moves) != 0 {
			t.Fatalf("journal filter: %v %v", missing, e)
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
