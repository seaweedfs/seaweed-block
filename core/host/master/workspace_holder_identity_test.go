package master

import (
	"context"
	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func leaseHolderIdentity(t *testing.T, epoch uint64, server string) *control.WorkspaceIdentity {
	t.Helper()
	q := &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: epoch}
	b, err := proto.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	if server != "" {
		b = protowire.AppendTag(b, 4, protowire.BytesType)
		b = protowire.AppendString(b, server)
	}
	if err = proto.Unmarshal(b, q); err != nil {
		t.Fatal(err)
	}
	return q
}

func TestWorkspaceLeaseRequiresHolderServerForGrantRenewAndRegrant(t *testing.T) {
	var nowNS atomic.Int64
	nowNS.Store(time.Now().UnixNano())
	h, conn, _ := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}, nil
	}, func(h *Host) {
		authority.WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return time.Unix(0, nowNS.Load()) })(h.Publisher())
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	reviewCreate(t, conn, ctx, "v")
	ws := control.NewWorkspaceServiceClient(conn)
	b := &control.WorkspaceBinding{VolumeId: "v", ServerId: "node-a", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
	if _, err := ws.RegisterWorkspaceBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "v", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal(err)
	}
	reviewHeartbeat(t, conn, ctx, "v", "node-a", b.DataAddr, b.CtrlAddr)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "v")
	refuse := func(stage string, epoch uint64) {
		t.Helper()
		for _, server := range []string{"", "node-b"} {
			before := h.Publisher().WorkspaceState("v")
			if _, err := ws.RenewWorkspaceLease(ctx, leaseHolderIdentity(t, epoch, server)); err == nil {
				t.Errorf("%s accepted lease for server %q", stage, server)
			}
			after := h.Publisher().WorkspaceState("v")
			if !reflect.DeepEqual(before, after) {
				t.Errorf("%s refused identity changed lease", stage)
			}
		}
	}
	refuse("first grant", line.Epoch)
	id := leaseHolderIdentity(t, line.Epoch, "node-a")
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal("matching server grant", err)
	}
	refuse("renew", line.Epoch)
	nowNS.Add(int64(2 * time.Minute)) // beyond both the old horizon and clock-skew barrier
	refuse("expired re-grant", line.Epoch)
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal("matching server expired re-grant", err)
	}
	if _, err := ws.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.RegisterWorkspaceBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	g, err := ws.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: "same-host", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: line.Epoch, TargetReplicaId: "r1", TargetServerId: "node-a", TargetDataAddr: b.DataAddr, TargetCtrlAddr: b.CtrlAddr, Kind: "clean"})
	if err != nil {
		t.Fatal(err)
	}
	refuse("re-grant", g.NewEpoch)
	if _, err := ws.RenewWorkspaceLease(ctx, leaseHolderIdentity(t, g.NewEpoch, "node-a")); err != nil {
		t.Fatal("matching server re-grant", err)
	}
}

// INV-AUTH-HOLDER-IDENTITY-001: an ACK licenses a clean move, so only the
// primary server/replica pair may record it, not another host of the same r1.
func TestWorkspaceRevocationACKRequiresHolderServer(t *testing.T) {
	for _, server := range []string{"", "node-b"} {
		t.Run("server="+server, func(t *testing.T) {
			h, conn, ctx := reviewBindingHost(t)
			reviewCreate(t, conn, ctx, "v")
			ws := control.NewWorkspaceServiceClient(conn)
			b := &control.WorkspaceBinding{VolumeId: "v", ServerId: "node-a", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
			if _, err := ws.RegisterWorkspaceBinding(ctx, b); err != nil {
				t.Fatal(err)
			}
			if _, err := ws.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "v", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
				t.Fatal(err)
			}
			reviewHeartbeat(t, conn, ctx, "v", "node-a", b.DataAddr, b.CtrlAddr)
			if _, err := h.RunLifecycleProductTick(); err != nil {
				t.Fatal(err)
			}
			line := waitAuthorityLine(t, h.Publisher(), "v")
			own := leaseHolderIdentity(t, line.Epoch, "node-a")
			if _, err := ws.RenewWorkspaceLease(ctx, own); err != nil {
				t.Fatal(err)
			}
			if _, err := ws.RevokeWorkspace(ctx, own); err != nil {
				t.Fatal(err)
			}
			if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: leaseHolderIdentity(t, line.Epoch, server), HeadGen: 9}); err == nil {
				t.Errorf("ACK accepted server %q with the right replica/epoch", server)
			}
			recorded, err := ws.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "v"})
			if err != nil {
				t.Fatal(err)
			}
			if recorded.GetRevocationAck() != nil {
				t.Errorf("foreign ACK recorded: %v", recorded.GetRevocationAck())
			}
			if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: own, HeadGen: 9}); err != nil {
				t.Fatal("own ACK refused", err)
			}
			recorded, err = ws.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "v"})
			if err != nil || recorded.GetRevocationAck().GetHeadGen() != 9 {
				t.Fatal("own ACK not recorded", recorded, err)
			}
		})
	}
}
