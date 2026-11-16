package master

import (
	"context"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"testing"
	"time"
)

type skewLeaseClient struct{ p *authority.Publisher }

func (c skewLeaseClient) RenewWorkspaceLease(ctx context.Context, id *control.WorkspaceIdentity, _ ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	l, err := c.p.RenewWorkspaceLease(ctx, id.VolumeId, id.ReplicaId, id.Epoch)
	if err != nil {
		return nil, err
	}
	return leaseFact(id.VolumeId, l), nil
}
func (skewLeaseClient) AckWorkspaceRevoked(context.Context, *control.WorkspaceRevocationAck, ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	panic("expiry must not ACK clean")
}

func TestWorkspaceSkewSeparatesOldServingFromForcedGrant(t *testing.T) {
	for _, check := range []string{"master-margin", "host-early-fence"} {
		t.Run(check, func(t *testing.T) {
			store, err := authority.NewFileAuthorityStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if err := store.Put(authority.DurableRecord{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1, DataAddr: "d", CtrlAddr: "c", WriteSeq: 1}); err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			masterNow, hostNow := start, start
			p := authority.NewPublisher(nil, authority.WithStore(store), authority.WithWorkspaceLeaseConfig(3*time.Second, func() time.Time { return masterNow }))
			g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}}, skewLeaseClient{p}, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return hostNow })
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			lease := p.WorkspaceState("v").Lease
			q := authority.WorkspaceMoveRequest{RequestID: "forced", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old", TargetHost: "new", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
			head := authority.WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old/r", RemoteLSN: 42}
			if check == "host-early-fence" {
				// Default skew is two nominal TTL/3 renewal intervals; host stops half that early.
				hostNow = start.Add(2 * time.Second)
				if g.Projection().Healthy {
					t.Fatal("old host serves past expiry minus half-skew")
				}
			} else {
				// Master advanced one tick farther than the old host since renewal.
				masterNow = time.Unix(0, lease.ExpiresAtNS)
				hostNow = start.Add(2900 * time.Millisecond)
				if _, err := p.MoveWorkspace(context.Background(), q, head); err == nil {
					t.Fatalf("forced grant before skew margin; old serving=%v", g.Projection().Healthy)
				}
				line, _ := p.VolumeAuthorityLine("v")
				if line.Epoch != 1 || len(p.WorkspaceState("v").Moves) != 0 {
					t.Fatal("early refusal changed authority/journal")
				}
			}
			masterNow = time.Unix(0, lease.ExpiresAtNS).Add(2 * time.Second)
			hostNow = start.Add(4 * time.Second)
			grant, err := p.MoveWorkspace(context.Background(), q, head)
			if err != nil {
				t.Fatal(err)
			}
			if !grant.Issued || grant.NewEpoch <= 1 || g.Projection().Healthy {
				t.Fatal("overlapping old serving at new grant", grant)
			}
		})
	}
}

func TestWorkspaceClockSkewConfigReachesLeaseReply(t *testing.T) {
	dir := t.TempDir()
	store, err := authority.NewFileAuthorityStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(authority.DurableRecord{VolumeID: "v", ReplicaID: "r", Epoch: 1, EndpointVersion: 1, DataAddr: "d", CtrlAddr: "c", WriteSeq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	h, err := New(Config{AuthorityStoreDir: dir, LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0", WorkspaceRPC: true, WorkspaceMaxClockSkew: 4 * time.Second, WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	defer closeTestMaster(t, h)
	if _, err := h.Lifecycle().Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "v", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	reply, err := (&services{host: h}).RenewWorkspaceLease(context.Background(), &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	lease := h.Publisher().WorkspaceState("v").Lease
	if lease.MaxClockSkewNS != int64(4*time.Second) || reply.ValidForNs <= 0 || reply.ValidForNs > int64(28*time.Second) {
		t.Fatal("configuration not applied to host validity", lease, reply)
	}
}
