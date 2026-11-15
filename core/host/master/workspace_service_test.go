package master

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type workspaceRPCView struct{ p frontend.Projection }

func (v workspaceRPCView) Projection() frontend.Projection { return v.p }

func TestWorkspaceRPCCleanMoveRecordsAckAndJournal(t *testing.T) {
	var detached atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/vol-a/HEAD" {
			http.NotFound(w, r)
			return
		}
		state := "attached"
		if detached.Load() {
			state = "detached"
		}
		fmt.Fprintf(w, `{"format":1,"gen":9,"epoch":1,"holder":"r2","remote_lsn":42,"state":%q}`, state)
	}))
	defer server.Close()
	reader, err := NewWorkspaceHEADReader(server.URL)
	if err != nil {
		t.Fatal(err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := &control.WorkspaceIdentity{VolumeId: "vol-a", Holder: "r2", Epoch: 1}
	stops := 0
	g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "vol-a", ReplicaID: "r2", Epoch: 1, Healthy: true}}, client, id,
		func(context.Context) (uint64, error) { stops++; return 9, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	q := &control.WorkspaceMove{RequestId: "clean-1", VolumeId: "vol-a", ExpectedHolder: "r2", ExpectedEpoch: 1, TargetHolder: "r1", TargetServerId: "node-b",
		TargetDataAddr: "127.0.0.1:9201", TargetCtrlAddr: "127.0.0.1:9101", Kind: "clean"}
	if _, err := client.MoveWorkspace(ctx, q); err == nil {
		t.Fatal("move without detached/ACK accepted")
	}
	if _, err := client.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if g.Projection().Healthy || stops != 1 {
		t.Fatal("revoked frontend remained admitted")
	}
	if _, err := client.MoveWorkspace(ctx, q); err == nil {
		t.Fatal("ACK without detached HEAD accepted")
	}
	detached.Store(true)
	grant, err := client.MoveWorkspace(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if !grant.Issued || grant.NewEpoch != 2 || grant.HeadGen != 9 || grant.RemoteLsn != 42 || grant.OldHost != "node-a" || grant.NewHost != "node-b" {
		t.Fatal(grant)
	}
	state, err := client.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-a"})
	if err != nil || len(state.GetMoves()) != 1 {
		t.Fatal(state, err)
	}
	if _, err := client.RenewWorkspaceLease(ctx, id); err == nil {
		t.Fatal("old authority renewed")
	}
	if _, err := client.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "vol-a", Holder: "r1", Epoch: 2}); err != nil {
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
	if line, ok := restarted.Publisher().VolumeAuthorityLine("vol-a"); !ok || line.Epoch != 2 || line.ReplicaID != "r1" {
		t.Fatal(line)
	}
	if state := restarted.Publisher().WorkspaceState("vol-a"); state == nil || len(state.Moves) != 1 || !state.Moves[0].Issued {
		t.Fatal(state)
	}
}
