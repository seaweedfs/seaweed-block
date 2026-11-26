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
