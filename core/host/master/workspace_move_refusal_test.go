package master

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// A refused move names the precondition that failed, in the gRPC status and
// in the master log: an unknown source, an unregistered target node, a
// target that is the source, or target endpoints that are not registered
// (with what is registered: the node's pair, or the volume's live bindings
// on that node).
func TestWorkspaceMoveRefusalNamesTheMissingPrecondition(t *testing.T) {
	logs := &syncBuffer{}
	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		Logger: log.New(logs, "", 0),
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
			return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}, nil
		}})
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ws, lc := control.NewWorkspaceServiceClient(conn), control.NewLifecycleServiceClient(conn)
	for _, n := range []lifecycle.NodeRegistration{
		{ServerID: "node-a", DataAddr: "127.0.0.1:19101", CtrlAddr: "127.0.0.1:19102"},
		{ServerID: "node-b", DataAddr: "127.0.0.1:19301", CtrlAddr: "127.0.0.1:19302"},
	} {
		if _, err := m.Lifecycle().Nodes.RegisterNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range []string{"placed", "unplaced", "bound"} {
		if _, err := lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: v, SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "placed", DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{VolumeID: "placed", ServerID: "node-a", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
	// "bound" has explicit per-volume bindings: its source on node-a and a
	// prepared target on node-b at slot 2.
	for _, b := range []*control.WorkspaceBinding{
		{VolumeId: "bound", ServerId: "node-a", DataAddr: "127.0.0.1:19121", CtrlAddr: "127.0.0.1:19122"},
		{VolumeId: "bound", ServerId: "node-b", DataAddr: "127.0.0.1:19321", CtrlAddr: "127.0.0.1:19322"},
	} {
		if _, err := ws.RegisterWorkspaceBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ws.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "bound", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal(err)
	}

	move := func(vol, server, data, ctrl string) *control.WorkspaceMove {
		return &control.WorkspaceMove{RequestId: vol + "-" + server + "-" + data, VolumeId: vol, ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1",
			TargetServerId: server, TargetDataAddr: data, TargetCtrlAddr: ctrl, Kind: "clean"}
	}
	for _, c := range []struct {
		name string
		q    *control.WorkspaceMove
		want string
	}{
		{"unknown source", move("unplaced", "node-b", "127.0.0.1:19301", "127.0.0.1:19302"), `move source unknown: no issued move or placement names replica "r1" at epoch 1`},
		{"unknown target node", move("placed", "node-z", "127.0.0.1:19301", "127.0.0.1:19302"), `move target node "node-z" is not registered`},
		{"target is the source", move("placed", "node-a", "127.0.0.1:19101", "127.0.0.1:19102"), `move target "node-a" is the current source host`},
		{"target on another host", move("placed", "node-b", "10.9.9.9:19311", "127.0.0.1:19312"),
			`move target endpoints 10.9.9.9:19311/127.0.0.1:19312 on "node-b" are not registered (not on the node's hosts: it registers 127.0.0.1:19301/127.0.0.1:19302; no per-volume binding is prepared)`},
		{"explicit binding elsewhere", move("bound", "node-b", "127.0.0.1:19301", "127.0.0.1:19302"),
			`move target endpoints 127.0.0.1:19301/127.0.0.1:19302 on "node-b" are not registered (live bindings on that node: 127.0.0.1:19321/127.0.0.1:19322)`},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := ws.MoveWorkspace(ctx, c.q)
			if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != c.want {
				t.Fatalf("refusal %v; want FailedPrecondition %q", err, c.want)
			}
			if line := `MoveWorkspace refused volume="` + c.q.VolumeId + `" request="` + c.q.RequestId + `": ` + c.want; !strings.Contains(logs.String(), line) {
				t.Fatalf("log lacks %q:\n%s", line, logs.String())
			}
		})
	}
	// Slot-0 and slot-1 targets on the node's host pass these preconditions
	// (they fail later, at the clean-move ACK check, for a different reason).
	for _, data := range []string{"127.0.0.1:19301", "127.0.0.1:19311"} {
		_, err = ws.MoveWorkspace(ctx, move("placed", "node-b", data, "127.0.0.1:19312"))
		if err == nil || strings.Contains(err.Error(), "move target") || strings.Contains(err.Error(), "move source") {
			t.Fatalf("target %s on the node's host: %v; want past the endpoint preconditions", data, err)
		}
	}
}

// blockingWriter blocks the first write that contains marker until
// released; other writes pass.
type blockingWriter struct {
	marker           string
	entered, release chan struct{}
	once             sync.Once
}

func (b *blockingWriter) Write(p []byte) (int, error) {
	if !strings.Contains(string(p), b.marker) {
		return len(p), nil
	}
	first := false
	b.once.Do(func() { first = true })
	if first {
		close(b.entered)
		<-b.release
	}
	return len(p), nil
}

// A refused move is logged after workspaceMu is released: while the log
// writer is blocked, another workspace RPC still gets the lock.
func TestWorkspaceMoveRefusalLogsOutsideTheWorkspaceLock(t *testing.T) {
	w := &blockingWriter{marker: "MoveWorkspace refused", entered: make(chan struct{}), release: make(chan struct{})}
	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, Logger: log.New(w, "", 0),
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) { return authority.WorkspaceHead{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	defer closeTestMaster(t, m)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(w.release) }) }
	defer release()
	conn, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ws := control.NewWorkspaceServiceClient(conn)
	if _, err := control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "v", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := ws.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: "r", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1",
			TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:19301", TargetCtrlAddr: "127.0.0.1:19302", Kind: "clean"})
		done <- err
	}()
	select {
	case <-w.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the refusal was never logged")
	}
	qctx, qcancel := context.WithTimeout(ctx, 5*time.Second)
	defer qcancel()
	if _, err := ws.QueryWorkspace(qctx, &control.WorkspaceVolume{VolumeId: "v"}); err != nil {
		t.Fatalf("workspace RPC while the refusal log is blocked: %v; want workspaceMu free", err)
	}
	release()
	select {
	case err := <-done:
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("move: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the move did not return after the log was released")
	}
}
