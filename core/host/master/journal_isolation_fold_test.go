package master

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storeident"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const colocatedMoveRefusal = `move target node "node-b" shares the authority store's file system (strict journal isolation)`

// issueMove serves vol from node-a at epoch 1 and moves it cleanly to node-b
// through the Workspace RPC. It returns the request and its grant.
func (r *isolationRig) issueMove(t *testing.T, vol, requestID string) (*control.WorkspaceMove, *control.WorkspaceGrant) {
	t.Helper()
	r.createVolume(t, vol)
	r.place(t, vol, "node-a")
	if err := r.m.ObservationHost().Ingest(authority.Observation{ServerID: "node-a", ObservedAt: time.Now().UTC(),
		Slots: []authority.SlotFact{{VolumeID: vol, ReplicaID: "r1", DataAddr: "127.0.0.1:19131", CtrlAddr: "127.0.0.1:19132"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.m.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, r.m.Publisher(), vol)
	if line.Epoch != 1 || line.ReplicaID != "r1" {
		t.Fatalf("fixture authority line: %+v", line)
	}
	id := &control.WorkspaceIdentity{VolumeId: vol, ReplicaId: "r1", Epoch: 1}
	if _, err := r.ws.RenewWorkspaceLease(r.ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ws.RevokeWorkspace(r.ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ws.AckWorkspaceRevoked(r.ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
		t.Fatal(err)
	}
	q := &control.WorkspaceMove{RequestId: requestID, VolumeId: vol, ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1",
		TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:19301", TargetCtrlAddr: "127.0.0.1:19302", Kind: "clean"}
	grant, err := r.ws.MoveWorkspace(r.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if !grant.Issued || grant.NewEpoch != 2 || grant.NewHost != "node-b" {
		t.Fatalf("fixture grant: %+v", grant)
	}
	return q, grant
}

// Strict isolation refuses new work, never an issued decision: a move issued
// to node-b before node-b reported its identity, whose reply was lost, is
// retried after node-b turned out co-located. The SAME request gets the
// identical durable grant (no new epoch, no second record); a new request id
// or a changed body to node-b is refused for co-location. Also after a
// master restart, whose co-located set starts empty.
func TestStrictReplaysAnIssuedMoveToANowColocatedTarget(t *testing.T) {
	authorityDir, lifecycleDir := t.TempDir(), t.TempDir()
	r := newIsolationRig(t, true, authorityDir, lifecycleDir)
	q, issued := r.issueMove(t, "moved", "m1") // the caller never sees this reply
	check := func(r *isolationRig, what string) {
		t.Helper()
		if err := r.heartbeat("node-b", masterStore); err != nil {
			t.Fatalf("%s: the issued move's target was refused: %v", what, err)
		}
		if !r.m.colocatedTargetRefused("node-b") {
			t.Fatalf("%s: node-b not recorded as co-located", what)
		}
		before, _ := r.m.Publisher().VolumeAuthorityLine("moved")
		again, err := r.ws.MoveWorkspace(r.ctx, q)
		if err != nil {
			t.Fatalf("%s: retry of the issued request: %v, want its durable grant", what, err)
		}
		if !proto.Equal(again, issued) {
			t.Fatalf("%s: retry returned %+v, want the issued grant %+v", what, again, issued)
		}
		after, _ := r.m.Publisher().VolumeAuthorityLine("moved")
		if after != before || after.Epoch != issued.NewEpoch {
			t.Fatalf("%s: retry published again: line %+v -> %+v", what, before, after)
		}
		if st := r.m.Publisher().WorkspaceState("moved"); st == nil || len(st.Moves) != 1 || !st.Moves[0].Issued {
			t.Fatalf("%s: move journal after the retry: %+v", what, st)
		}
		fresh := proto.Clone(q).(*control.WorkspaceMove)
		fresh.RequestId = "m2"
		changed := proto.Clone(q).(*control.WorkspaceMove)
		changed.Kind, changed.Force = "forced", true
		for name, m := range map[string]*control.WorkspaceMove{"new request id": fresh, "same id, changed body": changed} {
			_, err := r.ws.MoveWorkspace(r.ctx, m)
			if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != colocatedMoveRefusal {
				t.Fatalf("%s: %s to the co-located node: %v, want %q", what, name, err, colocatedMoveRefusal)
			}
		}
		if final, _ := r.m.Publisher().VolumeAuthorityLine("moved"); final != before {
			t.Fatalf("%s: a refused request changed authority: %+v -> %+v", what, before, final)
		}
	}
	check(r, "live master")
	r.close(t)
	check(newIsolationRig(t, true, authorityDir, lifecycleDir), "restarted master")
}

// The recorded placement repeated after its node turned out co-located keeps
// its idempotent result; a different placement on that node is refused.
func TestStrictKeepsAnIdempotentPlacementOnANowColocatedNode(t *testing.T) {
	r := newIsolationRig(t, true, t.TempDir(), t.TempDir())
	r.createVolume(t, "fresh")
	if _, err := r.ws.RegisterWorkspaceBinding(r.ctx, &control.WorkspaceBinding{VolumeId: "fresh", ServerId: "node-b", DataAddr: "127.0.0.1:19321", CtrlAddr: "127.0.0.1:19322"}); err != nil {
		t.Fatal(err)
	}
	place := &control.WorkspacePlacement{VolumeId: "fresh", ServerId: "node-b", ReplicaId: "r1"}
	if _, err := r.ws.PlaceWorkspace(r.ctx, place); err != nil {
		t.Fatal(err)
	}
	if err := r.heartbeat("node-b", masterStore); err != nil {
		t.Fatalf("the placed node was refused: %v", err)
	}
	if !r.m.colocatedTargetRefused("node-b") {
		t.Fatal("node-b not recorded as co-located")
	}
	if _, err := r.ws.PlaceWorkspace(r.ctx, place); err != nil {
		t.Fatalf("repeating the recorded placement: %v, want its idempotent result", err)
	}
	_, err := r.ws.PlaceWorkspace(r.ctx, &control.WorkspacePlacement{VolumeId: "fresh", ServerId: "node-b", ReplicaId: "r2"})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), colocatedLine) {
		t.Fatalf("a different placement on the co-located node: %v, want a co-location refusal", err)
	}
}

// On a first start the authority store directory does not exist before
// Bootstrap creates it. The master must sample the created directory: a
// heartbeat carrying that file system's identity is co-located (alarm by
// default, refusal under strict) without any test override.
func TestFirstStartSamplesTheCreatedAuthorityStore(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(map[bool]string{true: "strict", false: "default"}[strict], func(t *testing.T) {
			authorityDir := filepath.Join(t.TempDir(), "authority") // created by Bootstrap
			r := startIsolationRig(t, strict, authorityDir, t.TempDir(), nil)
			self := storeident.Of(authorityDir)
			if !self.Known() {
				t.Skip("file system identity is unknown on this platform")
			}
			err := r.heartbeat("node-a", self)
			if strict {
				if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), colocatedLine) {
					t.Fatalf("strict heartbeat on the fresh store's file system: %v, want a co-location refusal", err)
				}
			} else if err != nil {
				t.Fatalf("default heartbeat refused: %v", err)
			}
			if !strings.Contains(r.logs.String(), "node node-a "+colocatedLine) || r.m.WorkspaceLeaseStatus().ColocatedNodes != 1 {
				t.Fatalf("co-located node on the fresh store not detected:\n%s", r.logs.String())
			}
		})
	}
}

// A heartbeat never waits on the publisher lock, which writers hold across
// durable writes: with a write held under that lock, the heartbeat of an
// issued move's target (serving only through that move) completes at once,
// and a co-located node that serves nothing is still refused at once.
func TestStrictServingHeartbeatDoesNotWaitOnAHeldAuthorityWrite(t *testing.T) {
	r := newIsolationRig(t, true, t.TempDir(), t.TempDir())
	r.issueMove(t, "moved", "m1")
	if p, _ := r.m.Lifecycle().Placements.GetPlacement("moved"); len(p.Slots) != 1 || p.Slots[0].ServerID != "node-a" {
		t.Fatalf("fixture: node-b must serve through the issued move only: %+v", p)
	}
	r.createVolume(t, "held")
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	held := make(chan error, 1)
	// RecordWorkspacePlacement holds the publisher lock across its durable
	// write, as an extension does across its authority fsync.
	go func() {
		held <- r.m.Publisher().RecordWorkspacePlacement(r.ctx, "held", func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	var beats sync.WaitGroup
	t.Cleanup(func() { // release first, then join every worker
		releaseOnce.Do(func() { close(release) })
		select {
		case <-held:
		case <-time.After(5 * time.Second):
			t.Error("held write did not settle")
		}
		beats.Wait()
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the held write never started")
	}
	beat := func(server string) error {
		t.Helper()
		done := make(chan error, 1)
		start := time.Now()
		beats.Add(1)
		go func() { defer beats.Done(); done <- r.heartbeat(server, masterStore) }()
		select {
		case err := <-done:
			t.Logf("heartbeat of %s took %s with the authority write held", server, time.Since(start).Round(time.Millisecond))
			return err
		case <-time.After(2 * time.Second):
			t.Fatalf("heartbeat of %s still blocked %s behind the held authority write", server, time.Since(start).Round(time.Millisecond))
			return nil
		}
	}
	if err := beat("node-b"); err != nil {
		t.Fatalf("serving co-located node refused: %v", err)
	}
	if err := beat("node-c"); status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), colocatedLine) {
		t.Fatalf("idle co-located node: %v, want a co-location refusal", err)
	}
}

// stalledStatusRequest opens a connection to the status endpoint and sends a
// request whose declared body never comes.
func stalledStatusRequest(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := fmt.Fprintf(conn, "POST /status/workspace-lease HTTP/1.1\r\nHost: status\r\nContent-Length: 100\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	return conn
}

// readUntilClosed reads conn until the server closes it; it fails the test
// when the connection is still open at deadline.
func readUntilClosed(t *testing.T, conn net.Conn, deadline time.Time, what string) {
	t.Helper()
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	_, err := io.Copy(io.Discard, conn)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("%s: the stalled status connection was still open at the deadline", what)
	}
}

// A status client that declares a body and never sends it is cut off by the
// read bound, not held open forever.
func TestStatusEndpointCutsOffAStalledBody(t *testing.T) {
	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), StatusListen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	defer closeTestMaster(t, m)
	start := time.Now()
	conn := stalledStatusRequest(t, m.StatusAddr())
	readUntilClosed(t, conn, start.Add(statusReadTimeout+3*time.Second), "read bound")
	t.Logf("stalled body cut off after %s (bound %s)", time.Since(start).Round(time.Millisecond), statusReadTimeout)
}

// When graceful shutdown runs out, Close force-closes the status connections
// still open, long before their read bound would.
func TestHostCloseForceClosesAStalledStatusConnection(t *testing.T) {
	logs := &syncBuffer{}
	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), StatusListen: "127.0.0.1:0", Logger: log.New(logs, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	conn := stalledStatusRequest(t, m.StatusAddr())
	time.Sleep(200 * time.Millisecond) // the server has read the headers: the connection is active
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := m.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	readUntilClosed(t, conn, start.Add(2*time.Second), "Close")
	if took := time.Since(start); took >= statusReadTimeout-time.Second {
		t.Fatalf("connection closed after %s: that is the read bound, not Close", took)
	}
	if !strings.Contains(logs.String(), "status graceful shutdown") {
		t.Fatalf("expired graceful shutdown not logged:\n%s", logs.String())
	}
}
