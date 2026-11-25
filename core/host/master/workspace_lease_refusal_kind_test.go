package master

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// refusalOf returns the FailedPrecondition refusal's text and its typed
// detail (nil when the refusal carries none, which a client reads as OTHER).
func refusalOf(t *testing.T, stage string, err error) (string, *control.WorkspaceLeaseRefusal) {
	t.Helper()
	st, ok := status.FromError(err)
	if err == nil || !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("%s: %v; want a FailedPrecondition refusal", stage, err)
	}
	for _, d := range st.Details() {
		if r, ok := d.(*control.WorkspaceLeaseRefusal); ok {
			return st.Message(), r
		}
	}
	return st.Message(), nil
}

// Each renewal refusal carries its typed kind as a status detail with the
// code and text unchanged: BARRIER (with the time left until the barrier
// passes) for an expired re-grant before the barrier; MOVED only when the
// line names another replica; DELETED only for a committed deletion. The
// same owner at a newer epoch and an admitted (abortable) deletion carry no
// detail (OTHER): a bounded wait, never an early 78.
func TestLeaseRefusalKinds(t *testing.T) {
	var nowNS, headEpoch, headGen atomic.Int64
	var headHolder atomic.Value
	headHolder.Store("node-a/r1")
	nowNS.Store(time.Now().UnixNano())
	headEpoch.Store(1)
	headGen.Store(9)
	const ttl = 30 * time.Second
	h, conn, _ := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{Gen: uint64(headGen.Load()), Epoch: uint64(headEpoch.Load()), Holder: headHolder.Load().(string), RemoteLSN: 42, Detached: true}, nil
	}, func(h *Host) {
		authority.WithWorkspaceLeaseConfig(ttl, func() time.Time { return time.Unix(0, nowNS.Load()) })(h.Publisher())
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
	nodeB := &control.WorkspaceBinding{VolumeId: "v", ServerId: "node-b", DataAddr: "127.0.0.1:20000", CtrlAddr: "127.0.0.1:20001"}
	id := func(server string, epoch uint64) *control.WorkspaceIdentity {
		return &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: epoch, ServerId: server}
	}
	e1 := id("node-a", line.Epoch)
	assignment := func(server string) error {
		probeCtx, end := context.WithTimeout(ctx, 3*time.Second)
		defer end()
		stream, err := control.NewAssignmentServiceClient(conn).SubscribeAssignments(probeCtx, &control.SubscribeRequest{VolumeId: "v", ReplicaId: "r1", ServerId: server})
		if err == nil {
			_, err = stream.Recv()
		}
		return err
	}
	if err := assignment("node-b"); err != nil {
		t.Fatal("a prestarted target was mistaken for a moved source", err)
	}
	if _, err := ws.RenewWorkspaceLease(ctx, e1); err != nil {
		t.Fatal("first grant:", err)
	}

	// BARRIER: past the grant (TTL) but before the horizon (1.5 TTL) plus
	// the default skew (2/3 TTL), i.e. 65 s after the grant.
	nowNS.Add(int64(40 * time.Second))
	text, r := refusalOf(t, "expired re-grant before the barrier", func() error { _, err := ws.RenewWorkspaceLease(ctx, e1); return err }())
	if r == nil || r.GetKind() != control.WorkspaceLeaseRefusal_BARRIER {
		t.Fatalf("re-grant before the barrier: detail %v; want BARRIER", r)
	}
	if left := time.Duration(r.GetRetryAfterNs()); left < 24*time.Second || left > 26*time.Second {
		t.Fatalf("BARRIER retry_after %v; want 25 s (the barrier 65 s after the grant, now +40 s)", left)
	}
	if !strings.HasPrefix(text, "workspace forced move lease expiry plus clock-skew margin has not elapsed") || !strings.Contains(text, "re-grant refused until") {
		t.Fatalf("BARRIER text %q: want the barrier text with its time", text)
	}
	nowNS.Add(int64(2 * time.Minute))
	if _, err := ws.RenewWorkspaceLease(ctx, e1); err != nil {
		t.Fatal("re-grant after the barrier:", err)
	}

	// cleanMove revokes and ACKs the current holder, then moves the line to
	// the binding's host.
	cleanMove := func(req string, from *control.WorkspaceIdentity, to *control.WorkspaceBinding) uint64 {
		t.Helper()
		if _, err := ws.RevokeWorkspace(ctx, from); err != nil {
			t.Fatal(err)
		}
		headEpoch.Store(int64(from.GetEpoch()))
		headHolder.Store(from.GetServerId() + "/r1")
		headGen.Add(1)
		if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: from, HeadGen: uint64(headGen.Load())}); err != nil {
			t.Fatal(err)
		}
		if _, err := ws.RegisterWorkspaceBinding(ctx, to); err != nil {
			t.Fatal(err)
		}
		g, err := ws.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: req, VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: from.GetEpoch(),
			TargetReplicaId: "r1", TargetServerId: to.GetServerId(), TargetDataAddr: to.GetDataAddr(), TargetCtrlAddr: to.GetCtrlAddr(), Kind: "clean"})
		if err != nil {
			t.Fatal(err)
		}
		return g.GetNewEpoch()
	}

	// OTHER: the same owner at a newer epoch (a clean move back to this host).
	e2 := id("node-a", cleanMove("same-host", e1, b))
	text, r = refusalOf(t, "same owner, newer epoch", func() error { _, err := ws.RenewWorkspaceLease(ctx, e1); return err }())
	if r != nil || text != "workspace lease: server identity does not match the current holder" {
		t.Fatalf("same owner at a newer epoch: %q detail %v; want the unchanged text, no detail", text, r)
	}
	if _, err := ws.RenewWorkspaceLease(ctx, e2); err != nil {
		t.Fatal("grant at the newer epoch:", err)
	}

	// MOVED: a newer epoch owned by another known server. (A line naming
	// another replica is MOVED too; a v1 move keeps the replica id.)
	e3 := id("node-b", cleanMove("other-host", e2, nodeB))
	text, r = refusalOf(t, "moved to another host", func() error { _, err := ws.RenewWorkspaceLease(ctx, e2); return err }())
	if r == nil || r.GetKind() != control.WorkspaceLeaseRefusal_MOVED || text != "workspace lease: server identity does not match the current holder" {
		t.Fatalf("moved to another host: %q detail %v; want MOVED with the unchanged text", text, r)
	}
	text, r = refusalOf(t, "restarted source assignment stream", assignment("node-a"))
	if r == nil || r.GetKind() != control.WorkspaceLeaseRefusal_MOVED || !strings.Contains(text, "holder node-b at epoch ") {
		t.Fatalf("restarted source did not receive destination-labelled MOVED: %q %v", text, r)
	}

	// Move back to node-a: the stale node-a process at e2 is the same owner at
	// a newer epoch, so OTHER (no detail), never a false MOVED.
	if _, err := ws.RenewWorkspaceLease(ctx, e3); err != nil {
		t.Fatal("grant to the moved host:", err)
	}
	e4 := id("node-a", cleanMove("move-back", e3, b))
	if err := assignment("node-a"); err != nil {
		t.Fatal("a move back was rejected by historical source evidence", err)
	}
	text, r = refusalOf(t, "moved and moved back", func() error { _, err := ws.RenewWorkspaceLease(ctx, e2); return err }())
	if r != nil || text != "workspace lease: server identity does not match the current holder" {
		t.Fatalf("moved and moved back: %q detail %v; want the unchanged text, no detail", text, r)
	}

	// An admitted deletion is OTHER; a committed one is DELETED.
	if _, err := ws.RenewWorkspaceLease(ctx, e4); err != nil {
		t.Fatal("grant after the move back:", err)
	}
	if _, err := ws.RevokeWorkspace(ctx, e4); err != nil {
		t.Fatal(err)
	}
	headEpoch.Store(int64(e4.GetEpoch()))
	headHolder.Store("node-a/r1")
	headGen.Add(1)
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: e4, HeadGen: uint64(headGen.Load())}); err != nil {
		t.Fatal(err)
	}
	res, err := control.NewLifecycleServiceClient(conn).MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "del-1"})
	if err != nil || res.GetState() != lifecycle.ReservationAdmitted {
		t.Fatalf("admit the delete: %+v %v", res, err)
	}
	text, r = refusalOf(t, "admitted deletion", func() error { _, err := ws.RenewWorkspaceLease(ctx, e4); return err }())
	if r != nil || text != "workspace: volume is being deleted" {
		t.Fatalf("admitted deletion: %q detail %v; want the unchanged text, no detail", text, r)
	}
	if _, err := h.lifecycle.Deletions.Commit("v", "del-1"); err != nil {
		t.Fatal(err)
	}
	text, r = refusalOf(t, "committed deletion", func() error { _, err := ws.RenewWorkspaceLease(ctx, e4); return err }())
	if r == nil || r.GetKind() != control.WorkspaceLeaseRefusal_DELETED || text != "workspace: volume is being deleted" {
		t.Fatalf("committed deletion: %q detail %v; want DELETED with the unchanged text", text, r)
	}
}

// MOVED needs a line at a newer epoch than the caller's: another replica or
// another known owner. A line at or below the caller's epoch, the same owner,
// an unknown owner or a caller without a server id is OTHER (F6).
func TestRenewalReplicaMismatchAtOlderLineEpochIsOther(t *testing.T) {
	owner := func(s string) func() string { return func() string { return s } }
	for _, c := range []struct {
		name                  string
		lineEpoch             uint64
		lineReplica, lineHost string
		server                string
		epoch                 uint64
		want                  bool
	}{
		{"another replica at an older line epoch", 2, "r2", "node-b", "node-a", 3, false},
		{"another replica at the same epoch", 3, "r2", "node-b", "node-a", 3, false},
		{"another replica at a newer epoch", 4, "r2", "node-b", "node-a", 3, true},
		{"another known owner at a newer epoch", 4, "r1", "node-b", "node-a", 3, true},
		{"the same owner at a newer epoch (move and move back)", 4, "r1", "node-a", "node-a", 3, false},
		{"an unknown owner at a newer epoch", 4, "r1", "", "node-a", 3, false},
		{"no server id on the call", 4, "r1", "node-b", "", 3, false},
	} {
		if got := lineMovedFrom(c.lineEpoch, c.lineReplica, owner(c.lineHost), "r1", c.server, c.epoch); got != c.want {
			t.Errorf("%s: MOVED=%v; want %v", c.name, got, c.want)
		}
	}
}

// A holder whose server id is not registered is refused with the typed
// IDENTITY detail (kind unregistered), on the lease renewal and on the
// assignment stream, text unchanged; the volume waits on it like refused
// credentials instead of exiting 75.
func TestIdentityRefusalIsTyped(t *testing.T) {
	h, conn, _ := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{Gen: 1, Epoch: 1, Holder: "node-a/r1", Detached: true}, nil
	}, nil)
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
	check := func(rpc string, err error) {
		t.Helper()
		text, r := refusalOf(t, rpc, err)
		if r == nil || r.GetKind() != control.WorkspaceLeaseRefusal_IDENTITY || r.GetIdentity() != "unregistered" ||
			!strings.Contains(text, `server id "node-x" is not registered`) {
			t.Fatalf("%s from an unregistered server: %q detail %v; want IDENTITY unregistered, text unchanged", rpc, text, r)
		}
	}
	_, err := ws.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: line.Epoch, ServerId: "node-x"})
	check("renewal", err)
	stream, err := control.NewAssignmentServiceClient(conn).SubscribeAssignments(ctx, &control.SubscribeRequest{VolumeId: "v", ReplicaId: "r1", ServerId: "node-x"})
	if err == nil {
		_, err = stream.Recv()
	}
	check("assignment stream", err)
}
