package master

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A holder that exited never detaches or ACKs, so its live authority blocks
// a volume delete. The existing path recovers the volume: after the lease
// horizon plus skew a forced move grants epoch+1 on another host, that
// host claims HEAD and serves the last cut, then detaches with a clean ACK,
// and the delete is admitted. Real master runtime, real providers, the
// in-memory ChunkRemote (read by the master through a HEAD reader).
func TestWorkspaceExitedHolderForcedMoveThenCleanDelete(t *testing.T) {
	// The real lease horizon (1.5 x the 30 s TTL) plus skew takes ~45 s of
	// this budget; the rest must cover the work after it under load and -race.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	reader := func(ctx context.Context, _ string) (authority.WorkspaceHead, error) {
		h, err := remote.GetHead(ctx)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		return authority.WorkspaceHead{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN, Detached: h.State == storage.ChunkHeadDetached}, nil
	}
	const skew = 100 * time.Millisecond
	m, err := New(Config{WorkspaceMaxClockSkew: skew, Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true, LayoutCreateVolume: testLayoutCreateVolume, WorkspaceHeadReader: reader})
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
	ws, lc := control.NewWorkspaceServiceClient(conn), control.NewLifecycleServiceClient(conn)

	// vol-a placed on node-a as r1; node-b is a prepared target.
	for _, server := range []string{"node-a", "node-b"} {
		if _, err = m.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	src := &control.WorkspaceBinding{VolumeId: "vol-a", ServerId: "node-a", DataAddr: "127.0.0.1:19200", CtrlAddr: "127.0.0.1:19100"}
	dst := &control.WorkspaceBinding{VolumeId: "vol-a", ServerId: "node-b", DataAddr: "127.0.0.1:29200", CtrlAddr: "127.0.0.1:29100"}
	for _, b := range []*control.WorkspaceBinding{src, dst} {
		if _, err = ws.RegisterWorkspaceBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = ws.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "vol-a", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal(err)
	}
	if _, err = control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "vol-a", ReplicaId: "r1", DataAddr: src.DataAddr, CtrlAddr: src.CtrlAddr}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = m.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	oldLine := waitAuthorityLine(t, m.Publisher(), "vol-a")
	oldID := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "vol-a", ReplicaId: oldLine.ReplicaID, Epoch: oldLine.Epoch}
	if _, err = ws.RenewWorkspaceLease(ctx, oldID); err != nil {
		t.Fatal(err)
	}

	open := func(host string, line authority.AuthorityBasis) (*durable.DurableProvider, frontend.Backend) {
		t.Helper()
		p, err := durable.NewDurableProvider(durable.ProviderConfig{
			Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
			ChunkHolder:    host + "/" + line.ReplicaID,
			ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
		}, restoreProjection{frontend.Projection{VolumeID: "vol-a", ReplicaID: line.ReplicaID, Epoch: line.Epoch, EndpointVersion: line.EndpointVersion, Healthy: true}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		if _, err := p.EnsureStorage("vol-a"); err != nil {
			t.Fatal(err)
		}
		if _, err := p.RecoverVolume(ctx, "vol-a"); err != nil {
			t.Fatalf("%s recovery: %v", host, err)
		}
		b, err := p.Open(ctx, "vol-a")
		if err != nil {
			t.Fatal(err)
		}
		return p, b
	}

	// The holder writes, cuts, then exits without detaching or ACKing.
	oldProvider, oldBackend := open("node-a", oldLine)
	want := bytes.Repeat([]byte{0xE1}, 4096)
	if n, err := oldBackend.Write(ctx, 8*4096, want); err != nil || n != len(want) {
		t.Fatal(n, err)
	}
	if err := oldBackend.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	cut, err := oldProvider.CaptureVolume(ctx, "vol-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := oldProvider.Close(); err != nil {
		t.Fatal(err)
	}
	head, err := remote.GetHead(ctx)
	if err != nil || head.RemoteLSN != cut || head.State == storage.ChunkHeadDetached {
		t.Fatalf("HEAD after the holder exited: %+v %v", head, err)
	}

	// The exited holder's live authority blocks the delete and a clean move.
	if _, err := lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "vol-a", Token: "del-early"}); err == nil || !strings.Contains(err.Error(), "live authority requires clean revocation ACK") {
		t.Fatalf("delete under the exited holder's live authority: %v; want the clean revocation ACK refusal", err)
	}
	move := &control.WorkspaceMove{RequestId: "clean-1", VolumeId: "vol-a", ExpectedReplicaId: "r1", ExpectedEpoch: oldLine.Epoch, TargetReplicaId: "r1",
		TargetServerId: "node-b", TargetDataAddr: dst.DataAddr, TargetCtrlAddr: dst.CtrlAddr, Kind: "clean"}
	if _, err := ws.MoveWorkspace(ctx, move); err == nil {
		t.Fatal("clean move granted without the holder's detach and ACK")
	}
	move.RequestId, move.Kind, move.Force = "forced-1", "forced", true
	if _, err := ws.MoveWorkspace(ctx, move); err == nil {
		t.Fatal("forced move granted before the lease horizon plus skew")
	}

	// After the durable lease horizon plus skew, the forced move is granted.
	horizon := int64(0)
	if st := m.Publisher().WorkspaceState("vol-a"); st != nil && st.Lease != nil {
		horizon = st.Lease.ExpiresAtNS
	}
	if horizon == 0 {
		t.Fatal("no lease horizon recorded")
	}
	select {
	case <-time.After(time.Until(time.Unix(0, horizon)) + skew + 20*time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	grant, err := ws.MoveWorkspace(ctx, move)
	if err != nil {
		t.Fatal("forced move after the horizon:", err)
	}
	newLine, ok := m.Publisher().VolumeAuthorityLine("vol-a")
	if !grant.Issued || grant.NewEpoch != oldLine.Epoch+1 || !ok || newLine.Epoch != grant.NewEpoch || grant.HeadGen != head.Gen || grant.RemoteLsn != cut {
		t.Fatalf("forced grant %+v line %+v; want epoch %d bound to HEAD gen %d lsn %d", grant, newLine, oldLine.Epoch+1, head.Gen, cut)
	}
	if _, err := ws.RenewWorkspaceLease(ctx, oldID); err == nil {
		t.Fatal("the exited holder's authority renewed after the forced move")
	}

	// The new host claims HEAD, serves the last cut and continues past it.
	newProvider, newBackend := open("node-b", newLine)
	claimed, err := remote.GetHead(ctx)
	if err != nil || claimed.Epoch != newLine.Epoch || claimed.Holder != "node-b/r1" || claimed.RemoteLSN != cut || claimed.State == storage.ChunkHeadDetached {
		t.Fatalf("HEAD after the new host's recovery: %+v %v", claimed, err)
	}
	got := make([]byte, len(want))
	if n, err := newBackend.Read(ctx, 8*4096, got); err != nil || n != len(got) || !bytes.Equal(got, want) {
		t.Fatalf("new host read of the last cut: n=%d err=%v", n, err)
	}
	if n, err := newBackend.Write(ctx, 0, bytes.Repeat([]byte{0xE2}, 4096)); err != nil || n != 4096 {
		t.Fatal(n, err)
	}
	if err := newBackend.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if newCut, err := newProvider.CaptureVolume(ctx, "vol-a"); err != nil || newCut <= cut {
		t.Fatalf("new host cut %d err %v; want past %d", newCut, err, cut)
	}

	// The new holder detaches cleanly and ACKs; the delete is then admitted.
	newID := &control.WorkspaceIdentity{ServerId: "node-b", VolumeId: "vol-a", ReplicaId: newLine.ReplicaID, Epoch: newLine.Epoch}
	if _, err := ws.RenewWorkspaceLease(ctx, newID); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.RevokeWorkspace(ctx, newID); err != nil {
		t.Fatal(err)
	}
	detached, _, err := newProvider.DetachVolume(ctx, "vol-a")
	if err != nil || detached.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("detach: %+v %v", detached.Head, err)
	}
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: newID, HeadGen: detached.Head.Gen}); err != nil {
		t.Fatal(err)
	}
	res, err := lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "vol-a", Token: "del-1"})
	if err != nil || !res.GetKnown() || res.GetState() != lifecycle.ReservationAdmitted {
		t.Fatalf("delete after the clean ACK: %+v %v", res, err)
	}
	if _, err := ws.RenewWorkspaceLease(ctx, newID); err == nil {
		t.Fatal("authority renewed after the delete was admitted")
	}
	move.RequestId, move.ExpectedEpoch = "forced-2", newLine.Epoch
	// The RPC refuses first; publication admission refuses it again behind it.
	if _, err := ws.MoveWorkspace(ctx, move); err == nil || !strings.Contains(err.Error(), "workspace: volume is being deleted") {
		t.Fatalf("move after the delete was admitted: %v; want the RPC's deletion refusal", err)
	}
}
