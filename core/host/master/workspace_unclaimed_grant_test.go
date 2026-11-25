package master

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// seedDetachedHead installs the predecessor's detached final cut.
func seedDetachedHead(t *testing.T, ctx context.Context, remote *storage.MemChunkRemote, holder string, epoch, lsn uint64) storage.ChunkHead {
	t.Helper()
	h := storage.ChunkHead{Epoch: epoch, Holder: holder, RemoteLSN: lsn, State: storage.ChunkHeadDetached, DetachedBy: &storage.ChunkDetachedBy{Holder: holder, Epoch: epoch, Gen: 1}}
	gen, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, h)
	if err != nil {
		t.Fatal(err)
	}
	h.Gen = gen
	return h
}

// A clean move grants vol-a to node-b, whose host dies before its first
// renewal and so before it claims HEAD: HEAD still names node-a. A forced
// move to node-c is refused before the lease the issue started has expired
// plus the skew bound, without reading HEAD; after it, it reads HEAD (only
// then) and is granted on the predecessor's unchanged HEAD. A late claim by
// the dead grantee never displaces the new owner's higher epoch.
func TestWorkspaceUnclaimedGrantForcedReassignOverRPC(t *testing.T) {
	// The real lease horizon (1.5 x the 30 s TTL) plus skew takes ~45 s.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	remote := storage.NewMemChunkRemote()
	var reads atomic.Int64
	var lastReadNS atomic.Int64
	reader := func(ctx context.Context, _ string) (authority.WorkspaceHead, error) {
		reads.Add(1)
		lastReadNS.Store(time.Now().UnixNano())
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

	for _, server := range []string{"node-a", "node-b", "node-c"} {
		if _, err = m.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "vol-a", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	src := &control.WorkspaceBinding{VolumeId: "vol-a", ServerId: "node-a", DataAddr: "127.0.0.1:19200", CtrlAddr: "127.0.0.1:19100"}
	dst := &control.WorkspaceBinding{VolumeId: "vol-a", ServerId: "node-b", DataAddr: "127.0.0.1:29200", CtrlAddr: "127.0.0.1:29100"}
	third := &control.WorkspaceBinding{VolumeId: "vol-a", ServerId: "node-c", DataAddr: "127.0.0.1:39200", CtrlAddr: "127.0.0.1:39100"}
	for _, b := range []*control.WorkspaceBinding{src, dst, third} {
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

	// node-a detaches its final cut and ACKs; the clean move grants node-b.
	seed := seedDetachedHead(t, ctx, remote, "node-a/"+oldLine.ReplicaID, oldLine.Epoch, 42)
	if _, err = ws.RevokeWorkspace(ctx, oldID); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: oldID, HeadGen: seed.Gen}); err != nil {
		t.Fatal(err)
	}
	clean, err := ws.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: "clean-1", VolumeId: "vol-a", ExpectedReplicaId: "r1", ExpectedEpoch: oldLine.Epoch, TargetReplicaId: "r1",
		TargetServerId: "node-b", TargetDataAddr: dst.DataAddr, TargetCtrlAddr: dst.CtrlAddr, Kind: "clean"})
	if err != nil {
		t.Fatal("clean move:", err)
	}
	grantee := clean.GetNewEpoch()
	st := m.Publisher().WorkspaceState("vol-a")
	if st == nil || st.Lease == nil || st.Lease.ReplicaID != "r1" || st.Lease.Epoch != grantee || st.Lease.Revoking {
		t.Fatalf("the issued move must start node-b's lease: %+v", st)
	}
	barrier := time.Unix(0, st.Lease.ExpiresAtNS).Add(skew)
	if q, err := ws.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-a"}); err != nil || q.GetLease() == nil || q.GetLease().GetEpoch() != grantee {
		t.Fatalf("QueryWorkspace after the issue: %+v %v", q.GetLease(), err)
	}

	// node-b dies before renewing or claiming. Before expiry plus skew the
	// forced move is refused, and HEAD is not read for it.
	forced := &control.WorkspaceMove{RequestId: "forced-1", VolumeId: "vol-a", ExpectedReplicaId: "r1", ExpectedEpoch: grantee, TargetReplicaId: "r1",
		TargetServerId: "node-c", TargetDataAddr: third.DataAddr, TargetCtrlAddr: third.CtrlAddr, Kind: "forced", Force: true}
	before := reads.Load()
	if _, err := ws.MoveWorkspace(ctx, forced); err == nil || !strings.Contains(err.Error(), "expiry plus clock-skew margin has not elapsed") {
		t.Fatalf("forced move before the issued lease expired: %v", err)
	}
	if n := reads.Load() - before; n != 0 {
		t.Fatalf("HEAD read %d times for a forced move whose admission had not passed", n)
	}

	select {
	case <-time.After(time.Until(barrier) + 50*time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	grant, err := ws.MoveWorkspace(ctx, forced)
	if err != nil {
		t.Fatal("forced move after the issued lease expired:", err)
	}
	if reads.Load() == before || time.Unix(0, lastReadNS.Load()).Before(barrier) {
		t.Fatalf("the forced move's HEAD was not read after admission (reads %d, last read %s, barrier %s)", reads.Load()-before, time.Unix(0, lastReadNS.Load()), barrier)
	}
	line, _ := m.Publisher().VolumeAuthorityLine("vol-a")
	if !grant.Issued || grant.NewEpoch != grantee+1 || line.Epoch != grant.NewEpoch || grant.HeadGen != seed.Gen || grant.RemoteLsn != seed.RemoteLSN {
		t.Fatalf("forced grant %+v line %+v; want epoch %d on HEAD gen %d lsn %d", grant, line, grantee+1, seed.Gen, seed.RemoteLSN)
	}
	if _, err := ws.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "vol-a", ReplicaId: "r1", Epoch: grantee}); err == nil {
		t.Fatal("the dead grantee's epoch renewed after the forced move")
	}
}
