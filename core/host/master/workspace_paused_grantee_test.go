package master

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A grantee renews its issued lease, then pauses before claiming HEAD. The
// forced move away from it is admitted only after the GRANT's lease expiry
// plus skew (the predecessor's older lease never satisfies it), and is
// granted on the predecessor's unchanged HEAD. The grantee resumes before
// the new owner claims: HEAD's epoch has not moved yet, so the epoch alone
// would let it claim; the authority check at attach (its lease-gated
// projection) refuses it. The new owner then claims at the higher epoch.
func TestWorkspacePausedGranteeRefusedAtAttach(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := authority.NewFileAuthorityStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	const ttl = 3 * time.Second
	start := time.Now()
	masterNow, hostNow := start, start
	dir := &skewDirective{asks: make(chan authority.AssignmentAsk, 1)}
	p := authority.NewPublisher(dir, authority.WithStore(store), authority.WithWorkspaceLeaseConfig(ttl, func() time.Time { return masterNow }))
	seedSkewPublisher(t, p, dir) // v bound to r at epoch 1
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	oldBarrier := time.Unix(0, p.WorkspaceState("v").Lease.ExpiresAtNS)

	remote := storage.NewMemChunkRemote()
	readHead := func() authority.WorkspaceHead {
		t.Helper()
		h, err := remote.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return authority.WorkspaceHead{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN, Detached: h.State == storage.ChunkHeadDetached}
	}
	open := func(holder string, view frontend.ProjectionView) *durable.DurableProvider {
		t.Helper()
		prov, err := durable.NewDurableProvider(durable.ProviderConfig{
			Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
			ChunkHolder: holder, ChunkAssignmentTimeout: 2 * time.Second,
			ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
		}, view)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = prov.Close() })
		if _, err := prov.EnsureStorage("v"); err != nil {
			t.Fatal(err)
		}
		if _, err := prov.RecoverVolume(ctx, "v"); err != nil {
			t.Fatalf("%s recovery: %v", holder, err)
		}
		return prov
	}
	healthy := func(epoch uint64) restoreProjection {
		return restoreProjection{frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: epoch, EndpointVersion: 1, Healthy: true}}
	}

	// The predecessor writes, cuts and detaches; the clean move grants new-node.
	oldProv := open("old-node/r", healthy(1))
	b, err := oldProv.Open(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := b.Write(ctx, 0, bytes.Repeat([]byte{0xA5}, 4096)); err != nil || n != 4096 {
		t.Fatal(n, err)
	}
	if err := b.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := oldProv.CaptureVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	detached, _, err := oldProv.DetachVolume(ctx, "v")
	if err != nil || detached.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("detach: %+v %v", detached.Head, err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", authority.WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: detached.Head.Gen}); err != nil {
		t.Fatal(err)
	}
	// The lease reply's validity is measured on the real clock: keep the
	// fake clocks at real time for the move and the grantee's renewal.
	masterNow = time.Now()
	if min := start.Add(500 * time.Millisecond); masterNow.Before(min) {
		masterNow = min
	}
	hostNow = masterNow
	clean, err := p.MoveWorkspace(ctx, authority.WorkspaceMoveRequest{RequestID: "clean", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old-node", TargetHost: "new-node", ExpectedEpoch: 1,
		TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "clean"}, readHead())
	if err != nil || clean.NewEpoch != 2 {
		t.Fatal("clean move:", clean, err)
	}
	predecessor := readHead()

	// The grantee renews (its guard is valid), then pauses before its claim.
	guard, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 2, EndpointVersion: 1, Healthy: true}}, skewLeaseClient{p},
		&control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 2}, func(context.Context) (uint64, error) { return 0, nil }, func() time.Time { return hostNow })
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Refresh(ctx); err != nil {
		t.Fatal("grantee renewal:", err)
	}
	if !guard.Projection().Healthy {
		t.Fatal("grantee not serving after its renewal")
	}
	lease := p.WorkspaceState("v").Lease
	if lease == nil || lease.Epoch != 2 {
		t.Fatalf("grantee lease %+v", lease)
	}
	skew := time.Duration(lease.MaxClockSkewNS)
	grantBarrier := time.Unix(0, lease.ExpiresAtNS).Add(skew)
	forced := authority.WorkspaceMoveRequest{RequestID: "forced", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "new-node", TargetHost: "third-node", ExpectedEpoch: 2,
		TargetDataAddr: "td", TargetCtrlAddr: "tc", Kind: "forced", Force: true}

	// After the predecessor's lease (plus skew) but before the grant's: refused.
	masterNow = oldBarrier.Add(skew).Add(grantBarrier.Sub(oldBarrier.Add(skew)) / 2)
	if !masterNow.After(oldBarrier.Add(skew)) || !masterNow.Before(grantBarrier) {
		t.Fatalf("test clock: %s not between %s and %s", masterNow, oldBarrier.Add(skew), grantBarrier)
	}
	if _, err := p.MoveWorkspace(ctx, forced, readHead()); err == nil {
		t.Fatal("forced move admitted on the predecessor's lease before the grant's lease expired")
	}
	masterNow = grantBarrier
	r, err := p.MoveWorkspace(ctx, forced, readHead())
	if err != nil || r.NewEpoch != 3 || r.HeadGen != predecessor.Gen {
		t.Fatal("forced move after the grant's lease expired:", r, err)
	}

	// The grantee resumes before the new owner claims. Its attach is refused.
	hostNow = masterNow
	if guard.Projection().Healthy {
		t.Fatal("the paused grantee still serves after the forced move")
	}
	granteeProv := open("new-node/r", guard)
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if h := readHead(); h != predecessor {
			t.Fatalf("the paused grantee claimed HEAD after the forced move: %+v, was %+v", h, predecessor)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if bk, err := granteeProv.Open(ctx, "v"); err == nil {
		if n, err := bk.Write(ctx, 0, make([]byte, 4096)); err == nil && n == 4096 {
			if err := bk.Sync(ctx); err == nil {
				if _, err := granteeProv.CaptureVolume(ctx, "v"); err == nil {
					t.Fatal("the paused grantee published a cut after the forced move")
				}
			}
		}
	}
	if h := readHead(); h != predecessor {
		t.Fatalf("HEAD changed by the paused grantee: %+v", h)
	}

	// The new owner claims at the higher epoch.
	open("third-node/r", healthy(3))
	if h := readHead(); h.Epoch != 3 || h.Holder != "third-node/r" || h.RemoteLSN != predecessor.RemoteLSN {
		t.Fatalf("HEAD after the new owner's claim: %+v", h)
	}
}
