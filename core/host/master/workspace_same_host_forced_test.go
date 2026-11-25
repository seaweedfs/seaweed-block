package master

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/protobuf/proto"
)

type sameHostRecoveryView struct{ epoch uint64 }

func (v sameHostRecoveryView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v", ReplicaID: "r1", Epoch: v.epoch, Healthy: true}
}

// The child is a separate successor process with a different data directory.
// Its admission must still use the host-wide lock, not the root's WAL lock.
func TestWorkspaceSameHostSuccessorLockProbe(t *testing.T) {
	locks := os.Getenv("SWBLOCK_SAMEHOST_LOCK_PROBE")
	if locks == "" {
		return
	}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), NumBlocks: 16, BlockSize: 4096,
		HostVolumeLock: true, VolumeLockDirTestOnly: locks,
	}, sameHostRecoveryView{2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_, err = p.EnsureStorage("v")
	if os.Getenv("SWBLOCK_SAMEHOST_EXPECT_LOCKED") == "yes" {
		if !errors.Is(err, storage.ErrWALStoreLocked) {
			t.Fatalf("live predecessor did not exclude the successor: %v", err)
		}
	} else if err != nil {
		t.Fatal("successor still excluded after predecessor close", err)
	}
}

func TestWorkspaceRPCSameHostForcedRecoveryRecordsLoss(t *testing.T) {
	var nowNS atomic.Int64
	nowNS.Store(time.Now().UnixNano())
	var observed atomic.Pointer[authority.WorkspaceHead]
	observed.Store(&authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42})
	h, conn, ctx := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) {
		return *observed.Load(), nil
	}, func(h *Host) {
		authority.WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return time.Unix(0, nowNS.Load()) })(h.Publisher())
	})
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
	if _, err := ws.RenewWorkspaceLease(ctx, leaseHolderIdentity(t, line.Epoch, "node-a")); err != nil {
		t.Fatal(err)
	}
	guard, err := volume.NewWorkspaceLeaseGuard(sameHostRecoveryView{line.Epoch}, ws,
		leaseHolderIdentity(t, line.Epoch, "node-a"),
		func(context.Context) (uint64, error) { return 9, nil },
		func() time.Time { return time.Unix(0, nowNS.Load()) })
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	locks := t.TempDir()
	old, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), NumBlocks: 16, BlockSize: 4096,
		HostVolumeLock: true, VolumeLockDirTestOnly: locks,
	}, guard)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if _, err := old.EnsureStorage("v"); err != nil {
		t.Fatal(err)
	}
	if _, err := old.RecoverVolume(ctx, "v"); err != nil {
		t.Fatal(err)
	}
	backend, err := old.Open(ctx, "v")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := backend.Write(ctx, 0, make([]byte, 4096)); err != nil || n != 4096 {
		t.Fatalf("predecessor never served under its granted lease: %d %v", n, err)
	}
	q := &control.WorkspaceMove{RequestId: "same-host-recovery", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: line.Epoch, TargetReplicaId: "r1", TargetServerId: "node-a", TargetDataAddr: b.DataAddr, TargetCtrlAddr: b.CtrlAddr, Kind: "forced", Force: true}
	if _, err := ws.MoveWorkspace(ctx, q); err == nil {
		t.Fatal("same-host recovery bypassed a live lease")
	}
	nowNS.Add(int64(24 * time.Hour))
	if preview, err := ws.PreviewWorkspaceMove(ctx, q); err != nil || !preview.Admitted {
		t.Fatalf("same-host recovery preview after lease barrier: %+v %v", preview, err)
	}
	grant, err := ws.MoveWorkspace(ctx, q)
	if err != nil {
		t.Fatal("same-host recovery must return a new epoch and loss record", err)
	}
	if grant.NewEpoch != line.Epoch+1 || grant.LostRange == nil || grant.LostRange.LostAfterLsn != 42 || grant.LostRange.OldEpoch != line.Epoch {
		t.Fatalf("recovery omitted the abandoned suffix: %+v", grant)
	}
	if bindings, ok, err := h.Lifecycle().WorkspaceBindings.Get("v"); err != nil || !ok || len(bindings) != 1 || bindings[0].Retired {
		t.Fatalf("recovery retired its own target reservation: %+v %v", bindings, err)
	}
	if replay, err := ws.MoveWorkspace(ctx, q); err != nil || !proto.Equal(replay, grant) {
		t.Fatalf("same-host recovery replay changed: %+v %v", replay, err)
	}
	// The binding is still live for this server. It cannot distinguish epochs;
	// the predecessor's lease guard must independently refuse guest I/O.
	if n, err := backend.Write(ctx, 4096, make([]byte, 4096)); n != 0 || !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("live old-epoch process served after the forced barrier: %d %v", n, err)
	}
	probe := func(locked bool) {
		t.Helper()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkspaceSameHostSuccessorLockProbe$", "-test.count=1")
		for _, e := range os.Environ() {
			if !strings.HasPrefix(e, "SWBLOCK_SAMEHOST_") {
				cmd.Env = append(cmd.Env, e)
			}
		}
		cmd.Env = append(cmd.Env, "SWBLOCK_SAMEHOST_LOCK_PROBE="+locks)
		if locked {
			cmd.Env = append(cmd.Env, "SWBLOCK_SAMEHOST_EXPECT_LOCKED=yes")
		}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("successor process locked=%t: %v\n%s", locked, err, out)
		}
	}
	probe(true)
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	probe(false)
	id := leaseHolderIdentity(t, grant.NewEpoch, "node-a")
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	// The successor's ACK requires its own stopped image, not the live
	// predecessor HEAD used to exercise forced recovery above.
	observed.Store(&authority.WorkspaceHead{Gen: 10, Epoch: grant.NewEpoch, Holder: "node-a/r1", RemoteLSN: 42, Detached: true})
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 10}); err != nil {
		t.Fatal(err)
	}
	if bindings, ok, err := h.Lifecycle().WorkspaceBindings.Get("v"); err != nil || !ok || len(bindings) != 1 || !bindings[0].Retired {
		t.Fatalf("successor ACK did not retire its same-host binding: %+v %v", bindings, err)
	}
}
