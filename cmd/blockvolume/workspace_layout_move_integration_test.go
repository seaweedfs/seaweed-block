//go:build linux && subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	master "github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// moveHost is one blockvolume process of the move test: its addresses and
// the running binary.
type moveHost struct {
	server, data, ctrl, status, nvme, nqn string
	cmd                                   *exec.Cmd
	done                                  chan error
	exited                                bool
}

func newMoveHost(t *testing.T, server, nqn string) *moveHost {
	return &moveHost{server: server, data: leaseBinaryAddress(t), ctrl: leaseBinaryAddress(t), status: leaseBinaryAddress(t),
		nvme: leaseBinaryAddress(t), nqn: nqn}
}

// start runs the blockvolume binary for vol-a as replica r1 on this host,
// with the master-minted layout identity, and kills it at cleanup.
func (m *moveHost) start(t *testing.T, exe, masterAddr, filerGRPC, base string, layoutID layout.VolumeID) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), m.server+".log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	m.cmd = exec.Command(exe, "--master", masterAddr, "--server-id", m.server, "--volume-id", "vol-a", "--replica-id", "r1",
		"--data-addr", m.data, "--ctrl-addr", m.ctrl, "--t1-readiness", "--status-addr", m.status,
		"--durable-root", t.TempDir(), "--layout-id", layoutID.String(), "--volume-lock-dir-test-only", t.TempDir(),
		"--durable-impl", "walstore-chunk", "--durable-blocks", "64", "--chunk-filer", filerGRPC, "--chunk-path", base,
		"--chunk-control-endpoint", "--workspace-lease", "--nvme-listen", m.nvme, "--nvme-subsysnqn", m.nqn)
	stripSystemdForTestLockDir(m.cmd)
	m.cmd.Stdout, m.cmd.Stderr = log, log
	if err := m.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	m.done = make(chan error, 1)
	go func() { m.done <- m.cmd.Wait() }()
	t.Cleanup(func() {
		if !m.exited {
			_ = m.cmd.Process.Kill()
			select {
			case <-m.done:
			case <-time.After(5 * time.Second):
				t.Errorf("%s did not exit after kill", m.server)
			}
		}
		log.Close()
		if t.Failed() {
			b, _ := os.ReadFile(logPath)
			if len(b) > 14000 {
				b = b[len(b)-14000:]
			}
			t.Logf("%s log: %s", m.server, b)
		}
	})
}

// wait waits for the process to end on its own within d.
func (m *moveHost) wait(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-m.done:
		m.exited = true
	case <-time.After(d):
		t.Fatalf("%s did not exit", m.server)
	}
}

// projection is the volume's status as this host reports it.
func (m *moveHost) projection() (frontend.Projection, bool) {
	c := &http.Client{Timeout: time.Second}
	resp, err := c.Get("http://" + m.status + "/status?volume=vol-a")
	if err != nil {
		return frontend.Projection{}, false
	}
	defer resp.Body.Close()
	var p frontend.Projection
	return p, json.NewDecoder(resp.Body).Decode(&p) == nil
}

// layoutHead reads ref's HEAD as stored (claim id included).
func layoutHead(t *testing.T, cluster *w4WeedCluster, base string, ref chunkstore.VolumeRef) layout.Head {
	t.Helper()
	cl := cluster.client(t, base)
	cat, err := chunkstore.NewBCatalog(cl, cl, base)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, found, err := cat.ReadHead(ctx, ref)
	if err != nil || !found {
		t.Fatalf("HEAD of %s: found %v, %v", ref.Name, found, err)
	}
	return h
}

// A move on the content-named layout with real processes: the master in
// process, SeaweedFS and two blockvolume binaries. Clean: the source writes,
// cuts and stops (HEAD detached by the source's claim), the master moves the
// volume, the target claims HEAD at the higher epoch and reads back every
// acknowledged write. Forced: the source is killed after its cut and one more
// acknowledged write; after the master's barrier the forced move is issued
// with its lost range recorded, and the target claims and reads back what the
// cut published. HEAD's state, epoch, holder and claim are asserted at each
// step.
func TestIntegrationWorkspaceLayoutMove(t *testing.T) {
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	exe := blockvolumeUnderTest(t)
	cluster := startWorkspaceWeed(t)
	for _, kind := range []string{"clean", "forced"} {
		t.Run(kind, func(t *testing.T) { layoutMove(t, exe, cluster, kind) })
	}
}

func layoutMove(t *testing.T, exe string, cluster *w4WeedCluster, kind string) {
	base := fmt.Sprintf("/workspace-move-%s-%d", kind, time.Now().UnixNano())
	cl := cluster.client(t, base)
	headCat, err := chunkstore.NewBCatalog(cl, cl, base)
	if err != nil {
		t.Fatal(err)
	}
	var h *master.Host
	reader, err := master.NewLayoutWorkspaceHEADReader(headCat, func(ctx context.Context, name string) (chunkstore.VolumeRef, error) {
		return h.WorkspaceLayoutVolume(ctx, name)
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err = master.New(master.Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0",
		WorkspaceRPC: true, WorkspaceHeadReader: reader, WorkspaceMaxClockSkew: 100 * time.Millisecond,
		LayoutCreateVolume: layoutMasterWriter(t, cluster, base)})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = h.Close(ctx)
	})
	runProductLoop(t, h)
	src, dst := newMoveHost(t, "node-a", "nqn.2026-11.test:move-src-"+kind), newMoveHost(t, "node-b", "nqn.2026-11.test:move-dst-"+kind)
	// The master's create writes VOLUME and the unclaimed HEAD, then marks
	// the record store-ready; only a store-ready volume gets a placement.
	layoutID := createStoreReadyVolume(t, h, "vol-a", 64*4096)
	ref := chunkstore.VolumeRef{Name: "vol-a", ID: layoutID}
	for _, n := range []*moveHost{src, dst} {
		if _, err = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: n.server, DataAddr: n.data, CtrlAddr: n.ctrl}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "vol-a", DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{
		{VolumeID: "vol-a", ServerID: src.server, ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
	if err = h.ObservationHost().Ingest(authority.Observation{ServerID: src.server, ObservedAt: time.Now().UTC(),
		Slots: []authority.SlotFact{{VolumeID: "vol-a", ReplicaID: "r1", DataAddr: src.data, CtrlAddr: src.ctrl}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	leaseBinaryPoll(t, "no initial authority", func() bool { line, ok := h.Publisher().VolumeAuthorityLine("vol-a"); return ok && line.Epoch > 0 })
	line, _ := h.Publisher().VolumeAuthorityLine("vol-a")
	remote := cluster.remote(t, base, ref)

	// The source serves, takes acknowledged writes, cuts.
	src.start(t, exe, h.Addr(), cluster.filerGRPC, base, layoutID)
	leaseBinaryPoll(t, "source did not serve", func() bool { p, ok := src.projection(); return ok && p.Healthy && p.Epoch == line.Epoch })
	want, late := bytes.Repeat([]byte{0xD7}, 4096), bytes.Repeat([]byte{0x5A}, 4096)
	nv := gap1ConnectNVMe(t, src.nvme, src.nqn)
	nv.writeFlush(t, 8, want)
	nv.writeFlush(t, 9, want)
	leaseBinaryPoll(t, "source cut", func() bool { code, _, e := leaseBinaryPost(src.status, "cut"); return e == nil && code == 200 })
	cut := layoutHead(t, cluster, base, ref)
	if cut.Epoch != line.Epoch || cut.Holder != src.server+"/r1" || cut.ClaimID == "" || cut.State != layout.StateLive || cut.Current == nil {
		t.Fatalf("HEAD after the source's cut: %+v; want live, held by %s/r1 at epoch %d with a claim", cut, src.server, line.Epoch)
	}
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ws := control.NewWorkspaceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	q := &control.WorkspaceMove{RequestId: kind + "-move", VolumeId: "vol-a", ExpectedReplicaId: "r1", ExpectedEpoch: line.Epoch,
		TargetReplicaId: "r1", TargetServerId: dst.server, TargetDataAddr: dst.data, TargetCtrlAddr: dst.ctrl, Kind: kind, Force: kind == "forced"}
	if kind == "clean" {
		nv.close()
		code, b, e := leaseBinaryPost(src.status, "detach")
		if e != nil || code != 200 {
			t.Fatalf("detach: %d %s %v", code, b, e)
		}
		sealed := layoutHead(t, cluster, base, ref)
		if sealed.State != layout.StateDetached || sealed.Epoch != line.Epoch || sealed.Holder != cut.Holder || sealed.ClaimID != cut.ClaimID {
			t.Fatalf("HEAD after the detach: %+v; want detached by the source's claim %q at epoch %d", sealed, cut.ClaimID, line.Epoch)
		}
		if _, err = ws.RevokeWorkspace(ctx, &control.WorkspaceIdentity{VolumeId: "vol-a", ReplicaId: "r1", Epoch: line.Epoch}); err != nil {
			t.Fatal(err)
		}
		leaseBinaryPoll(t, "source did not ACK its drained stop", func() bool { s := h.Publisher().WorkspaceState("vol-a"); return s != nil && s.Ack != nil })
		src.wait(t, 15*time.Second)
	} else {
		// One more acknowledged write after the cut, then the source dies
		// without a stop: the forced move records it as possibly lost.
		nv.writeFlush(t, 10, late)
		nv.close()
		if err = src.cmd.Process.Signal(syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		src.wait(t, 10*time.Second)
		killed := layoutHead(t, cluster, base, ref)
		if killed.State != layout.StateLive || killed.Epoch != line.Epoch || killed.ClaimID != cut.ClaimID {
			t.Fatalf("HEAD after the kill: %+v; want still the source's live claim", killed)
		}
		// The master owns the barrier: wait for its affirmative preview.
		for {
			pv, err := ws.PreviewWorkspaceMove(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if pv.Admitted {
				break
			}
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				t.Fatalf("forced preview never admitted: %v (last reason %q)", ctx.Err(), pv.Reason)
			}
		}
	}
	before := layoutHead(t, cluster, base, ref)
	grant, err := ws.MoveWorkspace(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if !grant.Issued || grant.NewEpoch <= line.Epoch || grant.OldHost != src.server || grant.NewHost != dst.server || grant.HeadGen != before.Gen || grant.RemoteLsn != before.Position {
		t.Fatalf("grant %v not bound to HEAD %+v", grant, before)
	}
	if after := layoutHead(t, cluster, base, ref); after.Gen != before.Gen || after.Epoch != before.Epoch || after.ClaimID != before.ClaimID {
		t.Fatalf("the grant itself changed HEAD: %+v -> %+v", before, after)
	}
	if kind == "forced" {
		if grant.LostRange == nil || grant.LostRange.NewEpoch != grant.NewEpoch || grant.LostRange.OldEpoch != line.Epoch || grant.LostRange.LostAfterLsn != before.Position {
			t.Fatalf("forced grant lost range %v; want after LSN %d, epochs %d -> %d", grant.LostRange, before.Position, line.Epoch, grant.NewEpoch)
		}
		lr, err := ws.QueryWorkspaceLostRange(ctx, &control.WorkspaceLostRangeQuery{VolumeId: "vol-a", RequestId: q.RequestId})
		if err != nil || len(lr.GetRanges()) != 1 || lr.GetRanges()[0].GetLostAfterLsn() != before.Position {
			t.Fatalf("recorded lost range: %v %v", lr, err)
		}
	} else if grant.LostRange != nil {
		t.Fatalf("clean grant carries a lost range %v", grant.LostRange)
	}

	// The target claims HEAD at the new epoch and serves what was published.
	dst.start(t, exe, h.Addr(), cluster.filerGRPC, base, layoutID)
	leaseBinaryPoll(t, "target did not serve", func() bool { p, ok := dst.projection(); return ok && p.Healthy && p.Epoch == grant.NewEpoch })
	claimed := layoutHead(t, cluster, base, ref)
	if claimed.Epoch != grant.NewEpoch || claimed.Holder != dst.server+"/r1" || claimed.State != layout.StateLive || claimed.ClaimID == "" || claimed.ClaimID == cut.ClaimID || claimed.Gen <= before.Gen {
		t.Fatalf("HEAD after the target's start: %+v; want live, held by %s/r1 at epoch %d under a new claim", claimed, dst.server, grant.NewEpoch)
	}
	if eh, err := remote.GetHead(ctx); err != nil || eh.Epoch != grant.NewEpoch || eh.State != "active" || eh.DetachedBy != nil {
		t.Fatalf("engine view of HEAD after the claim: %+v %v", eh, err)
	}
	tv := gap1ConnectNVMe(t, dst.nvme, dst.nqn)
	tv.readExact(t, 8, want)
	tv.readExact(t, 9, want)
	tv.writeFlush(t, 11, want)
	tv.readExact(t, 11, want)
	tv.close()
	t.Logf("%s move on the layout: claim %s@%d -> %s@%d, HEAD gen %d -> %d", kind, cut.ClaimID, line.Epoch, claimed.ClaimID, grant.NewEpoch, before.Gen, claimed.Gen)
}
