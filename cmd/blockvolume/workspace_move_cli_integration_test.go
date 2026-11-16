//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"encoding/json"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
	master "github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type workspaceCLIProcess struct {
	cmd                                           *exec.Cmd
	done                                          chan error
	stopped                                       bool
	root, data, ctrl, status, iscsi, iqn, logPath string
}

func (p *workspaceCLIProcess) kill(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	_ = p.cmd.Process.Kill()
	select {
	case <-p.done:
		p.stopped = true
	case <-time.After(5 * time.Second):
		t.Fatal("blockvolume did not exit after kill")
	}
}

// A newly attached SCSI target may report one Unit Attention. Decode and
// acknowledge ONLY that protocol event; data/readiness failures remain failures.
func workspaceRestoredRead(t *testing.T, c *g8IscsiClient, lba uint32) []byte {
	t.Helper()
	_ = c.conn.SetDeadline(time.Now().Add(5 * time.Second))
	status, data := c.scsi(t, g8ReadCDB10(lba, 1), nil, 4096)
	if status == iscsi.StatusGood {
		return data
	}
	var senseCDB [16]byte
	senseCDB[0] = iscsi.ScsiRequestSense
	senseCDB[4] = 18
	senseStatus, sense := c.scsi(t, senseCDB, nil, 18)
	t.Logf("first restored READ status=%02x REQUEST_SENSE status=%02x data=%x", status, senseStatus, sense)
	if senseStatus != iscsi.StatusGood || len(sense) < 14 || sense[2]&15 != iscsi.SenseUnitAttention {
		t.Fatalf("restored READ failed; not Unit Attention: %x", sense)
	}
	return c.read10(t, lba, 1, 4096)
}

func TestWorkspaceCLIThreeVolumesCleanAndForcedMove(t *testing.T) {
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Skip("CHUNKSTORE_WEED not set")
	}
	binDir := t.TempDir()
	bv, cli := filepath.Join(binDir, "blockvolume"), filepath.Join(binDir, "sw-block")
	for _, x := range []struct{ output, pkg string }{{bv, "."}, {cli, "../sw-block"}} {
		cmd := exec.Command("go", "build", "-race", "-p", "4", "-o", x.output, x.pkg)
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("build %s: %v %s", x.pkg, e, out)
		}
	}
	cluster := startWorkspaceWeed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	base := fmt.Sprintf("/workspace-cli-%d", time.Now().UnixNano())
	reader, e := master.NewWorkspaceHEADReader("http://" + cluster.filerHTTP + base)
	if e != nil {
		t.Fatal(e)
	}
	h, e := master.New(master.Config{AuthorityStoreDir: t.TempDir(), LifecycleStoreDir: t.TempDir(), Listen: "127.0.0.1:0", WorkspaceRPC: true, WorkspaceHeadReader: reader})
	if e != nil {
		t.Fatal(e)
	}
	h.Start()
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if e := h.Close(c); e != nil {
			t.Error(e)
		}
	})
	conn, e := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	wc := control.NewWorkspaceServiceClient(conn)
	for _, server := range []string{"node-a", "node-b"} {
		if _, e = h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: leaseBinaryAddress(t), CtrlAddr: leaseBinaryAddress(t)}); e != nil {
			t.Fatal(e)
		}
	}
	call := func(op string, q proto.Message, force bool) ([]byte, error) {
		b, e := protojson.Marshal(q)
		if e != nil {
			t.Fatal(e)
		}
		args := []string{"ops", "workspace", op, "--master", h.Addr(), "--timeout", "5s"}
		if force {
			args = append(args, "--force")
		}
		cmd := exec.CommandContext(ctx, cli, args...)
		cmd.Stdin = bytes.NewReader(b)
		var out, errb bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &errb
		e = cmd.Run()
		t.Logf("CLI %s force=%v rc=%v JSON=%s stderr=%s", op, force, e, out.Bytes(), errb.Bytes())
		return out.Bytes(), e
	}
	must := func(op string, q proto.Message, force bool, reply proto.Message) {
		out, e := call(op, q, force)
		if e != nil {
			t.Fatalf("%s: %v %s", op, e, out)
		}
		if reply != nil {
			if e = protojson.Unmarshal(out, reply); e != nil {
				t.Fatal(e)
			}
		}
	}
	chunk := func(p *workspaceCLIProcess, op, vol string) {
		cmd := exec.CommandContext(ctx, cli, "ops", "chunk", op, "--volume", vol, "--status-addr", p.status, "--timeout", "5s")
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("chunk %s: %v %s", op, e, out)
		}
		t.Logf("chunk %s %s: %s", vol, op, out)
	}
	wait := func(what string, fn func() bool, serving func()) {
		until := time.Now().Add(35 * time.Second)
		for !fn() {
			if serving != nil {
				serving()
			}
			if time.Now().After(until) {
				t.Fatal(what)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	newProcess := func(vol string) *workspaceCLIProcess {
		return &workspaceCLIProcess{root: t.TempDir(), data: leaseBinaryAddress(t), ctrl: leaseBinaryAddress(t), status: leaseBinaryAddress(t), iscsi: leaseBinaryAddress(t), iqn: "iqn.2026-11.test:" + vol}
	}
	start := func(p *workspaceCLIProcess, vol, node string) {
		p.logPath = filepath.Join(t.TempDir(), "blockvolume.log")
		f, e := os.Create(p.logPath)
		if e != nil {
			t.Fatal(e)
		}
		args := []string{"--master", h.Addr(), "--server-id", node, "--volume-id", vol, "--replica-id", "r1", "--data-addr", p.data, "--ctrl-addr", p.ctrl, "--heartbeat-interval", "100ms", "--t1-readiness", "--status-addr", p.status, "--durable-root", p.root, "--durable-impl", "walstore-chunk", "--durable-blocks", "64", "--chunk-filer", cluster.filerGRPC, "--chunk-path", base, "--chunk-control-endpoint", "--workspace-lease", "--iscsi-listen", p.iscsi, "--iscsi-iqn", p.iqn}
		p.cmd = exec.Command(bv, args...)
		p.cmd.Stdout, p.cmd.Stderr = f, f
		if e = p.cmd.Start(); e != nil {
			t.Fatal(e)
		}
		p.done = make(chan error, 1)
		go func() { p.done <- p.cmd.Wait() }()
		t.Cleanup(func() {
			p.kill(t)
			f.Close()
			if t.Failed() {
				b, _ := os.ReadFile(p.logPath)
				if len(b) > 18000 {
					b = b[len(b)-18000:]
				}
				t.Logf("%s/%s log: %s", node, vol, b)
			}
		})
	}
	healthy := func(p *workspaceCLIProcess, vol string) bool {
		c := &http.Client{Timeout: 300 * time.Millisecond}
		r, e := c.Get("http://" + p.status + "/status?volume=" + vol)
		if e != nil {
			return false
		}
		defer r.Body.Close()
		var projection frontend.Projection
		e = json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&projection)
		if e != nil || !projection.Healthy {
			return false
		}
		d, e := c.Get("http://" + p.status + "/status/durable?volume=" + vol)
		if e != nil {
			return false
		}
		defer d.Body.Close()
		var st struct{ Volumes []durable.VolumeStatus }
		if e = json.NewDecoder(io.LimitReader(d.Body, 65536)).Decode(&st); e != nil || len(st.Volumes) != 1 {
			return false
		}
		b := st.Volumes[0]
		return b.Operational && b.Chunk != nil && b.Chunk.Attached && !b.Chunk.Fenced && !b.Chunk.AwaitingAuthority
	}
	vols := []string{"workspace-one", "workspace-two", "workspace-three"}
	sources := make([]*workspaceCLIProcess, 3)
	targets := make([]*workspaceCLIProcess, 3)
	clients := make([]*g8IscsiClient, 3)
	for i, vol := range vols {
		if _, e = control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: vol, SizeBytes: 64 * 4096, ReplicationFactor: 1}); e != nil {
			t.Fatal(e)
		}
		sources[i], targets[i] = newProcess(vol), newProcess(vol)
		for j, node := range []string{"node-a", "node-b"} {
			p := sources[i]
			if j == 1 {
				p = targets[i]
			}
			must("bind", &control.WorkspaceBinding{RequestId: "bind-" + vol + "-" + node, VolumeId: vol, ServerId: node, DataAddr: p.data, CtrlAddr: p.ctrl}, false, &control.WorkspaceBinding{})
		}
		must("place", &control.WorkspacePlacement{RequestId: "place-" + vol, VolumeId: vol, ServerId: "node-a", ReplicaId: "r1"}, false, &control.WorkspaceEmpty{})
		start(sources[i], vol, "node-a")
		wait("source not ready "+vol, func() bool {
			if _, e := h.RunLifecycleProductTick(); e != nil {
				t.Fatal(e)
			}
			return healthy(sources[i], vol)
		}, nil)
		w4WaitTCP(t, sources[i].iscsi, 5*time.Second)
		clients[i] = dialG8Iscsi(t, sources[i].iscsi, sources[i].iqn)
		defer clients[i].conn.Close()
	}
	round := func(i int, tag byte) {
		c := clients[i]
		_ = c.conn.SetDeadline(time.Now().Add(5 * time.Second))
		want := bytes.Repeat([]byte{tag}, 4096)
		c.write10(t, 0, want)
		c.syncCache10(t)
		if got := c.read10(t, 0, 1, 4096); !bytes.Equal(got, want) {
			t.Fatal("byte mismatch", vols[i])
		}
	}
	for i := range vols {
		round(i, byte(20+i))
		chunk(sources[i], "cut", vols[i])
	}
	moves := func(i int, id, kind string) *control.WorkspaceMove {
		p := targets[i]
		return &control.WorkspaceMove{RequestId: id, VolumeId: vols[i], ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: p.data, TargetCtrlAddr: p.ctrl, Kind: kind}
	}
	// CLEAN moves one workspace while both siblings perform real frontend I/O.
	servingClean := 0
	serveTwo := func() { round(1, 21); round(2, 22); servingClean++ }
	chunk(sources[0], "detach", vols[0])
	if _, e = wc.RevokeWorkspace(ctx, &control.WorkspaceIdentity{VolumeId: vols[0], ReplicaId: "r1", Epoch: 1}); e != nil {
		t.Fatal(e)
	}
	wait("CLEAN drain ACK absent", func() bool {
		st, e := wc.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: vols[0]})
		return e == nil && st.RevocationAck != nil
	}, serveTwo)
	serveTwo()
	var clean control.WorkspaceGrant
	must("move", moves(0, "clean-one", "clean"), false, &clean)
	if !clean.Issued || clean.NewEpoch <= 1 || clean.Kind != "clean" {
		t.Fatal("bad clean grant", &clean)
	}
	start(targets[0], vols[0], "node-b")
	wait("clean target not ready", func() bool { return healthy(targets[0], vols[0]) }, serveTwo)
	head, e := cluster.remote(t, base+"/"+vols[0]).GetHead(ctx)
	if e != nil || head.Holder != "node-b/r1" || head.Epoch != clean.NewEpoch || head.State != storage.ChunkHeadActive || head.DetachedBy != nil {
		t.Fatal("clean takeover before first I/O", head, e)
	}
	c1 := dialG8Iscsi(t, targets[0].iscsi, targets[0].iqn)
	defer c1.conn.Close()
	if got := workspaceRestoredRead(t, c1, 0); !bytes.Equal(got, bytes.Repeat([]byte{20}, 4096)) {
		t.Fatal("clean restore bytes")
	}
	_ = c1.conn.SetDeadline(time.Now().Add(5 * time.Second))
	c1.write10(t, 2, bytes.Repeat([]byte{0x31}, 4096))
	c1.syncCache10(t)
	chunk(targets[0], "cut", vols[0])
	newClean, e := cluster.remote(t, base+"/"+vols[0]).GetHead(ctx)
	if e != nil || newClean.RemoteLSN <= clean.RemoteLsn || newClean.Holder != "node-b/r1" {
		t.Fatal("clean target frontier/write", newClean, e)
	}

	t.Logf("three distinct source pairs; CLEAN sibling I/O rounds=%d HEAD=%+v", servingClean, head)
	// Crash after one remote cut plus a locally synced, uncut write. The journal
	// describes that last remote point, not the unsaved local tail.
	remote := cluster.remote(t, base+"/"+vols[1])
	old, e := remote.GetHead(ctx)
	if e != nil {
		t.Fatal(e)
	}
	_ = clients[1].conn.SetDeadline(time.Now().Add(5 * time.Second))
	clients[1].write10(t, 4, bytes.Repeat([]byte{99}, 4096))
	clients[1].syncCache10(t)
	sources[1].kill(t)
	force := moves(1, "forced-two", "forced")
	out, e := call("move", force, true)
	if e == nil {
		t.Fatal("forced move granted before lease expiry", string(out))
	}
	// --force is required separately from the JSON kind, even after expiry.
	out, e = call("move", force, false)
	if e == nil {
		t.Fatal("JSON bypassed --force", string(out))
	}
	forcedServing := 0
	wait("crashed lease did not expire", func() bool {
		st, e := wc.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: vols[1]})
		return e == nil && st.Lease != nil && time.Now().UnixNano() > st.Lease.ExpiresAtNs+int64(20*time.Millisecond)
	}, func() { round(2, 22); forcedServing++ })

	// Even an otherwise admissible expired lease plus force:true in JSON cannot
	// replace the operator's explicit --force flag.
	force.Force = true
	out, e = call("move", force, false)
	var refusal struct {
		Code string `json:"code"`
	}
	if e == nil || json.Unmarshal(out, &refusal) != nil || refusal.Code != "bad_request" {
		t.Fatal("expired lease/JSON bypassed --force", e, string(out))
	}
	force.Force = false
	var grant control.WorkspaceGrant
	must("move", force, true, &grant)
	if !grant.Issued || grant.Kind != "forced" || grant.NewEpoch <= 1 || grant.HeadGen != old.Gen || grant.RemoteLsn != old.RemoteLSN {
		t.Fatal("loss-window/epoch grant", &grant, old)
	}
	var journal control.WorkspaceStatus
	must("journal", &control.WorkspaceVolume{VolumeId: vols[1]}, false, &journal)
	if len(journal.Moves) != 1 || !proto.Equal(journal.Moves[0], &grant) {
		t.Fatal("journal differs from grant", &journal, &grant)
	}
	start(targets[1], vols[1], "node-b")
	wait("forced target not ready", func() bool { return healthy(targets[1], vols[1]) }, func() { round(2, 22); forcedServing++ })
	after, e := remote.GetHead(ctx)
	if e != nil || after.Epoch != grant.NewEpoch || after.Holder != "node-b/r1" || after.State != storage.ChunkHeadActive || after.DetachedBy != nil || after.RemoteLSN < old.RemoteLSN {
		t.Fatal("forced takeover BEFORE first I/O", old, after, e)
	}
	takeover := after // retain the exact pre-I/O observation for the report
	c2 := dialG8Iscsi(t, targets[1].iscsi, targets[1].iqn)
	defer c2.conn.Close()
	if got := workspaceRestoredRead(t, c2, 0); !bytes.Equal(got, bytes.Repeat([]byte{21}, 4096)) {
		t.Fatal("forced remote-cut bytes")
	}
	if got := c2.read10(t, 4, 1, 4096); !bytes.Equal(got, make([]byte, 4096)) {
		t.Fatal("uncut tail leaked into restored HEAD")
	}
	_ = c2.conn.SetDeadline(time.Now().Add(5 * time.Second))
	c2.write10(t, 1, bytes.Repeat([]byte{0x42}, 4096))
	c2.syncCache10(t)
	chunk(targets[1], "cut", vols[1])
	after, e = remote.GetHead(ctx)
	if e != nil || after.RemoteLSN <= old.RemoteLSN || after.Epoch != grant.NewEpoch {
		t.Fatal("forced target frontier/write", after, e)
	}

	// A delayed old-host upload is harmless: old-generation CAS cannot bind it.
	if e = uploadWorkspaceChunk(ctx, remote, "late-old-host-orphan", bytes.Repeat([]byte{0xDD}, 4096)); e != nil {
		t.Fatal(e)
	}
	if _, e = remote.CASHead(ctx, storage.ChunkHeadVersion{Gen: old.Gen, Epoch: old.Epoch}, old); !errors.Is(e, storage.ErrChunkHeadPrecondition) {
		t.Fatal("late old CAS not refused", e)
	}
	same, e := remote.GetHead(ctx)
	if e != nil || same.Gen != after.Gen || same.Epoch != after.Epoch {
		t.Fatal("late old CAS changed HEAD", same, e)
	}
	orphan, e := remote.Get(ctx, "late-old-host-orphan")
	if e != nil || !bytes.Equal(orphan, bytes.Repeat([]byte{0xDD}, 4096)) {
		t.Fatal("orphan upload evidence", e)
	}
	// Restart the actual old binary with its old local WAL/root and node identity.
	// It must refuse attach/capture and never install its uncut tail.
	restart := *sources[1]
	restart.stopped = false
	start(&restart, vols[1], "node-a")
	wait("old process did not report fenced attach", func() bool {
		b, _ := os.ReadFile(restart.logPath)
		return strings.Contains(string(b), storage.ErrChunkLayerFenced.Error())
	}, func() { round(2, 22); forcedServing++ })
	if healthy(&restart, vols[1]) {
		t.Fatal("restarted old host serves after fencing")
	}
	if r, e := (&http.Client{Timeout: time.Second}).Post("http://"+restart.status+"/chunk/cut", "application/json", strings.NewReader(`{"volumeID":"workspace-two"}`)); e == nil {
		defer r.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(r.Body, 65536))
		t.Logf("restarted old cut HTTP=%d JSON=%s", r.StatusCode, b)
		if r.StatusCode == 200 {
			t.Fatal("restarted old process cut succeeded")
		}
	}
	same, e = remote.GetHead(ctx)
	if e != nil || same.Gen != after.Gen || same.Holder != after.Holder {
		t.Fatal("restart changed target HEAD", same, e)
	}
	b, _ := os.ReadFile(restart.logPath)
	if len(b) > 5000 {
		b = b[len(b)-5000:]
	}
	t.Logf("old restart refused: %s", b)
	t.Logf("FORCED sibling I/O rounds=%d old HEAD=%+v takeover HEAD BEFORE I/O=%+v after target write/cut=%+v journal=%s", forcedServing, old, takeover, after, protojson.Format(&journal))
}
