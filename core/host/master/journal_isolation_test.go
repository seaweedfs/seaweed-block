package master

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storeident"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// masterStore is the identity the tests give the master's authority store
// (the real one is platform-dependent: unknown on Windows).
var masterStore = storeident.Identity{MachineID: "machine-1", Dev: 2049}

const colocatedLine = "shares the authority store's file system"

type isolationRig struct {
	m      *Host
	logs   *syncBuffer
	ws     control.WorkspaceServiceClient
	lc     control.LifecycleServiceClient
	obs    control.ObservationServiceClient
	ctx    context.Context
	closed bool
}

func newIsolationRig(t *testing.T, strict bool, authorityDir, lifecycleDir string) *isolationRig {
	t.Helper()
	return startIsolationRig(t, strict, authorityDir, lifecycleDir, &masterStore)
}

// startIsolationRig starts the rig's master; self, when not nil, replaces the
// identity the master sampled from its authority store.
func startIsolationRig(t *testing.T, strict bool, authorityDir, lifecycleDir string, self *storeident.Identity) *isolationRig {
	t.Helper()
	logs := &syncBuffer{}
	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: authorityDir, LifecycleStoreDir: lifecycleDir, WorkspaceRPC: true,
		StrictJournalIsolation: strict, Logger: log.New(logs, "", 0),
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
			return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	if self != nil {
		m.journal.setSelf(*self) // before Start: no RPC has run yet
	}
	m.Start()
	r := &isolationRig{m: m, logs: logs}
	t.Cleanup(func() {
		if !r.closed {
			closeTestMaster(t, m)
		}
	})
	conn, err := grpc.NewClient(m.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	for _, n := range []lifecycle.NodeRegistration{
		{ServerID: "node-a", DataAddr: "127.0.0.1:19101", CtrlAddr: "127.0.0.1:19102"},
		{ServerID: "node-b", DataAddr: "127.0.0.1:19301", CtrlAddr: "127.0.0.1:19302"},
	} {
		if _, err := m.Lifecycle().Nodes.RegisterNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r.ws, r.lc, r.obs, r.ctx = control.NewWorkspaceServiceClient(conn), control.NewLifecycleServiceClient(conn), control.NewObservationServiceClient(conn), ctx
	return r
}

func (r *isolationRig) close(t *testing.T) {
	t.Helper()
	r.closed = true
	closeTestMaster(t, r.m)
}

var hbPorts = map[string][2]string{
	"node-a": {"127.0.0.1:19111", "127.0.0.1:19112"},
	"node-b": {"127.0.0.1:19311", "127.0.0.1:19312"},
	"node-c": {"127.0.0.1:19511", "127.0.0.1:19512"},
}

func (r *isolationRig) heartbeat(server string, id storeident.Identity) error {
	_, err := r.obs.ReportHeartbeat(r.ctx, &control.HeartbeatReport{ServerId: server, SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		MachineId: id.MachineID, DurableRootDev: id.Dev, DurableRootFsid: id.FSID,
		Slots: []*control.HeartbeatSlot{{VolumeId: "hb-" + server, ReplicaId: "r-" + server, DataAddr: hbPorts[server][0], CtrlAddr: hbPorts[server][1]}}})
	return err
}

func (r *isolationRig) createVolume(t *testing.T, vol string) {
	t.Helper()
	if _, err := r.lc.CreateVolume(r.ctx, &control.CreateVolumeRequest{VolumeId: vol, SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
}

func (r *isolationRig) place(t *testing.T, vol, server string) {
	t.Helper()
	if _, err := r.m.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: vol, DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{VolumeID: vol, ServerID: server, ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
}

// Default (not strict): a node on the authority store's file system is
// alarmed once and counted, and its heartbeats are accepted. A different
// device, or a different machine with the same device number, is silent.
func TestColocatedNodeIsAlarmedButAcceptedByDefault(t *testing.T) {
	r := newIsolationRig(t, false, t.TempDir(), t.TempDir())
	for i := 0; i < 2; i++ {
		if err := r.heartbeat("node-a", masterStore); err != nil {
			t.Fatalf("heartbeat %d of the co-located node refused without strict isolation: %v", i, err)
		}
	}
	want := "blockmaster: node node-a shares the authority store's file system (machine machine-1, dev 2049): its fsyncs share the authority journal"
	if n := strings.Count(r.logs.String(), want); n != 1 {
		t.Fatalf("alarm logged %d times, want once:\n%s", n, r.logs.String())
	}
	if got := r.m.WorkspaceLeaseStatus().ColocatedNodes; got != 1 {
		t.Fatalf("colocated_nodes = %d, want 1", got)
	}
	for _, id := range []storeident.Identity{
		{MachineID: "machine-1", Dev: 2050}, // same machine, other file system
		{MachineID: "machine-2", Dev: 2049}, // same device number, other machine
		{},                                  // unknown
	} {
		if err := r.heartbeat("node-b", id); err != nil {
			t.Fatalf("heartbeat %+v: %v", id, err)
		}
	}
	if strings.Contains(r.logs.String(), "node node-b "+colocatedLine) {
		t.Fatalf("node-b alarmed:\n%s", r.logs.String())
	}
	if got := r.m.WorkspaceLeaseStatus().ColocatedNodes; got != 1 {
		t.Fatalf("colocated_nodes = %d after silent nodes, want 1", got)
	}
}

// A node that moves its durable root off the authority store's file system
// leaves the co-located set on its next heartbeat: strict isolation accepts
// it again and no longer refuses new work on it.
func TestColocatedNodeLeavesTheSetWhenItMovesOff(t *testing.T) {
	r := newIsolationRig(t, true, t.TempDir(), t.TempDir())
	if err := r.heartbeat("node-a", masterStore); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("strict heartbeat of a co-located new node: %v, want FailedPrecondition", err)
	}
	if !r.m.colocatedTargetRefused("node-a") || r.m.WorkspaceLeaseStatus().ColocatedNodes != 1 {
		t.Fatal("co-located node not recorded")
	}
	if err := r.heartbeat("node-a", storeident.Identity{MachineID: "machine-1", Dev: 2050}); err != nil {
		t.Fatalf("strict refused the node after it moved off the store's file system: %v", err)
	}
	if r.m.colocatedTargetRefused("node-a") || r.m.WorkspaceLeaseStatus().ColocatedNodes != 0 {
		t.Fatal("node still counted as co-located after it moved off")
	}
}

// Strict: a co-located node that serves no volume is refused
// (FailedPrecondition) and still alarmed; a node elsewhere is accepted.
func TestStrictRefusesColocatedNodeWithoutVolumes(t *testing.T) {
	r := newIsolationRig(t, true, t.TempDir(), t.TempDir())
	err := r.heartbeat("node-a", masterStore)
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), colocatedLine) {
		t.Fatalf("strict heartbeat of a co-located new node: %v, want FailedPrecondition naming the shared file system", err)
	}
	if !strings.Contains(r.logs.String(), "node node-a "+colocatedLine) || r.m.WorkspaceLeaseStatus().ColocatedNodes != 1 {
		t.Fatalf("refused node not alarmed/counted:\n%s", r.logs.String())
	}
	if err := r.heartbeat("node-b", storeident.Identity{MachineID: "machine-1", Dev: 2050}); err != nil {
		t.Fatalf("strict refused a node on another file system: %v", err)
	}
}

// Strict: a co-located node that already serves a volume keeps getting its
// heartbeats accepted (alarm only), also after a master restart: "serves" is
// read from durable state, not from what this process has seen.
func TestStrictAcceptsColocatedNodeThatAlreadyServes(t *testing.T) {
	authorityDir, lifecycleDir := t.TempDir(), t.TempDir()
	r := newIsolationRig(t, true, authorityDir, lifecycleDir)
	r.createVolume(t, "placed")
	r.place(t, "placed", "node-a")
	if err := r.heartbeat("node-a", masterStore); err != nil {
		t.Fatalf("strict refused a co-located node that serves a volume: %v", err)
	}
	if !strings.Contains(r.logs.String(), "node node-a "+colocatedLine) || r.m.WorkspaceLeaseStatus().ColocatedNodes != 1 {
		t.Fatalf("serving co-located node not alarmed:\n%s", r.logs.String())
	}
	r.close(t)

	again := newIsolationRig(t, true, authorityDir, lifecycleDir)
	if err := again.heartbeat("node-a", masterStore); err != nil {
		t.Fatalf("after a master restart, strict refused the serving co-located node: %v", err)
	}
	if !strings.Contains(again.logs.String(), "node node-a "+colocatedLine) {
		t.Fatalf("restarted master did not alarm:\n%s", again.logs.String())
	}
}

// Strict refuses a new placement or move onto a co-located node; without
// strict neither is refused for that reason.
func TestStrictRefusesPlacementAndMoveOntoColocatedNode(t *testing.T) {
	for _, strict := range []bool{true, false} {
		t.Run(map[bool]string{true: "strict", false: "default"}[strict], func(t *testing.T) {
			r := newIsolationRig(t, strict, t.TempDir(), t.TempDir())
			_ = r.heartbeat("node-b", masterStore) // strict refuses it (no volume); both modes record it
			if r.m.WorkspaceLeaseStatus().ColocatedNodes != 1 {
				t.Fatal("node-b not recorded as co-located")
			}
			r.createVolume(t, "placed")
			r.place(t, "placed", "node-a")
			r.createVolume(t, "fresh")
			if _, err := r.ws.RegisterWorkspaceBinding(r.ctx, &control.WorkspaceBinding{VolumeId: "fresh", ServerId: "node-b", DataAddr: "127.0.0.1:19321", CtrlAddr: "127.0.0.1:19322"}); err != nil {
				t.Fatal(err)
			}
			_, placeErr := r.ws.PlaceWorkspace(r.ctx, &control.WorkspacePlacement{VolumeId: "fresh", ServerId: "node-b", ReplicaId: "r1"})
			_, moveErr := r.ws.MoveWorkspace(r.ctx, &control.WorkspaceMove{RequestId: "m1", VolumeId: "placed", ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1",
				TargetServerId: "node-b", TargetDataAddr: "127.0.0.1:19301", TargetCtrlAddr: "127.0.0.1:19302", Kind: "clean"})
			if strict {
				if status.Code(placeErr) != codes.FailedPrecondition || !strings.Contains(status.Convert(placeErr).Message(), colocatedLine) {
					t.Fatalf("strict PlaceWorkspace onto a co-located node: %v", placeErr)
				}
				if _, ok := r.m.Lifecycle().Placements.GetPlacement("fresh"); ok {
					t.Fatal("refused placement was recorded")
				}
				want := `move target node "node-b" shares the authority store's file system (strict journal isolation)`
				if status.Code(moveErr) != codes.FailedPrecondition || status.Convert(moveErr).Message() != want {
					t.Fatalf("strict MoveWorkspace onto a co-located node: %v, want %q", moveErr, want)
				}
				if !strings.Contains(r.logs.String(), `MoveWorkspace refused volume="placed" request="m1": `+want) {
					t.Fatalf("move refusal not logged:\n%s", r.logs.String())
				}
				return
			}
			if placeErr != nil {
				t.Fatalf("default PlaceWorkspace onto a co-located node refused: %v", placeErr)
			}
			if moveErr != nil && strings.Contains(moveErr.Error(), colocatedLine) {
				t.Fatalf("default MoveWorkspace refused for co-location: %v", moveErr)
			}
		})
	}
}

// Strict: the product loop never creates a new placement on a co-located
// node (the only node with capacity here); without strict it does.
func TestStrictProductLoopSkipsColocatedNodeForNewPlacement(t *testing.T) {
	for _, strict := range []bool{true, false} {
		t.Run(map[bool]string{true: "strict", false: "default"}[strict], func(t *testing.T) {
			r := newIsolationRig(t, strict, t.TempDir(), t.TempDir())
			if _, err := r.m.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "node-c", DataAddr: "127.0.0.1:19501", CtrlAddr: "127.0.0.1:19502",
				Pools: []lifecycle.StoragePool{{PoolID: "p1", TotalBytes: 1 << 30, FreeBytes: 1 << 30, BlockSize: 4096}}}); err != nil {
				t.Fatal(err)
			}
			_ = r.heartbeat("node-c", masterStore)
			r.createVolume(t, "auto")
			if _, err := r.m.RunLifecycleProductTick(); err != nil {
				t.Fatal(err)
			}
			p, ok := r.m.Lifecycle().Placements.GetPlacement("auto")
			if strict && ok {
				t.Fatalf("strict product loop placed a new volume on the co-located node: %+v", p)
			}
			if !strict && (!ok || p.Slots[0].ServerID != "node-c") {
				t.Fatalf("default product loop did not place on node-c: %+v ok=%v", p, ok)
			}
		})
	}
}

// The status endpoint is loopback-only and serves the counters as JSON.
func TestStatusEndpointLoopbackOnlyAndServesJSON(t *testing.T) {
	for _, bad := range []string{"0.0.0.0:9335", ":9335", "10.0.0.5:9335", "[::]:9335", "example.com:9335"} {
		if err := ValidateStatusListen(bad); err == nil {
			t.Fatalf("ValidateStatusListen(%q) accepted a non-loopback address", bad)
		}
		if _, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), StatusListen: bad}); err == nil || !strings.Contains(err.Error(), "loopback") {
			t.Fatalf("master.New with --status-listen %q: %v, want a loopback refusal", bad, err)
		}
	}
	for _, ok := range []string{"", "127.0.0.1:9335", "127.1.2.3:0", "[::1]:9335", "localhost:9335"} {
		if err := ValidateStatusListen(ok); err != nil {
			t.Fatalf("ValidateStatusListen(%q): %v", ok, err)
		}
	}

	m, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: t.TempDir(), StatusListen: "127.0.0.1:0", StrictJournalIsolation: true})
	if err != nil {
		t.Fatal(err)
	}
	m.Start()
	defer closeTestMaster(t, m)
	resp, err := http.Get("http://" + m.StatusAddr() + "/status/workspace-lease")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("status %d content-type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"colocated_nodes", "headroom_alarms", "clamps_total", "clamps_last_minute", "min_headroom_ms", "strict_journal_isolation",
		"extension_write_last_ms", "extension_write_max_last_minute_ms"} {
		if _, ok := body[k]; !ok {
			t.Fatalf("status JSON lacks %q: %v", k, body)
		}
	}
	if body["strict_journal_isolation"] != true || body["colocated_nodes"] != float64(0) || body["min_headroom_ms"] != float64(-1) ||
		body["extension_write_last_ms"] != float64(-1) || body["extension_write_max_last_minute_ms"] != float64(-1) {
		t.Fatalf("status JSON values: %v", body)
	}
	if post, err := http.Post("http://"+m.StatusAddr()+"/status/workspace-lease", "application/json", nil); err != nil || post.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST: %v %v", post, err)
	} else {
		post.Body.Close()
	}
}
