//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/lifecycleclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStandalonePlannedHandoffToTheSameHost runs the planned hand-over back
// to the same host with the commands of docs/volume-lifecycle.md, in its
// order and form, against real blockmaster, blockvolume, sw-block and
// SeaweedFS processes on loopback: cluster spec, start, write, plain detach,
// revoke with the sealed generation, the old process acknowledges and exits
// by itself, show the ACK, clean move to the same host, start again, and the
// written block reads back unchanged.
func TestStandalonePlannedHandoffToTheSameHost(t *testing.T) {
	h, cluster, chunkPath, show, volArgs := startPlannedHandoff(t)
	must, mustU := fatalOn(t)
	const nqn = plannedHandoffNQN

	// 4. blockvolume adopts the volume.
	vol1 := h.start(t, "blockvolume-1", h.volume, volArgs...)
	e := mustU(adoptedEpoch(vol1.waitLog(t, 60*time.Second, "blockvolume: volume v1 adopted as PRIMARY at r1@")))
	h.waitPrimary(t, e)

	// 5. The lease.
	out, _ := h.command(t, show...)
	must(checkShowGranted(out, e))

	// 6, 7. Write a known block and flush it, then disconnect the writer.
	block := bytes.Repeat([]byte("planned-handoff!"), 4096/16)
	want := sha256.Sum256(block)
	c := dialNVMe(t, h.nvmeAddr, nqn)
	c.write(t, 0, block)
	c.flush(t)
	c.close()

	// 8. Seal with a plain detach.
	out, errOut := h.command(t, "ops", "chunk", "detach", "--plain", "--volume", "v1", "--status-addr", h.statusAddr, "--timeout", "120s")
	g := mustU(sealedGen(out, errOut, e))

	// 9. The seal is in the object store.
	chunkShow := []string{"ops", "chunk", "show", "--master", h.masterAddr, "--volume", "v1", "--chunk-filer", cluster.filerGRPC, "--chunk-path", chunkPath}
	out, _ = h.command(t, chunkShow...)
	l := mustU(sealedLayoutHeadPosition(out, e, g))

	// 10. Revoke with the sealed generation.
	out, _ = h.command(t, "ops", "workspace", "revoke", "--master", h.masterAddr, "--volume", "v1", "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--sealed-gen", fmt.Sprint(g))
	must(checkRevoke(out, e, g))

	// 11. The old blockvolume acknowledges and exits by itself (a signal
	// would suppress the ACK), within the 30 s lease.
	if err := vol1.wait(30 * time.Second); err != nil {
		t.Fatalf("blockvolume after the revocation: %v; want a clean exit by itself", err)
	}
	if !vol1.has(fmt.Sprintf("workspace lease: fenced volume=v1 replica=r1 epoch=%d: revoked by the master", e)) ||
		!vol1.has("blockvolume: stopping: the workspace lease guard fenced and requested the drain") {
		t.Fatal("blockvolume log does not show the revocation fence and drain")
	}
	if vol1.has("blockvolume: workspace lease closeout:") {
		t.Fatal("blockvolume reported a closeout error")
	}

	// 12. The master recorded the ACK at the sealed generation.
	poll(t, 30*time.Second, fmt.Sprintf("workspace show never reported the ACK of r1@%d at generation %d", e, g), func() bool {
		out, _ := h.command(t, show...)
		return showsAck(out, e, g)
	})

	// 13. Clean move to the same host; a rerun replays the issued move.
	move := []string{"ops", "workspace", "move", "--clean", "--master", h.masterAddr, "--volume", "v1", "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--target-host", "s1", "--target-data", h.dataAddr, "--target-ctrl", h.ctrlAddr,
		"--request-id", "move-v1-1"}
	moved, _ := h.command(t, move...)
	e2 := mustU(movedEpoch(moved, e, g, l))
	if again, _ := h.command(t, move...); again != moved {
		t.Fatalf("clean move rerun answered %q; want the issued move again %q", again, moved)
	}

	// 14. Start again with the same root, within the new lease (30 s).
	vol2 := h.start(t, "blockvolume-2", h.volume, volArgs...)
	vol2.waitLog(t, 60*time.Second, fmt.Sprintf("blockvolume: volume v1 adopted as PRIMARY at r1@%d", e2))
	for _, bad := range []string{"refusing to attach volume=v1", "assignment unavailable, not serving"} {
		if vol2.has(bad) {
			t.Fatalf("the restarted blockvolume logged %q", bad)
		}
	}
	h.waitPrimary(t, e2)

	// 15. The new lease and the move record.
	out, _ = h.command(t, show...)
	must(checkShowMoved(out, e, e2, g, l))

	// 16. The new epoch claimed HEAD.
	out, _ = h.command(t, chunkShow...)
	must(checkLayoutHeadReopened(out, e2, g, l))

	// 17. The block written before the hand-over reads back unchanged.
	c = dialNVMe(t, h.nvmeAddr, nqn)
	defer c.close()
	if got := sha256.Sum256(c.read(t, 0, 4096)); got != want {
		t.Fatalf("block 0 after the hand-over has sha256 %x; want %x", got, want)
	}
}

// TestStandaloneCleanMoveWithoutTheACKIsRefused is the chapter's negative
// check N1 made deterministic: the volume is sealed, its blockvolume is
// killed (a signal sends no ACK), and the lease is revoked with the sealed
// generation while no blockvolume runs. No ACK can arrive, so the clean move
// is refused with the documented error, exit 1, and the master keeps no ACK.
func TestStandaloneCleanMoveWithoutTheACKIsRefused(t *testing.T) {
	h, cluster, chunkPath, show, volArgs := startPlannedHandoff(t)
	must, mustU := fatalOn(t)

	vol := h.start(t, "blockvolume", h.volume, volArgs...)
	e := mustU(adoptedEpoch(vol.waitLog(t, 60*time.Second, "blockvolume: volume v1 adopted as PRIMARY at r1@")))
	h.waitPrimary(t, e)
	c := dialNVMe(t, h.nvmeAddr, plannedHandoffNQN)
	c.write(t, 0, bytes.Repeat([]byte{0xa5}, 4096))
	c.flush(t)
	c.close()
	out, errOut := h.command(t, "ops", "chunk", "detach", "--plain", "--volume", "v1", "--status-addr", h.statusAddr, "--timeout", "120s")
	g := mustU(sealedGen(out, errOut, e))
	out, _ = h.command(t, "ops", "chunk", "show", "--master", h.masterAddr, "--volume", "v1", "--chunk-filer", cluster.filerGRPC, "--chunk-path", chunkPath)
	mustU(sealedLayoutHeadPosition(out, e, g)) // HEAD is sealed: only the ACK is missing
	vol.kill(t)

	out, _ = h.command(t, "ops", "workspace", "revoke", "--master", h.masterAddr, "--volume", "v1", "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--sealed-gen", fmt.Sprint(g))
	must(checkRevoke(out, e, g))
	out, errOut, code := h.run(t, "ops", "workspace", "move", "--clean", "--master", h.masterAddr, "--volume", "v1", "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--target-host", "s1", "--target-data", h.dataAddr, "--target-ctrl", h.ctrlAddr,
		"--request-id", "move-v1-1")
	must(checkCleanMoveRefused(out, errOut, code))
	out, _ = h.command(t, show...)
	if showsAck(out, e, g) || !hasLine(out, "recorded ACK: none") {
		t.Fatalf("workspace show after the refused move:\n%s\nwant recorded ACK: none", out)
	}
}

const plannedHandoffNQN = "nqn.2026-11.io.seaweedfs:handoff"

// startPlannedHandoff runs the chapter's steps 1-3 for one host, s1, and
// returns the blockvolume arguments of step 4 and the show command.
func startPlannedHandoff(t *testing.T) (h *plannedHandoffHarness, cluster *w4WeedCluster, chunkPath string, show, volArgs []string) {
	t.Helper()
	env := startHandoffEnv(t, 1)
	return env.h, env.cluster, env.chunkPath, env.show, env.volArgs("s1")
}

// handoffEnv is a running setup: weed, blockmaster with the cluster spec
// naming every host, and the hosts' blockvolume endpoints. The volume starts
// on s1.
type handoffEnv struct {
	h         *plannedHandoffHarness
	cluster   *w4WeedCluster
	chunkPath string
	show      []string
	hosts     map[string]bvHost
	// volExtra is appended to every blockvolume command (a test that pins
	// a default the page leaves alone, for example).
	volExtra []string
	// A crash test restarts exactly this owned master with the same stores.
	masterProcess *daemon
	masterArgs    []string
	layoutMu      sync.Mutex
	layoutIDs     map[string]string
	// tls, when set, runs every process with mutual TLS (procedure_tls_harness_test.go).
	tls *procedureTLS
}

// bvHost is one host's blockvolume: its server id, endpoints, local root
// and the volume it serves (v1 when empty). server and replica, when set,
// replace id as the server id and r1 as the replica id (several volumes
// under one server id).
type bvHost struct {
	id, data, ctrl, status, nvme, root string
	volume                             string
	server, replica                    string
}

// startHandoffEnv runs the chapter's steps 1-3: a weed cluster, the cluster
// spec with hosts s1..sN, blockmaster with the lifecycle store (and
// masterArgs), and the setup check.
func startHandoffEnv(t *testing.T, n int, masterArgs ...string) *handoffEnv {
	t.Helper()
	return startHandoffEnvWith(t, n, nil, masterArgs...)
}

// startHandoffEnvWith is startHandoffEnv with restore targets: each of
// targets (a volume id) is placed on a node of its own, named after it, with
// its own endpoints and local root. A target has placement only, no volume
// record: its restore creates it.
func startHandoffEnvWith(t *testing.T, n int, targets []string, masterArgs ...string) *handoffEnv {
	t.Helper()
	return startHandoffEnvConfigured(t, n, targets, procedureTLSConfig(), false, masterArgs...)
}

// startHandoffEnvVolumes is startHandoffEnvWith with ordinary volumes: each
// of volumes gets a volume record from the cluster spec, so it starts
// without a restore.
func startHandoffEnvVolumes(t *testing.T, n int, volumes []string, masterArgs ...string) *handoffEnv {
	t.Helper()
	return startHandoffEnvConfigured(t, n, volumes, procedureTLSConfig(), true, masterArgs...)
}

// procedureTLSConfig is nil without mutual TLS, else an empty expiry map.
func procedureTLSConfig() map[string]time.Time {
	if procedureTLSOn() {
		return map[string]time.Time{}
	}
	return nil
}

// startHandoffEnvTLS is startHandoffEnvWith with mutual TLS when tlsNotAfter
// is non-nil (it overrides chosen node certificates' expiry).
func startHandoffEnvTLS(t *testing.T, n int, targets []string, tlsNotAfter map[string]time.Time, masterArgs ...string) *handoffEnv {
	return startHandoffEnvConfigured(t, n, targets, tlsNotAfter, true, masterArgs...)
}

// startHandoffEnvConfigured can reserve target placement without importing a
// VolumeRecord. The restore-create test uses that form so the master creates
// the target's identity and restored image in one request.
func startHandoffEnvConfigured(t *testing.T, n int, targets []string, tlsNotAfter map[string]time.Time, precreateTargets bool, masterArgs ...string) *handoffEnv {
	t.Helper()
	return startHandoffEnvOnCluster(t, n, targets, tlsNotAfter, precreateTargets, nil, masterArgs...)
}

// startHandoffEnvOnCluster also admits a caller-owned stock filer behind a
// fault-injection front; ordinary callers keep the default cluster unchanged.
func startHandoffEnvOnCluster(t *testing.T, n int, targets []string, tlsNotAfter map[string]time.Time, precreateTargets bool, cluster *w4WeedCluster, masterArgs ...string) *handoffEnv {
	t.Helper()
	h := newPlannedHandoffHarness(t)
	if cluster == nil {
		cluster = startWorkspaceWeed(t)
	}
	env := &handoffEnv{h: h, cluster: cluster, chunkPath: fmt.Sprintf("/handoff-%d", time.Now().UnixNano()), hosts: map[string]bvHost{}}
	must, _ := fatalOn(t)
	env.hosts["s1"] = bvHost{id: "s1", data: h.dataAddr, ctrl: h.ctrlAddr, status: h.statusAddr, nvme: h.nvmeAddr, root: filepath.Join(h.dir, "vol")}
	for i := 2; i <= n; i++ {
		id := fmt.Sprintf("s%d", i)
		env.hosts[id] = bvHost{id: id, data: handoffAddress(t), ctrl: handoffAddress(t), status: handoffAddress(t), nvme: handoffAddress(t), root: filepath.Join(h.dir, "vol-"+id)}
	}
	for _, v := range targets {
		id := "n-" + v
		env.hosts[id] = bvHost{id: id, data: handoffAddress(t), ctrl: handoffAddress(t), status: handoffAddress(t), nvme: handoffAddress(t), root: filepath.Join(h.dir, "vol-"+id), volume: v}
	}
	chunkPath := env.chunkPath
	if tlsNotAfter != nil {
		servers := make([]string, 0, len(env.hosts))
		for _, b := range env.hosts {
			servers = append(servers, b.id)
		}
		env.tls = newProcedureTLS(t, h.dir, servers, tlsNotAfter)
		h.cliEnv = env.tls.cliEnv("admin")
		masterArgs = append(env.tls.masterArgs(), masterArgs...)
	}

	// 1. The cluster spec.
	var nodes, volumes strings.Builder
	node := func(b bvHost) {
		fmt.Fprintf(&nodes, "  - server_id: %s\n    data_addr: %s\n    ctrl_addr: %s\n", b.id, b.data, b.ctrl)
	}
	volume := func(v, server string, create bool) {
		size := ""
		if create {
			size = "    size_bytes: 268435456\n"
		}
		fmt.Fprintf(&volumes, "  - id: %s\n%s    replication_factor: 1\n    placements:\n      - server_id: %s\n        replica_id: r1\n", v, size, server)
	}
	for i := 1; i <= n; i++ {
		node(env.hosts[fmt.Sprintf("s%d", i)])
	}
	volume("v1", "s1", true)
	for _, v := range targets {
		node(env.hosts["n-"+v])
		volume(v, "n-"+v, precreateTargets)
	}
	spec := filepath.Join(h.dir, "cluster-spec.yaml")
	must(os.WriteFile(spec, []byte("nodes:\n"+nodes.String()+"volumes:\n"+volumes.String()), 0o644))

	// 2. blockmaster with the lifecycle store and the cluster spec.
	mstate := filepath.Join(h.dir, "master")
	env.masterArgs = append([]string{
		"--authority-store", filepath.Join(mstate, "authority"), "--lifecycle-store", filepath.Join(mstate, "lifecycle"),
		"--cluster-spec", spec, "--expected-slots-per-volume", "1", "--listen", h.masterAddr,
		"--workspace-rpc"}, masterArgs...)
	env.masterArgs = append(env.masterArgs, "--chunk-filer", cluster.filerGRPC, "--chunk-path", chunkPath)
	master := h.start(t, "blockmaster", h.master, env.masterArgs...)
	env.masterProcess = master
	if l := master.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded="); !strings.Contains(l, "listen="+h.masterAddr) {
		t.Fatalf("blockmaster start line %q does not name listen=%s", l, h.masterAddr)
	}
	if master.has("blockmaster: WARNING: --topology not supplied or empty") {
		t.Fatal("blockmaster warned about a missing topology although the cluster spec gives it")
	}

	// 3. The setup answers before any blockvolume runs.
	env.show = []string{"ops", "workspace", "show", "--master", h.masterAddr, "--volume", "v1"}
	out, _ := h.command(t, env.show...)
	must(checkShowBeforeStart(out))
	// Cache the identities imported by the master while it is available. Some
	// tests assemble restart arguments after deliberately stopping the master.
	env.layoutIDs = make(map[string]string)
	env.layoutID("v1")
	if precreateTargets {
		for _, v := range targets {
			env.layoutID(v)
		}
	}
	return env
}

// volArgs is the chapter's blockvolume command for one host.
func (env *handoffEnv) volArgs(id string) []string { return env.volArgsFor(env.hosts[id]) }

func (env *handoffEnv) layoutID(volume string) string {
	t := env.h.t
	t.Helper()
	env.layoutMu.Lock()
	defer env.layoutMu.Unlock()
	if id := env.layoutIDs[volume]; id != "" {
		return id
	}
	conn, err := grpc.NewClient(env.h.masterAddr, env.masterDialOption(t, "admin"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// blockmaster logs "lock acquired" before it imports the cluster spec
	// (each volume's layout created on the filer) and only serves after
	// that, so a lookup right after that line can find it not serving yet:
	// retry while it is unavailable, within 60 s. Any other answer (not
	// found, refused) fails at once.
	var id layout.VolumeID
	for end := time.Now().Add(60 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		id, err = lifecycleclient.ResolveVolume(ctx, control.NewLifecycleServiceClient(conn), volume)
		cancel()
		if err == nil {
			break
		}
		if c := status.Code(err); (c != codes.Unavailable && c != codes.DeadlineExceeded) || time.Now().After(end) {
			t.Fatalf("resolve layout identity for %s: %v", volume, err)
		}
	}
	if env.layoutIDs == nil {
		env.layoutIDs = make(map[string]string)
	}
	env.layoutIDs[volume] = id.String()
	return id.String()
}

// volArgsFor is the chapter's blockvolume command for b. The default
// scheduled snapshot (daily:7, at a per-volume offset) stays on, as the page
// says; a snapshot inside a test could only make a test fail, never pass.
func (env *handoffEnv) volArgsFor(b bvHost) []string {
	lockDir := b.root + ".locks" // each simulated host has its own
	_ = os.MkdirAll(lockDir, 0o700)
	vol, server, replica := b.volume, b.server, b.replica
	if vol == "" {
		vol = "v1"
	}
	if server == "" {
		server = b.id
	}
	if replica == "" {
		replica = "r1"
	}
	return append([]string{"--volume-lock-dir-test-only", lockDir, "--master", env.h.masterAddr, "--server-id", server, "--volume-id", vol, "--replica-id", replica,
		"--layout-id", env.layoutID(vol),
		"--data-addr", b.data, "--ctrl-addr", b.ctrl, "--status-addr", b.status,
		"--durable-root", b.root, "--durable-impl", "walstore-chunk",
		"--durable-blocks", "65536", "--durable-blocksize", "4096",
		"--chunk-filer", env.cluster.filerGRPC, "--chunk-path", env.chunkPath, "--chunk-control-endpoint",
		"--workspace-lease", "--nvme-listen", b.nvme, "--nvme-subsysnqn", plannedHandoffNQN}, append(env.tlsVolumeArgs(server), env.volExtra...)...)
}

// tlsVolumeArgs is server's node identity when the env runs with mutual TLS.
func (env *handoffEnv) tlsVolumeArgs(server string) []string {
	if env.tls == nil {
		return nil
	}
	return env.tls.volumeArgs(server)
}

func fatalOn(t *testing.T) (func(error), func(uint64, error) uint64) {
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	return must, func(v uint64, err error) uint64 {
		t.Helper()
		must(err)
		return v
	}
}

// plannedHandoffHarness owns real binaries, loopback endpoints and process
// lifetimes.
type plannedHandoffHarness struct {
	dir                                                  string
	master, volume, cli                                  string
	masterAddr, dataAddr, ctrlAddr, statusAddr, nvmeAddr string
	// t logs every sw-block command (logCommand) until the test ends
	// (logDone): a runCLI goroutine that outlives it must not call t.Logf.
	t       *testing.T
	logMu   sync.Mutex
	logDone bool
	// cliEnv is added to every sw-block command's environment (its master
	// TLS identity when the env runs with mutual TLS).
	cliEnv []string
}

func newPlannedHandoffHarness(t *testing.T) *plannedHandoffHarness {
	t.Helper()
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Fatal("CHUNKSTORE_WEED must name the pinned real SeaweedFS binary")
	}
	h := &plannedHandoffHarness{dir: t.TempDir(), t: t}
	t.Cleanup(func() {
		h.logMu.Lock()
		h.logDone = true
		h.logMu.Unlock()
	})
	suffix := ""
	if isWindows() {
		suffix = ".exe"
	}
	for _, b := range []struct {
		name string
		dst  *string
	}{
		{"blockmaster", &h.master}, {"blockvolume", &h.volume}, {"sw-block", &h.cli},
	} {
		*b.dst = filepath.Join(h.dir, b.name+suffix)
		if d := os.Getenv("SWBLOCK_TEST_BINS"); d != "" {
			// Prebuilt binaries, for a test host without a Go toolchain.
			*b.dst = filepath.Join(d, b.name+suffix)
			if _, err := os.Stat(*b.dst); err != nil {
				t.Fatalf("SWBLOCK_TEST_BINS: %v", err)
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", *b.dst, "github.com/seaweedfs/seaweed-block/cmd/"+b.name)
		cmd.WaitDelay = 5 * time.Second
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("build %s: %v\n%s", b.name, err, out)
		}
	}
	h.masterAddr = handoffAddress(t)
	h.dataAddr = handoffAddress(t)
	h.ctrlAddr = handoffAddress(t)
	h.statusAddr = handoffAddress(t)
	h.nvmeAddr = handoffAddress(t)
	return h
}

// captureOutput writes each sw-block answer to $HANDOFF_CAPTURE_DIR, when set,
// as <test>-<n>-<verb>.stdout/.stderr: the source of testdata/planned-handoff.
func captureOutput(t *testing.T, args []string, stdout, stderr string) {
	dir := os.Getenv("HANDOFF_CAPTURE_DIR")
	if dir == "" || len(args) < 3 {
		return
	}
	captureSeq++
	base := filepath.Join(dir, fmt.Sprintf("%s-%02d-%s-%s", t.Name(), captureSeq, args[1], args[2]))
	_ = os.WriteFile(base+".stdout", []byte(stdout), 0o644)
	_ = os.WriteFile(base+".stderr", []byte(stderr), 0o644)
}

var captureSeq int

var handoffPorts struct {
	sync.Mutex
	used map[int]bool
}

// handoffAddress returns a free loopback address in 10000-19999: below the
// kernel's ephemeral range (32768 and up on Linux), where an outgoing
// connection can take a port while a restarted daemon waits to bind it
// again, and below the 20000-49999 range startWorkspaceWeed draws from.
func handoffAddress(t *testing.T) string {
	t.Helper()
	handoffPorts.Lock()
	defer handoffPorts.Unlock()
	if handoffPorts.used == nil {
		handoffPorts.used = map[int]bool{}
	}
	for i := 0; i < 500; i++ {
		lo, hi := testPortSpan(10000, 20000)
		p := lo + rand.Intn(hi-lo)
		if handoffPorts.used[p] {
			continue
		}
		addr := fmt.Sprintf("%s:%d", testListenHost(), p)
		if ln, err := net.Listen("tcp", addr); err == nil {
			ln.Close()
			handoffPorts.used[p] = true
			return addr
		}
	}
	t.Fatal("no free port below the ephemeral range on " + testListenHost())
	return ""
}

// daemon is one started child: its log file and its exit.
type daemon struct {
	name, log string
	done      chan struct{}
	err       error
	cancel    context.CancelFunc
	proc      *os.Process
}

// term sends SIGTERM, as systemctl stop or restart does, and waits for the
// child to exit.
func (d *daemon) term(t *testing.T, timeout time.Duration) {
	t.Helper()
	if err := d.proc.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM %s: %v", d.name, err)
	}
	select {
	case <-d.done:
	case <-time.After(timeout):
		t.Fatalf("%s did not exit within %s of SIGTERM", d.name, timeout)
	}
}

// kill kills the child (no ACK is sent on a signal) and waits for it.
func (d *daemon) kill(t *testing.T) {
	t.Helper()
	d.cancel()
	select {
	case <-d.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not exit after kill", d.name)
	}
}

// start registers cancellation and a bounded join before starting a daemon.
// Each child writes to its own file, and cleanup targets only that child.
// Cancellation kills the child; a test that needs a child to exit by itself
// waits for it with wait.
func (h *plannedHandoffHarness) start(t *testing.T, name, binary string, args ...string) *daemon {
	t.Helper()
	log, err := os.Create(filepath.Join(h.dir, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cmd := stripSystemdForTestLockDir(exec.CommandContext(ctx, binary, args...))
	cmd.Dir, cmd.Stdout, cmd.Stderr = h.dir, log, log
	cmd.WaitDelay = 5 * time.Second
	d := &daemon{name: name, log: log.Name(), done: make(chan struct{}), cancel: cancel}
	started := false
	t.Cleanup(func() {
		cancel()
		if started {
			select {
			case <-d.done:
			case <-time.After(10 * time.Second):
				t.Errorf("%s did not exit after cancellation", name)
			}
		}
		_ = log.Close()
		if t.Failed() {
			keepFullLog(t, d)
			data, _ := os.ReadFile(d.log)
			if len(data) > 16000 {
				data = data[len(data)-16000:]
			}
			t.Logf("%s log:\n%s", name, data)
		}
	})
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	started = true
	d.proc = cmd.Process
	go func() { d.err = cmd.Wait(); close(d.done) }()
	return d
}

// keepFullLog copies a failed test's daemon log, whole, into
// $SWBLOCK_TEST_LOG_DIR when that is set (a gate run collects it): the
// test's own output keeps only the log's tail, and a bring-up decision is
// at its head.
func keepFullLog(t *testing.T, d *daemon) {
	if err := keepLogCopy(t.Name(), d.name, d.log); err != nil {
		t.Logf("%s: full log not kept: %v", d.name, err)
	}
}

// wait waits up to timeout for the child to exit by itself and returns its
// exit error (nil: status 0).
func (d *daemon) wait(timeout time.Duration) error {
	select {
	case <-d.done:
		return d.err
	case <-time.After(timeout):
		return fmt.Errorf("%s still running after %s", d.name, timeout)
	}
}

// line returns the first log line containing s ("" if none).
func (d *daemon) line(s string) string {
	data, _ := os.ReadFile(d.log)
	for _, l := range strings.Split(string(data), "\n") {
		if strings.Contains(l, s) {
			return l
		}
	}
	return ""
}

func (d *daemon) has(s string) bool { return d.line(s) != "" }

// waitLog waits for a log line containing s and returns it. A child that
// exits first fails the test at once.
func (d *daemon) waitLog(t *testing.T, timeout time.Duration, s string) string {
	t.Helper()
	end := time.Now().Add(timeout)
	for {
		if l := d.line(s); l != "" {
			return l
		}
		select {
		case <-d.done:
			if l := d.line(s); l != "" {
				return l
			}
			t.Fatalf("%s exited (%v) before logging %q", d.name, d.err, s)
		default:
		}
		if time.Now().After(end) {
			t.Fatalf("%s did not log %q within %s", d.name, s, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// command keeps stdout separate from stderr: detach stdout remains JSON while
// its sealed-generation line is read from stderr by the documented sequence.
// The bound covers the detach's own --timeout 120s.
func (h *plannedHandoffHarness) command(t *testing.T, args ...string) (string, string) {
	t.Helper()
	stdout, stderr, code := h.run(t, args...)
	if code != 0 {
		t.Fatalf("sw-block %q: exit %d\nstdout: %s\nstderr: %s", args, code, stdout, stderr)
	}
	return stdout, stderr
}

// run runs sw-block and returns its output and exit status; only a failure
// to run it at all (or its timeout) fails the test.
func (h *plannedHandoffHarness) run(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, h.cli, args...)
	cmd.Dir = h.dir
	if len(h.cliEnv) > 0 {
		cmd.Env = append(os.Environ(), h.cliEnv...)
	}
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	captureOutput(t, args, stdout.String(), stderr.String())
	h.logCommand(args, stdout.String(), stderr.String(), err)
	var exit *exec.ExitError
	switch {
	case err == nil:
		return stdout.String(), stderr.String(), 0
	case errors.As(err, &exit) && ctx.Err() == nil:
		return stdout.String(), stderr.String(), exit.ExitCode()
	}
	t.Fatalf("sw-block %q: %v\nstdout: %s\nstderr: %s", args, err, stdout.String(), stderr.String())
	return "", "", -1
}

// logCommandCap bounds each logged stream of one sw-block command.
const logCommandCap = 4096

// logCommand writes one sw-block command's argv, exit status and output to
// the test log, so a refusal is visible without a rerun. It is safe from the
// goroutines runCLI runs on; after the test ended it logs nothing.
func (h *plannedHandoffHarness) logCommand(args []string, stdout, stderr string, err error) {
	h.logMu.Lock()
	defer h.logMu.Unlock()
	if h.t == nil || h.logDone {
		return
	}
	status := "exit 0"
	if err != nil {
		status = err.Error()
	}
	h.t.Logf("sw-block %q: %s\nstdout: %s\nstderr: %s", args, status, capLog(stdout), capLog(stderr))
}

func capLog(s string) string {
	if len(s) <= logCommandCap {
		return s
	}
	return s[:logCommandCap] + fmt.Sprintf("... (%d more bytes)", len(s)-logCommandCap)
}

// waitPrimary polls the status endpoint until the volume serves as a
// healthy primary at epoch.
func (h *plannedHandoffHarness) waitPrimary(t *testing.T, epoch uint64) {
	t.Helper()
	h.waitPrimaryAt(t, h.statusAddr, epoch)
}

// waitPrimaryAt polls one host's status endpoint until the volume serves
// there as a healthy primary at epoch.
func (h *plannedHandoffHarness) waitPrimaryAt(t *testing.T, statusAddr string, epoch uint64) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	poll(t, 60*time.Second, fmt.Sprintf("status at %s never showed a healthy primary at epoch %d", statusAddr, epoch), func() bool {
		resp, err := c.Get("http://" + statusAddr + "/status?volume=v1")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return statusPrimary(b, epoch)
	})
}

func poll(t *testing.T, timeout time.Duration, what string, fn func() bool) {
	t.Helper()
	end := time.Now().Add(timeout)
	for !fn() {
		if time.Now().After(end) {
			t.Fatal(what)
		}
		time.Sleep(time.Second)
	}
}

// nvmeInitiator is the smallest NVMe/TCP host the test needs: one admin and
// one IO queue, Write, Flush and Read of namespace 1. It runs in the test
// process, so the test needs neither root nor the kernel's nvme_tcp.
type nvmeInitiator struct {
	admin, io net.Conn
	r         *nvme.Reader
	w         *nvme.Writer
	cid       uint16
}

func dialNVMe(t *testing.T, addr, subNQN string) *nvmeInitiator {
	t.Helper()
	c := &nvmeInitiator{}
	cd := nvme.ConnectData{HostID: [16]byte{0x5e, 0xa1}, SubNQN: subNQN, HostNQN: "nqn.2026-11.io.seaweedfs:handoff-test"}
	var cntlID uint16
	for qid := uint32(0); qid <= 1; qid++ {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("nvme dial %s: %v", addr, err)
		}
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
		r, w := nvme.NewReader(conn), nvme.NewWriter(conn)
		if err := w.SendHeaderOnly(0x0, &nvme.ICRequest{PDUMaxR2T: 1}, 120); err != nil {
			t.Fatalf("nvme ICReq: %v", err)
		}
		if ch, err := r.Dequeue(); err != nil || ch.Type != 0x1 {
			t.Fatalf("nvme ICResp: type %v err %v", ch, err)
		}
		if err := r.Receive(&nvme.ICResponse{}); err != nil {
			t.Fatalf("nvme ICResp body: %v", err)
		}
		// The admin queue asks for a controller; the IO queue names it.
		cd.CNTLID = 0xFFFF
		if qid == 1 {
			cd.CNTLID = cntlID
		}
		buf := make([]byte, 1024)
		cd.Marshal(buf)
		c.cid++
		cmd := nvme.CapsuleCommand{OpCode: 0x7F, FCType: 0x01, CID: c.cid, D10: qid << 16}
		if err := w.SendWithData(0x4, 0, &cmd, 64, buf); err != nil {
			t.Fatalf("nvme Connect qid %d: %v", qid, err)
		}
		resp := nvmeResponse(t, r)
		if resp.Status != 0 {
			t.Fatalf("nvme Connect qid %d status 0x%04x", qid, resp.Status)
		}
		if qid == 0 {
			c.admin, cntlID = conn, uint16(resp.DW0&0xFFFF)
		} else {
			c.io, c.r, c.w = conn, r, w
		}
	}
	return c
}

func nvmeResponse(t *testing.T, r *nvme.Reader) nvme.CapsuleResponse {
	t.Helper()
	ch, err := r.Dequeue()
	if err != nil || ch.Type != 0x5 {
		t.Fatalf("nvme response: type %v err %v", ch, err)
	}
	var resp nvme.CapsuleResponse
	if err := r.Receive(&resp); err != nil {
		t.Fatalf("nvme response body: %v", err)
	}
	return resp
}

func (c *nvmeInitiator) ioCmd(op uint8, lba uint64, blocks int) nvme.CapsuleCommand {
	c.cid++
	cmd := nvme.CapsuleCommand{OpCode: op, CID: c.cid, NSID: 1}
	if blocks > 0 {
		cmd.D10, cmd.D11, cmd.D12 = uint32(lba), uint32(lba>>32), uint32(blocks-1)
	}
	return cmd
}

func (c *nvmeInitiator) write(t *testing.T, lba uint64, data []byte) {
	t.Helper()
	cmd := c.ioCmd(0x01, lba, len(data)/4096)
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("nvme Write: %v", err)
	}
	ch, err := c.r.Dequeue()
	if err != nil || ch.Type != 0x9 {
		t.Fatalf("nvme Write: want R2T, got type %v err %v", ch, err)
	}
	var r2t nvme.R2THeader
	if err := c.r.Receive(&r2t); err != nil {
		t.Fatalf("nvme R2T: %v", err)
	}
	h2c := nvme.H2CDataHeader{CCCID: cmd.CID, TAG: r2t.TAG, DATAL: uint32(len(data))}
	if err := c.w.SendWithData(0x6, 0, &h2c, 16, data); err != nil {
		t.Fatalf("nvme H2CData: %v", err)
	}
	if resp := nvmeResponse(t, c.r); resp.Status != 0 {
		t.Fatalf("nvme Write status 0x%04x", resp.Status)
	}
}

func (c *nvmeInitiator) flush(t *testing.T) {
	t.Helper()
	cmd := c.ioCmd(0x00, 0, 0)
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("nvme Flush: %v", err)
	}
	if resp := nvmeResponse(t, c.r); resp.Status != 0 {
		t.Fatalf("nvme Flush status 0x%04x", resp.Status)
	}
}

func (c *nvmeInitiator) read(t *testing.T, lba uint64, n int) []byte {
	t.Helper()
	cmd := c.ioCmd(0x02, lba, n/4096)
	if err := c.w.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("nvme Read: %v", err)
	}
	var data []byte
	for {
		ch, err := c.r.Dequeue()
		if err != nil {
			t.Fatalf("nvme Read: %v", err)
		}
		if ch.Type == 0x5 {
			var resp nvme.CapsuleResponse
			if err := c.r.Receive(&resp); err != nil || resp.Status != 0 {
				t.Fatalf("nvme Read status 0x%04x err %v", resp.Status, err)
			}
			break
		}
		if ch.Type != 0x7 {
			t.Fatalf("nvme Read: unexpected PDU type 0x%x", ch.Type)
		}
		var dh nvme.C2HDataHeader
		if err := c.r.Receive(&dh); err != nil {
			t.Fatalf("nvme C2HData: %v", err)
		}
		part := make([]byte, c.r.Length())
		if err := c.r.ReceiveData(part); err != nil {
			t.Fatalf("nvme C2HData payload: %v", err)
		}
		data = append(data, part...)
	}
	if len(data) != n {
		t.Fatalf("nvme Read returned %d bytes; want %d", len(data), n)
	}
	return data
}

func (c *nvmeInitiator) close() {
	for _, conn := range []net.Conn{c.io, c.admin} {
		if conn != nil {
			_ = conn.Close()
		}
	}
}
