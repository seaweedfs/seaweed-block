//go:build subprocess && chunkstore_integration

package main_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// TestStandalonePlannedHandoffToTheSameHost runs the planned hand-over back
// to the same host with the commands of docs/volume-lifecycle.md, in its
// order and form, against real blockmaster, blockvolume, sw-block and
// SeaweedFS processes on loopback: cluster spec, start, write, plain detach,
// revoke with the sealed generation, the old process acknowledges and exits
// by itself, show the ACK, clean move to the same host, start again, and the
// written block reads back unchanged.
func TestStandalonePlannedHandoffToTheSameHost(t *testing.T) {
	h := newPlannedHandoffHarness(t)
	cluster := startWorkspaceWeed(t)
	chunkPath := fmt.Sprintf("/handoff-%d", time.Now().UnixNano())
	const nqn = "nqn.2026-11.io.seaweedfs:handoff"
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	mustU := func(v uint64, err error) uint64 {
		t.Helper()
		must(err)
		return v
	}

	// 1. The cluster spec.
	spec := filepath.Join(h.dir, "cluster-spec.yaml")
	must(os.WriteFile(spec, []byte(fmt.Sprintf(`nodes:
  - server_id: s1
    data_addr: %s
    ctrl_addr: %s
volumes:
  - id: v1
    size_bytes: 268435456
    replication_factor: 1
    placements:
      - server_id: s1
        replica_id: r1
`, h.dataAddr, h.ctrlAddr)), 0o644))

	// 2. blockmaster with the lifecycle store and the cluster spec.
	mstate := filepath.Join(h.dir, "master")
	master := h.start(t, "blockmaster", h.master,
		"--authority-store", filepath.Join(mstate, "authority"), "--lifecycle-store", filepath.Join(mstate, "lifecycle"),
		"--cluster-spec", spec, "--expected-slots-per-volume", "1", "--listen", h.masterAddr,
		"--workspace-rpc", "--workspace-head-base-url", "http://"+cluster.filerHTTP+chunkPath)
	if l := master.waitLog(t, 30*time.Second, "blockmaster: lock acquired, reloaded="); !strings.Contains(l, "listen="+h.masterAddr) {
		t.Fatalf("blockmaster start line %q does not name listen=%s", l, h.masterAddr)
	}
	if master.has("blockmaster: WARNING: --topology not supplied or empty") {
		t.Fatal("blockmaster warned about a missing topology although the cluster spec gives it")
	}

	// 3. The setup answers before any blockvolume runs.
	show := []string{"ops", "workspace", "show", "--master", h.masterAddr, "--volume", "v1"}
	out, _ := h.command(t, show...)
	must(checkShowBeforeStart(out))

	// 4. blockvolume adopts the volume.
	volArgs := []string{"--master", h.masterAddr, "--server-id", "s1", "--volume-id", "v1", "--replica-id", "r1",
		"--data-addr", h.dataAddr, "--ctrl-addr", h.ctrlAddr, "--status-addr", h.statusAddr,
		"--durable-root", filepath.Join(h.dir, "vol"), "--durable-impl", "walstore-chunk",
		"--durable-blocks", "65536", "--durable-blocksize", "4096",
		"--chunk-filer", cluster.filerGRPC, "--chunk-path", chunkPath, "--chunk-control-endpoint",
		"--workspace-lease", "--nvme-listen", h.nvmeAddr, "--nvme-subsysnqn", nqn}
	vol1 := h.start(t, "blockvolume-1", h.volume, volArgs...)
	e := mustU(adoptedEpoch(vol1.waitLog(t, 60*time.Second, "blockvolume: volume v1 adopted as PRIMARY at r1@")))
	vol1.waitLog(t, 60*time.Second, "durable: HEAD claim for volume=v1 done in")
	h.waitPrimary(t, e)

	// 5. The lease.
	out, _ = h.command(t, show...)
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
	chunkShow := []string{"ops", "chunk", "show", "--volume", "v1", "--chunk-filer", cluster.filerGRPC, "--chunk-path", chunkPath}
	out, _ = h.command(t, chunkShow...)
	l := mustU(sealedHeadLSN(out, e, g))

	// 10. Revoke with the sealed generation.
	out, _ = h.command(t, "ops", "workspace", "revoke", "--master", h.masterAddr, "--volume", "v1", "--replica", "r1",
		"--epoch", fmt.Sprint(e), "--sealed-gen", fmt.Sprint(g))
	must(checkRevoke(out, e, g))

	// 11. The old blockvolume acknowledges and exits by itself (a signal
	// would suppress the ACK).
	if err := vol1.wait(45 * time.Second); err != nil {
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
	vol2.waitLog(t, 60*time.Second, fmt.Sprintf(`durable: HEAD claim for volume=v1 starting: target epoch=%d holder="s1/r1"`, e2))
	must(checkClaimDone(vol2.waitLog(t, 60*time.Second, "durable: HEAD claim for volume=v1 done in"), e2))
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
	must(checkHeadReopened(out, e2, g, l))

	// 17. The block written before the hand-over reads back unchanged.
	c = dialNVMe(t, h.nvmeAddr, nqn)
	defer c.close()
	if got := sha256.Sum256(c.read(t, 0, 4096)); got != want {
		t.Fatalf("block 0 after the hand-over has sha256 %x; want %x", got, want)
	}
}

// plannedHandoffHarness owns real binaries, loopback endpoints and process
// lifetimes.
type plannedHandoffHarness struct {
	dir                                                  string
	master, volume, cli                                  string
	masterAddr, dataAddr, ctrlAddr, statusAddr, nvmeAddr string
}

func newPlannedHandoffHarness(t *testing.T) *plannedHandoffHarness {
	t.Helper()
	if os.Getenv("CHUNKSTORE_WEED") == "" {
		t.Fatal("CHUNKSTORE_WEED must name the pinned real SeaweedFS binary")
	}
	h := &plannedHandoffHarness{dir: t.TempDir()}
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
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", *b.dst, "github.com/seaweedfs/seaweed-block/cmd/"+b.name)
		cmd.WaitDelay = 5 * time.Second
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("build %s: %v\n%s", b.name, err, out)
		}
	}
	h.masterAddr = leaseBinaryAddress(t)
	h.dataAddr = leaseBinaryAddress(t)
	h.ctrlAddr = leaseBinaryAddress(t)
	h.statusAddr = leaseBinaryAddress(t)
	h.nvmeAddr = leaseBinaryAddress(t)
	return h
}

// daemon is one started child: its log file and its exit.
type daemon struct {
	name, log string
	done      chan struct{}
	err       error
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
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = h.dir, log, log
	cmd.WaitDelay = 5 * time.Second
	d := &daemon{name: name, log: log.Name(), done: make(chan struct{})}
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
	go func() { d.err = cmd.Wait(); close(d.done) }()
	return d
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
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, h.cli, args...)
	cmd.Dir = h.dir
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("sw-block %q: %v\nstdout: %s\nstderr: %s", args, err, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String()
}

// waitPrimary polls the status endpoint until the volume serves as a
// healthy primary at epoch.
func (h *plannedHandoffHarness) waitPrimary(t *testing.T, epoch uint64) {
	t.Helper()
	c := &http.Client{Timeout: 5 * time.Second}
	poll(t, 60*time.Second, fmt.Sprintf("status never showed a healthy primary at epoch %d", epoch), func() bool {
		resp, err := c.Get("http://" + h.statusAddr + "/status?volume=v1")
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
