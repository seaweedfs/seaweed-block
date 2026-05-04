// Ownership: QA (from sketch-approved test spec v3-phase-15-t2-test-spec.md for T2 First Real Frontends).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t2-test-spec.md
// Maps to ledger rows: INV-FRONTEND-NVME-001 (Integration, L2-harness),
//                      INV-FRONTEND-PROTOCOL-001 (Integration, L2-harness)
//
// T2 L2 EVIDENCE CLASS: L2-harness
//
// Test layer: Integration (L2-harness only)
// Protocol: NVMe/TCP
// Bounded fate (mirror of iSCSI L2-harness):
//   - Real cmd/blockmaster + cmd/blockvolume subprocesses; volume
//     serves NVMe/TCP via --nvme-listen; Go initiator (this test)
//     attaches, round-trips Write/Read bytes.
//   - After a product-route authority move (primary volume restart
//     with new data/ctrl addrs → IntentRefreshEndpoint → EV bumps),
//     a fresh fabric Connect to the same NVMe addr serves the new
//     lineage correctly (L2.n4 reopen-after-move).
//
// NOT covered (per QA checkpoint-5 scope C5.3, applied here for
// NVMe symmetry): T2.L2.n2 / n3 (stale Write / Read after move).
// Those are L2-OS REQUIRED critical cells per sketch §10 — Linux
// nvme-cli / kernel behavior around ANA Transition is load-bearing
// evidence the harness cannot substitute for. Tracked for L2-OS
// on m01.
//
// Sign-off tier per this checkpoint: T2B-NVMe-provisional
// (QA Owner + sw lead per §8B.9). Product-ready requires L2-OS.
package nvme_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
)

// T2.L2.n1 — real product subprocess + Go NVMe/TCP initiator;
// attach + Write + Read round-trip.
func TestT2Process_NVMe_AttachWriteRead(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; skipped under -short")
	}
	h := startL2NvmeHarness(t)
	defer h.Close()

	cli := dialAndConnect(t, h.nvmeAddr)
	defer cli.close()

	payload := make([]byte, nvme.DefaultBlockSize)
	copy(payload, []byte("l2-harness-nvme-roundtrip"))

	status := cli.writeCmd(t, 0, 1, payload)
	expectStatusSuccess(t, status, "Write")

	status, data := cli.readCmd(t, 0, 1, int(nvme.DefaultBlockSize))
	expectStatusSuccess(t, status, "Read")
	if !bytes.Equal(data, payload) {
		t.Fatalf("Read mismatch: first16=%q want %q", first16N(data), first16N(payload))
	}
}

// T2.L2.n4 — reopen after authority move serves the new lineage.
func TestT2Process_NVMe_ReopenAfterMove_ServesNewLineage(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; skipped under -short")
	}
	h := startL2NvmeHarness(t)
	defer h.Close()

	// Pre-move write.
	cli := dialAndConnect(t, h.nvmeAddr)
	payload := make([]byte, nvme.DefaultBlockSize)
	copy(payload, []byte("pre-move"))
	status := cli.writeCmd(t, 0, 1, payload)
	expectStatusSuccess(t, status, "Write pre-move")
	cli.close()

	// Restart r1 with new data/ctrl addrs (same status + nvme addrs).
	h.restartR1WithNewFrontendEndpoints(t,
		"127.0.0.1:9071", "127.0.0.1:9171")

	// Wait for projection to come back Healthy on new lineage.
	waitForStatusHealthyN(t, h.statusAddr, "v1", 30*time.Second)

	// Fresh Connect — new lineage serves writes/reads.
	cli2 := dialAndConnect(t, h.nvmeAddr)
	defer cli2.close()
	post := make([]byte, nvme.DefaultBlockSize)
	copy(post, []byte("post-move"))
	status = cli2.writeCmd(t, 0, 1, post)
	expectStatusSuccess(t, status, "Write post-move")
	status, data := cli2.readCmd(t, 0, 1, int(nvme.DefaultBlockSize))
	expectStatusSuccess(t, status, "Read post-move")
	if !bytes.Contains(data, []byte("post-move")) {
		t.Fatalf("post-move Read does not contain post-move payload: first16=%q", first16N(data))
	}
}

func first16N(b []byte) string {
	if len(b) > 16 {
		return string(b[:16])
	}
	return string(b)
}

// ----------- harness -----------

type l2NvmeHarness struct {
	bins       l2bins
	master     *subproc
	volR1      *subproc
	volR2      *subproc
	volR3      *subproc
	statusAddr string
	nvmeAddr   string
	masterAddr string
	subsysNQN  string
}

func (h *l2NvmeHarness) Close() {
	for _, p := range []*subproc{h.master, h.volR1, h.volR2, h.volR3} {
		if p != nil {
			p.stop()
		}
	}
}

func (h *l2NvmeHarness) restartR1WithNewFrontendEndpoints(t *testing.T, newDataAddr, newCtrlAddr string) {
	t.Helper()
	h.volR1.stop()
	time.Sleep(500 * time.Millisecond)
	h.volR1 = startVolumeWithNvme(t, h.bins, h.masterAddr, "s1", "v1", "r1",
		newDataAddr, newCtrlAddr, h.statusAddr, h.nvmeAddr, h.subsysNQN)
}

func startL2NvmeHarness(t *testing.T) *l2NvmeHarness {
	t.Helper()
	art := t.TempDir()
	bins := buildL2NvmeBinaries(t, art)
	topoPath := filepath.Join(art, "topology.yaml")
	writeNvmeTopo(t, topoPath)
	storeDir := filepath.Join(art, "store")

	statusAddr := reserveLoopbackPortN(t)
	nvmeAddr := reserveLoopbackPortN(t)

	master := startMasterN(t, bins.master, storeDir, topoPath)
	const subsys = "nqn.2026-04.example.v3:subsys"
	volR1 := startVolumeWithNvme(t, bins, master.addr, "s1", "v1", "r1",
		"127.0.0.1:9061", "127.0.0.1:9161", statusAddr, nvmeAddr, subsys)
	volR2 := startVolumeBareN(t, bins, master.addr, "s2", "v1", "r2",
		"127.0.0.1:9062", "127.0.0.1:9162")
	volR3 := startVolumeBareN(t, bins, master.addr, "s3", "v1", "r3",
		"127.0.0.1:9063", "127.0.0.1:9163")

	waitNvmeReachable(t, nvmeAddr, 15*time.Second)
	return &l2NvmeHarness{
		bins:       bins,
		master:     master,
		volR1:      volR1,
		volR2:      volR2,
		volR3:      volR3,
		statusAddr: statusAddr,
		nvmeAddr:   nvmeAddr,
		masterAddr: master.addr,
		subsysNQN:  subsys,
	}
}

func waitNvmeReachable(t *testing.T, addr string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("nvme target not reachable at %s within %v", addr, deadline)
}

func waitForStatusHealthyN(t *testing.T, statusAddr, volumeID string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if queryHealthy(statusAddr, volumeID) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("projection did not return to Healthy at %s within %v", statusAddr, deadline)
}

func queryHealthy(statusAddr, volumeID string) bool {
	conn, err := net.DialTimeout("tcp", statusAddr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()
	req := "GET /status?volume=" + volumeID + " HTTP/1.0\r\nHost: local\r\n\r\n"
	_, _ = conn.Write([]byte(req))
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	body, _ := io.ReadAll(conn)
	idx := bytes.Index(body, []byte("\r\n\r\n"))
	if idx < 0 {
		return false
	}
	var p struct{ Healthy bool }
	if json.Unmarshal(body[idx+4:], &p) != nil {
		return false
	}
	return p.Healthy
}

func reserveLoopbackPortN(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// ----------- subproc -----------

type l2bins struct{ master, volume string }

func buildL2NvmeBinaries(t *testing.T, outDir string) l2bins {
	t.Helper()
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	b := l2bins{
		master: filepath.Join(outDir, "blockmaster"+ext),
		volume: filepath.Join(outDir, "blockvolume"+ext),
	}
	for pkg, out := range map[string]string{
		"github.com/seaweedfs/seaweed-block/cmd/blockmaster": b.master,
		"github.com/seaweedfs/seaweed-block/cmd/blockvolume": b.volume,
	} {
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("build %s: %v", pkg, err)
		}
	}
	return b
}

func writeNvmeTopo(t *testing.T, path string) {
	t.Helper()
	body := `volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
      - replica_id: r2
        server_id: s2
      - replica_id: r3
        server_id: s3
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write topology: %v", err)
	}
}

type subproc struct {
	label string
	cmd   *exec.Cmd
	mu    sync.Mutex
	addr  string
}

func startMasterN(t *testing.T, bin, storeDir, topoPath string) *subproc {
	t.Helper()
	cmd := exec.Command(bin,
		"--authority-store", storeDir,
		"--listen", "127.0.0.1:0",
		"--topology", topoPath,
		"--t0-print-ready",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start master: %v", err)
	}
	sp := &subproc{label: "master", cmd: cmd}
	br := bufio.NewReader(stdout)
	line, err := br.ReadBytes('\n')
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("master ready line: %v", err)
	}
	var rl struct {
		Addr string `json:"addr"`
	}
	if err := json.Unmarshal(line, &rl); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("parse master ready line: %v", err)
	}
	sp.addr = rl.Addr
	go func() { _, _ = io.Copy(io.Discard, br) }()
	return sp
}

func startVolumeBareN(t *testing.T, bins l2bins, masterAddr, serverID, volumeID, replicaID, dataAddr, ctrlAddr string) *subproc {
	t.Helper()
	cmd := exec.Command(bins.volume,
		"--master", masterAddr,
		"--server-id", serverID,
		"--volume-id", volumeID,
		"--replica-id", replicaID,
		"--data-addr", dataAddr,
		"--ctrl-addr", ctrlAddr,
		"--heartbeat-interval", "200ms",
		"--t1-readiness",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start volume %s: %v", serverID, err)
	}
	return &subproc{label: "volume-" + replicaID, cmd: cmd}
}

func startVolumeWithNvme(t *testing.T, bins l2bins, masterAddr, serverID, volumeID, replicaID, dataAddr, ctrlAddr, statusAddr, nvmeAddr, subsysNQN string) *subproc {
	t.Helper()
	cmd := exec.Command(bins.volume,
		"--master", masterAddr,
		"--server-id", serverID,
		"--volume-id", volumeID,
		"--replica-id", replicaID,
		"--data-addr", dataAddr,
		"--ctrl-addr", ctrlAddr,
		"--heartbeat-interval", "200ms",
		"--t1-readiness",
		"--status-addr", statusAddr,
		"--nvme-listen", nvmeAddr,
		"--nvme-subsysnqn", subsysNQN,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start volume %s (nvme): %v", serverID, err)
	}
	return &subproc{label: "volume-" + replicaID, cmd: cmd}
}

func (s *subproc) stop() {
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = s.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = s.cmd.Process.Kill()
		<-done
	}
}
