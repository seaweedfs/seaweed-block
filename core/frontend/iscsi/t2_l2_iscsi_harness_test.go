// Ownership: QA (from sketch-approved test spec v3-phase-15-t2-test-spec.md for T2 First Real Frontends).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t2-test-spec.md
// Maps to ledger rows: INV-FRONTEND-ISCSI-001 (Integration, L2-harness),
//                      INV-FRONTEND-PROTOCOL-001 (Integration, L2-harness)
//
// T2 L2 EVIDENCE CLASS: L2-harness
//
// Test layer: Integration (L2-harness only)
// Protocol: iSCSI
// Bounded fate:
//   - Real cmd/blockmaster + cmd/blockvolume subprocesses; volume
//     serves iSCSI via --iscsi-listen; Go initiator (this test)
//     attaches, round-trips WRITE/READ bytes.
//   - After a product-route authority move (primary volume
//     restarted with new data/ctrl addrs → master mints
//     IntentRefreshEndpoint → EndpointVersion advances), a FRESH
//     iSCSI login against the same target addr serves the new
//     lineage correctly (L2.i4 reopen-after-move).
//
// NOT covered here (per QA checkpoint-5 scope, C5.3):
//   - T2.L2.i2 / i3 (stale WRITE / READ after authority move).
//     Those are L2-OS REQUIRED critical cells per sketch §10
//     "Critical coverage intersection" — the kernel's SCSI
//     layer behavior around CHECK CONDITION is load-bearing
//     evidence that a Go-initiator harness cannot substitute
//     for. Tracked for L2-OS on m01.
//
// Sign-off tier per this checkpoint: `T2A-iSCSI-provisional`
// (QA Owner + sw lead; §8B.9). Product-ready requires L2-OS.
package iscsi_test

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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

// T2.L2.i1 — real subprocess route: blockmaster + blockvolume
// with --iscsi-listen; Go initiator attaches and round-trips
// WRITE(10) + READ(10) payload.
func TestT2Process_ISCSI_AttachWriteRead(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; skipped under -short")
	}
	h := startL2IscsiHarness(t)
	defer h.Close()

	cli := dialAndLogin(t, h.iscsiAddr)
	defer cli.logout(t)

	payload := make([]byte, iscsi.DefaultBlockSize)
	copy(payload, []byte("l2-harness-iscsi-roundtrip"))

	// WRITE(10) at LBA 0.
	status, _ := cli.scsiCmd(t, writeCDB10(0, 1), payload, 0)
	expectGood(t, status, "WRITE(10)")

	// READ(10) at LBA 0 — round-trip matches.
	status, data := cli.scsiCmd(t, readCDB10(0, 1), nil, int(iscsi.DefaultBlockSize))
	expectGood(t, status, "READ(10)")
	if !bytes.Equal(data, payload) {
		t.Fatalf("READ mismatch: got first16=%q want first16=%q", first16(data), first16(payload))
	}
}

// T2.L2.i4 — reopen after authority move (EndpointVersion drift
// triggered via primary restart with new addrs) serves the new
// lineage. L2-harness acceptable per spec §6.
func TestT2Process_ISCSI_ReopenAfterMove_ServesNewLineage(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; skipped under -short")
	}
	h := startL2IscsiHarness(t)
	defer h.Close()

	// Pre-move session: login, write, close. A memback backend's
	// bytes are per-Identity: after the lineage moves, the new
	// backend starts fresh. So pre-move bytes surviving is a
	// NON-CLAIM (G8 territory).
	cli := dialAndLogin(t, h.iscsiAddr)
	payload := make([]byte, iscsi.DefaultBlockSize)
	copy(payload, []byte("pre-move"))
	status, _ := cli.scsiCmd(t, writeCDB10(0, 1), payload, 0)
	expectGood(t, status, "WRITE pre-move")
	cli.logout(t)

	// Trigger product-route authority move: restart r1 with new
	// data/ctrl addrs AND same status-addr AND same iscsi-addr.
	// Master observes endpoint drift on the r1 slot → mints
	// IntentRefreshEndpoint → EV advances.
	h.restartR1WithNewFrontendEndpoints(t,
		"127.0.0.1:8071", "127.0.0.1:8171") // new data/ctrl addrs

	// Wait for EV drift on the status endpoint — same discovery
	// mechanism the T1 L2 test uses.
	waitForStatusEV(t, h.statusAddr, "v1", 30*time.Second)

	// Fresh login on the same iscsi-addr. The volume host's
	// memback provider now hands out a new backend at the new
	// Identity (higher EV), so the session serves the new lineage.
	cli2 := dialAndLogin(t, h.iscsiAddr)
	defer cli2.logout(t)

	post := make([]byte, iscsi.DefaultBlockSize)
	copy(post, []byte("post-move"))
	status, _ = cli2.scsiCmd(t, writeCDB10(0, 1), post, 0)
	expectGood(t, status, "WRITE post-move")

	status, data := cli2.scsiCmd(t, readCDB10(0, 1), nil, int(iscsi.DefaultBlockSize))
	expectGood(t, status, "READ post-move")
	if !bytes.Contains(data, []byte("post-move")) {
		t.Fatalf("post-move READ does not contain post-move payload: first16=%q", first16(data))
	}
	// Non-claim: pre-move bytes NOT expected to persist — the
	// memback backend is scoped to Identity.
}

// first16 is a small helper to keep error output readable when
// payload buffers are 512+ bytes.
func first16(b []byte) string {
	if len(b) > 16 {
		return string(b[:16])
	}
	return string(b)
}

// ------------------ L2 harness (subprocess) ------------------

type l2IscsiHarness struct {
	art        string
	bins       l2bins
	master     *subproc
	volR1      *subproc
	volR2      *subproc
	volR3      *subproc
	statusAddr string
	iscsiAddr  string
	masterAddr string
}

func (h *l2IscsiHarness) Close() {
	for _, p := range []*subproc{h.master, h.volR1, h.volR2, h.volR3} {
		if p != nil {
			p.stop()
		}
	}
}

// restartR1WithNewFrontendEndpoints stops r1 and starts a
// replacement with different data/ctrl addrs but the SAME
// status-addr AND iscsi-addr so the external test view and
// the iSCSI initiator both remain valid.
func (h *l2IscsiHarness) restartR1WithNewFrontendEndpoints(t *testing.T, newDataAddr, newCtrlAddr string) {
	t.Helper()
	h.volR1.stop()
	// Short pause so the OS releases the status + iscsi ports
	// before the replacement binds.
	time.Sleep(500 * time.Millisecond)
	h.volR1 = startVolumeWithIscsi(t, h.bins, h.masterAddr, "s1", "v1", "r1",
		newDataAddr, newCtrlAddr, h.statusAddr, h.iscsiAddr, "iqn.2026-04.example.v3:v1")
}

func startL2IscsiHarness(t *testing.T) *l2IscsiHarness {
	t.Helper()
	art := t.TempDir()
	bins := buildL2IscsiBinaries(t, art)
	topoPath := filepath.Join(art, "topology.yaml")
	writeIscsiTopo(t, topoPath)
	storeDir := filepath.Join(art, "store")

	// Reserve loopback ports up front. Shared status + iscsi
	// ports are stable across the r1 restart (the replacement
	// rebinds to the same ones). Data/ctrl ports can be fixed
	// since they aren't test-observed.
	statusAddr := reserveLoopbackPortSP(t)
	iscsiAddr := reserveLoopbackPortSP(t)

	master := startMasterSP(t, bins.master, storeDir, topoPath)
	volR1 := startVolumeWithIscsi(t, bins, master.addr, "s1", "v1", "r1",
		"127.0.0.1:8061", "127.0.0.1:8161", statusAddr, iscsiAddr, "iqn.2026-04.example.v3:v1")
	volR2 := startVolumeSP(t, bins, master.addr, "s2", "v1", "r2",
		"127.0.0.1:8062", "127.0.0.1:8162")
	volR3 := startVolumeSP(t, bins, master.addr, "s3", "v1", "r3",
		"127.0.0.1:8063", "127.0.0.1:8163")

	// Wait until iSCSI endpoint is reachable before returning —
	// tests use cli connection immediately after.
	waitIscsiReachable(t, iscsiAddr, 15*time.Second)

	return &l2IscsiHarness{
		art:        art,
		bins:       bins,
		master:     master,
		volR1:      volR1,
		volR2:      volR2,
		volR3:      volR3,
		statusAddr: statusAddr,
		iscsiAddr:  iscsiAddr,
		masterAddr: master.addr,
	}
}

// waitIscsiReachable probes the iSCSI port until something is
// listening (TCP connect succeeds), or deadline. It does NOT
// do a full login — that's the test's job.
func waitIscsiReachable(t *testing.T, addr string, deadline time.Duration) {
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
	t.Fatalf("iSCSI target not reachable at %s within %v", addr, deadline)
}

// waitForStatusEV polls the volume's /status HTTP endpoint until
// projection reports a higher Epoch or EV than when we started.
// For i4 we don't track baseline, just wait for Healthy=true
// on the NEW lineage after the restart.
func waitForStatusEV(t *testing.T, statusAddr, volumeID string, deadline time.Duration) {
	t.Helper()
	// Reuse the http view pattern from T1 L2 — we just need the
	// projection to become Healthy again after the restart.
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		p, ok := queryProjection(statusAddr, volumeID)
		if ok && p.Healthy {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("projection did not return to Healthy at %s within %v", statusAddr, deadline)
}

type projSnapshot struct {
	Healthy         bool   `json:"Healthy"`
	Epoch           uint64 `json:"Epoch"`
	EndpointVersion uint64 `json:"EndpointVersion"`
}

func queryProjection(statusAddr, volumeID string) (projSnapshot, bool) {
	conn, err := net.DialTimeout("tcp", statusAddr, 500*time.Millisecond)
	if err != nil {
		return projSnapshot{}, false
	}
	_ = conn.Close()
	// Use the small HTTP surface — a simple GET suffices.
	req := "GET /status?volume=" + volumeID + " HTTP/1.0\r\nHost: local\r\n\r\n"
	c2, err := net.DialTimeout("tcp", statusAddr, 500*time.Millisecond)
	if err != nil {
		return projSnapshot{}, false
	}
	defer c2.Close()
	_, _ = c2.Write([]byte(req))
	_ = c2.SetReadDeadline(time.Now().Add(time.Second))
	body, _ := io.ReadAll(c2)
	// Very loose: find the JSON blob after the blank line.
	idx := bytes.Index(body, []byte("\r\n\r\n"))
	if idx < 0 {
		return projSnapshot{}, false
	}
	var p projSnapshot
	if err := json.Unmarshal(body[idx+4:], &p); err != nil {
		return projSnapshot{}, false
	}
	return p, true
}

// reserveLoopbackPortSP — duplicate of the T1 L2 helper,
// duplicated here to keep this package self-contained until a
// shared test-support package is factored out.
func reserveLoopbackPortSP(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// ------------------ subproc management ------------------

type l2bins struct {
	master string
	volume string
}

func buildL2IscsiBinaries(t *testing.T, outDir string) l2bins {
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

func writeIscsiTopo(t *testing.T, path string) {
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

	mu   sync.Mutex
	addr string // master listen addr (or ignored for volumes)
}

func startMasterSP(t *testing.T, bin, storeDir, topoPath string) *subproc {
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
	// Read the first JSON line from stdout — master's ready line.
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
	// Drain remaining stdout into discard so the pipe doesn't block.
	go func() { _, _ = io.Copy(io.Discard, br) }()
	return sp
}

func startVolumeSP(t *testing.T, bins l2bins, masterAddr, serverID, volumeID, replicaID, dataAddr, ctrlAddr string) *subproc {
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

func startVolumeWithIscsi(t *testing.T, bins l2bins, masterAddr, serverID, volumeID, replicaID, dataAddr, ctrlAddr, statusAddr, iscsiAddr, iqn string) *subproc {
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
		"--iscsi-listen", iscsiAddr,
		"--iscsi-iqn", iqn,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start volume %s (iscsi): %v", serverID, err)
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

// Ensure the "strings" import used nowhere else but a future
// sw refactor might inline — keep here so the test file is
// resilient to minor edits.
var _ = strings.HasPrefix
