// Ownership: QA (from sketch-approved test spec v3-phase-15-t1-test-spec.md for T1 Frontend Contract Smoke).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t1-test-spec.md
// Maps to ledger rows: INV-FRONTEND-001 + INV-FRONTEND-002 + INV-OBS-REALPROC-001 (consume)
//
// Test layer: Integration (L2) over the real product route.
//
// What this L2 PROVES (claimable in the closure report):
//   1. cmd/blockmaster + cmd/blockvolume as separate OS
//      processes reach engine.ModeHealthy via the real
//      publication route.
//   2. Volume exposes frontend.ProjectionView over HTTP;
//      frontend.Provider.Open returns a Backend whose Identity
//      matches the assignment lineage the master authored.
//   3. AFTER A PRODUCT-ROUTE IntentRefreshEndpoint AUTHORITY
//      MOVE (primary restarted with different data/ctrl
//      addresses → master mints a new EndpointVersion), the
//      old backend's per-op fence returns ErrStalePrimary,
//      and a fresh Open captures the new lineage.
//
// What this L2 DOES NOT PROVE (scope caveat for closure):
//
//	Cross-replica primary change at L2 (e.g. r1 → r2 via
//	TopologyController reassign). That path requires a
//	TopologyController-driven Reassign emission and is not
//	producible from the StaticDirective/P14-only route T0
//	ships. Cross-replica fail-closed is proved at the UNIT
//	layer (core/host/volume/projection_bridge_test.go +
//	supersede_integration_test.go) via Host.recordOtherLine →
//	Host.IsSuperseded → AdapterProjectionView. Promotion of
//	that claim to L2 Integration is deferred to T6 (G8
//	Failover Data Continuity).
//
// This is the T1 product-process closure loop (PM verdict
// 2026-04-21): each Tn must land at least one real-process
// closure. Architect follow-ups 2026-04-21 sharpened this L2
// to (a) exercise the stale-after-move arm and (b) call out
// what facet remains unit/component-only.
package frontend_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/memback"
)

// reserveLoopbackPort binds 127.0.0.1:0, captures the OS-assigned
// port, closes the listener, and returns the address as
// "127.0.0.1:PORT". Replaces the previously-hardcoded 7165 so
// concurrent test runs / CI jobs don't collide. There's a small
// race window between Close and the subprocess re-binding, but
// in practice on loopback after graceful close the window is
// negligible. If it matters later, we can upgrade the executor
// to pass the raw file descriptor.
func reserveLoopbackPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}
	return addr
}


// httpProjectionView implements frontend.ProjectionView by
// calling a volume host's /status HTTP endpoint. Each call is
// one HTTP GET — the contract requires every Read/Write to
// re-validate lineage, so caching would subvert the fence.
type httpProjectionView struct {
	statusURL string
	volumeID  string
	client    *http.Client
}

func newHTTPProjectionView(statusAddr, volumeID string) *httpProjectionView {
	return &httpProjectionView{
		statusURL: "http://" + statusAddr + "/status?volume=" + volumeID,
		volumeID:  volumeID,
		client:    &http.Client{Timeout: 2 * time.Second},
	}
}

func (v *httpProjectionView) Projection() frontend.Projection {
	resp, err := v.client.Get(v.statusURL)
	if err != nil {
		return frontend.Projection{VolumeID: v.volumeID, Healthy: false}
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return frontend.Projection{VolumeID: v.volumeID, Healthy: false}
	}
	var p frontend.Projection
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return frontend.Projection{VolumeID: v.volumeID, Healthy: false}
	}
	return p
}

func TestT1Process_FrontendProviderOverProductRoute(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; skipped under -short")
	}
	// Reserve an OS-assigned loopback port for r1's status
	// endpoint. The harness uses this same addr on both the
	// initial start and the restart so the view URL stays
	// valid. No hardcoded port → no CI/dev-host collision.
	r1StatusAddr := reserveLoopbackPort(t)

	h := startT1Harness(t, r1StatusAddr)
	defer h.Close()

	h.waitStatusBound(t, "r1", r1StatusAddr, 15*time.Second)
	view := newHTTPProjectionView(r1StatusAddr, "v1")

	// PHASE 1: wait for Healthy on real engine projection.
	projCtx, projCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer projCancel()
	initial := waitForHealthy(t, projCtx, view)
	if initial.ReplicaID != "r1" || initial.Epoch == 0 {
		t.Fatalf("initial projection: %+v", initial)
	}

	provider := memback.NewProvider(view)

	opCtx, opCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer opCancel()

	// PHASE 2: open backend on current lineage; write + read.
	oldBackend, err := provider.Open(opCtx, "v1")
	if err != nil {
		t.Fatalf("Open over product route: %v", err)
	}
	defer oldBackend.Close()
	oldID := oldBackend.Identity()
	if oldID.Epoch == 0 {
		t.Fatalf("old backend Identity: %+v", oldID)
	}
	if _, err := oldBackend.Write(opCtx, 0, []byte("l2-real-route-pre")); err != nil {
		t.Fatalf("Write pre-move: %v", err)
	}
	got := make([]byte, len("l2-real-route-pre"))
	if _, err := oldBackend.Read(opCtx, 0, got); err != nil {
		t.Fatalf("Read pre-move: %v", err)
	}
	if !bytes.Equal(got, []byte("l2-real-route-pre")) {
		t.Fatalf("Read pre-move mismatch: %q", got)
	}

	// PHASE 3: trigger a PRODUCT-ROUTE authority move.
	// Restart r1 with new DataAddr/CtrlAddr (same status-addr
	// so the view URL stays valid). Master's topology
	// controller observes the slot's endpoints drifted from
	// the published line → emits IntentRefreshEndpoint →
	// publisher mints (Epoch kept, EndpointVersion+=1). This
	// is a real P14 authority move, not a test-local
	// mutation.
	h.restartR1WithNewAddrs(t, r1StatusAddr,
		"127.0.0.1:7071", "127.0.0.1:7171")

	// Wait for the view to report the new lineage (EV drift).
	driftCtx, driftCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer driftCancel()
	newProj := waitForLineageAdvance(t, driftCtx, view, oldID)
	if newProj.Epoch < oldID.Epoch {
		t.Fatalf("post-move epoch regressed: old=%d new=%d", oldID.Epoch, newProj.Epoch)
	}
	if newProj.Epoch == oldID.Epoch && newProj.EndpointVersion <= oldID.EndpointVersion {
		t.Fatalf("post-move EV did not advance: old=(E=%d EV=%d) new=(E=%d EV=%d)",
			oldID.Epoch, oldID.EndpointVersion, newProj.Epoch, newProj.EndpointVersion)
	}

	// PHASE 4: old backend's per-op fence must reject on the
	// new lineage. This is the invariant architect-review
	// demanded L2 prove.
	buf := make([]byte, len("l2-real-route-pre"))
	if _, err := oldBackend.Read(opCtx, 0, buf); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("old Read after authority move: expected ErrStalePrimary, got %v", err)
	}
	if _, err := oldBackend.Write(opCtx, 8, []byte("x")); !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("old Write after authority move: expected ErrStalePrimary, got %v", err)
	}

	// PHASE 5: fresh Open captures new lineage; round-trip
	// under new Identity succeeds.
	newBackend, err := provider.Open(opCtx, "v1")
	if err != nil {
		t.Fatalf("Open post-move: %v", err)
	}
	defer newBackend.Close()
	newID := newBackend.Identity()
	if newID.Epoch != newProj.Epoch || newID.EndpointVersion != newProj.EndpointVersion {
		t.Fatalf("new backend Identity: %+v want E=%d EV=%d", newID, newProj.Epoch, newProj.EndpointVersion)
	}
	postPayload := []byte("l2-real-route-post")
	if _, err := newBackend.Write(opCtx, 0, postPayload); err != nil {
		t.Fatalf("Write post-move: %v", err)
	}
	postGot := make([]byte, len(postPayload))
	if _, err := newBackend.Read(opCtx, 0, postGot); err != nil {
		t.Fatalf("Read post-move: %v", err)
	}
	if !bytes.Equal(postGot, postPayload) {
		t.Fatalf("Read post-move mismatch: %q", postGot)
	}

	// EXPLICIT NON-CLAIM: not asserting pre-move byte survival,
	// no replicated-data-continuity claim (that's G8).
}

// waitForHealthy polls view until Healthy=true or ctx expires.
func waitForHealthy(t *testing.T, ctx context.Context, view frontend.ProjectionView) frontend.Projection {
	t.Helper()
	for {
		p := view.Projection()
		if p.Healthy {
			return p
		}
		select {
		case <-ctx.Done():
			t.Fatalf("projection never became Healthy over product route (last=%+v)", p)
			return frontend.Projection{}
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// waitForLineageAdvance polls view until a Healthy projection
// with a lineage strictly newer than `old` is observed.
// Intermediate not-Healthy snapshots are ignored — they just
// indicate the transient restart window.
func waitForLineageAdvance(t *testing.T, ctx context.Context, view frontend.ProjectionView, old frontend.Identity) frontend.Projection {
	t.Helper()
	var last frontend.Projection
	for {
		p := view.Projection()
		last = p
		if p.Healthy {
			if p.Epoch > old.Epoch {
				return p
			}
			if p.Epoch == old.Epoch && p.EndpointVersion > old.EndpointVersion {
				return p
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("lineage did not advance past old=(E=%d EV=%d) (last=%+v)",
				old.Epoch, old.EndpointVersion, last)
			return frontend.Projection{}
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ---------- minimal L2 harness (self-contained) ----------

type t1Harness struct {
	bins     procBins
	master   *testProc
	volR1    *testProc
	others   []*testProc
	cfg      t1HarnessConfig
}

type t1HarnessConfig struct {
	masterAddr string
	art        string
}

func (h *t1Harness) Close() {
	for _, p := range append([]*testProc{h.master, h.volR1}, h.others...) {
		if p != nil {
			p.stop()
		}
	}
}

// waitStatusBound polls the status endpoint until it returns
// a 200 or 4xx response (anything that proves the HTTP server
// is live), or deadline.
func (h *t1Harness) waitStatusBound(t *testing.T, replicaID, statusAddr string, deadline time.Duration) {
	t.Helper()
	url := "http://" + statusAddr + "/status?volume=v1"
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("status endpoint for %s not reachable at %s within %v", replicaID, statusAddr, deadline)
}

// restartR1WithNewAddrs stops the existing r1 volume process
// and starts a replacement with different DataAddr/CtrlAddr
// but the SAME loopback status-addr so the view URL stays
// valid. Master observes the slot's endpoints changed →
// IntentRefreshEndpoint fires → EndpointVersion advances.
func (h *t1Harness) restartR1WithNewAddrs(t *testing.T, statusAddr, newDataAddr, newCtrlAddr string) {
	t.Helper()
	h.volR1.stop()
	// Brief pause so the OS fully releases the status-addr
	// before the replacement binds. On Windows TIME_WAIT is
	// short for local 127.0.0.1 + graceful close.
	time.Sleep(500 * time.Millisecond)
	h.volR1 = startVolumeProcFixed(t, h.bins.volume, h.cfg.masterAddr, "volume-r1",
		"s1", "v1", "r1", newDataAddr, newCtrlAddr, statusAddr)
}

func startT1Harness(t *testing.T, r1StatusAddr string) *t1Harness {
	t.Helper()
	art := t.TempDir()
	bins := buildL2Binaries(t, art)
	topoPath := filepath.Join(art, "topology.yaml")
	writeTopo(t, topoPath)
	storeDir := filepath.Join(art, "store")

	master := startMasterProc(t, bins.master, storeDir, topoPath)
	// Data/ctrl addrs are harness-owned. If a future test needs
	// them dynamic too, reserve more ports with reserveLoopbackPort.
	volR1 := startVolumeProcFixed(t, bins.volume, master.addr, "volume-r1",
		"s1", "v1", "r1", "127.0.0.1:7061", "127.0.0.1:7161", r1StatusAddr)
	volR2 := startVolumeProc(t, bins.volume, master.addr, "volume-r2",
		"s2", "v1", "r2", "127.0.0.1:7062", "127.0.0.1:7162", false)
	volR3 := startVolumeProc(t, bins.volume, master.addr, "volume-r3",
		"s3", "v1", "r3", "127.0.0.1:7063", "127.0.0.1:7163", false)
	return &t1Harness{
		bins:   bins,
		master: master,
		volR1:  volR1,
		others: []*testProc{volR2, volR3},
		cfg:    t1HarnessConfig{masterAddr: master.addr, art: art},
	}
}

func writeTopo(t *testing.T, path string) {
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

type procBins struct {
	master string
	volume string
}

func buildL2Binaries(t *testing.T, outDir string) procBins {
	t.Helper()
	ext := ""
	if goOSIsWindows() {
		ext = ".exe"
	}
	bins := procBins{
		master: filepath.Join(outDir, "blockmaster"+ext),
		volume: filepath.Join(outDir, "blockvolume"+ext),
	}
	for pkg, out := range map[string]string{
		"github.com/seaweedfs/seaweed-block/cmd/blockmaster": bins.master,
		"github.com/seaweedfs/seaweed-block/cmd/blockvolume": bins.volume,
	} {
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("build %s: %v", pkg, err)
		}
	}
	return bins
}

func startMasterProc(t *testing.T, bin, storeDir, topoPath string) *testProc {
	t.Helper()
	p := newTestProc(t, "master", bin,
		"--authority-store", storeDir,
		"--listen", "127.0.0.1:0",
		"--topology", topoPath,
		"--t0-print-ready",
	)
	p.start(t)
	p.addr = p.waitAddr(t, "listening", 15*time.Second)
	return p
}

// startVolumeProc starts a replica with no status server.
func startVolumeProc(t *testing.T, bin, masterAddr, label, serverID, volumeID, replicaID, dataAddr, ctrlAddr string, captureStatus bool) *testProc {
	t.Helper()
	args := []string{
		"--master", masterAddr,
		"--server-id", serverID,
		"--volume-id", volumeID,
		"--replica-id", replicaID,
		"--data-addr", dataAddr,
		"--ctrl-addr", ctrlAddr,
		"--heartbeat-interval", "200ms",
		"--t1-readiness",
	}
	if captureStatus {
		args = append(args, "--status-addr", "127.0.0.1:0")
	}
	p := newTestProc(t, label, bin, args...)
	p.start(t)
	return p
}

// startVolumeProcFixed starts a replica with an explicit fixed
// status-addr (caller pins the loopback port). Used for the
// primary replica so the test view URL stays stable across the
// RefreshEndpoint-triggering restart.
func startVolumeProcFixed(t *testing.T, bin, masterAddr, label, serverID, volumeID, replicaID, dataAddr, ctrlAddr, statusAddr string) *testProc {
	t.Helper()
	args := []string{
		"--master", masterAddr,
		"--server-id", serverID,
		"--volume-id", volumeID,
		"--replica-id", replicaID,
		"--data-addr", dataAddr,
		"--ctrl-addr", ctrlAddr,
		"--heartbeat-interval", "200ms",
		"--t1-readiness",
		"--status-addr", statusAddr,
	}
	p := newTestProc(t, label, bin, args...)
	p.start(t)
	return p
}

func goOSIsWindows() bool {
	return runtimeGOOS() == "windows"
}
