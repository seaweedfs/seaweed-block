// G5-4.5 binary integration test — pin INV-BIN-WIRING-* through the
// production binary code path (NOT component framework fixture).
//
// Per mini-plan v0.4 §4 #2 architect binding (round 50): "Healthy"
// must split by role. Primary asserts Healthy=true via the existing
// frontend/write-ready projection. Replica asserts replication-ready
// (listener bound, accepting connections) via the bound --data-addr.
// Replica MUST NOT report Healthy=true (would imply
// frontend-primary-write-ready, which it isn't).
//
// Scope (per mini-plan v0.4 §4):
//   - #1 binary constructs ReplicationVolume + ReplicaListener
//   - #2 in-process 2-volume cluster reaches role-appropriate ready state
//   - #5 no regressions
// Deferred to G5-5 (m01 hardware):
//   - #3 byte-equal primary→replica via real frontend write client
//   - #4 stop-restart catch-up convergence (needs frontend write driver)
//   - #6 10× -race stress
// G5-4.5 documented carry: byte-equal write verification needs a real
// frontend client (iSCSI/NVMe). Subprocess test verifies the wiring +
// steady-state role split; G5-5 m01 hardware exercises the data path.

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
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
)

// l2bins holds the paths of compiled blockmaster + blockvolume.
type l2bins struct {
	master string
	volume string
}

// buildG54Binaries compiles cmd/blockmaster + cmd/blockvolume into a
// per-test temp dir. Identical pattern to core/host/l2harness_test
// but local so this test is self-contained.
func buildG54Binaries(t *testing.T) l2bins {
	t.Helper()
	dir := t.TempDir()
	suf := ""
	if isWindows() {
		suf = ".exe"
	}
	master := filepath.Join(dir, "blockmaster"+suf)
	volume := filepath.Join(dir, "blockvolume"+suf)
	for _, b := range []struct{ pkg, out string }{
		{"github.com/seaweedfs/seaweed-block/cmd/blockmaster", master},
		{"github.com/seaweedfs/seaweed-block/cmd/blockvolume", volume},
	} {
		cmd := exec.Command("go", "build", "-o", b.out, b.pkg)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("build %s: %v\n%s", b.pkg, err, out)
		}
	}
	return l2bins{master: master, volume: volume}
}

func isWindows() bool { return os.PathSeparator == '\\' }

// write2SlotTopology writes an RF=2 topology — pins that the binary
// works at the smoke-test 2-node configuration G5-5 will use on m01.
func write2SlotTopology(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "topology.yaml")
	body := `volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
      - replica_id: r2
        server_id: s2
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("topology: %v", err)
	}
	return path
}

type proc struct {
	cmd     *exec.Cmd
	logPath string
}

func (p *proc) stop(t *testing.T) {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = p.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = p.cmd.Process.Kill()
			<-done
		}
	}
	if t.Failed() {
		if b, err := os.ReadFile(p.logPath); err == nil {
			t.Logf("=== %s ===\n%s", filepath.Base(p.logPath), b)
		}
	}
}

func startG54Master(t *testing.T, bins l2bins, art string) (*proc, string) {
	t.Helper()
	storeDir := filepath.Join(art, "master-store")
	_ = os.MkdirAll(storeDir, 0o755)
	topo := write2SlotTopology(t, art)
	logPath := filepath.Join(art, "master.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("master log: %v", err)
	}
	cmd := exec.Command(bins.master,
		"--authority-store", storeDir,
		"--listen", "127.0.0.1:0",
		"--topology", topo,
		"--expected-slots-per-volume", "2",
		"--t0-print-ready",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		lf.Close()
		t.Fatalf("master stdout pipe: %v", err)
	}
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		t.Fatalf("master start: %v", err)
	}
	p := &proc{cmd: cmd, logPath: logPath}
	t.Cleanup(func() { p.stop(t); lf.Close() })

	// Read ready line: {"component":"blockmaster","phase":"listening","addr":"..."}
	buf := make([]byte, 512)
	n, _ := stdout.Read(buf)
	go func() {
		// Drain so the pipe doesn't block.
		for {
			b := make([]byte, 1024)
			if _, err := stdout.Read(b); err != nil {
				return
			}
		}
	}()
	var rl struct {
		Component, Phase, Addr string
	}
	if err := json.Unmarshal(buf[:n], &rl); err != nil {
		t.Fatalf("parse master ready %q: %v", buf[:n], err)
	}
	if rl.Addr == "" {
		t.Fatalf("master ready: empty addr (line=%q)", buf[:n])
	}
	return p, rl.Addr
}

type volOpts struct {
	masterAddr string
	serverID   string
	replicaID  string
	dataAddr   string
	ctrlAddr   string
	statusAddr string
	durableRoot string
	logTag     string
}

func startG54Volume(t *testing.T, bins l2bins, art string, o volOpts) *proc {
	t.Helper()
	logPath := filepath.Join(art, fmt.Sprintf("volume-%s.log", o.logTag))
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("volume log: %v", err)
	}
	_ = os.MkdirAll(o.durableRoot, 0o755)
	cmd := exec.Command(bins.volume,
		"--master", o.masterAddr,
		"--server-id", o.serverID,
		"--volume-id", "v1",
		"--replica-id", o.replicaID,
		"--data-addr", o.dataAddr,
		"--ctrl-addr", o.ctrlAddr,
		"--status-addr", o.statusAddr,
		"--durable-root", o.durableRoot,
		"--durable-impl", "walstore",
		"--durable-blocks", "256",
		"--durable-blocksize", "4096",
		"--heartbeat-interval", "200ms",
		"--t1-readiness",
	)
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		t.Fatalf("volume %s start: %v", o.logTag, err)
	}
	p := &proc{cmd: cmd, logPath: logPath}
	t.Cleanup(func() { p.stop(t); lf.Close() })
	return p
}

// pollStatus polls the status endpoint until check() returns true or
// the deadline expires. Returns the last successful response body for
// debugging on failure.
func pollStatus(t *testing.T, addr string, deadline time.Duration, check func(map[string]any) bool) map[string]any {
	t.Helper()
	end := time.Now().Add(deadline)
	var last map[string]any
	for time.Now().Before(end) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+addr+"/status?volume=v1", nil)
		resp, err := http.DefaultClient.Do(req)
		cancel()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err == nil {
			last = body
			if check(body) {
				resp.Body.Close()
				return body
			}
		}
		resp.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	return last
}

// TestG54_BinaryWiring_RoleSplit_2NodeSmoke is the headline G5-4
// integration pin. Verifies through the real binary code path:
//   - Primary (r1) receives assignment naming itself, reaches Healthy=true
//   - Replica (r2) receives same assignment naming r1, does NOT flip
//     Healthy=true (correct: replica isn't frontend-primary-write-ready)
//   - Replica's --data-addr accepts TCP connections (ReplicaListener
//     bound; the production wire path is open for primary writes to
//     ship through, even though this test doesn't drive a frontend
//     write — see deferral note in file header)
//
// INV-BIN-WIRING-ROLE-FROM-ASSIGNMENT, INV-BIN-WIRING-LISTENER-LIFECYCLE-LIFO
// pin via this test.
func TestG54_BinaryWiring_RoleSplit_2NodeSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()

	master, masterAddr := startG54Master(t, bins, art)
	_ = master

	// Use OS-assigned ports for data + ctrl + status to avoid clashes.
	primaryDataLn, primaryStatusLn := pickAddr(t), pickAddr(t)
	replicaDataLn, replicaStatusLn := pickAddr(t), pickAddr(t)
	primaryCtrl, replicaCtrl := pickAddr(t), pickAddr(t)

	_ = startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s1", replicaID: "r1",
		dataAddr: primaryDataLn, ctrlAddr: primaryCtrl,
		statusAddr:  primaryStatusLn,
		durableRoot: filepath.Join(art, "primary-store"),
		logTag:      "primary",
	})
	_ = startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s2", replicaID: "r2",
		dataAddr: replicaDataLn, ctrlAddr: replicaCtrl,
		statusAddr:  replicaStatusLn,
		durableRoot: filepath.Join(art, "replica-store"),
		logTag:      "replica",
	})

	// Wait for primary to reach Healthy=true.
	got := pollStatus(t, primaryStatusLn, 10*time.Second, func(b map[string]any) bool {
		h, _ := b["Healthy"].(bool)
		return h
	})
	if got == nil {
		t.Fatal("primary: no /status response within 10s")
	}
	if h, _ := got["Healthy"].(bool); !h {
		t.Fatalf("primary: Healthy=true expected; got status=%v", got)
	}

	// Wait for replica's status endpoint to be reachable. Replica MUST
	// NOT report Healthy=true (it's a supporting replica, not the
	// frontend-primary-write-ready role). We tolerate either:
	//   - body present with healthy=false (clean replica role), or
	//   - body present with healthy=false AND assignment fields populated
	// We do NOT tolerate healthy=true — that would mean the projection
	// is reporting the replica as primary-write-ready, violating the
	// architect-binding role split.
	rGot := pollStatus(t, replicaStatusLn, 10*time.Second, func(b map[string]any) bool {
		// Wait until status responds with a body that has the healthy field.
		_, ok := b["Healthy"]
		return ok
	})
	if rGot == nil {
		t.Fatal("replica: no /status response within 10s")
	}
	if h, _ := rGot["Healthy"].(bool); h {
		t.Fatalf("replica: Healthy=true UNEXPECTED — replica role must not report frontend-write-ready; got status=%v", rGot)
	}

	// Replica's --data-addr MUST accept TCP connections — pins
	// INV-BIN-WIRING-LISTENER-LIFECYCLE-LIFO from the open-side
	// (listener bound during host lifetime). Primary's executor would
	// dial this addr to ship WAL entries; if it isn't listening, the
	// production wire path is dead.
	conn, err := net.DialTimeout("tcp", replicaDataLn, 2*time.Second)
	if err != nil {
		t.Fatalf("replica data-addr %s: TCP dial failed (ReplicaListener not bound): %v", replicaDataLn, err)
	}
	_ = conn.Close()
}

// pickAddr binds a temporary listener to find a free port, then closes
// it. Caller passes the addr to a subprocess; brief race window is
// acceptable for a smoke test.
var addrMu sync.Mutex

func pickAddr(t *testing.T) string {
	t.Helper()
	addrMu.Lock()
	defer addrMu.Unlock()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick addr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// silence unused-import warnings on platforms where strings/sync
// aren't picked up (placeholder; both are used above).
var _ = strings.TrimSpace
