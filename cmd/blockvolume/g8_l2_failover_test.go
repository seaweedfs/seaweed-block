package main_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func startG8Master(t *testing.T, bins l2bins, art string) (*proc, string) {
	t.Helper()
	storeDir := filepath.Join(art, "master-store")
	_ = os.MkdirAll(storeDir, 0o755)
	topo := write2SlotTopology(t, art)
	logPath := filepath.Join(art, "master-g8.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("master log: %v", err)
	}
	cmd := exec.Command(bins.master,
		"--authority-store", storeDir,
		"--listen", "127.0.0.1:0",
		"--topology", topo,
		"--expected-slots-per-volume", "2",
		"--freshness-window", "800ms",
		"--pending-grace", "100ms",
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

	ready := readMasterReadyLine(t, stdout)
	return p, ready
}

func readMasterReadyLine(t *testing.T, stdout interface{ Read([]byte) (int, error) }) string {
	t.Helper()
	buf := make([]byte, 512)
	n, _ := stdout.Read(buf)
	go func() {
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
	return rl.Addr
}

func TestG8B_L2PrimaryKill_ReassignsAuthorityToReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess failover test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()

	_, masterAddr := startG8Master(t, bins, art)

	r1Data, r1Status := pickAddr(t), pickAddr(t)
	r2Data, r2Status := pickAddr(t), pickAddr(t)
	r1Ctrl, r2Ctrl := pickAddr(t), pickAddr(t)

	r1 := startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s1", replicaID: "r1",
		dataAddr: r1Data, ctrlAddr: r1Ctrl,
		statusAddr:  r1Status,
		durableRoot: filepath.Join(art, "r1-store"),
		logTag:      "g8-r1",
	})
	_ = startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s2", replicaID: "r2",
		dataAddr: r2Data, ctrlAddr: r2Ctrl,
		statusAddr:  r2Status,
		durableRoot: filepath.Join(art, "r2-store"),
		logTag:      "g8-r2",
	})

	pollStatus(t, r1Status, 10*time.Second, func(b map[string]any) bool {
		h, _ := b["Healthy"].(bool)
		rid, _ := b["ReplicaID"].(string)
		return h && rid == "r1"
	})
	r2Initial := pollStatus(t, r2Status, 10*time.Second, func(b map[string]any) bool {
		_, ok := b["Healthy"]
		return ok
	})
	if r2Initial == nil {
		t.Fatal("r2: no initial status response")
	}
	if h, _ := r2Initial["Healthy"].(bool); h {
		t.Fatalf("r2 must not be primary before r1 kill; status=%v", r2Initial)
	}

	r1.stop(t)

	r2Failover := pollStatus(t, r2Status, 8*time.Second, func(b map[string]any) bool {
		h, _ := b["Healthy"].(bool)
		rid, _ := b["ReplicaID"].(string)
		epoch, _ := b["Epoch"].(float64)
		return h && rid == "r2" && epoch >= 2
	})
	if r2Failover == nil {
		t.Fatal("r2: no status response after r1 kill")
	}
	if h, _ := r2Failover["Healthy"].(bool); !h {
		t.Fatalf("r2 did not become Healthy after r1 kill; status=%v", r2Failover)
	}
}
