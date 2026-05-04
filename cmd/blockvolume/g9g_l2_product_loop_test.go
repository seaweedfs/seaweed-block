package main_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestG9G_L2ProductLoopPublishesAssignmentToBlockvolume(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess product-loop test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()
	lifecycleDir := filepath.Join(art, "lifecycle")
	specPath := writeG9GClusterSpec(t, art)

	_, masterAddr := startG9GMaster(t, bins, art, lifecycleDir, specPath)

	r2Data, r2Status := pickAddr(t), pickAddr(t)
	r2Ctrl := pickAddr(t)
	_ = startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s2", replicaID: "r2",
		dataAddr: r2Data, ctrlAddr: r2Ctrl,
		statusAddr:  r2Status,
		durableRoot: filepath.Join(art, "g9g-r2-store"),
		logTag:      "g9g-r2",
	})

	r2Ready := pollStatus(t, r2Status, 10*time.Second, func(b map[string]any) bool {
		h, _ := b["Healthy"].(bool)
		rid, _ := b["ReplicaID"].(string)
		epoch, _ := b["Epoch"].(float64)
		return h && rid == "r2" && epoch >= 1
	})
	if r2Ready == nil {
		t.Fatal("r2: no status response after product-loop assignment")
	}
	if h, _ := r2Ready["Healthy"].(bool); !h {
		t.Fatalf("r2 did not become Healthy from product-loop assignment; status=%v", r2Ready)
	}
}

func writeG9GClusterSpec(t *testing.T, art string) string {
	t.Helper()
	specPath := filepath.Join(art, "m01.yaml")
	raw := []byte(`
volumes:
  - id: v1
    size_bytes: 1048576
    replication_factor: 1
    placements:
      - server_id: s2
        replica_id: r2
        source: existing_replica
`)
	if err := os.WriteFile(specPath, raw, 0o644); err != nil {
		t.Fatalf("write cluster spec: %v", err)
	}
	return specPath
}

func startG9GMaster(t *testing.T, bins l2bins, art, lifecycleDir, specPath string) (*proc, string) {
	t.Helper()
	storeDir := filepath.Join(art, "master-store")
	_ = os.MkdirAll(storeDir, 0o755)
	logPath := filepath.Join(art, "master-g9g.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("master log: %v", err)
	}
	cmd := exec.Command(bins.master,
		"--authority-store", storeDir,
		"--lifecycle-store", lifecycleDir,
		"--cluster-spec", specPath,
		"--lifecycle-product-loop-interval", "100ms",
		"--listen", "127.0.0.1:0",
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
	return p, rl.Addr
}
