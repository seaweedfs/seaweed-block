package main_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestG9G_L2ProductLoopPublishesAssignmentToBlockvolume(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess product-loop test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()
	lifecycleDir := filepath.Join(art, "lifecycle")
	seedPath := writeG9GPlacementSeed(t, art)

	_, masterAddr := startG9GMaster(t, bins, art, lifecycleDir, seedPath)

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

func writeG9GPlacementSeed(t *testing.T, art string) string {
	t.Helper()
	seedPath := filepath.Join(art, "g9g-placement-seed.json")
	raw, err := json.MarshalIndent([]lifecycle.PlacementIntent{{
		VolumeID:  "v1",
		DesiredRF: 1,
		Slots: []lifecycle.PlacementSlotIntent{{
			ServerID:  "s2",
			ReplicaID: "r2",
			Source:    lifecycle.PlacementSourceExistingReplica,
		}},
	}}, "", "  ")
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	if err := os.WriteFile(seedPath, raw, 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	return seedPath
}

func startG9GMaster(t *testing.T, bins l2bins, art, lifecycleDir, seedPath string) (*proc, string) {
	t.Helper()
	storeDir := filepath.Join(art, "master-store")
	_ = os.MkdirAll(storeDir, 0o755)
	topo := write2SlotTopology(t, art)
	logPath := filepath.Join(art, "master-g9g.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("master log: %v", err)
	}
	cmd := exec.Command(bins.master,
		"--authority-store", storeDir,
		"--lifecycle-store", lifecycleDir,
		"--lifecycle-placement-seed", seedPath,
		"--lifecycle-product-loop-interval", "100ms",
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
