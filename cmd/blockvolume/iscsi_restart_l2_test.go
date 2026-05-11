//go:build subprocess

package main_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestISCSI_L2DurableRestartReconnect_PreservesData(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess durable restart test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()

	_, masterAddr := startSingleSlotMaster(t, bins, art)

	iqn := "iqn.2026-05.example.restart:v1"
	store := filepath.Join(art, "r1-store")
	dataAddr, ctrlAddr := pickAddr(t), pickAddr(t)
	statusAddr, iscsiAddr := pickAddr(t), pickAddr(t)

	r1 := startG54Volume(t, bins, art, volOpts{
		masterAddr:  masterAddr,
		serverID:    "s1",
		replicaID:   "r1",
		dataAddr:    dataAddr,
		ctrlAddr:    ctrlAddr,
		statusAddr:  statusAddr,
		iscsiAddr:   iscsiAddr,
		iscsiIQN:    iqn,
		durableRoot: store,
		logTag:      "iscsi-restart-r1-initial",
	})
	waitHealthyReplica(t, statusAddr, "r1", 10*time.Second)

	const durableBlockSize = 4096
	payload := bytes.Repeat([]byte{0x6d}, durableBlockSize)
	copy(payload, []byte("iscsi-durable-restart-reconnect-payload"))

	c1 := dialG8Iscsi(t, iscsiAddr, iqn)
	c1.write10(t, 11, payload)
	c1.close(t)

	r1.stop(t)
	time.Sleep(500 * time.Millisecond)

	r1Restart := startG54Volume(t, bins, art, volOpts{
		masterAddr:  masterAddr,
		serverID:    "s1",
		replicaID:   "r1",
		dataAddr:    dataAddr,
		ctrlAddr:    ctrlAddr,
		statusAddr:  statusAddr,
		iscsiAddr:   iscsiAddr,
		iscsiIQN:    iqn,
		durableRoot: store,
		logTag:      "iscsi-restart-r1-restarted",
	})
	_ = r1Restart
	waitHealthyReplica(t, statusAddr, "r1", 10*time.Second)

	c2 := dialG8Iscsi(t, iscsiAddr, iqn)
	got := c2.read10(t, 11, 1, durableBlockSize)
	c2.close(t)
	if !bytes.Equal(got, payload) {
		t.Fatalf("read after blockvolume durable restart mismatch: got prefix=%x want prefix=%x", got[:32], payload[:32])
	}
}

func startSingleSlotMaster(t *testing.T, bins l2bins, art string) (*proc, string) {
	t.Helper()
	storeDir := filepath.Join(art, "master-store")
	_ = os.MkdirAll(storeDir, 0o755)
	topo := writeSingleSlotTopology(t, art)
	logPath := filepath.Join(art, "master-single-slot.log")
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("master log: %v", err)
	}
	cmd := exec.Command(bins.master,
		"--authority-store", storeDir,
		"--listen", "127.0.0.1:0",
		"--topology", topo,
		"--expected-slots-per-volume", "1",
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

func writeSingleSlotTopology(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "topology-single-slot.yaml")
	body := `volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("single-slot topology: %v", err)
	}
	return path
}

func waitHealthyReplica(t *testing.T, statusAddr, replicaID string, deadline time.Duration) map[string]any {
	t.Helper()
	got := pollStatus(t, statusAddr, deadline, func(b map[string]any) bool {
		h, _ := b["Healthy"].(bool)
		rid, _ := b["ReplicaID"].(string)
		return h && rid == replicaID
	})
	if got == nil {
		t.Fatalf("%s: no Healthy status within %s", replicaID, deadline)
	}
	if h, _ := got["Healthy"].(bool); !h {
		t.Fatalf("%s: Healthy=true expected; got status=%v", replicaID, got)
	}
	return got
}
