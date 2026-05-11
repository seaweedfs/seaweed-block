//go:build subprocess

package main_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
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
	waitProductIscsiReady(t, iscsiAddr, 5*time.Second)

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
	waitProductIscsiReady(t, iscsiAddr, 5*time.Second)

	c2 := dialG8Iscsi(t, iscsiAddr, iqn)
	got := c2.read10(t, 11, 1, durableBlockSize)
	c2.close(t)
	if !bytes.Equal(got, payload) {
		t.Fatalf("read after blockvolume durable restart mismatch: got prefix=%x want prefix=%x", bytePrefix(got, 32), bytePrefix(payload, 32))
	}
}

func TestISCSI_L2DurableRestartReconnect_RepeatedCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess durable restart cycle test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()

	_, masterAddr := startSingleSlotMaster(t, bins, art)

	iqn := "iqn.2026-05.example.restart-cycles:v1"
	store := filepath.Join(art, "r1-store")
	dataAddr, ctrlAddr := pickAddr(t), pickAddr(t)
	statusAddr, iscsiAddr := pickAddr(t), pickAddr(t)

	var r1 *proc
	start := func(tag string) {
		t.Helper()
		r1 = startG54Volume(t, bins, art, volOpts{
			masterAddr:  masterAddr,
			serverID:    "s1",
			replicaID:   "r1",
			dataAddr:    dataAddr,
			ctrlAddr:    ctrlAddr,
			statusAddr:  statusAddr,
			iscsiAddr:   iscsiAddr,
			iscsiIQN:    iqn,
			durableRoot: store,
			logTag:      tag,
		})
		waitHealthyReplica(t, statusAddr, "r1", 10*time.Second)
		waitProductIscsiReady(t, iscsiAddr, 5*time.Second)
	}
	stop := func() {
		t.Helper()
		r1.stop(t)
		time.Sleep(500 * time.Millisecond)
	}

	start("iscsi-restart-cycle-0")
	const durableBlockSize = 4096
	writes := make(map[uint32][]byte)
	for cycle := 0; cycle < 3; cycle++ {
		lba := uint32(20 + cycle)
		payload := bytes.Repeat([]byte{byte(0x70 + cycle)}, durableBlockSize)
		copy(payload, []byte("iscsi-durable-restart-repeated-cycle"))
		payload[len(payload)-1] = byte(cycle)

		cli := dialG8Iscsi(t, iscsiAddr, iqn)
		cli.write10(t, lba, payload)
		for priorLBA, want := range writes {
			got := cli.read10(t, priorLBA, 1, durableBlockSize)
			if !bytes.Equal(got, want) {
				cli.close(t)
				t.Fatalf("cycle %d prior LBA %d mismatch: got prefix=%x want prefix=%x", cycle, priorLBA, bytePrefix(got, 32), bytePrefix(want, 32))
			}
		}
		cli.close(t)

		writes[lba] = payload
		stop()
		start(fmt.Sprintf("iscsi-restart-cycle-%d", cycle+1))
	}

	cli := dialG8Iscsi(t, iscsiAddr, iqn)
	defer cli.close(t)
	for lba, want := range writes {
		got := cli.read10(t, lba, 1, durableBlockSize)
		if !bytes.Equal(got, want) {
			t.Fatalf("final LBA %d mismatch: got prefix=%x want prefix=%x", lba, bytePrefix(got, 32), bytePrefix(want, 32))
		}
	}
}

func TestISCSI_L2DurableSyncCacheRestart_AcceptsSyncAndPreservesWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess durable sync-cache restart test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()

	_, masterAddr := startSingleSlotMaster(t, bins, art)

	iqn := "iqn.2026-05.example.sync-restart:v1"
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
		logTag:      "iscsi-sync-restart-r1-initial",
	})
	waitHealthyReplica(t, statusAddr, "r1", 10*time.Second)
	waitProductIscsiReady(t, iscsiAddr, 5*time.Second)

	const durableBlockSize = 4096
	expected := make(map[uint32][]byte)
	cli := dialG8Iscsi(t, iscsiAddr, iqn)
	for i := 0; i < 12; i++ {
		lba := uint32(40 + i)
		payload := bytes.Repeat([]byte{byte(0x30 + i)}, durableBlockSize)
		copy(payload, []byte("iscsi-durable-sync-cache-restart-payload"))
		payload[len(payload)-1] = byte(i)
		cli.write10(t, lba, payload)
		expected[lba] = payload
		if (i+1)%4 == 0 {
			cli.syncCache10(t)
		}
	}
	cli.syncCache10(t)
	cli.close(t)

	r1.stop(t)
	time.Sleep(500 * time.Millisecond)

	_ = startG54Volume(t, bins, art, volOpts{
		masterAddr:  masterAddr,
		serverID:    "s1",
		replicaID:   "r1",
		dataAddr:    dataAddr,
		ctrlAddr:    ctrlAddr,
		statusAddr:  statusAddr,
		iscsiAddr:   iscsiAddr,
		iscsiIQN:    iqn,
		durableRoot: store,
		logTag:      "iscsi-sync-restart-r1-restarted",
	})
	waitHealthyReplica(t, statusAddr, "r1", 10*time.Second)
	waitProductIscsiReady(t, iscsiAddr, 5*time.Second)

	cli2 := dialG8Iscsi(t, iscsiAddr, iqn)
	defer cli2.close(t)
	for lba, want := range expected {
		got := cli2.read10(t, lba, 1, durableBlockSize)
		if !bytes.Equal(got, want) {
			t.Fatalf("synced LBA %d mismatch after restart: got prefix=%x want prefix=%x", lba, bytePrefix(got, 32), bytePrefix(want, 32))
		}
	}
}

func startSingleSlotMaster(t *testing.T, bins l2bins, art string) (*proc, string) {
	t.Helper()
	storeDir := filepath.Join(art, "master-store")
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		t.Fatalf("master store dir %s: %v", storeDir, err)
	}
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

	br := bufio.NewReader(stdout)
	type readyResult struct {
		line []byte
		err  error
	}
	readyCh := make(chan readyResult, 1)
	go func() {
		line, err := br.ReadBytes('\n')
		readyCh <- readyResult{line: line, err: err}
	}()
	var ready readyResult
	select {
	case ready = <-readyCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("master ready line timeout")
	}
	if ready.err != nil {
		t.Fatalf("master ready line: %v", ready.err)
	}
	go func() { _, _ = io.Copy(io.Discard, br) }()
	var rl struct {
		Component, Phase, Addr string
	}
	if err := json.Unmarshal(ready.line, &rl); err != nil {
		t.Fatalf("parse master ready %q: %v", ready.line, err)
	}
	if rl.Addr == "" {
		t.Fatalf("master ready: empty addr (line=%q)", ready.line)
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

func waitProductIscsiReady(t *testing.T, iscsiAddr string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		conn, err := net.DialTimeout("tcp", iscsiAddr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("iSCSI listener %s not reachable within %s: last error: %v", iscsiAddr, deadline, lastErr)
}

func bytePrefix(b []byte, n int) []byte {
	if len(b) < n {
		return b
	}
	return b[:n]
}
