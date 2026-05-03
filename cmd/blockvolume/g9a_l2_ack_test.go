package main_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

func TestG9A_L2SyncQuorumWriteFailsWhenReplicaDown(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess strict ACK test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()

	_, masterAddr := startG8Master(t, bins, art)

	iqn := "iqn.2026-05.example.g9a:v1"
	r1Data, r1Status, r1Iscsi := pickAddr(t), pickAddr(t), pickAddr(t)
	r2Data, r2Status := pickAddr(t), pickAddr(t)
	r1Ctrl, r2Ctrl := pickAddr(t), pickAddr(t)

	_ = startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s1", replicaID: "r1",
		dataAddr: r1Data, ctrlAddr: r1Ctrl,
		statusAddr:     r1Status,
		iscsiAddr:      r1Iscsi,
		iscsiIQN:       iqn,
		durableRoot:    filepath.Join(art, "g9a-sync-r1-store"),
		logTag:         "g9a-sync-r1",
		replicationAck: "sync-quorum",
	})
	r2 := startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s2", replicaID: "r2",
		dataAddr: r2Data, ctrlAddr: r2Ctrl,
		statusAddr:  r2Status,
		durableRoot: filepath.Join(art, "g9a-sync-r2-store"),
		logTag:      "g9a-sync-r2",
	})

	pollStatus(t, r1Status, 10*time.Second, func(b map[string]any) bool {
		h, _ := b["Healthy"].(bool)
		rid, _ := b["ReplicaID"].(string)
		return h && rid == "r1"
	})
	pollStatus(t, r2Status, 10*time.Second, func(b map[string]any) bool {
		_, ok := b["Healthy"]
		return ok
	})

	const durableBlockSize = 4096
	payload := bytes.Repeat([]byte{0x9a}, durableBlockSize)
	copy(payload, []byte("g9a-sync-quorum-first-write"))
	c := dialG8Iscsi(t, r1Iscsi, iqn)
	c.write10(t, 9, payload)

	r2.stop(t)

	copy(payload, []byte("g9a-sync-quorum-write-while-r2-down"))
	status := c.write10Status(t, 10, payload)
	c.close(t)
	if status == iscsi.StatusGood {
		t.Fatalf("sync-quorum RF=2 write while secondary down returned GOOD; want foreground ACK failure")
	}
}

func TestG9A_L2BestEffortWriteSucceedsWhenReplicaDown(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess best-effort ACK test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()

	_, masterAddr := startG8Master(t, bins, art)

	iqn := "iqn.2026-05.example.g9a-best-effort:v1"
	r1Data, r1Status, r1Iscsi := pickAddr(t), pickAddr(t), pickAddr(t)
	r2Data, r2Status := pickAddr(t), pickAddr(t)
	r1Ctrl, r2Ctrl := pickAddr(t), pickAddr(t)

	_ = startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s1", replicaID: "r1",
		dataAddr: r1Data, ctrlAddr: r1Ctrl,
		statusAddr:  r1Status,
		iscsiAddr:   r1Iscsi,
		iscsiIQN:    iqn,
		durableRoot: filepath.Join(art, "g9a-best-effort-r1-store"),
		logTag:      "g9a-best-effort-r1",
	})
	r2 := startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s2", replicaID: "r2",
		dataAddr: r2Data, ctrlAddr: r2Ctrl,
		statusAddr:  r2Status,
		durableRoot: filepath.Join(art, "g9a-best-effort-r2-store"),
		logTag:      "g9a-best-effort-r2",
	})

	pollStatus(t, r1Status, 10*time.Second, func(b map[string]any) bool {
		h, _ := b["Healthy"].(bool)
		rid, _ := b["ReplicaID"].(string)
		return h && rid == "r1"
	})
	pollStatus(t, r2Status, 10*time.Second, func(b map[string]any) bool {
		_, ok := b["Healthy"]
		return ok
	})

	const durableBlockSize = 4096
	payload := bytes.Repeat([]byte{0xbe}, durableBlockSize)
	copy(payload, []byte("g9a-best-effort-first-write"))
	c := dialG8Iscsi(t, r1Iscsi, iqn)
	c.write10(t, 11, payload)

	r2.stop(t)

	copy(payload, []byte("g9a-best-effort-write-while-r2-down"))
	status := c.write10Status(t, 12, payload)
	c.close(t)
	if status != iscsi.StatusGood {
		t.Fatalf("best-effort write while secondary down status=0x%02x want GOOD", status)
	}
}
