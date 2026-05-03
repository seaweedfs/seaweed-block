package main_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func TestG9B_L2GenesisObservationRequiresAssignmentBeforeISCSIReady(t *testing.T) {
	if testing.Short() {
		t.Skip("L2 subprocess join lifecycle test; -short skip")
	}
	bins := buildG54Binaries(t)
	art := t.TempDir()

	_, masterAddr := startG8Master(t, bins, art)

	iqn := "iqn.2026-05.example.g9b:v1"
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
		durableRoot: filepath.Join(art, "g9b-r1-store"),
		logTag:      "g9b-r1",
	})

	// With RF=2 topology and only r1 observed, r1 is a candidate,
	// not a frontend primary. This pins the G9B rule that heartbeat /
	// observation alone does not mint authority.
	r1Candidate := pollStatus(t, r1Status, 5*time.Second, func(b map[string]any) bool {
		_, ok := b["Healthy"]
		return ok
	})
	if r1Candidate == nil {
		t.Fatal("r1 candidate: no /status response")
	}
	if h, _ := r1Candidate["Healthy"].(bool); h {
		t.Fatalf("r1 became Healthy before full topology/assignment; status=%v", r1Candidate)
	}
	time.Sleep(800 * time.Millisecond)
	r1StillCandidate := pollStatus(t, r1Status, 2*time.Second, func(b map[string]any) bool {
		_, ok := b["Healthy"]
		return ok
	})
	if r1StillCandidate == nil {
		t.Fatal("r1 candidate recheck: no /status response")
	}
	if h, _ := r1StillCandidate["Healthy"].(bool); h {
		t.Fatalf("r1 became Healthy from observation alone; status=%v", r1StillCandidate)
	}

	_ = startG54Volume(t, bins, art, volOpts{
		masterAddr: masterAddr,
		serverID:   "s2", replicaID: "r2",
		dataAddr: r2Data, ctrlAddr: r2Ctrl,
		statusAddr:  r2Status,
		durableRoot: filepath.Join(art, "g9b-r2-store"),
		logTag:      "g9b-r2",
	})

	r1Ready := pollStatus(t, r1Status, 10*time.Second, func(b map[string]any) bool {
		h, _ := b["Healthy"].(bool)
		rid, _ := b["ReplicaID"].(string)
		epoch, _ := b["Epoch"].(float64)
		return h && rid == "r1" && epoch >= 1
	})
	if r1Ready == nil {
		t.Fatal("r1: no frontend-ready status after r2 joined")
	}
	if h, _ := r1Ready["Healthy"].(bool); !h {
		t.Fatalf("r1 did not become Healthy after assignment stream; status=%v", r1Ready)
	}

	const durableBlockSize = 4096
	payload := bytes.Repeat([]byte{0xb9}, durableBlockSize)
	copy(payload, []byte("g9b-genesis-assignment-before-iscsi-ready"))
	c := dialG8Iscsi(t, r1Iscsi, iqn)
	c.write10(t, 13, payload)
	got := c.read10(t, 13, 1, durableBlockSize)
	c.close(t)
	if !bytes.Equal(got, payload) {
		t.Fatalf("iSCSI read mismatch after assignment: got prefix=%x want prefix=%x", got[:32], payload[:32])
	}
}
