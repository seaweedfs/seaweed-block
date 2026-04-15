package transport

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func setupPrimaryReplica(t *testing.T) (*storage.BlockStore, *storage.BlockStore, *ReplicaListener) {
	t.Helper()
	primary := storage.NewBlockStore(64, 4096)
	replica := storage.NewBlockStore(64, 4096)

	listener, err := NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(func() { listener.Stop() })
	return primary, replica, listener
}

func writeTestBlocks(store *storage.BlockStore, count uint32) {
	for i := uint32(0); i < count; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		data[1] = byte(i + 0xA0)
		store.Write(i, data)
	}
	store.Sync()
}

func assertDataMatch(t *testing.T, label string, primary, replica *storage.BlockStore, count uint32) {
	t.Helper()
	for i := uint32(0); i < count; i++ {
		pd, _ := primary.Read(i)
		rd, _ := replica.Read(i)
		if !bytes.Equal(pd, rd) {
			t.Fatalf("%s: LBA %d data mismatch (primary[0]=%d replica[0]=%d)",
				label, i, pd[0], rd[0])
		}
	}
}

// --- Healthy: probe shows R >= H, data matches ---
func TestTransport_Healthy_ProbeShowsCaughtUp(t *testing.T) {
	primary, replica, listener := setupPrimaryReplica(t)

	// Write same data on both.
	for i := uint32(0); i < 5; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		lsn, _ := primary.Write(i, data)
		replica.ApplyEntry(i, data, lsn)
	}
	primary.Sync()
	replica.Sync()

	exec := NewBlockExecutor(primary, listener.Addr())
	result := exec.Probe("r1", listener.Addr(), listener.Addr(), 1, 1)

	if !result.Success {
		t.Fatalf("probe failed: %s", result.FailReason)
	}

	_, _, pH := primary.Boundaries()
	if result.ReplicaFlushedLSN != pH {
		t.Fatalf("R=%d != H=%d — should be exactly caught up",
			result.ReplicaFlushedLSN, pH)
	}

	assertDataMatch(t, "healthy", primary, replica, 5)
}

// --- Catch-up: ships blocks, barrier confirms actual frontier, data matches ---
func TestTransport_CatchUp_ShipsAndBarrierConfirms(t *testing.T) {
	primary, replica, listener := setupPrimaryReplica(t)

	writeTestBlocks(primary, 10)

	exec := NewBlockExecutor(primary, listener.Addr())
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := primary.Boundaries()
	if err := exec.StartCatchUp("r1", 1, 1, 1, pH); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-closeCh:
		if !result.Success {
			t.Fatalf("catch-up failed: %s", result.FailReason)
		}
		// AchievedLSN must equal the primary head — actual frontier, not intent.
		if result.AchievedLSN != pH {
			t.Fatalf("achievedLSN=%d != primaryH=%d", result.AchievedLSN, pH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	// Replica frontier must match.
	rR, _, _ := replica.Boundaries()
	if rR != pH {
		t.Fatalf("replica frontier R=%d != primaryH=%d", rR, pH)
	}

	// Data integrity.
	assertDataMatch(t, "catch-up", primary, replica, 10)
}

// --- Rebuild: streams all blocks, frontier matches, data intact ---
func TestTransport_Rebuild_StreamsAndAdvancesFrontier(t *testing.T) {
	primary, replica, listener := setupPrimaryReplica(t)

	writeTestBlocks(primary, 10)

	exec := NewBlockExecutor(primary, listener.Addr())
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := primary.Boundaries()
	if err := exec.StartRebuild("r1", 1, 1, 1, pH); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-closeCh:
		if !result.Success {
			t.Fatalf("rebuild failed: %s", result.FailReason)
		}
		if result.AchievedLSN != pH {
			t.Fatalf("achievedLSN=%d != primaryH=%d", result.AchievedLSN, pH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	// Replica frontier must match primary.
	rR, _, _ := replica.Boundaries()
	if rR != pH {
		t.Fatalf("replica frontier R=%d != primaryH=%d after rebuild", rR, pH)
	}

	// Data integrity — this is the critical check that catches the
	// old bug where rebuild corrupted LBA 0 with zeros.
	assertDataMatch(t, "rebuild", primary, replica, 10)
}

// --- Rebuild does not corrupt LBA 0 (regression test) ---
func TestTransport_Rebuild_DoesNotCorruptLBA0(t *testing.T) {
	primary, replica, listener := setupPrimaryReplica(t)

	// Write a known pattern at LBA 0.
	data0 := make([]byte, 4096)
	data0[0] = 0xDE
	data0[1] = 0xAD
	primary.Write(0, data0)
	primary.Sync()

	exec := NewBlockExecutor(primary, listener.Addr())
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := primary.Boundaries()
	exec.StartRebuild("r1", 1, 1, 1, pH)

	select {
	case r := <-closeCh:
		if !r.Success {
			t.Fatal(r.FailReason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}

	// LBA 0 must have the original data, NOT zeros.
	rd, _ := replica.Read(0)
	if rd[0] != 0xDE || rd[1] != 0xAD {
		t.Fatalf("LBA 0 corrupted: got [%02x %02x], want [DE AD]", rd[0], rd[1])
	}
}

func TestTransport_Rebuild_InvalidatedSessionStopsWithoutCallback(t *testing.T) {
	primary, replica, listener := setupPrimaryReplica(t)
	writeTestBlocks(primary, 64)

	exec := NewBlockExecutor(primary, listener.Addr())
	exec.stepDelay = 2 * time.Millisecond
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := primary.Boundaries()
	if err := exec.StartRebuild("r1", 7, 1, 1, pH); err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)
	exec.InvalidateSession("r1", 7, "epoch_bump")

	select {
	case r := <-closeCh:
		t.Fatalf("invalidated session should not callback, got %+v", r)
	case <-time.After(150 * time.Millisecond):
	}

	rR, _, _ := replica.Boundaries()
	if rR == pH {
		t.Fatalf("invalidated rebuild should not reach target frontier %d", pH)
	}
}

func TestTransport_ReplicaRejectsStaleMutationLineage(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)

	newData := make([]byte, 4096)
	newData[0], newData[1] = 0xAA, 0xBB
	oldData := make([]byte, 4096)
	oldData[0], oldData[1] = 0x11, 0x22

	sendRebuildBlock := func(lineage RecoveryLineage, data []byte) error {
		conn, err := net.Dial("tcp", listener.Addr())
		if err != nil {
			return err
		}
		defer conn.Close()
		if err := WriteMsg(conn, MsgRebuildBlock, EncodeRebuildBlock(lineage, 0, data)); err != nil {
			return err
		}
		return nil
	}

	if err := sendRebuildBlock(RecoveryLineage{
		SessionID: 2, Epoch: 2, EndpointVersion: 2, TargetLSN: 10,
	}, newData); err != nil {
		t.Fatalf("send newer lineage: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if err := sendRebuildBlock(RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 5,
	}, oldData); err != nil {
		t.Fatalf("send stale lineage: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	got, err := replica.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0xAA || got[1] != 0xBB {
		t.Fatalf("stale lineage overwrote accepted block: got [%02x %02x], want [aa bb]", got[0], got[1])
	}
}

func TestTransport_RebuildBlock_UsesTargetLSNBeforeDone(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)

	conn, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	data := make([]byte, 4096)
	data[0], data[1] = 0xCA, 0xFE
	lineage := RecoveryLineage{
		SessionID:       3,
		Epoch:           3,
		EndpointVersion: 3,
		TargetLSN:       77,
	}
	if err := WriteMsg(conn, MsgRebuildBlock, EncodeRebuildBlock(lineage, 7, data)); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	got, err := replica.Read(7)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0xCA || got[1] != 0xFE {
		t.Fatalf("rebuild block data mismatch: got [%02x %02x], want [ca fe]", got[0], got[1])
	}

	_, _, h := replica.Boundaries()
	if h != 77 {
		t.Fatalf("rebuild block should advance walHead to targetLSN before done, got H=%d want 77", h)
	}
}

func TestTransport_Rebuild_InvalidateInterruptsBlockedAckRead(t *testing.T) {
	primary := storage.NewBlockStore(8, 4096)
	writeTestBlocks(primary, 2)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	doneReceived := make(chan struct{})
	clientClosed := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()

		for {
			msgType, _, err := ReadMsg(conn)
			if err != nil {
				serverErr <- err
				return
			}
			if msgType == MsgRebuildDone {
				close(doneReceived)
				_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				var b [1]byte
				_, err := conn.Read(b[:])
				if err == nil {
					serverErr <- nil
					return
				}
				close(clientClosed)
				serverErr <- err
				return
			}
		}
	}()

	exec := NewBlockExecutor(primary, ln.Addr().String())
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := primary.Boundaries()
	if err := exec.StartRebuild("r1", 9, 1, 1, pH); err != nil {
		t.Fatal(err)
	}

	select {
	case <-doneReceived:
	case err := <-serverErr:
		t.Fatalf("server failed before rebuild done: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for rebuild done")
	}

	exec.InvalidateSession("r1", 9, "test_interrupt_blocked_read")

	select {
	case <-clientClosed:
	case <-time.After(1 * time.Second):
		t.Fatal("invalidate did not close client conn while ack read was blocked")
	}

	select {
	case r := <-closeCh:
		t.Fatalf("invalidated blocked rebuild should not callback, got %+v", r)
	case <-time.After(150 * time.Millisecond):
	}
}
