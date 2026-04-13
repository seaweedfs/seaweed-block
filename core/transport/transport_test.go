package transport

import (
	"bytes"
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

	exec := NewBlockExecutor(primary, listener.Addr(), 1)
	result := exec.Probe("r1", listener.Addr(), listener.Addr())

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

	exec := NewBlockExecutor(primary, listener.Addr(), 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := primary.Boundaries()
	if err := exec.StartCatchUp("r1", 1, pH); err != nil {
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

	exec := NewBlockExecutor(primary, listener.Addr(), 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := primary.Boundaries()
	if err := exec.StartRebuild("r1", 1, pH); err != nil {
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

	exec := NewBlockExecutor(primary, listener.Addr(), 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, pH := primary.Boundaries()
	exec.StartRebuild("r1", 1, pH)

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
