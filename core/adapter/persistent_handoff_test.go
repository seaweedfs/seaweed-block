package adapter_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// P13 S1: prove the accepted P12 handoff/rejoin mechanism on
// persistent WALStore RF2, without inventing new semantics.
//
// These tests drive the full adapter + engine + transport route
// with WALStore on all three nodes (old primary, new primary,
// replica). They prove:
//
//   - persistent handoff convergence
//   - persistent rejoin after reopen
//   - persistent catch-up branch
//   - persistent rebuild branch

const (
	testNumBlocks = 64
	testBlockSize = 4096
)

func createWALStore(t *testing.T, name string) *storage.WALStore {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name+".bin")
	s, err := storage.CreateWALStore(path, testNumBlocks, testBlockSize)
	if err != nil {
		t.Fatalf("create WALStore %s: %v", name, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func reopenWALStore(t *testing.T, path string) *storage.WALStore {
	t.Helper()
	s, err := storage.OpenWALStore(path)
	if err != nil {
		t.Fatalf("open WALStore: %v", err)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatalf("recover WALStore: %v", err)
	}
	return s
}

func writeToStore(t *testing.T, s storage.LogicalStorage, lbas []uint32, tag byte) {
	t.Helper()
	for _, lba := range lbas {
		d := make([]byte, testBlockSize)
		d[0] = tag
		d[1] = byte(lba)
		if _, err := s.Write(lba, d); err != nil {
			t.Fatalf("write LBA %d: %v", lba, err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func waitMode(t *testing.T, a *adapter.VolumeReplicaAdapter, want engine.Mode, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		p := a.Projection()
		if p.Mode == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for mode=%s, last=%s phase=%s decision=%s",
				want, p.Mode, p.SessionPhase, p.RecoveryDecision)
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestPersistent_Handoff_Convergence proves the full P12 route on
// WALStore: old primary (epoch=1) replicates to a persistent
// replica, new primary (epoch=2) takes over and converges. After
// handoff, replica data matches the new primary block-for-block.
func TestPersistent_Handoff_Convergence(t *testing.T) {
	replicaStore := createWALStore(t, "replica")
	ln, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatal(err)
	}
	ln.Serve()
	t.Cleanup(func() { ln.Stop() })

	// --- Primary A (epoch=1) ---
	primaryA := createWALStore(t, "primaryA")
	writeToStore(t, primaryA, []uint32{0, 1, 2}, 0xAA)

	execA := transport.NewBlockExecutor(primaryA, ln.Addr())
	adapterA := adapter.NewVolumeReplicaAdapter(execA)
	adapterA.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitMode(t, adapterA, engine.ModeHealthy, 5*time.Second)

	// --- Primary B (epoch=2) with more data ---
	primaryB := createWALStore(t, "primaryB")
	writeToStore(t, primaryB, []uint32{0, 1, 2, 3, 4}, 0xBB)

	execB := transport.NewBlockExecutor(primaryB, ln.Addr())
	adapterB := adapter.NewVolumeReplicaAdapter(execB)
	adapterB.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitMode(t, adapterB, engine.ModeHealthy, 5*time.Second)

	// Verify replica data matches new primary.
	for _, lba := range []uint32{0, 1, 2, 3, 4} {
		expected, _ := primaryB.Read(lba)
		got, _ := replicaStore.Read(lba)
		if !bytes.Equal(got, expected) {
			t.Fatalf("LBA %d: replica[0]=0x%02x primary[0]=0x%02x", lba, got[0], expected[0])
		}
	}
}

// TestPersistent_Rejoin_AfterReopen proves that a real P12 handoff
// (old primary A → new primary B) on WALStore survives replica
// restart. After A reaches healthy, B takes over at epoch=2 with
// different data, B's session completes, then the replica is
// stopped/closed/reopened/recovered. The reopened store must hold
// B's data (the new primary's truth), not A's.
func TestPersistent_Rejoin_AfterReopen(t *testing.T) {
	dir := t.TempDir()
	replicaPath := filepath.Join(dir, "replica.bin")
	replicaStore, err := storage.CreateWALStore(replicaPath, testNumBlocks, testBlockSize)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatal(err)
	}
	ln.Serve()

	// --- Primary A (epoch=1) replicates its data ---
	primaryA := createWALStore(t, "primaryA")
	writeToStore(t, primaryA, []uint32{0, 1, 2}, 0xAA)

	execA := transport.NewBlockExecutor(primaryA, ln.Addr())
	adapterA := adapter.NewVolumeReplicaAdapter(execA)
	adapterA.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitMode(t, adapterA, engine.ModeHealthy, 5*time.Second)

	// --- Primary B (epoch=2) takes over with different data ---
	primaryB := createWALStore(t, "primaryB")
	writeToStore(t, primaryB, []uint32{0, 1, 2, 3, 4}, 0xBB)

	execB := transport.NewBlockExecutor(primaryB, ln.Addr())
	adapterB := adapter.NewVolumeReplicaAdapter(execB)
	adapterB.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitMode(t, adapterB, engine.ModeHealthy, 5*time.Second)

	// Record expected state: should reflect B's data, not A's.
	_, _, expectedH := replicaStore.Boundaries()
	expectedBytes := make(map[uint32][]byte)
	for _, lba := range []uint32{0, 1, 2, 3, 4} {
		b, _ := primaryB.Read(lba)
		cp := make([]byte, len(b))
		copy(cp, b)
		expectedBytes[lba] = cp
	}

	// Stop listener first so no new writes land, then close the
	// replica store so all file handles are released on Windows.
	ln.Stop()
	if err := replicaStore.Close(); err != nil {
		t.Fatalf("close replica: %v", err)
	}

	// Reopen and recover.
	reopened := reopenWALStore(t, replicaPath)
	defer reopened.Close()

	recoveredR, _, recoveredH := reopened.Boundaries()
	if recoveredH < expectedH {
		t.Fatalf("recovered H=%d < expected H=%d", recoveredH, expectedH)
	}
	if recoveredR == 0 {
		t.Fatalf("recovered R=0, want non-zero (handoff had completed)")
	}

	// The reopened replica must hold B's data, not A's.
	for lba, expected := range expectedBytes {
		got, _ := reopened.Read(lba)
		if !bytes.Equal(got, expected) {
			t.Fatalf("after reopen: LBA %d mismatch (expected B's 0x%02x, got 0x%02x)",
				lba, expected[0], got[0])
		}
		if got[0] != 0xBB {
			t.Fatalf("after reopen: LBA %d carries 0x%02x, expected B's 0xBB", lba, got[0])
		}
	}
}

// TestPersistent_CatchUp_Branch proves the catch-up path (R >= S,
// R < H) works on WALStore. A first session brings the replica to
// R=N, then the primary writes more data. The second probe finds
// R < H with R >= S, triggering catch-up instead of rebuild.
func TestPersistent_CatchUp_Branch(t *testing.T) {
	replicaStore := createWALStore(t, "replica")
	ln, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatal(err)
	}
	ln.Serve()
	t.Cleanup(func() { ln.Stop() })

	primary := createWALStore(t, "primary")

	// Phase 1: replicate initial data.
	writeToStore(t, primary, []uint32{0, 1, 2}, 0xDD)

	exec := transport.NewBlockExecutor(primary, ln.Addr())
	a := adapter.NewVolumeReplicaAdapter(exec)
	a.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitMode(t, a, engine.ModeHealthy, 5*time.Second)

	// Phase 2: primary writes MORE data (replica is now behind).
	writeToStore(t, primary, []uint32{3, 4, 5}, 0xEE)

	// New adapter at epoch=2 should see R < H (catch-up territory)
	// because primary's S is still low (WAL not trimmed).
	exec2 := transport.NewBlockExecutor(primary, ln.Addr())
	a2 := adapter.NewVolumeReplicaAdapter(exec2)
	a2.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitMode(t, a2, engine.ModeHealthy, 5*time.Second)

	// Assert the engine picked catch-up, not rebuild. Without this,
	// a regression that escalated the branch to rebuild would still
	// pass the byte-level assertion below.
	trace := a2.Trace()
	foundCatchUp := false
	foundRebuild := false
	for _, e := range trace {
		if e.Step == "decision" && e.Detail == "catch_up (R >= S, R < H)" {
			foundCatchUp = true
		}
		if e.Step == "decision" && e.Detail == "rebuild (R < S)" {
			foundRebuild = true
		}
	}
	if !foundCatchUp {
		t.Fatalf("engine did not trace catch_up decision; trace=%+v", trace)
	}
	if foundRebuild {
		t.Fatal("engine escalated to rebuild on what should be the catch-up branch")
	}

	// Verify the new data reached the replica.
	for _, lba := range []uint32{3, 4, 5} {
		got, _ := replicaStore.Read(lba)
		if got[0] != 0xEE {
			t.Fatalf("catch-up LBA %d: got 0x%02x want 0xEE", lba, got[0])
		}
	}
}

// TestPersistent_Rebuild_Branch proves the rebuild path (R < S)
// works on WALStore. The primary advances its WAL tail past the
// replica's R, forcing a full rebuild instead of catch-up.
func TestPersistent_Rebuild_Branch(t *testing.T) {
	replicaStore := createWALStore(t, "replica")
	ln, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatal(err)
	}
	ln.Serve()
	t.Cleanup(func() { ln.Stop() })

	primary := createWALStore(t, "primary")
	writeToStore(t, primary, []uint32{0, 1, 2, 3}, 0xFF)

	// Advance WAL tail past where a fresh replica would be.
	// Replica starts at R=0; primary S after this = some value > 0.
	primary.AdvanceWALTail(3)

	exec := transport.NewBlockExecutor(primary, ln.Addr())
	a := adapter.NewVolumeReplicaAdapter(exec)
	a.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitMode(t, a, engine.ModeHealthy, 5*time.Second)

	// Engine must have decided rebuild (R=0 < S=3).
	// Verify data reached the replica.
	for _, lba := range []uint32{0, 1, 2, 3} {
		expected, _ := primary.Read(lba)
		got, _ := replicaStore.Read(lba)
		if !bytes.Equal(got, expected) {
			t.Fatalf("rebuild LBA %d: got[0]=0x%02x want[0]=0x%02x", lba, got[0], expected[0])
		}
	}

	// Verify the engine traced a rebuild decision.
	trace := a.Trace()
	foundRebuild := false
	for _, e := range trace {
		if e.Step == "decision" && e.Detail == "rebuild (R < S)" {
			foundRebuild = true
			break
		}
	}
	if !foundRebuild {
		t.Fatal("engine did not trace a rebuild decision")
	}
}
