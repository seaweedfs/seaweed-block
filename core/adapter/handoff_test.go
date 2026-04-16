package adapter_test

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// These tests prove the P12 bounded replicated-durable-slice
// contract: given an external reassignment (epoch+1), the old
// primary's stale execution is rejected, the new primary converges,
// and the replica's data matches the new primary.
//
// The tests drive two real adapters + real transport against a
// shared replica — no mocks. This is the product-level proof that
// the lower institutions (lineage gate, session lifecycle, data
// sync) compose into a working handoff mechanism.

func setupReplica(t *testing.T) (*storage.BlockStore, *transport.ReplicaListener) {
	t.Helper()
	replica := storage.NewBlockStore(64, 4096)
	ln, err := transport.NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		t.Fatal(err)
	}
	ln.Serve()
	t.Cleanup(func() { ln.Stop() })
	return replica, ln
}

func writeBlocks(store *storage.BlockStore, lbas []uint32, tag byte) {
	for _, lba := range lbas {
		d := make([]byte, 4096)
		d[0] = tag
		d[1] = byte(lba)
		store.Write(lba, d)
	}
	store.Sync()
}

func waitProjection(t *testing.T, a *adapter.VolumeReplicaAdapter, want engine.Mode, timeout time.Duration) {
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

// TestHandoff_OldPrimaryToNewPrimary_Converges proves the core P12
// route end-to-end:
//
//  1. Primary A (epoch=1) replicates data to replica R, reaches healthy
//  2. Primary B (epoch=2) receives assignment for the same replica
//  3. B probes R, decides recovery, executes, converges to healthy
//  4. Replica R's data matches B's data (the "new truth")
//
// This does NOT test stale-traffic interleaving (that's the next
// test); it proves the happy path of the handoff mechanism.
func TestHandoff_OldPrimaryToNewPrimary_Converges(t *testing.T) {
	replica, ln := setupReplica(t)

	// --- Primary A (epoch=1) ---
	primaryA := storage.NewBlockStore(64, 4096)
	writeBlocks(primaryA, []uint32{0, 1, 2}, 0xAA)

	execA := transport.NewBlockExecutor(primaryA, ln.Addr())
	adapterA := adapter.NewVolumeReplicaAdapter(execA)

	adapterA.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitProjection(t, adapterA, engine.ModeHealthy, 3*time.Second)

	// Verify replica has A's data.
	for _, lba := range []uint32{0, 1, 2} {
		rd, _ := replica.Read(lba)
		if rd[0] != 0xAA {
			t.Fatalf("after A: replica LBA %d = 0x%02x, want 0xAA", lba, rd[0])
		}
	}

	// --- Primary B (epoch=2) ---
	primaryB := storage.NewBlockStore(64, 4096)
	writeBlocks(primaryB, []uint32{0, 1, 2, 3}, 0xBB)

	execB := transport.NewBlockExecutor(primaryB, ln.Addr())
	adapterB := adapter.NewVolumeReplicaAdapter(execB)

	adapterB.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitProjection(t, adapterB, engine.ModeHealthy, 3*time.Second)

	// Verify replica has B's data (the "new truth").
	for _, lba := range []uint32{0, 1, 2, 3} {
		rd, _ := replica.Read(lba)
		if rd[0] != 0xBB {
			t.Fatalf("after B: replica LBA %d = 0x%02x, want 0xBB", lba, rd[0])
		}
	}
}

// TestHandoff_StaleOldPrimaryTraffic_Rejected proves that after a
// new primary B establishes its lineage (epoch=2) on the replica,
// any in-flight or delayed traffic from old primary A (epoch=1) is
// rejected at the data plane. No stale data can leak into the
// replica's state after the epoch bump.
func TestHandoff_StaleOldPrimaryTraffic_Rejected(t *testing.T) {
	replica, ln := setupReplica(t)

	// Primary A writes and replicates at epoch=1.
	primaryA := storage.NewBlockStore(64, 4096)
	writeBlocks(primaryA, []uint32{0, 1}, 0xAA)

	execA := transport.NewBlockExecutor(primaryA, ln.Addr())
	adapterA := adapter.NewVolumeReplicaAdapter(execA)
	adapterA.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitProjection(t, adapterA, engine.ModeHealthy, 3*time.Second)

	// Primary B starts at epoch=2 with MORE data so recovery is
	// required (R < H). B's mutating traffic must establish
	// epoch=2 lineage on the replica — without this, the replica's
	// lineage gate would still be at epoch=1 because probes are
	// lineage-free.
	primaryB := storage.NewBlockStore(64, 4096)
	writeBlocks(primaryB, []uint32{0, 1, 2, 3}, 0xBB)

	execB := transport.NewBlockExecutor(primaryB, ln.Addr())
	adapterB := adapter.NewVolumeReplicaAdapter(execB)
	adapterB.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitProjection(t, adapterB, engine.ModeHealthy, 3*time.Second)

	// Now simulate stale A traffic: manually send a ship entry at
	// epoch=1 to the replica. It must be rejected because B already
	// bumped the activeLineage to epoch=2.
	conn, err := net.Dial("tcp", ln.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	staleData := make([]byte, 4096)
	staleData[0] = 0xFF // poison: if this lands, data is corrupted
	stalePayload := transport.EncodeShipEntry(transport.ShipEntry{
		Lineage: transport.RecoveryLineage{
			SessionID: 999, Epoch: 1, EndpointVersion: 1, TargetLSN: 100,
		},
		LBA:  0,
		LSN:  100,
		Data: staleData,
	})
	// Write may succeed (replica reads the frame before rejecting)
	// or may fail (replica closed the conn). Either is fine — the
	// assertion is that the data didn't land.
	_ = transport.WriteMsg(conn, transport.MsgShipEntry, stalePayload)
	time.Sleep(100 * time.Millisecond)

	rd, _ := replica.Read(0)
	if rd[0] == 0xFF {
		t.Fatal("stale epoch=1 traffic corrupted replica after epoch=2 handoff")
	}
	if rd[0] != 0xBB {
		t.Fatalf("replica LBA 0 = 0x%02x, want 0xBB (new primary's data)", rd[0])
	}
}

// TestHandoff_RejoinAfterNewPrimary_DataConsistent proves the
// durability boundary: after handoff, the replica's complete data
// set matches the new primary block-for-block. This is the
// "rejoin convergence" claim — everything the new primary has is
// on the replica, and nothing stale from the old primary leaked.
func TestHandoff_RejoinAfterNewPrimary_DataConsistent(t *testing.T) {
	replica, ln := setupReplica(t)

	// A writes a subset.
	primaryA := storage.NewBlockStore(64, 4096)
	writeBlocks(primaryA, []uint32{0, 1, 2, 3, 4}, 0xAA)
	execA := transport.NewBlockExecutor(primaryA, ln.Addr())
	adapterA := adapter.NewVolumeReplicaAdapter(execA)
	adapterA.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitProjection(t, adapterA, engine.ModeHealthy, 3*time.Second)

	// B writes a different, larger set.
	primaryB := storage.NewBlockStore(64, 4096)
	writeBlocks(primaryB, []uint32{0, 1, 2, 3, 4, 5, 6, 7}, 0xBB)
	execB := transport.NewBlockExecutor(primaryB, ln.Addr())
	adapterB := adapter.NewVolumeReplicaAdapter(execB)
	adapterB.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 2, EndpointVersion: 2,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitProjection(t, adapterB, engine.ModeHealthy, 3*time.Second)

	// Every LBA that B wrote must match on the replica.
	allB := primaryB.AllBlocks()
	for lba, expected := range allB {
		got, err := replica.Read(lba)
		if err != nil {
			t.Fatalf("replica read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("replica LBA %d diverges from new primary: got[0]=0x%02x want[0]=0x%02x",
				lba, got[0], expected[0])
		}
	}
}

// TestHandoff_OldPrimaryDemoted_NoResurrection proves that after
// the old primary's adapter receives a removal event, late session
// callbacks from the old executor cannot resurrect a healthy
// projection. The old adapter must settle to non-healthy with
// no active recovery.
func TestHandoff_OldPrimaryDemoted_NoResurrection(t *testing.T) {
	_, ln := setupReplica(t)

	primaryA := storage.NewBlockStore(64, 4096)
	writeBlocks(primaryA, []uint32{0, 1}, 0xAA)
	execA := transport.NewBlockExecutor(primaryA, ln.Addr())
	adapterA := adapter.NewVolumeReplicaAdapter(execA)
	adapterA.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: 1, EndpointVersion: 1,
		DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
	})
	waitProjection(t, adapterA, engine.ModeHealthy, 3*time.Second)

	// Master removes A from the roster.
	adapterA.OnRemoval("r1", "reassigned")

	time.Sleep(100 * time.Millisecond)

	p := adapterA.Projection()
	if p.Mode == engine.ModeHealthy {
		t.Fatal("removed adapter must not stay healthy")
	}
	if p.SessionPhase == engine.PhaseRunning || p.SessionPhase == engine.PhaseStarting {
		t.Fatalf("removed adapter has active session: phase=%s", p.SessionPhase)
	}
}
