package adapter_test

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// P13 S2: qualify the persistent RF2 route under abrupt-fault /
// unclean-stop conditions on EVERY persistent backend. Each test
// runs once per backend via persistentBackends.
//
// Abrupt stop is simulated via SimulateAbruptStop() on the backend
// — file handle released without graceful Close: no superblock
// persist, no final flush, no group-committer drain. On-disk state
// is exactly what Sync() made durable before the call.

func createReplicaAt(t *testing.T, b persistentBackend, path string) (storage.LogicalStorage, *transport.ReplicaListener) {
	t.Helper()
	s := b.create(t, path)
	ln, err := transport.NewReplicaListener("127.0.0.1:0", s)
	if err != nil {
		t.Fatal(err)
	}
	ln.Serve()
	return s, ln
}

func driveHealthy(t *testing.T, primary storage.LogicalStorage, addr string, epoch, endpointVersion uint64) *adapter.VolumeReplicaAdapter {
	t.Helper()
	exec := transport.NewBlockExecutor(primary, addr)
	a := adapter.NewVolumeReplicaAdapter(exec)
	a.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "vol1", ReplicaID: "r1",
		Epoch: epoch, EndpointVersion: endpointVersion,
		DataAddr: addr, CtrlAddr: addr,
	})
	waitMode(t, a, engine.ModeHealthy, 5*time.Second)
	return a
}

// TestAbrupt_StopAfterConvergence_AckedDataSurvives: after a
// recovery session completes and the replica barrier-acks the
// frontier, abrupt stop still lets the reopened replica recover
// the acked data and frontier. Runs on both backends.
func TestAbrupt_StopAfterConvergence_AckedDataSurvives(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			replicaPath := filepath.Join(dir, "replica.bin")

			replica, ln := createReplicaAt(t, b, replicaPath)

			primary, _ := b.createStore(t, "primary")
			writeToStore(t, primary, []uint32{0, 1, 2, 3}, 0xAB)
			driveHealthy(t, primary, ln.Addr(), 1, 1)

			// Capture R and H before abrupt stop. We do NOT assert S
			// here: on the replica side S is just the initial WAL
			// tail (no AdvanceWALTail ever runs), so asserting on it
			// would be trivially true and misleading. R and H are
			// the frontiers that actually move during recovery, and
			// those are the ones the envelope claim "acked data +
			// recovered R/H do not regress" pins.
			expectedR, _, expectedH := replica.Boundaries()
			expected := make(map[uint32][]byte)
			for lba := uint32(0); lba < 4; lba++ {
				v, _ := replica.Read(lba)
				cp := make([]byte, len(v))
				copy(cp, v)
				expected[lba] = cp
			}

			ln.Stop()
			b.abruptStop(t, replica)

			reopened := b.reopen(t, replicaPath)
			defer reopened.Close()

			recoveredR, _, recoveredH := reopened.Boundaries()
			if recoveredR < expectedR {
				t.Fatalf("after abrupt stop: recovered R=%d < expected R=%d",
					recoveredR, expectedR)
			}
			if recoveredH < expectedH {
				t.Fatalf("after abrupt stop: recovered H=%d < expected H=%d",
					recoveredH, expectedH)
			}

			for lba, want := range expected {
				got, err := reopened.Read(lba)
				if err != nil {
					t.Fatalf("read LBA %d: %v", lba, err)
				}
				if got[0] != want[0] {
					t.Fatalf("abrupt stop lost acked data at LBA %d: got[0]=0x%02x want[0]=0x%02x",
						lba, got[0], want[0])
				}
			}
		})
	}
}

// TestAbrupt_StopAfterCatchUp_AckedDataSurvives: catch-up branch
// to healthy → abrupt stop → reopen preserves both rebuild (0xC1)
// and catch-up (0xC2) blocks. Both backends.
func TestAbrupt_StopAfterCatchUp_AckedDataSurvives(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			replicaPath := filepath.Join(dir, "replica.bin")

			replica, ln := createReplicaAt(t, b, replicaPath)
			primary, _ := b.createStore(t, "primary")

			writeToStore(t, primary, []uint32{0, 1, 2}, 0xC1)
			driveHealthy(t, primary, ln.Addr(), 1, 1)

			writeToStore(t, primary, []uint32{3, 4, 5}, 0xC2)
			a2 := driveHealthy(t, primary, ln.Addr(), 2, 2)
			assertTraceHasDecision(t, a2, "catch_up (R >= S, R < H)")

			expectedR, _, expectedH := replica.Boundaries()

			ln.Stop()
			b.abruptStop(t, replica)

			reopened := b.reopen(t, replicaPath)
			defer reopened.Close()

			recoveredR, _, recoveredH := reopened.Boundaries()
			if recoveredR < expectedR {
				t.Fatalf("catch-up abrupt: recovered R=%d < expected R=%d",
					recoveredR, expectedR)
			}
			if recoveredH < expectedH {
				t.Fatalf("catch-up abrupt: recovered H=%d < expected H=%d",
					recoveredH, expectedH)
			}

			for _, lba := range []uint32{0, 1, 2} {
				got, _ := reopened.Read(lba)
				if got[0] != 0xC1 {
					t.Fatalf("catch-up abrupt: LBA %d (rebuild) corrupted: 0x%02x",
						lba, got[0])
				}
			}
			for _, lba := range []uint32{3, 4, 5} {
				got, _ := reopened.Read(lba)
				if got[0] != 0xC2 {
					t.Fatalf("catch-up abrupt: LBA %d (catch-up) corrupted: 0x%02x",
						lba, got[0])
				}
			}
		})
	}
}

// TestAbrupt_StopAfterRebuild_AckedDataSurvives: rebuild branch →
// abrupt stop → reopen preserves rebuild blocks. Both backends.
func TestAbrupt_StopAfterRebuild_AckedDataSurvives(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			replicaPath := filepath.Join(dir, "replica.bin")

			replica, ln := createReplicaAt(t, b, replicaPath)
			primary, _ := b.createStore(t, "primary")
			writeToStore(t, primary, []uint32{0, 1, 2, 3}, 0xD0)
			primary.AdvanceWALTail(3)

			a := driveHealthy(t, primary, ln.Addr(), 1, 1)
			assertTraceHasDecision(t, a, "rebuild (R < S)")

			expectedR, _, expectedH := replica.Boundaries()

			ln.Stop()
			b.abruptStop(t, replica)

			reopened := b.reopen(t, replicaPath)
			defer reopened.Close()

			recoveredR, _, recoveredH := reopened.Boundaries()
			if recoveredR < expectedR {
				t.Fatalf("rebuild abrupt: recovered R=%d < expected R=%d",
					recoveredR, expectedR)
			}
			if recoveredH < expectedH {
				t.Fatalf("rebuild abrupt: recovered H=%d < expected H=%d",
					recoveredH, expectedH)
			}

			for _, lba := range []uint32{0, 1, 2, 3} {
				got, _ := reopened.Read(lba)
				if got[0] != 0xD0 {
					t.Fatalf("rebuild abrupt: LBA %d corrupted: 0x%02x", lba, got[0])
				}
			}
		})
	}
}

// TestAbrupt_RejoinAfterAbruptStop_Converges: replica abrupt-
// stopped → reopened → fresh assignment at higher epoch converges;
// reopened store reflects new primary's truth. Both backends.
func TestAbrupt_RejoinAfterAbruptStop_Converges(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			replicaPath := filepath.Join(dir, "replica.bin")

			replica1, ln1 := createReplicaAt(t, b, replicaPath)
			primary1, _ := b.createStore(t, "primary1")
			writeToStore(t, primary1, []uint32{0, 1, 2}, 0xE1)
			driveHealthy(t, primary1, ln1.Addr(), 1, 1)

			ln1.Stop()
			b.abruptStop(t, replica1)

			reopened := b.reopen(t, replicaPath)
			defer reopened.Close()

			ln2, err := transport.NewReplicaListener("127.0.0.1:0", reopened)
			if err != nil {
				t.Fatal(err)
			}
			ln2.Serve()
			t.Cleanup(func() { ln2.Stop() })

			primary2, _ := b.createStore(t, "primary2")
			writeToStore(t, primary2, []uint32{0, 1, 2, 3, 4}, 0xE2)
			driveHealthy(t, primary2, ln2.Addr(), 2, 2)

			for _, lba := range []uint32{0, 1, 2, 3, 4} {
				got, _ := reopened.Read(lba)
				if got[0] != 0xE2 {
					t.Fatalf("rejoin: LBA %d did not converge to new primary: got 0x%02x want 0xE2",
						lba, got[0])
				}
			}
		})
	}
}

// TestAbrupt_StaleLineageAfterReopen_Rejected: after abrupt stop
// + reopen, once a new-epoch primary establishes lineage via real
// recovery traffic, stale old-epoch traffic is still rejected.
// Both backends.
func TestAbrupt_StaleLineageAfterReopen_Rejected(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			replicaPath := filepath.Join(dir, "replica.bin")

			replica1, ln1 := createReplicaAt(t, b, replicaPath)
			primary1, _ := b.createStore(t, "primary1")
			writeToStore(t, primary1, []uint32{0, 1}, 0xF1)
			driveHealthy(t, primary1, ln1.Addr(), 1, 1)

			ln1.Stop()
			b.abruptStop(t, replica1)

			reopened := b.reopen(t, replicaPath)
			defer reopened.Close()

			ln2, err := transport.NewReplicaListener("127.0.0.1:0", reopened)
			if err != nil {
				t.Fatal(err)
			}
			ln2.Serve()
			t.Cleanup(func() { ln2.Stop() })

			primary2, _ := b.createStore(t, "primary2")
			writeToStore(t, primary2, []uint32{0, 1, 2}, 0xF2)
			primary2.AdvanceWALTail(2)
			driveHealthy(t, primary2, ln2.Addr(), 2, 2)

			// Inject stale epoch=1 Ship at wire level. Replica's
			// active lineage is now epoch=2 from phase-2 recovery
			// traffic — stale frame must be rejected.
			staleData := make([]byte, testBlockSize)
			staleData[0] = 0xFF
			conn, err := net.Dial("tcp", ln2.Addr())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			stalePayload := transport.EncodeShipEntry(transport.ShipEntry{
				Lineage: transport.RecoveryLineage{
					SessionID: 9999, Epoch: 1, EndpointVersion: 1, TargetLSN: 100,
				},
				LBA:  0,
				LSN:  100,
				Data: staleData,
			})
			_ = transport.WriteMsg(conn, transport.MsgShipEntry, stalePayload)
			time.Sleep(100 * time.Millisecond)

			got, _ := reopened.Read(0)
			if got[0] == 0xFF {
				t.Fatal("stale epoch=1 traffic corrupted reopened replica after epoch=2 lineage established")
			}
			if got[0] != 0xF2 {
				t.Fatalf("LBA 0 = 0x%02x, want 0xF2 (primary2's data)", got[0])
			}
		})
	}
}

func assertTraceHasDecision(t *testing.T, a *adapter.VolumeReplicaAdapter, want string) {
	t.Helper()
	for _, e := range a.Trace() {
		if e.Step == "decision" && e.Detail == want {
			return
		}
	}
	t.Fatalf("engine did not trace decision %q; trace=%+v", want, a.Trace())
}
