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
// persistent RF2 across every LogicalStorage backend that claims
// persistence. Each test runs once per backend via
// persistentBackends (walstore, smartwal).
//
// Drives the full adapter + engine + transport route — no
// transport-only shortcuts.

const (
	testNumBlocks = 64
	testBlockSize = 4096
)

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

// TestPersistent_Handoff_Convergence: old primary A (epoch=1)
// replicates, new primary B (epoch=2) takes over with more data,
// replica converges byte-for-byte with B. Runs on both backends.
func TestPersistent_Handoff_Convergence(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			replica, _ := b.createStore(t, "replica")
			ln, err := transport.NewReplicaListener("127.0.0.1:0", replica)
			if err != nil {
				t.Fatal(err)
			}
			ln.Serve()
			t.Cleanup(func() { ln.Stop() })

			primaryA, _ := b.createStore(t, "primaryA")
			writeToStore(t, primaryA, []uint32{0, 1, 2}, 0xAA)

			execA := transport.NewBlockExecutor(primaryA, ln.Addr())
			adapterA := adapter.NewVolumeReplicaAdapter(execA)
			adapterA.OnAssignment(adapter.AssignmentInfo{
				VolumeID: "vol1", ReplicaID: "r1",
				Epoch: 1, EndpointVersion: 1,
				DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
			})
			waitMode(t, adapterA, engine.ModeHealthy, 5*time.Second)

			primaryB, _ := b.createStore(t, "primaryB")
			writeToStore(t, primaryB, []uint32{0, 1, 2, 3, 4}, 0xBB)

			execB := transport.NewBlockExecutor(primaryB, ln.Addr())
			adapterB := adapter.NewVolumeReplicaAdapter(execB)
			adapterB.OnAssignment(adapter.AssignmentInfo{
				VolumeID: "vol1", ReplicaID: "r1",
				Epoch: 2, EndpointVersion: 2,
				DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
			})
			waitMode(t, adapterB, engine.ModeHealthy, 5*time.Second)

			for _, lba := range []uint32{0, 1, 2, 3, 4} {
				expected, _ := primaryB.Read(lba)
				got, _ := replica.Read(lba)
				if !bytes.Equal(got, expected) {
					t.Fatalf("LBA %d: replica[0]=0x%02x primary[0]=0x%02x",
						lba, got[0], expected[0])
				}
			}
		})
	}
}

// TestPersistent_Rejoin_AfterReopen: real A(epoch=1) → B(epoch=2)
// handoff completes, replica is stopped + closed + reopened +
// Recover()'d. The reopened store holds B's data (new primary's
// truth), not A's. Runs on both backends.
func TestPersistent_Rejoin_AfterReopen(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			replicaPath := filepath.Join(dir, "replica.bin")
			replica := b.create(t, replicaPath)

			ln, err := transport.NewReplicaListener("127.0.0.1:0", replica)
			if err != nil {
				t.Fatal(err)
			}
			ln.Serve()

			primaryA, _ := b.createStore(t, "primaryA")
			writeToStore(t, primaryA, []uint32{0, 1, 2}, 0xAA)
			execA := transport.NewBlockExecutor(primaryA, ln.Addr())
			adapterA := adapter.NewVolumeReplicaAdapter(execA)
			adapterA.OnAssignment(adapter.AssignmentInfo{
				VolumeID: "vol1", ReplicaID: "r1",
				Epoch: 1, EndpointVersion: 1,
				DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
			})
			waitMode(t, adapterA, engine.ModeHealthy, 5*time.Second)

			primaryB, _ := b.createStore(t, "primaryB")
			writeToStore(t, primaryB, []uint32{0, 1, 2, 3, 4}, 0xBB)
			execB := transport.NewBlockExecutor(primaryB, ln.Addr())
			adapterB := adapter.NewVolumeReplicaAdapter(execB)
			adapterB.OnAssignment(adapter.AssignmentInfo{
				VolumeID: "vol1", ReplicaID: "r1",
				Epoch: 2, EndpointVersion: 2,
				DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
			})
			waitMode(t, adapterB, engine.ModeHealthy, 5*time.Second)

			expectedBytes := make(map[uint32][]byte)
			for _, lba := range []uint32{0, 1, 2, 3, 4} {
				v, _ := primaryB.Read(lba)
				cp := make([]byte, len(v))
				copy(cp, v)
				expectedBytes[lba] = cp
			}

			ln.Stop()
			if err := replica.Close(); err != nil {
				t.Fatalf("close replica: %v", err)
			}

			reopened := b.reopen(t, replicaPath)
			defer reopened.Close()

			_, _, recoveredH := reopened.Boundaries()
			if recoveredH == 0 {
				t.Fatalf("recovered H=0, want non-zero after handoff+reopen")
			}

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
		})
	}
}

// TestPersistent_CatchUp_Branch: first session brings replica to
// R=N; primary writes more so H > N; second session at epoch=2
// probes, decides catch_up (R >= S, R < H). Trace assertion
// prevents regression that would escalate to rebuild. Both backends.
func TestPersistent_CatchUp_Branch(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			replica, _ := b.createStore(t, "replica")
			ln, err := transport.NewReplicaListener("127.0.0.1:0", replica)
			if err != nil {
				t.Fatal(err)
			}
			ln.Serve()
			t.Cleanup(func() { ln.Stop() })

			primary, _ := b.createStore(t, "primary")
			writeToStore(t, primary, []uint32{0, 1, 2}, 0xDD)

			execA := transport.NewBlockExecutor(primary, ln.Addr())
			a := adapter.NewVolumeReplicaAdapter(execA)
			a.OnAssignment(adapter.AssignmentInfo{
				VolumeID: "vol1", ReplicaID: "r1",
				Epoch: 1, EndpointVersion: 1,
				DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
			})
			waitMode(t, a, engine.ModeHealthy, 5*time.Second)

			writeToStore(t, primary, []uint32{3, 4, 5}, 0xEE)

			exec2 := transport.NewBlockExecutor(primary, ln.Addr())
			a2 := adapter.NewVolumeReplicaAdapter(exec2)
			a2.OnAssignment(adapter.AssignmentInfo{
				VolumeID: "vol1", ReplicaID: "r1",
				Epoch: 2, EndpointVersion: 2,
				DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
			})
			waitMode(t, a2, engine.ModeHealthy, 5*time.Second)

			assertTraceHasDecision(t, a2, "catch_up (R >= S, R < H)")
			if _, _, _ = a2.Projection().SessionPhase, a2.Projection().Mode, a2.Projection().RecoveryDecision; false {
			}
			for _, e := range a2.Trace() {
				if e.Step == "decision" && e.Detail == "rebuild (R < S)" {
					t.Fatal("engine escalated to rebuild on what should be the catch-up branch")
				}
			}

			for _, lba := range []uint32{3, 4, 5} {
				got, _ := replica.Read(lba)
				if got[0] != 0xEE {
					t.Fatalf("catch-up LBA %d: got 0x%02x want 0xEE", lba, got[0])
				}
			}
		})
	}
}

// TestPersistent_Rebuild_Branch: primary's WAL tail advanced past
// fresh replica's R=0; engine traces rebuild (R < S); data matches.
// Both backends.
func TestPersistent_Rebuild_Branch(t *testing.T) {
	for _, b := range persistentBackends {
		t.Run(b.name, func(t *testing.T) {
			replica, _ := b.createStore(t, "replica")
			ln, err := transport.NewReplicaListener("127.0.0.1:0", replica)
			if err != nil {
				t.Fatal(err)
			}
			ln.Serve()
			t.Cleanup(func() { ln.Stop() })

			primary, _ := b.createStore(t, "primary")
			writeToStore(t, primary, []uint32{0, 1, 2, 3}, 0xFF)
			primary.AdvanceWALTail(3)

			exec := transport.NewBlockExecutor(primary, ln.Addr())
			a := adapter.NewVolumeReplicaAdapter(exec)
			a.OnAssignment(adapter.AssignmentInfo{
				VolumeID: "vol1", ReplicaID: "r1",
				Epoch: 1, EndpointVersion: 1,
				DataAddr: ln.Addr(), CtrlAddr: ln.Addr(),
			})
			waitMode(t, a, engine.ModeHealthy, 5*time.Second)

			assertTraceHasDecision(t, a, "rebuild (R < S)")

			for _, lba := range []uint32{0, 1, 2, 3} {
				expected, _ := primary.Read(lba)
				got, _ := replica.Read(lba)
				if !bytes.Equal(got, expected) {
					t.Fatalf("rebuild LBA %d: got[0]=0x%02x want[0]=0x%02x",
						lba, got[0], expected[0])
				}
			}
		})
	}
}
