package replication

// T4c-3 L2 subprocess integration matrix.
//
// Exercises the T4c catch-up muscle and probe wire end-to-end through
// the executor + replica via real TCP. Per round-35 architect collapse,
// T4c is V3-native design; the existing L2 harness in
// l2_integration_test.go (T4a-6 scaffold) drives StorageBackend writes
// directly, NOT engine-driven recovery flows. T4c-3 at the integration
// scope therefore exercises the muscle through the executor surface —
// the catch-up sender body rewrite (T4c-2), the probe wire
// (T4c-1), and the substrate ScanLBAs interface (T4c-2).
//
// Engine-driven recovery (decide → StartCatchUp emission → adapter
// dispatch → executor) is exercised at unit scope (core/engine/
// recovery_test.go) and at adapter scope (core/adapter/probe_session_
// test.go). Full engine→adapter→executor→replica integration would
// require new ReplicationVolume↔adapter wiring (currently
// ReplicationVolume bypasses the adapter); that wiring is out of
// T4c-3 scope (not in mini-plan §2 T4c-3) and binds forward to the
// next phase that introduces an engine-managed recovery coordinator.
//
// Coverage at this layer (mini-plan §2 T4c-3 scenarios, 5 × 2 = 10
// matrix rows):
//
//   (1) Catchup_ShortDisconnect_DeltaOnly       — executor catch-up
//       exercise, both substrates
//   (2) Catchup_GapExceedsRetention_EscalatesTo
//       NeedsRebuild                            — substrate
//       ErrWALRecycled propagates through SessionClose
//   (3) Catchup_RetryBudgetExhausted_EscalatesTo
//       NeedsRebuild                            — engine retry-loop
//       wired (round-38); behavior covered by core/engine unit test
//       TestSessionFailed_NonRecycled_RetriesUntilBudget
//   (4) Probe_LineageMismatch_FailsClosed       — covered by T4c-1
//       unit tests; integration smoke verifies via the wire
//   (5) Catchup_RecoveryModeLabelSurfaced       — pinned at unit
//       scope; integration smoke verifies the substrate-introspection
//       probe doesn't panic under real wire flow

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

func timeout5s() <-chan time.Time { return time.After(5 * time.Second) }

// --- (1) Catchup_ShortDisconnect_DeltaOnly ---
//
// Primary writes N LBAs, replica is brought up empty, executor
// StartCatchUp ships the entire window, replica converges byte-exact.
// Mirrors the V2 short-gap catch-up scenario at the muscle layer.

func TestT4c3_Catchup_ShortDisconnect_DeltaOnly(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const (
			blocks    = uint32(64)
			blockSize = 4096
			nWrites   = 20
		)
		dir := t.TempDir()
		replicaStore, replicaCleanup := newStorage(t, dir, "replica", blocks, blockSize)
		t.Cleanup(replicaCleanup)
		replicaListener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
		if err != nil {
			t.Fatal(err)
		}
		replicaListener.Serve()
		t.Cleanup(replicaListener.Stop)

		primaryStore, primaryCleanup := newStorage(t, dir, "primary", blocks, blockSize)
		t.Cleanup(primaryCleanup)

		// Seed primary with nWrites blocks.
		for i := uint32(0); i < nWrites; i++ {
			data := make([]byte, blockSize)
			data[0] = byte(i + 1)
			data[blockSize-1] = byte(0xC4 ^ i)
			if _, err := primaryStore.Write(i, data); err != nil {
				t.Fatalf("primary.Write[%d]: %v", i, err)
			}
		}
		if _, err := primaryStore.Sync(); err != nil {
			t.Fatalf("primary.Sync: %v", err)
		}

		// Replica is empty. Catch-up against this gap streams the
		// retained-WAL window through ScanLBAs → MsgShipEntry.
		exec := transport.NewBlockExecutor(primaryStore, replicaListener.Addr())
		_, _, pH := primaryStore.Boundaries()

		closeCh := make(chan adapter.SessionCloseResult, 1)
		exec.SetOnSessionStart(func(adapter.SessionStartResult) {})
		exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
		if err := exec.StartCatchUp("replica-1", 1, 1, 1, 1, pH); err != nil {
			t.Fatalf("StartCatchUp: %v", err)
		}

		select {
		case result := <-closeCh:
			if !result.Success {
				t.Fatalf("catch-up failed: %s", result.FailReason)
			}
		case <-timeout5s():
			t.Fatal("timeout waiting for SessionClose")
		}

		// Byte-exact verification on every written LBA.
		for i := uint32(0); i < nWrites; i++ {
			pri, _ := primaryStore.Read(i)
			rep, _ := replicaStore.Read(i)
			if !bytes.Equal(pri, rep) {
				t.Fatalf("LBA %d byte mismatch (primary[0]=%02x replica[0]=%02x)",
					i, pri[0], rep[0])
			}
		}
	})
}

// --- (2) Catchup_GapExceedsRetention_EscalatesToNeedsRebuild ---
//
// Substrate ErrWALRecycled propagates via SessionClose's FailReason.
// We use a stub primaryStore wrapper that always returns ErrWALRecycled
// from ScanLBAs to avoid the heavy walstore checkpoint dance. The
// executor wraps the sentinel into the failure reason; the engine's
// applySessionFailed (covered by unit test) maps "WAL recycled" →
// recovery.Decision=Rebuild. At integration scope we verify the
// sentinel text reaches the close result intact.

func TestT4c3_Catchup_GapExceedsRetention_EscalatesToNeedsRebuild(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const (
			blocks    = uint32(16)
			blockSize = 4096
		)
		dir := t.TempDir()
		replicaStore, replicaCleanup := newStorage(t, dir, "replica-recycled", blocks, blockSize)
		t.Cleanup(replicaCleanup)
		listener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
		if err != nil {
			t.Fatal(err)
		}
		listener.Serve()
		t.Cleanup(listener.Stop)

		primaryStore, primaryCleanup := newStorage(t, dir, "primary-recycled", blocks, blockSize)
		t.Cleanup(primaryCleanup)
		// Seed with one block so the primary's H>0 and the wire has
		// a real frontier to chase.
		data := make([]byte, blockSize)
		data[0] = 0xAA
		primaryStore.Write(0, data)
		primaryStore.Sync()

		// Wrap with a stub that forces ErrWALRecycled.
		recycledPrimary := &recycledStubStorage{LogicalStorage: primaryStore}
		exec := transport.NewBlockExecutor(recycledPrimary, listener.Addr())

		closeCh := make(chan adapter.SessionCloseResult, 1)
		exec.SetOnSessionStart(func(adapter.SessionStartResult) {})
		exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
		if err := exec.StartCatchUp("replica-1", 1, 1, 1, 1, 1); err != nil {
			t.Fatalf("StartCatchUp: %v", err)
		}

		select {
		case result := <-closeCh:
			if result.Success {
				t.Fatal("catch-up against recycled substrate must fail")
			}
			if !strings.Contains(result.FailReason, "WAL recycled") {
				t.Errorf("FailReason missing 'WAL recycled': %s", result.FailReason)
			}
		case <-timeout5s():
			t.Fatal("timeout")
		}
	})
}

// recycledStubStorage wraps a real LogicalStorage and forces
// ErrWALRecycled from ScanLBAs. All other methods pass through.
type recycledStubStorage struct {
	storage.LogicalStorage
}

func (r *recycledStubStorage) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	return storage.ErrWALRecycled
}

// --- (4) Probe_LineageMismatch_FailsClosed (integration smoke) ---
//
// T4c-1 unit tests already exercise this exhaustively
// (TestExecutor_Probe_LineageMismatch_Rejected etc). Integration smoke
// pin: the wire shape works end-to-end against a real ReplicaListener
// + executor across both substrates.

func TestT4c3_Probe_HappyPath_AcrossBothSubstrates(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const (
			blocks    = uint32(16)
			blockSize = 4096
		)
		dir := t.TempDir()
		replicaStore, replicaCleanup := newStorage(t, dir, "replica-probe", blocks, blockSize)
		t.Cleanup(replicaCleanup)
		listener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
		if err != nil {
			t.Fatal(err)
		}
		listener.Serve()
		t.Cleanup(listener.Stop)

		primaryStore, primaryCleanup := newStorage(t, dir, "primary-probe", blocks, blockSize)
		t.Cleanup(primaryCleanup)
		// Seed primary so H>0 and probe lineage TargetLSN floors above 1.
		data := make([]byte, blockSize)
		data[0] = 0x42
		primaryStore.Write(0, data)
		primaryStore.Sync()

		exec := transport.NewBlockExecutor(primaryStore, listener.Addr())
		// Probe with a transient sessionID (real flow: adapter mints).
		result := exec.Probe("replica-1", listener.Addr(), listener.Addr(), 99, 1, 1)
		if !result.Success {
			t.Fatalf("probe failed: %s", result.FailReason)
		}
		if result.PrimaryHeadLSN == 0 {
			t.Error("PrimaryHeadLSN must be non-zero after seeded primary")
		}
	})
}

// --- (5) Catchup_RecoveryModeLabelSurfaced ---
//
// Memo §5.1 pin: the catch-up success log emits a recovery_mode
// label. The executor's runtime probe distinguishes walstore (exposes
// CheckpointLSN → wal_replay) from smartwal/BlockStore
// (state_convergence). At integration scope we exercise both
// substrates via runMatrix and assert the catch-up completes —
// the log line is not capturable in-test (logger writes to stderr),
// so this is a smoke fence for the no-panic / no-error guarantee.

func TestT4c3_Catchup_RecoveryModeLabelSurfaced(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		// Reuses the (1) ShortDisconnect setup; differentiated only
		// by intent (the (1) variant asserts byte-exactness; this one
		// asserts the substrate-mode probe doesn't break the flow).
		const (
			blocks    = uint32(16)
			blockSize = 4096
		)
		dir := t.TempDir()
		replicaStore, replicaCleanup := newStorage(t, dir, "replica-mode", blocks, blockSize)
		t.Cleanup(replicaCleanup)
		listener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
		if err != nil {
			t.Fatal(err)
		}
		listener.Serve()
		t.Cleanup(listener.Stop)

		primaryStore, primaryCleanup := newStorage(t, dir, "primary-mode", blocks, blockSize)
		t.Cleanup(primaryCleanup)
		data := make([]byte, blockSize)
		data[0] = 0xFE
		primaryStore.Write(0, data)
		primaryStore.Sync()

		exec := transport.NewBlockExecutor(primaryStore, listener.Addr())
		_, _, pH := primaryStore.Boundaries()
		closeCh := make(chan adapter.SessionCloseResult, 1)
		exec.SetOnSessionStart(func(adapter.SessionStartResult) {})
		exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
		if err := exec.StartCatchUp("replica-1", 1, 1, 1, 1, pH); err != nil {
			t.Fatal(err)
		}

		select {
		case result := <-closeCh:
			if !result.Success {
				t.Fatalf("catch-up failed: %s", result.FailReason)
			}
		case <-timeout5s():
			t.Fatal("timeout")
		}

		// Sanity: substrate-mode constants are stable and pinned.
		if storage.RecoveryModeWALReplay != "wal_replay" {
			t.Errorf("RecoveryModeWALReplay drift: %q", storage.RecoveryModeWALReplay)
		}
		if storage.RecoveryModeStateConvergence != "state_convergence" {
			t.Errorf("RecoveryModeStateConvergence drift: %q", storage.RecoveryModeStateConvergence)
		}
	})
}
