package replication

// T4b-6 L2 subprocess integration matrix — T4b batch close gate.
//
// Exercises each of the three DurabilityMode semantics end-to-end
// through StorageBackend.Sync → observer.Sync → DurabilityCoordinator
// → peer.Barrier → real ReplicaListener. Mirrors the T4a-6 harness
// shape (matrix runner, real TCP between in-process pairs) and adds
// mode-specific assertions:
//
//   - SyncAll happy path (2 healthy peers)
//   - SyncAll fails when any peer barriers fail
//   - SyncQuorum RF=3 tolerates 1 of 2 peers failing
//   - BestEffort silences all-peers-failed (peers Degraded, Sync nil)
//
// Plus a wire-shape scope-leak fence asserting BarrierResponse
// stays at the architect round-21 full-lineage-echo form (≥40B,
// 32B lineage + 8B AchievedLSN). Catches any future refactor that
// silently weakens to epoch-only or short form.
//
// Matrix discipline: smartwal is product-signing substrate; walstore
// is also gating in this harness (T4a-6 follow-up removed the
// 'non-gating' fiction). Both impls expected to pass at L2.

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// --- Test harness extensions ---

// reservedDeadAddr returns an addr that's guaranteed not to accept
// connections — listens then closes immediately, so the OS won't
// reuse the port within the test's lifetime (typically). Used for
// "peer is unreachable" scenarios.
func reservedDeadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// addPeerToRig wires a ReplicaTarget into the primary's
// ReplicationVolume via UpdateReplicaSet, preserving any existing
// peers (the test caller passes the full set on each call). Tests
// use this to incrementally compose RF=2 / RF=3 scenarios.
func setReplicaSet(t *testing.T, p *primaryRig, gen uint64, targets []ReplicaTarget) {
	t.Helper()
	if err := p.repVol.UpdateReplicaSet(gen, targets); err != nil {
		t.Fatalf("UpdateReplicaSet gen=%d: %v", gen, err)
	}
}

// writePrimaryBlocks writes count distinct LBAs through
// StorageBackend.Write so the local sync path exercises the full
// frontend → storage → observer.Observe chain. Returns the highest
// LSN written (best-effort — primary's walHead).
func writePrimaryBlocks(t *testing.T, p *primaryRig, count int, blockSize int) uint64 {
	t.Helper()
	for i := 0; i < count; i++ {
		data := make([]byte, blockSize)
		data[0] = byte(i + 1)
		data[1] = byte(0xA0 + (i & 0x1F))
		offset := int64(i) * int64(blockSize)
		if _, err := p.backend.Write(context.Background(), offset, data); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}
	_, _, h := p.store.Boundaries()
	return h
}

// --- Scenario 1: sync_all two peers all healthy ---

// TestT4b6_SyncAll_TwoPeers_AllHealthy — DurabilitySyncAll requires
// EVERY peer to ack. With 2 healthy peers, Sync must return nil and
// both replicas must reach the primary's frontier. End-to-end flow
// through StorageBackend.Sync → observer.Sync → DurabilityCoordinator
// → peer.Barrier × 2 in parallel.
func TestT4b6_SyncAll_TwoPeers_AllHealthy(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const blocks = uint32(64)
		const blockSize = 4096
		const nWrites = 20
		dir := t.TempDir()

		// Two replica rigs.
		r1 := newReplica(t, dir, "syncall-r1", newStorage, blocks, blockSize)
		t.Cleanup(r1.cleanup)
		r2 := newReplica(t, dir, "syncall-r2", newStorage, blocks, blockSize)
		t.Cleanup(r2.cleanup)
		// Primary registered last so its cleanup runs first (LIFO),
		// closing peer conns before listener.Stop() blocks.
		primary := newPrimary(t, dir, "syncall-happy", newStorage, blocks, blockSize)
		primary.repVol.SetDurabilityMode(DurabilitySyncAll)
		t.Cleanup(primary.cleanup)

		setReplicaSet(t, primary, 1, []ReplicaTarget{
			{ReplicaID: "r1", DataAddr: r1.addr, ControlAddr: r1.addr, Epoch: 1, EndpointVersion: 1},
			{ReplicaID: "r2", DataAddr: r2.addr, ControlAddr: r2.addr, Epoch: 1, EndpointVersion: 1},
		})

		writePrimaryBlocks(t, primary, nWrites, blockSize)
		// Wait for last LBA to land before Sync so the barrier has
		// real data to confirm.
		waitForReplicaBlock(t, r1.store, uint32(nWrites-1), byte(nWrites), 3*time.Second)
		waitForReplicaBlock(t, r2.store, uint32(nWrites-1), byte(nWrites), 3*time.Second)

		if err := primary.backend.Sync(context.Background()); err != nil {
			t.Fatalf("sync_all happy must succeed: %v", err)
		}

		// Both peers stay Healthy after happy-path Sync.
		for _, peerID := range []string{"r1", "r2"} {
			primary.repVol.mu.Lock()
			state := primary.repVol.peers[peerID].State()
			primary.repVol.mu.Unlock()
			if state != ReplicaHealthy {
				t.Fatalf("peer %s state after sync_all happy: got %s want healthy", peerID, state)
			}
		}

		// Replica byte-exact (forward-carry: T4a-6 BasicEndToEnd
		// invariant continues to hold).
		for i := uint32(0); i < nWrites; i++ {
			pri, _ := primary.store.Read(i)
			rep1, _ := r1.store.Read(i)
			rep2, _ := r2.store.Read(i)
			if !bytes.Equal(pri, rep1) {
				t.Fatalf("LBA %d: primary != r1", i)
			}
			if !bytes.Equal(pri, rep2) {
				t.Fatalf("LBA %d: primary != r2", i)
			}
		}

		_ = primary.repVol.Close()
	})
}

// --- Scenario 2: sync_all fails on barrier error ---

// TestT4b6_SyncAll_OnePeer_FailsOnBarrierError — DurabilitySyncAll
// rejects ANY peer failure. With 1 healthy peer + 1 unreachable peer,
// Sync must return ErrDurabilityBarrierFailed. The healthy peer's
// barrier still ran successfully; the failure is mode-driven, not
// transport-driven.
func TestT4b6_SyncAll_OnePeer_FailsOnBarrierError(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const blocks = uint32(64)
		const blockSize = 4096
		dir := t.TempDir()

		r1 := newReplica(t, dir, "syncall-fail-r1", newStorage, blocks, blockSize)
		t.Cleanup(r1.cleanup)
		deadAddr := reservedDeadAddr(t)
		primary := newPrimary(t, dir, "syncall-fail", newStorage, blocks, blockSize)
		primary.repVol.SetDurabilityMode(DurabilitySyncAll)
		t.Cleanup(primary.cleanup)

		setReplicaSet(t, primary, 1, []ReplicaTarget{
			{ReplicaID: "r1", DataAddr: r1.addr, ControlAddr: r1.addr, Epoch: 1, EndpointVersion: 1},
			{ReplicaID: "r-dead", DataAddr: deadAddr, ControlAddr: deadAddr, Epoch: 1, EndpointVersion: 1},
		})

		// Write a few blocks so the local sync has something to flush.
		writePrimaryBlocks(t, primary, 5, blockSize)

		err := primary.backend.Sync(context.Background())
		if err == nil {
			t.Fatal("sync_all with one peer unreachable must error")
		}
		if !errors.Is(err, ErrDurabilityBarrierFailed) {
			t.Fatalf("expected ErrDurabilityBarrierFailed, got: %v", err)
		}

		// Dead peer must be Degraded (peer.Barrier translation,
		// INV-REPL-BARRIER-FAILURE-DEGRADES-PEER).
		primary.repVol.mu.Lock()
		deadState := primary.repVol.peers["r-dead"].State()
		healthyState := primary.repVol.peers["r1"].State()
		primary.repVol.mu.Unlock()
		if deadState != ReplicaDegraded {
			t.Fatalf("dead peer state: got %s want degraded", deadState)
		}
		// Healthy peer should still be Healthy (its barrier succeeded
		// even though sync_all failed overall).
		if healthyState != ReplicaHealthy {
			t.Fatalf("healthy peer state after sync_all-fails: got %s want healthy",
				healthyState)
		}

		_ = primary.repVol.Close()
	})
}

// --- Scenario 3: sync_quorum RF=3 tolerant of 1 failure ---

// TestT4b6_SyncQuorum_RF3_TolerantOfOneFailure — DurabilitySyncQuorum
// with RF=3 (primary + 2 peers) tolerates 1 peer failure. Quorum =
// rf/2+1 = 2; primary(1) + healthy peer(1) = 2 durable. Sync nil.
// Quorum arithmetic from V2 dist_group_commit.go preserved end-to-end.
func TestT4b6_SyncQuorum_RF3_TolerantOfOneFailure(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const blocks = uint32(64)
		const blockSize = 4096
		dir := t.TempDir()

		r1 := newReplica(t, dir, "quorum-r1", newStorage, blocks, blockSize)
		t.Cleanup(r1.cleanup)
		deadAddr := reservedDeadAddr(t)
		primary := newPrimary(t, dir, "quorum", newStorage, blocks, blockSize)
		primary.repVol.SetDurabilityMode(DurabilitySyncQuorum)
		t.Cleanup(primary.cleanup)

		setReplicaSet(t, primary, 1, []ReplicaTarget{
			{ReplicaID: "r1", DataAddr: r1.addr, ControlAddr: r1.addr, Epoch: 1, EndpointVersion: 1},
			{ReplicaID: "r-dead", DataAddr: deadAddr, ControlAddr: deadAddr, Epoch: 1, EndpointVersion: 1},
		})

		writePrimaryBlocks(t, primary, 5, blockSize)

		// sync_quorum: primary(1) + r1(1) = 2 durable >= 2 quorum → pass.
		if err := primary.backend.Sync(context.Background()); err != nil {
			t.Fatalf("sync_quorum should tolerate 1/2 peer failure: %v", err)
		}

		primary.repVol.mu.Lock()
		deadState := primary.repVol.peers["r-dead"].State()
		primary.repVol.mu.Unlock()
		if deadState != ReplicaDegraded {
			t.Fatalf("dead peer state: got %s want degraded", deadState)
		}

		// Sanity: with BOTH peers dead, sync_quorum should fail.
		// Use a fresh primary so we don't conflict with the prior
		// peer-state mutations.
		r1b := newReplica(t, dir, "quorum-r1b", newStorage, blocks, blockSize)
		t.Cleanup(r1b.cleanup)
		_ = r1b // not actually used; primary has both dead
		primary2 := newPrimary(t, dir, "quorum-bothfail", newStorage, blocks, blockSize)
		primary2.repVol.SetDurabilityMode(DurabilitySyncQuorum)
		t.Cleanup(primary2.cleanup)

		dead2 := reservedDeadAddr(t)
		dead3 := reservedDeadAddr(t)
		setReplicaSet(t, primary2, 1, []ReplicaTarget{
			{ReplicaID: "r-dead-1", DataAddr: dead2, ControlAddr: dead2, Epoch: 1, EndpointVersion: 1},
			{ReplicaID: "r-dead-2", DataAddr: dead3, ControlAddr: dead3, Epoch: 1, EndpointVersion: 1},
		})
		writePrimaryBlocks(t, primary2, 5, blockSize)

		if err := primary2.backend.Sync(context.Background()); err == nil {
			t.Fatal("sync_quorum with 0/2 peer durable must fail")
		} else if !errors.Is(err, ErrDurabilityQuorumLost) {
			t.Fatalf("expected ErrDurabilityQuorumLost, got: %v", err)
		}

		_ = primary.repVol.Close()
		_ = primary2.repVol.Close()
	})
}

// --- Scenario 4: best_effort silences all-failed ---

// TestT4b6_BestEffort_AllPeersFail_StillSucceeds — DurabilityBestEffort
// returns nil even with all peers failing; per-peer Invalidate
// still happens (via peer.Barrier T4b-3 wrapper). Sync's job in
// best_effort is "make local durable; replication is informational."
func TestT4b6_BestEffort_AllPeersFail_StillSucceeds(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const blocks = uint32(64)
		const blockSize = 4096
		dir := t.TempDir()

		dead1 := reservedDeadAddr(t)
		dead2 := reservedDeadAddr(t)
		primary := newPrimary(t, dir, "besteffort", newStorage, blocks, blockSize)
		primary.repVol.SetDurabilityMode(DurabilityBestEffort)
		t.Cleanup(primary.cleanup)

		setReplicaSet(t, primary, 1, []ReplicaTarget{
			{ReplicaID: "r-dead-1", DataAddr: dead1, ControlAddr: dead1, Epoch: 1, EndpointVersion: 1},
			{ReplicaID: "r-dead-2", DataAddr: dead2, ControlAddr: dead2, Epoch: 1, EndpointVersion: 1},
		})

		writePrimaryBlocks(t, primary, 5, blockSize)

		// best_effort: even with all peers dead, Sync returns nil
		// because primary's local fsync succeeded.
		if err := primary.backend.Sync(context.Background()); err != nil {
			t.Fatalf("best_effort with all peers failed must NOT surface error: %v", err)
		}

		// Both peers must be Degraded (per-peer Invalidate from
		// peer.Barrier; INV-REPL-BARRIER-FAILURE-DEGRADES-PEER).
		for _, peerID := range []string{"r-dead-1", "r-dead-2"} {
			primary.repVol.mu.Lock()
			state := primary.repVol.peers[peerID].State()
			primary.repVol.mu.Unlock()
			if state != ReplicaDegraded {
				t.Fatalf("peer %s state: got %s want degraded", peerID, state)
			}
		}

		// Local sync still happened — primary's frontier must have
		// advanced to walHead.
		_, _, pH := primary.store.Boundaries()
		if pH < 5 {
			t.Fatalf("primary walHead: got %d want >=5 (local fsync should run)", pH)
		}

		_ = primary.repVol.Close()
	})
}

// --- Wire-shape scope-leak fence ---

// TestT4b6_BarrierWire_FullLineageEcho_StillEnforced is the round-22
// scope-leak fence at the T4b batch close. Catches any future
// refactor that silently weakens BarrierResponse from full-lineage
// echo (40B = 32B lineage + 8B AchievedLSN) back toward epoch-only
// or short form. If barrierRespSize ever changes or
// EncodeBarrierResp produces a smaller payload, this test trips —
// independent of whether the matrix scenarios above happen to also
// fail.
func TestT4b6_BarrierWire_FullLineageEcho_StillEnforced(t *testing.T) {
	sample := transport.BarrierResponse{
		Lineage: transport.RecoveryLineage{
			SessionID:       1,
			Epoch:           1,
			EndpointVersion: 1,
			TargetLSN:       1,
		},
		AchievedLSN: 99,
	}
	encoded := transport.EncodeBarrierResp(sample)
	if len(encoded) != 40 {
		t.Fatalf("BarrierResp encoded length = %d; T4b-1 wire shape (40B = 32B lineage + 8B achievedLSN) regressed",
			len(encoded))
	}

	// Round-trip must preserve every lineage field byte-exact (T4b-1
	// invariant; this is the round-22 scope-leak fence equivalent
	// of the T4a-1 envelope tests at the matrix level).
	got, err := transport.DecodeBarrierResp(encoded)
	if err != nil {
		t.Fatalf("DecodeBarrierResp on freshly-encoded sample: %v", err)
	}
	if got.Lineage != sample.Lineage {
		t.Fatalf("lineage drift: got %+v want %+v", got.Lineage, sample.Lineage)
	}
	if got.AchievedLSN != sample.AchievedLSN {
		t.Fatalf("AchievedLSN drift: got %d want %d", got.AchievedLSN, sample.AchievedLSN)
	}

	// And: a 39-byte payload must fail decode (would happen if
	// barrierRespSize were silently weakened to 8 + something <32).
	short := encoded[:39]
	if _, err := transport.DecodeBarrierResp(short); err == nil {
		t.Fatal("DecodeBarrierResp on 39B payload must reject; T4b-1 fail-closed regressed")
	}
}

// --- Sanity: unused import guard ---

var _ = storage.NewBlockStore
