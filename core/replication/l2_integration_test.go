package replication

// T4a-6 L2 subprocess integration test.
//
// Exercises the storage→replication→transport→replica wire using
// real TCP between two in-process BlockExecutor + ReplicaListener
// pairs. Covers T4a-1 through T4a-4 plus the T4a-6 StorageBackend
// WriteObserver hook added in this landing.
//
// Scope note (T4a-6 follow-up): T4a-5's Host authority-callback
// ordering (install-or-refuse, fail-closed) is covered by
// core/host/volume/apply_fact_test.go, NOT by this file — the
// integration test here drives ReplicationVolume.UpdateReplicaSet
// directly and bypasses Host.applyFact. Both sets of tests
// together cover the T4a landing; see 2ed72bd for the honest
// separation.
//
// Matrix: smartwal is the sign-bearing substrate. walstore is a
// second row that also gates in this harness (QA finding #4
// follow-up removed the earlier "non-gating" fiction; Go's testing
// framework has no clean demote-subtest-failure API, so pretending
// walstore could fail without failing the test was incorrect). If
// walstore ever regresses under BUG-007 the test fails honestly
// and we invest in a real demotion harness.
//
// Frontend is approximated via direct StorageBackend.Write calls.
// The full iSCSI/NVMe PDU layer is NOT exercised (iSCSI and NVMe
// integration tests already cover that translation in
// core/frontend/iscsi and core/frontend/nvme); the T4a-6 scope is
// the replication seam past StorageBackend.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// --- Matrix harness ---

// storageFactory opens a fresh LogicalStorage under the given dir,
// exclusive to this test. Returns the store + a cleanup func.
type storageFactory func(t *testing.T, dir, label string, blocks uint32, blockSize int) (storage.LogicalStorage, func())

// smartwalFactory is the sign-bearing substrate.
func smartwalFactory(t *testing.T, dir, label string, blocks uint32, blockSize int) (storage.LogicalStorage, func()) {
	t.Helper()
	path := filepath.Join(dir, label+".smartwal")
	s, err := smartwal.CreateStore(path, blocks, blockSize)
	if err != nil {
		t.Fatalf("%s: CreateStore: %v", label, err)
	}
	cleanup := func() {
		_ = s.Close()
		_ = os.Remove(path)
	}
	return s, cleanup
}

// walstoreFactory is the second matrix row. Both rows are currently
// gating — T4a-6 follow-up (QA finding #4) removed the earlier
// "non-gating" fiction since Go's testing framework has no clean
// demote-subtest-failure API and the previous defer-based harness
// didn't actually demote. Both backends pass today; if walstore
// regresses under BUG-007 the test fails honestly and we invest in
// a real demotion harness then.
func walstoreFactory(t *testing.T, dir, label string, blocks uint32, blockSize int) (storage.LogicalStorage, func()) {
	t.Helper()
	path := filepath.Join(dir, label+".walstore")
	s, err := storage.CreateWALStore(path, blocks, blockSize)
	if err != nil {
		t.Fatalf("%s: CreateWALStore: %v", label, err)
	}
	cleanup := func() {
		_ = s.Close()
		_ = os.Remove(path)
	}
	return s, cleanup
}

// runMatrix runs the given scenario against each storage backend.
// Both rows are gating in the current harness — see walstoreFactory
// comment for the QA-review history on the earlier non-gating
// fiction. T4a close requires smartwal to pass; walstore is
// expected to pass today (BUG-007 doesn't affect the best-effort
// write path T4a-6 exercises) but any regression will fail the
// test honestly.
func runMatrix(t *testing.T, scenario func(t *testing.T, newStorage storageFactory)) {
	t.Helper()
	t.Run("smartwal", func(t *testing.T) {
		scenario(t, smartwalFactory)
	})
	t.Run("walstore", func(t *testing.T) {
		scenario(t, walstoreFactory)
	})
}

// --- Test harness types ---

// primaryRig is a fully-wired primary: LogicalStorage + StorageBackend
// + ReplicationVolume, with newExec overridden so each peer target
// binds to its own BlockExecutor aimed at that peer's address.
type primaryRig struct {
	store     storage.LogicalStorage
	backend   *durable.StorageBackend
	repVol    *ReplicationVolume
	cleanup   func()
}

// replicaRig is a replica: LogicalStorage + ReplicaListener listening
// on a real TCP port.
type replicaRig struct {
	store    storage.LogicalStorage
	listener *transport.ReplicaListener
	addr     string
	cleanup  func()
}

func newReplica(t *testing.T, dir, label string, newStorage storageFactory, blocks uint32, blockSize int) *replicaRig {
	t.Helper()
	store, cleanup := newStorage(t, dir, "replica-"+label, blocks, blockSize)
	listener, err := transport.NewReplicaListener("127.0.0.1:0", store)
	if err != nil {
		cleanup()
		t.Fatalf("%s: NewReplicaListener: %v", label, err)
	}
	listener.Serve()
	return &replicaRig{
		store:    store,
		listener: listener,
		addr:     listener.Addr(),
		cleanup: func() {
			listener.Stop()
			cleanup()
		},
	}
}

func newPrimary(t *testing.T, dir, label string, newStorage storageFactory, blocks uint32, blockSize int) *primaryRig {
	t.Helper()
	store, storeCleanup := newStorage(t, dir, "primary-"+label, blocks, blockSize)

	// Use a plain projection view that always returns Healthy. This
	// matches the "frontend session is already lineage-gated" model;
	// T4a-6 scope is the data-plane seam, not frontend identity.
	id := frontend.Identity{
		VolumeID:        "vol-" + label,
		ReplicaID:       "primary-" + label,
		Epoch:           1,
		EndpointVersion: 1,
	}
	view := &alwaysHealthyView{proj: frontend.Projection{
		VolumeID:        id.VolumeID,
		ReplicaID:       id.ReplicaID,
		Epoch:           id.Epoch,
		EndpointVersion: id.EndpointVersion,
		Healthy:         true,
	}}

	backend := durable.NewStorageBackend(store, view, id)
	backend.SetOperational(true, "test harness ready")

	repVol := NewReplicationVolume(id.VolumeID, store)
	backend.SetWriteObserver(repVol)

	return &primaryRig{
		store:   store,
		backend: backend,
		repVol:  repVol,
		cleanup: func() {
			_ = backend.Close()
			_ = repVol.Close()
			storeCleanup()
		},
	}
}

type alwaysHealthyView struct {
	proj frontend.Projection
}

func (v *alwaysHealthyView) Projection() frontend.Projection { return v.proj }

// --- Scenario 1: basic end-to-end fan-out ---

// TestT4a6_BasicEndToEnd_PrimaryWriteReachesReplicaByteExact is the
// T4a batch-close gate: N LBAs written to the primary's
// StorageBackend flow through the full wire and land byte-exact on
// the replica's LogicalStorage.
func TestT4a6_BasicEndToEnd_PrimaryWriteReachesReplicaByteExact(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const (
			blocks    = uint32(128)
			blockSize = 4096
			nWrites   = 100
		)
		dir := t.TempDir()
		// Register in order: REPLICA CLEANUP FIRST (runs LAST, stops
		// listener only after primary has closed peer conns). Go's
		// t.Cleanup is LIFO; if replica.cleanup runs first it will
		// listener.Stop() → wg.Wait() → forever because handleConn
		// is blocked on the still-open session conn that the primary's
		// BlockExecutor owns.
		replica := newReplica(t, dir, "e2e", newStorage, blocks, blockSize)
		t.Cleanup(replica.cleanup)
		primary := newPrimary(t, dir, "e2e", newStorage, blocks, blockSize)
		t.Cleanup(primary.cleanup)

		// Fake-master UpdateReplicaSet with generation=1.
		if err := primary.repVol.UpdateReplicaSet(1, []ReplicaTarget{{
			ReplicaID:       "replica-e2e",
			DataAddr:        replica.addr,
			ControlAddr:     replica.addr,
			Epoch:           1,
			EndpointVersion: 1,
		}}); err != nil {
			t.Fatalf("UpdateReplicaSet: %v", err)
		}

		// Write nWrites distinct LBAs with distinct content markers.
		for i := 0; i < nWrites; i++ {
			data := make([]byte, blockSize)
			data[0] = byte((i + 1) & 0xFF)
			data[1] = byte((i + 1) >> 8)
			// Tail marker to catch off-by-one in byte translation.
			data[blockSize-1] = byte(0xE0 + (i & 0x1F))

			offset := int64(i) * int64(blockSize)
			n, err := primary.backend.Write(context.Background(), offset, data)
			if err != nil {
				t.Fatalf("Write[%d] offset=%d: %v", i, offset, err)
			}
			if n != blockSize {
				t.Fatalf("Write[%d]: short write %d/%d", i, n, blockSize)
			}
		}

		// Wait for last LBA to settle on replica (fan-out is in-flight
		// on real TCP; no barrier in T4a best_effort, so we poll).
		waitForReplicaBlock(t, replica.store, uint32(nWrites-1), byte(nWrites&0xFF), 3*time.Second)

		// Byte-exact comparison: primary vs replica on every written LBA.
		for i := 0; i < nWrites; i++ {
			pri, err := primary.store.Read(uint32(i))
			if err != nil {
				t.Fatalf("primary.Read lba=%d: %v", i, err)
			}
			rep, err := replica.store.Read(uint32(i))
			if err != nil {
				t.Fatalf("replica.Read lba=%d: %v", i, err)
			}
			if !bytes.Equal(pri, rep) {
				t.Fatalf("byte mismatch at lba=%d: primary[0..2,-1]=[%02x %02x ... %02x] replica[0..2,-1]=[%02x %02x ... %02x]",
					i, pri[0], pri[1], pri[blockSize-1],
					rep[0], rep[1], rep[blockSize-1])
			}
		}

		// Peer must still be Healthy at end.
		peers := primary.repVol.PeerCount()
		if peers != 1 {
			t.Fatalf("PeerCount=%d want 1", peers)
		}
	})
}

// --- Scenario 2: best-effort disconnect + reassign-driven resume ---

// TestT4a6_BestEffort_DisconnectThenReassign covers the QA-requested
// G5 pass-gate shape: primary ships part of a stream, the peer's
// conn is killed mid-stream; primary continues shipping locally
// (best-effort: later writes see write-error → peer Degraded, but
// caller continues); a fake-master re-assignment triggers peer
// recreation; primary ships the remainder; the union of pre- and
// post-reconnect writes lands byte-exact on the replica.
//
// This does NOT test full catch-up (that's T4c). Scope here is
// "best-effort is non-fatal to the writer and authority-driven
// peer rebuild is the recovery path."
func TestT4a6_BestEffort_DisconnectThenReassign(t *testing.T) {
	runMatrix(t, func(t *testing.T, newStorage storageFactory) {
		const (
			blocks    = uint32(128)
			blockSize = 4096
			firstBatch = 30
			secondBatch = 30
		)
		dir := t.TempDir()
		// Register replica first (runs last) so primary's peer close
		// beats listener.Stop() — see basic-e2e comment.
		replica := newReplica(t, dir, "g5", newStorage, blocks, blockSize)
		t.Cleanup(replica.cleanup)
		primary := newPrimary(t, dir, "g5", newStorage, blocks, blockSize)
		t.Cleanup(primary.cleanup)

		if err := primary.repVol.UpdateReplicaSet(1, []ReplicaTarget{{
			ReplicaID:       "replica-g5",
			DataAddr:        replica.addr,
			ControlAddr:     replica.addr,
			Epoch:           1,
			EndpointVersion: 1,
		}}); err != nil {
			t.Fatalf("UpdateReplicaSet gen=1: %v", err)
		}

		writeBatch := func(start, count int, marker byte) {
			for i := 0; i < count; i++ {
				data := make([]byte, blockSize)
				data[0] = marker
				data[1] = byte((start + i) & 0xFF)
				offset := int64(start+i) * int64(blockSize)
				if _, err := primary.backend.Write(context.Background(), offset, data); err != nil {
					t.Fatalf("Write[%d] marker=%02x: %v", start+i, marker, err)
				}
			}
		}

		// First batch: arrives at replica (peer is Healthy).
		writeBatch(0, firstBatch, 0xAA)
		waitForReplicaBlock(t, replica.store, uint32(firstBatch-1), 0xAA, 3*time.Second)

		// Mid-stream transport kill: force-close the primary's session
		// conn directly. This simulates the wire being severed from
		// underneath a healthy session — the primary's next Ship will
		// hit a dead conn and error; ReplicaPeer translates the error
		// to Degraded + Invalidate (T4a-3 CARRY-1 forward-carry).
		//
		// NOT stopping the replica listener here: Stop() is a blocking
		// wg.Wait and any handleConn goroutine stuck on ReadMsg on the
		// still-open client-side conn would wedge the test (we've hit
		// that before in volume_test cleanup). Closing the client
		// conn propagates to the listener's handleConn naturally as
		// a Read error → handleConn returns → wg.Done.
		primary.repVol.mu.Lock()
		peer := primary.repVol.peers["replica-g5"]
		primary.repVol.mu.Unlock()
		if peer == nil {
			t.Fatal("precondition: peer 'replica-g5' not tracked after gen=1 UpdateReplicaSet")
		}
		peer.executor.InvalidateSession(peer.target.ReplicaID, peer.sessionID, "T4a-6 mid-stream kill")
		// Also mark the peer Degraded so the next ShipEntry returns
		// immediately with "degraded" (without a new lazy-dial on the
		// now-possibly-stopped session).
		peer.Invalidate("T4a-6 mid-stream kill")

		// Second batch (against degraded peer): best-effort — writes
		// succeed locally; ShipEntry returns "degraded" errors that
		// OnLocalWrite logs and swallows; OnLocalWrite still returns
		// nil to caller.
		writeBatch(firstBatch, secondBatch, 0xBB)
		// Primary's LogicalStorage has the data; replica never got it
		// (because the peer was Degraded for the duration of the batch).
		if got, _ := primary.store.Read(uint32(firstBatch)); got == nil || got[0] != 0xBB {
			t.Fatalf("primary did not persist second batch locally: got %v", got)
		}

		// Bring a fresh replica listener up on a new port, using a
		// fresh store. (Resume-via-fresh-replica is the T4a best-effort
		// shape; preserving replica state across disconnect is T4c
		// rebuild territory.)
		replica2 := newReplica(t, dir, "g5-resume", newStorage, blocks, blockSize)
		t.Cleanup(replica2.cleanup)

		// Fake-master re-assignment: bump generation, point at
		// replica2.addr. UpdateReplicaSet tears down the old
		// (Degraded) peer and creates a fresh one — this is the
		// T4a-3 INV-REPL-PEER-REBUILD-ON-AUTHORITY-CHANGE path.
		if err := primary.repVol.UpdateReplicaSet(2, []ReplicaTarget{{
			ReplicaID:       "replica-g5", // same ID; peer recreated due to addr change
			DataAddr:        replica2.addr,
			ControlAddr:     replica2.addr,
			Epoch:           1,
			EndpointVersion: 2, // bump EV so peer rebuilds
		}}); err != nil {
			t.Fatalf("UpdateReplicaSet gen=2: %v", err)
		}

		// Third batch: should reach the fresh replica.
		thirdStart := firstBatch + secondBatch
		writeBatch(thirdStart, 10, 0xCC)
		waitForReplicaBlock(t, replica2.store, uint32(thirdStart+9), 0xCC, 3*time.Second)

		// Third batch must be byte-exact on replica2.
		for i := 0; i < 10; i++ {
			lba := uint32(thirdStart + i)
			pri, _ := primary.store.Read(lba)
			rep, _ := replica2.store.Read(lba)
			if !bytes.Equal(pri, rep) {
				t.Fatalf("post-reconnect lba=%d mismatch: pri[0]=%02x rep[0]=%02x",
					lba, pri[0], rep[0])
			}
		}

		// T4a best-effort policy: the SECOND batch (written while peer
		// was dead) is NOT expected to appear on replica2 in T4a
		// scope — that's catch-up, owned by T4c. We explicitly assert
		// the gap here so a future refactor that silently pulls
		// catch-up into T4a is caught.
		missingFromReplica := 0
		for i := firstBatch; i < firstBatch+secondBatch; i++ {
			if rep, _ := replica2.store.Read(uint32(i)); rep == nil || rep[0] != 0xBB {
				missingFromReplica++
			}
		}
		if missingFromReplica != secondBatch {
			t.Fatalf("T4a-6 pass-gate expectation violated: %d/%d second-batch LBAs somehow reached fresh replica without T4c catch-up (catch-up is T4c territory; premature appearance indicates a scope leak)",
				secondBatch-missingFromReplica, secondBatch)
		}

		// Orderly shutdown: close primary's replication volume BEFORE
		// the listener cleanups fire, so no handleConn goroutine is
		// left blocking on an open client-side conn. Without this,
		// replica2.cleanup runs first (LIFO) and its listener.Stop()
		// would wait forever on the still-attached session conn.
		_ = primary.repVol.Close()
	})
}

// --- Helpers ---

func waitForReplicaBlock(t *testing.T, s storage.LogicalStorage, lba uint32, wantByte0 byte, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got, err := s.Read(lba)
		if err == nil && got != nil && got[0] == wantByte0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := s.Read(lba)
	var snippet string
	if got != nil {
		snippet = fmt.Sprintf("[%02x %02x ...]", got[0], got[1])
	} else {
		snippet = "<nil>"
	}
	t.Fatalf("replica LBA %d did not settle to marker %02x within %v (got %s)",
		lba, wantByte0, timeout, snippet)
}
