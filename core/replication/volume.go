package replication

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// LocalWrite carries one acked primary-side write across the layer
// boundary from the Backend wrapper into ReplicationVolume's fan-out.
// LSN is assigned by LogicalStorage.Write and is the authoritative
// order key for replica delivery.
type LocalWrite struct {
	LBA  uint32
	Data []byte
	LSN  uint64
}

// ReplicationVolume is the per-volume fan-out owner. It:
//   - tracks the authoritative replica set (peers) from master
//     assignments;
//   - serializes per-write fan-out so the LSN-order invariant
//     V2 enforced via BlockVol.shipMu survives into V3 even though
//     the LSN allocation seam (LogicalStorage.Write) is now split
//     from the ship seam (this type).
//
// Lifecycle: borrowed LogicalStorage — Provider owns the engine;
// ReplicationVolume must NEVER call store.Close() (BUG-005 discipline).
type ReplicationVolume struct {
	volumeID string
	store    storage.LogicalStorage // borrowed, NEVER closed by us
	newExec  executorFactory        // test seam; default dials real TCP
	coord    *DurabilityCoordinator // used by Sync; stateless

	mu                    sync.Mutex // serializes UpdateReplicaSet + OnLocalWrite entry AND fan-out AND Sync
	peers                 map[string]*ReplicaPeer
	closed                bool
	lastAppliedGeneration uint64         // monotonic guard; 0 means "no generation applied yet"
	durabilityMode        DurabilityMode // set via SetDurabilityMode; default is BestEffort

	// replayedGens counts UpdateReplicaSet calls dropped as stale
	// (generation > 0 && generation <= lastAppliedGeneration). Exposed
	// only to same-package tests for now; a public Stats() or
	// Prometheus hook is a T4-end observability pass.
	replayedGens atomic.Uint64

	// G5-5C probe loop integration. Set once via ConfigureProbeLoop;
	// started via StartProbeLoop after primary admit; stopped FIRST
	// during Close (before peer teardown) so an in-flight probe
	// callback never lands on a closed volume / closed peer set.
	// Read+written under v.mu.
	probeLoop    *ProbeLoop
	probeCfg     ProbeLoopConfig // remembered for SetProbeCooldownConfig push-down on UpdateReplicaSet
	probeCfgSet  bool            // true after ConfigureProbeLoop succeeds
}

// executorFactory lets tests inject a BlockExecutor constructor that
// binds to a specific replica address. Production uses the real
// transport.NewBlockExecutor.
type executorFactory func(store storage.LogicalStorage, replicaAddr string) *transport.BlockExecutor

// NewReplicationVolume constructs a per-volume fan-out coordinator.
// The returned volume borrows store — it is a read-only handle from
// the volume's perspective and is never closed here (Provider owns
// the engine).
//
// Called by: DurableProvider / Host composition root at volume
// lifecycle start, after LogicalStorage is recovered and ready.
// Owns: the peers map; all *ReplicaPeer lifecycles (Close on remove);
// the volume-level serialization mutex.
// Borrows: store (LogicalStorage). Provider owns engine lifecycle;
// ReplicationVolume MUST NOT call store.Close() (BUG-005).
func NewReplicationVolume(volumeID string, store storage.LogicalStorage) *ReplicationVolume {
	return &ReplicationVolume{
		volumeID:       volumeID,
		store:          store,
		newExec:        transport.NewBlockExecutor,
		coord:          NewDurabilityCoordinator(),
		peers:          make(map[string]*ReplicaPeer),
		durabilityMode: DurabilityBestEffort, // zero value; explicit for clarity
	}
}

// SetDurabilityMode configures the per-volume durability semantic
// that Sync uses. Safe to call at any time; effect applies from the
// next Sync call forward. Per mini-plan §5, T4b does not support
// per-Sync-call mode override — mode is a per-volume setting.
//
// Called by: Host / Provider composition root at volume lifecycle
// start, or on operator reconfiguration.
// Owns: durabilityMode field under v.mu.
// Borrows: nothing.
func (v *ReplicationVolume) SetDurabilityMode(mode DurabilityMode) {
	v.mu.Lock()
	v.durabilityMode = mode
	v.mu.Unlock()
}

// DurabilityMode returns the currently-configured mode. Read-only
// accessor for tests and diagnostics.
func (v *ReplicationVolume) DurabilityMode() DurabilityMode {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.durabilityMode
}

// Sync runs the per-volume durability closure for host-requested
// cache flushes. Delegates to DurabilityCoordinator.SyncLocalAndReplicas
// with the volume's configured mode + current peer set + a localSync
// closure that wraps LogicalStorage.Sync.
//
// Lock scope: v.mu is held across the FULL call. This preserves the
// same discipline as OnLocalWrite (architect T4a-4 round-15
// Condition A): LSN-order fan-out serialization at the replication
// layer. Concurrent Sync and OnLocalWrite calls are thus serialized
// on v.mu, matching V2's shipMu semantics extended from ship-only
// into ship+barrier. Accepted correctness-first trade-off: a slow
// peer barrier stalls other writers on the same volume; async-queue
// optimization (Option Z) is deferred post-T4 per mini-plan §6.
//
// Forward-carry: INV-REPL-LSN-ORDER-FANOUT-001 (T4a-4) must
// continue to pass under the new code path; the adversarial
// TestReplicationVolume_OnLocalWrite_ConcurrentLSNs_OrderedAtReplica
// test and the new TestReplicationVolume_Sync_PreservesLSNOrder
// UnderConcurrency pin verify no regression.
//
// Called by: StorageBackend.Sync (when a WriteObserver is installed)
// per host-side FLUSH / SYNCHRONIZE_CACHE.
// Owns: v.mu across the full SyncLocalAndReplicas call; peer snapshot
// assembly.
// Borrows: ctx + targetLSN from caller.
func (v *ReplicationVolume) Sync(ctx context.Context, targetLSN uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return fmt.Errorf("replication: Sync: volume %s closed", v.volumeID)
	}

	// Snapshot the peers under v.mu — DurabilityCoordinator reads
	// them from a slice so no further lock coordination is needed.
	peers := make([]*ReplicaPeer, 0, len(v.peers))
	for _, p := range v.peers {
		peers = append(peers, p)
	}

	mode := v.durabilityMode
	localSync := func(ctx context.Context) (uint64, error) {
		return v.store.Sync()
	}

	_, err := v.coord.SyncLocalAndReplicas(ctx, mode, targetLSN, localSync, peers)
	return err
}

// UpdateReplicaSet applies the authoritative replica set from a master
// assignment event. Adds new peers, removes deleted ones, and tears
// down + recreates peers whose lineage (Epoch / EndpointVersion) has
// bumped. Lineage-bump tear-down is the T4a MVP shape; in-place
// lineage update on existing peers is a T4c refinement when recovery
// sessions thread through.
//
// Generation rule (T4a-5.0 decision §9.4):
//   - generation == 0: unversioned apply. Peer map IS mutated, but
//     lastAppliedGeneration is NOT advanced. Intended for test /
//     fake-master use only; production master MUST emit >= 1.
//   - generation > 0 && generation > lastAppliedGeneration: apply
//     + advance lastAppliedGeneration.
//   - generation > 0 && generation <= lastAppliedGeneration: stale
//     replay. Peer map NOT mutated; replayedGens counter increments;
//     debug log emits a peer-ID-set delta diff for forensics. Returns
//     nil (idempotent replay is success, not error — consistent with
//     Ship's epoch-== silent-drop pattern).
//
// The empty-peer-set case (targets == [] with any generation) flows
// through the same teardown path as N → M-1 removal, just iterated to
// completion. No special branch. Standalone / RF=1 / operator-drained
// volumes are legal authoritative state.
//
// Called by: Host authority-callback path (T4a-5), on every
// assignment event that carries a replica-set delta.
// Owns: peers map mutations under v.mu; *ReplicaPeer lifecycle (New
// on add, Close on remove / lineage bump); the per-peer BlockExecutor
// created via newExec; the lastAppliedGeneration monotonic guard.
// Borrows: targets slice — caller retains; we read-only copy the
// fields we need.
func (v *ReplicationVolume) UpdateReplicaSet(generation uint64, targets []ReplicaTarget) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return fmt.Errorf("replication: UpdateReplicaSet: volume %s closed", v.volumeID)
	}

	// Generation guard (T4a-5.0 §9.4 three-way rule).
	if generation > 0 && generation <= v.lastAppliedGeneration {
		v.replayedGens.Add(1)
		// Log peer-ID-set delta for forensics (Q2 binding — IDs only,
		// not full target structs). `had` is the current in-memory
		// peer ID set; `got` is the incoming set. Equal sets are the
		// normal replay case; unequal sets indicate a master-side
		// oddity worth a grep.
		had := peerIDSet(v.peers)
		got := targetIDSet(targets)
		if !stringSetEqual(had, got) {
			log.Printf("replication: volume %s stale-gen replay (gen=%d, lastApplied=%d) with differing peers — had=%s got=%s",
				v.volumeID, generation, v.lastAppliedGeneration,
				formatIDSet(had), formatIDSet(got))
		}
		return nil
	}

	want := make(map[string]ReplicaTarget, len(targets))
	for _, t := range targets {
		if t.ReplicaID == "" {
			return fmt.Errorf("replication: UpdateReplicaSet: empty ReplicaID in targets")
		}
		want[t.ReplicaID] = t
	}

	// Remove peers no longer in the authoritative set. Same teardown
	// path is used for N → 0 (empty targets) — no special branch.
	for id, peer := range v.peers {
		if _, keep := want[id]; !keep {
			_ = peer.Close()
			delete(v.peers, id)
		}
	}

	// Add new peers + recreate on lineage bump.
	for id, t := range want {
		if existing, ok := v.peers[id]; ok {
			cur := existing.Target()
			if cur.Epoch == t.Epoch && cur.EndpointVersion == t.EndpointVersion && cur.DataAddr == t.DataAddr {
				continue
			}
			// Lineage or address bumped → tear down + recreate.
			_ = existing.Close()
			delete(v.peers, id)
		}
		executor := v.newExec(v.store, t.DataAddr)
		peer, err := NewReplicaPeer(t, executor)
		if err != nil {
			return fmt.Errorf("replication: UpdateReplicaSet: add peer %s: %w", id, err)
		}
		// G5-5C: push the volume-level probe cooldown config onto the
		// fresh peer (architect 2026-04-27 guidance #3). A new peer
		// (whether first add or post-lineage-bump recreate) starts
		// with cooldown reset to defaults; the prior peer's cooldown
		// state cannot leak across the lineage boundary because that
		// state lived on the now-closed *ReplicaPeer.
		if v.probeCfgSet {
			peer.SetProbeCooldownConfig(PeerProbeCooldown{
				Base: v.probeCfg.CooldownBase,
				Cap:  v.probeCfg.CooldownCap,
			})
		}
		v.peers[id] = peer
	}

	// Advance the monotonic guard only for real (non-zero) generations.
	if generation > 0 {
		v.lastAppliedGeneration = generation
	}
	return nil
}

// peerIDSet extracts the set of peer IDs from the current peers map.
// Caller must hold v.mu.
func peerIDSet(peers map[string]*ReplicaPeer) map[string]struct{} {
	out := make(map[string]struct{}, len(peers))
	for id := range peers {
		out[id] = struct{}{}
	}
	return out
}

// targetIDSet extracts the set of replica IDs from an incoming targets
// slice.
func targetIDSet(targets []ReplicaTarget) map[string]struct{} {
	out := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		out[t.ReplicaID] = struct{}{}
	}
	return out
}

// stringSetEqual compares two string sets.
func stringSetEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// formatIDSet renders a string set in deterministic brace form for
// diff logs: {id1,id2} with IDs sorted ascending. Empty → {}.
func formatIDSet(s map[string]struct{}) string {
	if len(s) == 0 {
		return "{}"
	}
	ids := make([]string, 0, len(s))
	for id := range s {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return "{" + strings.Join(ids, ",") + "}"
}

// OnLocalWrite fans out one acked local write to every tracked peer.
//
// Contract: OnLocalWrite serializes fan-out in LSN order for a given
// volume. Caller order is NOT trusted as the correctness mechanism —
// the volume-level mutex v.mu is held across the entire fan-out loop
// (architect Condition A). A future refactor that releases v.mu
// before issuing per-peer Ship calls silently reintroduces the
// BUG-001-class out-of-order-ship hazard that V2 BlockVol.shipMu was
// designed to prevent.
//
// Best-effort semantics: per-peer ship errors are logged and the
// offending peer is marked Degraded (by ReplicaPeer.ShipEntry's own
// error-handling path, T4a-3 forward-carry CARRY-1). A peer error
// does NOT fail this OnLocalWrite call — the remaining peers still
// receive the entry. Durability closure (sync_all / sync_quorum)
// arrives at T4b.
//
// Accepted T4a trade-off (mini-plan change log): this is a
// correctness-first serialized design. Throughput may be reduced or
// peer slowness may amplify into writer latency when a peer's dial /
// write is slow. Revisit (e.g., Option Z async queue) only after
// INV-REPL-LSN-ORDER-FANOUT-001 is proven.
//
// Called by: Backend.Write wrapper (future T4a-5+ wire) immediately
// after LogicalStorage.Write returns with the assigned LSN.
// Owns: the per-write mutex hold across fan-out (Condition A lock
// scope); per-peer error aggregation and logging.
// Borrows: w.Data slice — caller retains; fan-out must not mutate.
func (v *ReplicationVolume) OnLocalWrite(ctx context.Context, w LocalWrite) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return fmt.Errorf("replication: OnLocalWrite: volume %s closed", v.volumeID)
	}
	// G5-5 instrumentation: log entry with peer count so we can
	// disambiguate "OnLocalWrite never called" vs "called but
	// peers map is empty" vs "called and fans out".
	log.Printf("replication: OnLocalWrite volume=%s lba=%d lsn=%d peers=%d",
		v.volumeID, w.LBA, w.LSN, len(v.peers))

	// Fan out under the mutex — LSN-order invariant (Condition A).
	// Lineage is informational; each peer uses its own registered
	// lineage for authority framing (T4a-3 peer owns its session).
	informational := transport.RecoveryLineage{}
	for _, peer := range v.peers {
		if err := peer.ShipEntry(ctx, informational, w.LBA, w.LSN, w.Data); err != nil {
			log.Printf("replication: volume %s peer %s ship failed lsn=%d: %v",
				v.volumeID, peer.Target().ReplicaID, w.LSN, err)
			// Best-effort: continue iterating peers. Peer is already
			// marked Degraded inside ShipEntry.
		}
	}
	return nil
}

// Stop is the T4d-4 canonical lifecycle entry point. Tears down all
// peers (their executor sessions are invalidated), serializes against
// concurrent OnLocalWrite/Sync via the volume mutex, and is
// idempotent. Does NOT close the borrowed store
// (`INV-REPL-LIFECYCLE-HANDLE-BORROWED-001` per BUG-005 discipline).
//
// Stop and Close are equivalent in T4d-4 (Stop delegates to Close);
// the rename clarifies semantic intent — "Stop the volume's lifecycle
// activity" reads more clearly than "Close the volume struct."
// Future expansions (drain pending I/O, stop background goroutines)
// land here under the Stop name.
//
// Pinned by: TestReplicationVolume_Stop_Idempotent,
// TestReplicationVolume_Stop_DoesNotCloseBorrowedStore.
//
// Called by: Provider teardown.
// Owns: peer-set teardown via peer.Close().
// Borrows: nothing (store is BORROWED — never closed).
func (v *ReplicationVolume) Stop() error {
	return v.Close()
}

// Close releases all peers' registered sessions. Idempotent. Does
// NOT close the borrowed store (BUG-005 / INV-REPL-LIFECYCLE-HANDLE-
// BORROWED-001).
//
// T4d-4 (round-46) renaming: Stop() is the canonical entry point;
// Close() retained for backward compatibility with existing callers.
// Both do the same thing.
//
// Called by: Provider teardown when the volume shuts down.
// Owns: close flag; invalidation of each peer's executor session
// (via peer.Close()).
// Borrows: nothing.
func (v *ReplicationVolume) Close() error {
	// G5-5C ordering (architect 2026-04-27 guidance #2): stop the
	// probe loop FIRST, before acquiring v.mu and tearing down
	// peers. This ensures any in-flight probe callback completes /
	// is cancelled before peers are closed; without this, a probeFn
	// blocked on transport could observe a peer.Close() race or
	// deadlock against UpdateReplicaSet's own peer teardown path.
	//
	// Snapshot the loop pointer under v.mu, then Stop with the lock
	// released — Stop waits for the loop's goroutine, which itself
	// calls peersFn that needs v.mu.
	v.mu.Lock()
	loop := v.probeLoop
	v.mu.Unlock()
	if loop != nil {
		loop.Stop()
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	for id, peer := range v.peers {
		_ = peer.Close()
		delete(v.peers, id)
	}
	return nil
}

// Observe satisfies the durable.WriteObserver seam by wrapping
// the caller's params into a LocalWrite and delegating to
// OnLocalWrite. This lets StorageBackend call ReplicationVolume
// without importing LocalWrite, and without ReplicationVolume
// importing core/frontend/durable (one-way data-plane coupling:
// frontend → replication, control flow only).
//
// Called by: core/frontend/durable.StorageBackend.writeBytes
// after a successful LogicalStorage.Write.
// Owns: same serialization and fan-out semantics as OnLocalWrite.
// Borrows: data slice; see OnLocalWrite for the full contract.
func (v *ReplicationVolume) Observe(ctx context.Context, lba uint32, lsn uint64, data []byte) error {
	return v.OnLocalWrite(ctx, LocalWrite{LBA: lba, Data: data, LSN: lsn})
}

// PeerCount returns the current number of tracked peers. Test helper
// and diagnostic accessor.
func (v *ReplicationVolume) PeerCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.peers)
}

// ConfigureProbeLoop installs the per-volume degraded-peer probe loop
// (G5-5C). Idempotent? NO — calling Configure twice is rejected to
// prevent silent replacement of an active loop. Configure once at
// volume composition time; Start when primary role is admitted; Stop
// is implicit in volume Close().
//
// The probeFn is host-injected: in production it dials executor.Probe
// and forwards the ProbeResult to the per-(volume, replica) adapter
// via OnProbeResult so the engine drives Decision (catch-up / rebuild
// / none). Tests inject a stub that records the dispatch.
//
// Cooldown gating is wired automatically using DefaultProbeCooldownFn
// + DefaultProbeResultFn over each peer's ProbeIfDegraded /
// OnProbeAttempt (G5-5C #2). Newly-added peers (UpdateReplicaSet)
// receive the cooldown config via SetProbeCooldownConfig.
//
// Pinned by:
//   - INV-G5-5C-PRIMARY-RECOVERY-AUTHORITY-BOUNDED (peersFn snapshots
//     v.peers under v.mu; never enumerates network-discoverable addrs)
//
// Called by: host composition root after constructing
// ReplicationVolume and choosing a probeFn.
// Owns: probeLoop field; probeCfg copy.
// Borrows: probeFn — caller retains.
func (v *ReplicationVolume) ConfigureProbeLoop(cfg ProbeLoopConfig, probeFn ProbeFn, now func() time.Time) error {
	if probeFn == nil {
		return fmt.Errorf("replication: ConfigureProbeLoop: probeFn is nil")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return fmt.Errorf("replication: ConfigureProbeLoop: volume %s closed", v.volumeID)
	}
	if v.probeLoop != nil {
		return fmt.Errorf("replication: ConfigureProbeLoop: volume %s already configured (Configure-once contract)", v.volumeID)
	}

	// PeerSourceFn snapshots v.peers under v.mu. Lock ordering
	// discipline (architect 2026-04-27 guidance #2): v.mu always
	// acquired BEFORE peer.mu, never the reverse. The probe loop's
	// tick takes v.mu in peersFn, releases it, then takes peer.mu in
	// ProbeIfDegraded — no nested locking, no inversion.
	peersFn := func() []*ReplicaPeer {
		v.mu.Lock()
		defer v.mu.Unlock()
		if v.closed {
			return nil
		}
		out := make([]*ReplicaPeer, 0, len(v.peers))
		for _, p := range v.peers {
			out = append(out, p)
		}
		return out
	}

	cooldownFn := DefaultProbeCooldownFn(now)
	resultFn := DefaultProbeResultFn(now)

	loop, err := NewProbeLoop(cfg, peersFn, probeFn, cooldownFn, resultFn)
	if err != nil {
		return fmt.Errorf("replication: ConfigureProbeLoop: %w", err)
	}

	v.probeLoop = loop
	v.probeCfg = cfg
	v.probeCfgSet = true

	// Push cooldown config onto already-existing peers. Future peers
	// added via UpdateReplicaSet pick up the config in that path.
	for _, peer := range v.peers {
		peer.SetProbeCooldownConfig(PeerProbeCooldown{
			Base: cfg.CooldownBase,
			Cap:  cfg.CooldownCap,
		})
	}
	return nil
}

// StartProbeLoop starts the configured probe loop. Returns an error
// if ConfigureProbeLoop was not called, or if the volume is closed.
// Idempotent — second and later calls are no-ops (delegated to
// ProbeLoop.Start which uses sync.Once).
//
// Architect 2026-04-27 guidance #1: only start after primary role is
// admitted and cooldown config is in place. The loop will simply
// observe an empty peer set if started early; no panic. But starting
// before peers exist is a wasted goroutine wakeup, so production
// callers SHOULD defer Start until at least one assignment fact has
// been applied.
//
// Called by: host composition root once primary admit is complete.
// Owns: nothing additional (delegates to ProbeLoop.Start).
func (v *ReplicationVolume) StartProbeLoop() error {
	v.mu.Lock()
	loop := v.probeLoop
	closed := v.closed
	v.mu.Unlock()
	if closed {
		return fmt.Errorf("replication: StartProbeLoop: volume %s closed", v.volumeID)
	}
	if loop == nil {
		return fmt.Errorf("replication: StartProbeLoop: volume %s probe loop not configured", v.volumeID)
	}
	return loop.Start()
}

// ProbeLoopForTest exposes the underlying loop pointer for in-package
// test introspection (lifecycle assertions). Not part of the public
// surface; renamed if exported elsewhere is needed.
func (v *ReplicationVolume) probeLoopForTest() *ProbeLoop {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.probeLoop
}
