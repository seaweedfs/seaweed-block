package replication

import (
	"context"
	"errors"
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

type RuntimeRecoveryRequest struct {
	ReplicaID       string
	TargetDataAddr  string
	SessionID       uint64
	Epoch           uint64
	EndpointVersion uint64
	FromLSN         uint64
	FrontierHintLSN uint64
	BasePinLSN      uint64
}

// VolumeStats is a read-only contention and queue snapshot for write-path
// gates. The counters do not affect acknowledgement or ordering semantics.
type VolumeStats struct {
	WriteOps           uint64
	WriteLockWaitNanos uint64
	WriteFanoutNanos   uint64
	WriteAckWaitNanos  uint64
	PeerQueueMaxDepth  uint64
	PeerQueueSaturated uint64
	SyncOps            uint64
	SyncOrderWaitNanos uint64
	SyncLockWaitNanos  uint64
	SyncDurationNanos  uint64
}

type orderedLocalWrite struct {
	write  LocalWrite
	result chan error
}

type localSyncResult struct {
	err error
}

type RuntimeRecoveryStatus struct {
	State       string
	ReplicaID   string
	SessionID   uint64
	AchievedLSN uint64
	FailureKind string
	FailReason  string
}

// ReplicationVolume is the per-volume fan-out owner. It:
//   - tracks the authoritative replica set (peers) from master
//     assignments;
//   - resequences concurrent local callbacks by global LSN;
//   - dispatches writes and barriers through one bounded FIFO per peer
//     lineage, preserving wire order without holding a whole-volume network
//     lock.
//
// Lifecycle: borrowed LogicalStorage — Provider owns the engine;
// ReplicationVolume must NEVER call store.Close() (BUG-005 discipline).
type ReplicationVolume struct {
	volumeID    string
	store       storage.LogicalStorage  // borrowed, NEVER closed by us
	newExec     executorFactory         // test seam; default dials real TCP
	newDualExec dualLaneExecutorFactory // optional dual-lane override; nil = use newExec

	mu                    sync.Mutex // protects membership, lineage, and lifecycle snapshots
	peers                 map[string]*ReplicaPeer
	peerQueues            map[string]*peerWorkQueue
	peerQueueDepth        int
	closed                bool
	lastAppliedGeneration uint64         // monotonic guard; 0 means "no generation applied yet"
	durabilityMode        DurabilityMode // set via SetDurabilityMode; default is BestEffort

	// replayedGens counts UpdateReplicaSet calls dropped as stale
	// (generation > 0 && generation <= lastAppliedGeneration).
	replayedGens atomic.Uint64

	// Write/sync counters expose snapshot, dispatch, queue, and ACK costs.
	writeOps      atomic.Uint64
	writeWait     atomic.Uint64
	writeFanout   atomic.Uint64
	writeAckWait  atomic.Uint64
	queueMaxDepth atomic.Uint64
	queueFull     atomic.Uint64
	syncOps       atomic.Uint64
	syncOrderWait atomic.Uint64
	syncWait      atomic.Uint64
	syncDuration  atomic.Uint64

	orderMu     sync.Mutex
	nextShipLSN uint64
	inflightLSN uint64
	pending     map[uint64]*orderedLocalWrite
	draining    bool
	orderClosed bool
	progress    chan struct{}
	lifecycle   context.Context
	cancel      context.CancelFunc

	// Probe loop integration. Set once via ConfigureProbeLoop;
	// started via StartProbeLoop after primary admit; stopped FIRST
	// during Close (before peer teardown) so an in-flight probe
	// callback never lands on a closed volume / closed peer set.
	// Read+written under v.mu.
	probeLoop   *ProbeLoop
	probeCfg    ProbeLoopConfig // remembered for SetProbeCooldownConfig push-down on UpdateReplicaSet
	probeCfgSet bool            // true after ConfigureProbeLoop succeeds

	// Peer-lifecycle hook. Optional pair of callbacks invoked from
	// UpdateReplicaSet on peer add / remove (including the
	// lineage-bump teardown + recreate path). Used by the host to
	// maintain a per-peer adapter registry in lockstep with the
	// authoritative peer set.
	onPeerAdded   func(*ReplicaPeer)
	onPeerRemoved func(string) // by ReplicaID
}

const promotionSeedBarrierTimeout = 5 * time.Second
const defaultPeerWorkQueueDepth = 1024

// executorFactory lets tests inject a BlockExecutor constructor that
// binds to a specific replica address. Production uses the real
// transport.NewBlockExecutor.
type executorFactory func(store storage.LogicalStorage, replicaAddr string) *transport.BlockExecutor

// dualLaneExecutorFactory is an optional alternative factory used
// when the daemon is started in --recovery-mode=dual-lane. When non-nil,
// `UpdateReplicaSet` calls it instead of `executorFactory` for each new
// peer, giving the factory both the peer's data address AND its
// replica ID so it can construct a BlockExecutor configured for the
// dual-lane recovery package (per docs/recovery-wiring-plan.md §2).
//
// Default (legacy mode): nil; falls back to executorFactory.
type dualLaneExecutorFactory func(store storage.LogicalStorage, replicaAddr, replicaID string) *transport.BlockExecutor

// NewReplicationVolume constructs a per-volume fan-out coordinator.
// The returned volume borrows store — it is a read-only handle from
// the volume's perspective and is never closed here (Provider owns
// the engine).
//
// Called by: DurableProvider / Host composition root at volume
// lifecycle start, after LogicalStorage is recovered and ready.
// Owns: the peers map; all *ReplicaPeer lifecycles (Close on remove);
// the LSN resequencer and lineage-scoped work queues.
// Borrows: store (LogicalStorage). Provider owns engine lifecycle;
// ReplicationVolume MUST NOT call store.Close() (BUG-005).
func NewReplicationVolume(volumeID string, store storage.LogicalStorage) *ReplicationVolume {
	lifecycle, cancel := context.WithCancel(context.Background())
	return &ReplicationVolume{
		volumeID:       volumeID,
		store:          store,
		newExec:        transport.NewBlockExecutor,
		peers:          make(map[string]*ReplicaPeer),
		peerQueues:     make(map[string]*peerWorkQueue),
		peerQueueDepth: defaultPeerWorkQueueDepth,
		durabilityMode: DurabilityBestEffort, // zero value; explicit for clarity
		nextShipLSN:    store.NextLSN(),
		pending:        make(map[uint64]*orderedLocalWrite),
		progress:       make(chan struct{}),
		lifecycle:      lifecycle,
		cancel:         cancel,
	}
}

// SetDualLaneExecutorFactory injects the dual-lane BlockExecutor
// constructor so subsequent `UpdateReplicaSet` calls build per-peer
// executors via the recovery package's PrimaryBridge instead of the
// legacy single-lane path. Idempotent; pass nil to revert to legacy.
//
// Caller (cmd/blockvolume) is responsible for:
//   - Building a per-volume `recovery.PeerShipCoordinator` ONCE and
//     capturing it in the closure (so MinPinAcrossActiveSessions
//     reports the true minimum across all peers).
//   - Translating peer.replicaAddr (data port) → dual-lane port via
//     deployment convention.
//   - Starting the local dual-lane listener (see
//     `recovery.AcceptDualLaneLoop`).
//
// MUST be called BEFORE the first `UpdateReplicaSet` so the very
// first peer is built with the right factory; later switches DO NOT
// retroactively re-construct existing peers.
//
// Per docs/recovery-wiring-plan.md §3 (lifecycle alignment) — mode is
// exclusive: tests don't mix legacy and dual-lane on the same volume.
func (v *ReplicationVolume) SetDualLaneExecutorFactory(f func(store storage.LogicalStorage, replicaAddr, replicaID string) *transport.BlockExecutor) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.newDualExec = f
}

// SetDurabilityMode configures the per-volume write and Sync
// acknowledgement semantic. Safe to call at any time; each operation uses the
// mode captured with its membership snapshot.
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

// Sync queues one barrier behind every write through targetLSN, runs the local
// fsync in parallel, and applies the configured acknowledgement policy. The
// membership mutex is held only long enough to snapshot lineage-scoped queues.
// A sync-quorum call may return after the local fsync plus enough peer barriers
// succeed; queued work for a slow non-quorum peer continues in its own FIFO.
func (v *ReplicationVolume) Sync(ctx context.Context, targetLSN uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	orderStart := time.Now()
	if err := v.waitUntilShipped(ctx, targetLSN); err != nil {
		return err
	}
	v.syncOrderWait.Add(replicationDurationNanos(time.Since(orderStart)))
	lockStart := time.Now()
	v.mu.Lock()
	v.syncOps.Add(1)
	v.syncWait.Add(replicationDurationNanos(time.Since(lockStart)))
	if v.closed {
		v.mu.Unlock()
		return fmt.Errorf("replication: Sync: volume %s closed", v.volumeID)
	}
	mode := v.durabilityMode
	localResult := make(chan localSyncResult, 1)
	go func() {
		_, err := v.store.Sync()
		localResult <- localSyncResult{err: err}
	}()

	peerCount := len(v.peerQueues)
	results := make(chan peerWorkResult, peerCount)
	for _, q := range v.peerQueues {
		depth, err := q.enqueueBarrier(targetLSN, results)
		v.observeQueueDepth(depth)
		if err != nil {
			v.observeQueueError(err)
			results <- peerWorkResult{peerID: q.peerID, err: err}
			v.replaceTerminalPeerQueueLocked(q.peerID, q)
		}
	}
	v.mu.Unlock()
	defer func() {
		v.syncDuration.Add(replicationDurationNanos(time.Since(start)))
	}()
	return waitForSyncAcks(ctx, mode, targetLSN, peerCount, localResult, results)
}

// UpdateReplicaSet applies the authoritative replica set from a master
// assignment event. Adds new peers, removes deleted ones, and tears
// down + recreates peers whose lineage (Epoch / EndpointVersion) has
// bumped. Lineage-bump tear-down is the current shape; in-place
// lineage update on existing peers is a future refinement when
// recovery sessions thread through.
//
// Generation rule:
//   - generation == 0: unversioned apply. Peer map IS mutated, but
//     lastAppliedGeneration is NOT advanced. Intended for test /
//     fake-master use only; production master MUST emit >= 1.
//   - generation > 0 && generation > lastAppliedGeneration: apply and
//     advance lastAppliedGeneration.
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
// Called by: Host authority-callback path, on every assignment event
// that carries a replica-set delta.
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

	// Generation guard (three-way rule).
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
			v.closePeerQueueLocked(id)
			_ = peer.Close()
			delete(v.peers, id)
			// Notify peer-lifecycle hook AFTER peer.Close to mirror
			// the existing teardown ordering. Hook is called under
			// v.mu (lock-order: v.mu → host registry's mu).
			if v.onPeerRemoved != nil {
				v.onPeerRemoved(id)
			}
		}
	}

	// Add new peers + recreate on lineage bump.
	addedPeers := make([]*ReplicaPeer, 0, len(want))
	for id, t := range want {
		if existing, ok := v.peers[id]; ok {
			cur := existing.Target()
			if cur.Epoch == t.Epoch && cur.EndpointVersion == t.EndpointVersion && cur.DataAddr == t.DataAddr {
				continue
			}
			// Lineage or address bumped → tear down + recreate.
			v.closePeerQueueLocked(id)
			_ = existing.Close()
			delete(v.peers, id)
			// Lineage-bump teardown also notifies the hook so the
			// per-peer adapter is dropped before the fresh adapter is
			// added below (new peer instance, fresh engine state).
			if v.onPeerRemoved != nil {
				v.onPeerRemoved(id)
			}
		}
		var executor *transport.BlockExecutor
		if v.newDualExec != nil {
			executor = v.newDualExec(v.store, t.DataAddr, id)
		} else {
			executor = v.newExec(v.store, t.DataAddr)
		}
		peer, err := NewReplicaPeer(t, executor)
		if err != nil {
			return fmt.Errorf("replication: UpdateReplicaSet: add peer %s: %w", id, err)
		}
		peer.setOnHealthy(func() {
			v.resetTerminalPeerQueue(peer)
		})
		// Push the volume-level probe cooldown config onto the fresh
		// peer. A new peer (whether first add or post-lineage-bump
		// recreate) starts with cooldown reset to defaults; the prior
		// peer's cooldown state cannot leak across the lineage
		// boundary because that state lived on the now-closed
		// *ReplicaPeer.
		if v.probeCfgSet {
			peer.SetProbeCooldownConfig(PeerProbeCooldown{
				Base: v.probeCfg.CooldownBase,
				Cap:  v.probeCfg.CooldownCap,
			})
		}
		v.peers[id] = peer
		v.peerQueues[id] = newPeerWorkQueue(peer, v.peerQueueDepth)
		// Notify peer-lifecycle hook AFTER the peer is installed in
		// the map so the host's registry can construct the per-peer
		// adapter and prime it with the peer's identity.
		if v.onPeerAdded != nil {
			v.onPeerAdded(peer)
		}
		addedPeers = append(addedPeers, peer)
	}

	v.seedNewPeerLiveCursorsLocked(addedPeers)

	// Advance the monotonic guard only for real (non-zero) generations.
	if generation > 0 {
		v.lastAppliedGeneration = generation
	}
	return nil
}

func (v *ReplicationVolume) closePeerQueueLocked(replicaID string) {
	if q := v.peerQueues[replicaID]; q != nil {
		q.closeAndWait()
		delete(v.peerQueues, replicaID)
	}
}

func (v *ReplicationVolume) resetTerminalPeerQueue(peer *ReplicaPeer) {
	id := peer.Target().ReplicaID
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.peers[id] != peer {
		return
	}
	v.replaceTerminalPeerQueueLocked(id, v.peerQueues[id])
}

func (v *ReplicationVolume) replaceTerminalPeerQueueLocked(id string, old *peerWorkQueue) *peerWorkQueue {
	if old == nil || v.peerQueues[id] != old || !old.isTerminal() {
		return v.peerQueues[id]
	}
	old.closeAndWait()
	peer := v.peers[id]
	if peer == nil || v.closed {
		delete(v.peerQueues, id)
		return nil
	}
	replacement := newPeerWorkQueue(peer, v.peerQueueDepth)
	v.peerQueues[id] = replacement
	return replacement
}

// seedNewPeerLiveCursorsLocked verifies that newly installed peers have
// durably reached this node's local frontier, then seeds their live
// ship cursor so the first post-promotion write starts at frontier+1.
// This is the promoted-primary handoff proof: peers that cannot answer
// the barrier, or answer below the frontier, are degraded and therefore
// cannot satisfy sync-quorum/sync-all write acknowledgement.
func (v *ReplicationVolume) seedNewPeerLiveCursorsLocked(peers []*ReplicaPeer) {
	if len(peers) == 0 {
		return
	}
	frontier, err := v.store.Sync()
	if err != nil {
		log.Printf("replication: volume %s live cursor seed skipped: local sync failed: %v",
			v.volumeID, err)
		return
	}
	if frontier == 0 {
		return
	}
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), promotionSeedBarrierTimeout)
		ack, err := peer.Barrier(ctx, frontier)
		cancel()
		if err != nil {
			peer.Invalidate(fmt.Sprintf("promotion seed barrier failed: required=%d err=%v",
				frontier, err))
			log.Printf("replication: volume %s live cursor seed failed peer=%s frontier=%d: %v",
				v.volumeID, peer.Target().ReplicaID, frontier, err)
			continue
		}
		if ack.AchievedLSN < frontier {
			peer.Invalidate(fmt.Sprintf("promotion seed barrier below frontier: achieved=%d required=%d",
				ack.AchievedLSN, frontier))
			log.Printf("replication: volume %s live cursor seed rejected peer=%s achieved=%d required=%d",
				v.volumeID, peer.Target().ReplicaID, ack.AchievedLSN, frontier)
			continue
		}
		peer.SeedLiveShipCursor(frontier, "assignment frontier barrier")
	}
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

// OnLocalWrite orders one acked local write by its storage-assigned LSN and
// fans it out to every tracked peer.
//
// Caller arrival order is not trusted: concurrent StorageBackend writes can
// allocate LSN N before LSN N+1 but reach this method in the opposite order.
// The resequencer retains owned buffers until every preceding LSN has been
// dispatched to the lineage-scoped peer queues. Each queue then preserves the
// same order independently.
//
// Best-effort semantics: per-peer ship errors are logged and the
// offending peer is marked Degraded (by ReplicaPeer.ShipEntry's own
// error-handling path). A peer error does NOT fail this OnLocalWrite
// call — the remaining peers still receive the entry. Stricter
// durability closure (sync_all / sync_quorum) is layered above this.
//
// Called by: Backend.Write wrapper immediately after
// LogicalStorage.Write returns with the assigned LSN.
// Owns: an immutable copy of w.Data while queued; per-peer error aggregation
// and logging.
func (v *ReplicationVolume) OnLocalWrite(ctx context.Context, w LocalWrite) error {
	req, err := v.enqueueLocalWrite(w)
	if err != nil {
		return err
	}

	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (v *ReplicationVolume) enqueueLocalWrite(w LocalWrite) (*orderedLocalWrite, error) {
	if w.LSN == 0 {
		return nil, errors.New("replication: OnLocalWrite: LSN must be nonzero")
	}
	owned := append([]byte(nil), w.Data...)
	req := &orderedLocalWrite{
		write:  LocalWrite{LBA: w.LBA, Data: owned, LSN: w.LSN},
		result: make(chan error, 1),
	}

	v.orderMu.Lock()
	if v.orderClosed {
		v.orderMu.Unlock()
		return nil, fmt.Errorf("replication: OnLocalWrite: volume %s closed", v.volumeID)
	}
	if w.LSN < v.nextShipLSN {
		next := v.nextShipLSN
		v.orderMu.Unlock()
		return nil, fmt.Errorf("replication: OnLocalWrite: stale LSN %d below next ship LSN %d", w.LSN, next)
	}
	if _, exists := v.pending[w.LSN]; exists {
		v.orderMu.Unlock()
		return nil, fmt.Errorf("replication: OnLocalWrite: duplicate pending LSN %d", w.LSN)
	}
	if w.LSN == v.inflightLSN {
		v.orderMu.Unlock()
		return nil, fmt.Errorf("replication: OnLocalWrite: duplicate in-flight LSN %d", w.LSN)
	}
	v.pending[w.LSN] = req
	leader := !v.draining
	if leader {
		v.draining = true
	}
	v.orderMu.Unlock()

	if leader {
		go v.drainOrderedWrites()
	}
	return req, nil
}

func (v *ReplicationVolume) drainOrderedWrites() {
	for {
		v.orderMu.Lock()
		if v.orderClosed {
			v.draining = false
			v.orderMu.Unlock()
			return
		}
		req := v.pending[v.nextShipLSN]
		if req == nil {
			v.draining = false
			v.orderMu.Unlock()
			return
		}
		delete(v.pending, v.nextShipLSN)
		v.inflightLSN = req.write.LSN
		v.orderMu.Unlock()

		acks := v.dispatchLocalWrite(req.write)

		v.orderMu.Lock()
		v.inflightLSN = 0
		v.nextShipLSN++
		if !v.orderClosed {
			close(v.progress)
			v.progress = make(chan struct{})
		}
		v.orderMu.Unlock()

		go func(req *orderedLocalWrite, acks writeAckSet) {
			req.result <- v.waitForWriteAcks(v.lifecycle, acks)
		}(req, acks)
	}
}

func (v *ReplicationVolume) waitUntilShipped(ctx context.Context, targetLSN uint64) error {
	for {
		v.orderMu.Lock()
		if targetLSN < v.nextShipLSN {
			v.orderMu.Unlock()
			return nil
		}
		if v.orderClosed {
			v.orderMu.Unlock()
			return fmt.Errorf("replication: Sync: volume %s closed", v.volumeID)
		}
		progress := v.progress
		v.orderMu.Unlock()

		select {
		case <-progress:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type writeAckSet struct {
	mode      DurabilityMode
	rf        int
	peerCount int
	results   <-chan peerWorkResult
	directErr error
}

func (v *ReplicationVolume) dispatchLocalWrite(w LocalWrite) writeAckSet {
	start := time.Now()
	lockStart := time.Now()
	v.mu.Lock()
	v.writeOps.Add(1)
	v.writeWait.Add(replicationDurationNanos(time.Since(lockStart)))
	if v.closed {
		v.mu.Unlock()
		return writeAckSet{directErr: fmt.Errorf("replication: OnLocalWrite: volume %s closed", v.volumeID)}
	}
	mode := v.durabilityMode
	peerCount := len(v.peerQueues)
	results := make(chan peerWorkResult, peerCount)
	for _, q := range v.peerQueues {
		depth, err := q.enqueueWrite(w, results)
		v.observeQueueDepth(depth)
		if err != nil {
			v.observeQueueError(err)
			results <- peerWorkResult{peerID: q.peerID, err: err}
			replacement := v.replaceTerminalPeerQueueLocked(q.peerID, q)
			if replacement != nil && replacement != q {
				shadowDepth, shadowErr := replacement.enqueueWrite(w, nil)
				v.observeQueueDepth(shadowDepth)
				if shadowErr != nil {
					v.observeQueueError(shadowErr)
					log.Printf("replication: retained write enqueue failed peer=%s lsn=%d: %v",
						q.peerID, w.LSN, shadowErr)
				}
			}
		}
	}
	v.mu.Unlock()

	log.Printf("replication: OnLocalWrite volume=%s lba=%d lsn=%d peers=%d",
		v.volumeID, w.LBA, w.LSN, peerCount)

	v.writeFanout.Add(replicationDurationNanos(time.Since(start)))
	return writeAckSet{
		mode:      mode,
		rf:        peerCount + 1,
		peerCount: peerCount,
		results:   results,
	}
}

func (v *ReplicationVolume) waitForWriteAcks(ctx context.Context, acks writeAckSet) error {
	if acks.directErr != nil {
		return acks.directErr
	}
	start := time.Now()
	defer func() {
		v.writeAckWait.Add(replicationDurationNanos(time.Since(start)))
	}()
	return waitForPeerAcks(ctx, acks.mode, acks.rf, acks.peerCount, acks.results, "write")
}

// Stop is the canonical lifecycle entry point. Tears down all peers
// (their executor sessions are invalidated), closes ordered work queues,
// and is idempotent. Does NOT close the borrowed store
// (`INV-REPL-LIFECYCLE-HANDLE-BORROWED-001`).
//
// Stop and Close are equivalent (Stop delegates to Close); the
// alias clarifies semantic intent. Future expansions (drain pending
// I/O, stop background goroutines) land here under the Stop name.
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
// NOT close the borrowed store (INV-REPL-LIFECYCLE-HANDLE-BORROWED-001).
//
// Stop() is the canonical entry point; Close() is retained for
// backward compatibility with existing callers. Both do the same thing.
//
// Called by: Provider teardown when the volume shuts down.
// Owns: close flag; invalidation of each peer's executor session
// (via peer.Close()).
// Borrows: nothing.
func (v *ReplicationVolume) Close() error {
	v.cancel()
	v.orderMu.Lock()
	if !v.orderClosed {
		v.orderClosed = true
		close(v.progress)
		for lsn, req := range v.pending {
			req.result <- fmt.Errorf("replication: OnLocalWrite: volume %s closed before LSN %d shipped", v.volumeID, lsn)
			delete(v.pending, lsn)
		}
	}
	v.orderMu.Unlock()

	// Ordering: stop the
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
	for id := range v.peerQueues {
		v.closePeerQueueLocked(id)
	}
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
// Owns: same resequencing and fan-out semantics as OnLocalWrite.
// Borrows: data slice; see OnLocalWrite for the full contract.
func (v *ReplicationVolume) Observe(ctx context.Context, lba uint32, lsn uint64, data []byte) error {
	return v.OnLocalWrite(ctx, LocalWrite{LBA: lba, Data: data, LSN: lsn})
}

// ObserveBatch submits a contiguous storage batch to the LSN resequencer
// before waiting for any individual acknowledgement. This preserves storage
// batching while each peer FIFO retains the wire-order invariant.
func (v *ReplicationVolume) ObserveBatch(ctx context.Context, startLBA uint32, lsns []uint64, blocks [][]byte) error {
	if len(lsns) != len(blocks) {
		return fmt.Errorf("replication: ObserveBatch: %d LSNs for %d blocks", len(lsns), len(blocks))
	}
	if len(lsns) == 0 {
		return nil
	}
	for i, lsn := range lsns {
		if lsn == 0 {
			return fmt.Errorf("replication: ObserveBatch: zero LSN at index %d", i)
		}
		if i > 0 && lsn != lsns[i-1]+1 {
			return fmt.Errorf("replication: ObserveBatch: non-contiguous LSN %d after %d", lsn, lsns[i-1])
		}
	}

	requests := make([]*orderedLocalWrite, 0, len(lsns))
	for i, lsn := range lsns {
		req, err := v.enqueueLocalWrite(LocalWrite{
			LBA:  startLBA + uint32(i),
			LSN:  lsn,
			Data: blocks[i],
		})
		if err != nil {
			return err
		}
		requests = append(requests, req)
	}

	for _, req := range requests {
		select {
		case err := <-req.result:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// PeerCount returns the current number of tracked peers. Test helper
// and diagnostic accessor.
func (v *ReplicationVolume) PeerCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.peers)
}

// Stats returns a stable diagnostic snapshot without exposing mutable state.
func (v *ReplicationVolume) Stats() VolumeStats {
	return VolumeStats{
		WriteOps:           v.writeOps.Load(),
		WriteLockWaitNanos: v.writeWait.Load(),
		WriteFanoutNanos:   v.writeFanout.Load(),
		WriteAckWaitNanos:  v.writeAckWait.Load(),
		PeerQueueMaxDepth:  v.queueMaxDepth.Load(),
		PeerQueueSaturated: v.queueFull.Load(),
		SyncOps:            v.syncOps.Load(),
		SyncOrderWaitNanos: v.syncOrderWait.Load(),
		SyncLockWaitNanos:  v.syncWait.Load(),
		SyncDurationNanos:  v.syncDuration.Load(),
	}
}

func (v *ReplicationVolume) observeQueueDepth(depth int) {
	if depth <= 0 {
		return
	}
	value := uint64(depth)
	for {
		current := v.queueMaxDepth.Load()
		if value <= current || v.queueMaxDepth.CompareAndSwap(current, value) {
			return
		}
	}
}

func (v *ReplicationVolume) observeQueueError(err error) {
	if errors.Is(err, ErrPeerQueueSaturated) {
		v.queueFull.Add(1)
	}
}

func replicationDurationNanos(d time.Duration) uint64 {
	if d <= 0 {
		return 1
	}
	return uint64(d.Nanoseconds())
}

// PeerStatuses returns a stable, sorted diagnostic snapshot of all
// currently tracked peers. This is intentionally a status surface, not
// a control surface: callers cannot mutate the returned peer state.
func (v *ReplicationVolume) PeerStatuses() []ReplicaPeerStatus {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]ReplicaPeerStatus, 0, len(v.peers))
	for _, peer := range v.peers {
		out = append(out, peer.Status())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ReplicaID < out[j].ReplicaID
	})
	return out
}

func (v *ReplicationVolume) StartRuntimeRecovery(ctx context.Context, req RuntimeRecoveryRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if req.ReplicaID == "" {
		return fmt.Errorf("replication: runtime recovery: replicaID is required")
	}
	if req.SessionID == 0 || req.Epoch == 0 || req.EndpointVersion == 0 || req.FrontierHintLSN == 0 {
		return fmt.Errorf("replication: runtime recovery: nonzero sessionID, epoch, endpointVersion, and frontierHintLsn are required")
	}
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return fmt.Errorf("replication: runtime recovery: volume %s closed", v.volumeID)
	}
	peer := v.peers[req.ReplicaID]
	v.mu.Unlock()
	if peer == nil {
		return fmt.Errorf("replication: runtime recovery: peer %s not found", req.ReplicaID)
	}
	return peer.StartRuntimeRecovery(req)
}

func (v *ReplicationVolume) RuntimeRecoveryStatus(ctx context.Context, req RuntimeRecoveryRequest) (RuntimeRecoveryStatus, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeRecoveryStatus{}, err
	}
	if req.ReplicaID == "" {
		return RuntimeRecoveryStatus{}, fmt.Errorf("replication: runtime recovery status: replicaID is required")
	}
	if req.SessionID == 0 || req.Epoch == 0 || req.EndpointVersion == 0 {
		return RuntimeRecoveryStatus{}, fmt.Errorf("replication: runtime recovery status: nonzero sessionID, epoch, and endpointVersion are required")
	}
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return RuntimeRecoveryStatus{}, fmt.Errorf("replication: runtime recovery status: volume %s closed", v.volumeID)
	}
	peer := v.peers[req.ReplicaID]
	v.mu.Unlock()
	if peer == nil {
		return RuntimeRecoveryStatus{}, fmt.Errorf("replication: runtime recovery status: peer %s not found", req.ReplicaID)
	}
	return peer.RuntimeRecoveryStatus(req)
}

func runtimeStatusFromTransport(status transport.RecoverySessionStatus) RuntimeRecoveryStatus {
	return RuntimeRecoveryStatus{
		State:       status.State,
		ReplicaID:   status.ReplicaID,
		SessionID:   status.SessionID,
		AchievedLSN: status.AchievedLSN,
		FailureKind: status.FailureKind,
		FailReason:  status.FailReason,
	}
}

// ConfigureProbeLoop installs the per-volume degraded-peer probe loop.
// NOT idempotent — calling Configure twice is rejected to prevent
// silent replacement of an active loop. Configure once at volume
// composition time; Start when primary role is admitted; Stop is
// implicit in volume Close().
//
// The probeFn is host-injected: in production it dials executor.Probe
// and forwards the ProbeResult to the per-(volume, replica) adapter
// via OnProbeResult so the engine drives Decision (catch-up / rebuild
// / none). Tests inject a stub that records the dispatch.
//
// Cooldown gating is wired automatically using DefaultProbeCooldownFn
// + DefaultProbeResultFn over each peer's ProbeIfDegraded /
// OnProbeAttempt. Newly-added peers (UpdateReplicaSet) receive the
// cooldown config via SetProbeCooldownConfig.
//
// peersFn snapshots v.peers under v.mu; never enumerates network-
// discoverable addresses (the authority boundary stays at the volume).
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

	// PeerSourceFn snapshots v.peers under v.mu. Lock ordering: v.mu
	// always acquired BEFORE peer.mu, never the reverse. The probe
	// loop's tick takes v.mu in peersFn, releases it, then takes
	// peer.mu in ProbeIfDegraded — no nested locking, no inversion.
	//
	// HARD RULE: code on the probe path (peersFn → ProbeIfDegraded →
	// probeFn → OnProbeAttempt) MUST NOT re-enter v.mu while holding
	// peer.mu. Any future "peer callback into volume" that needs
	// v.mu must release peer.mu first. Violating this introduces a
	// peer → v reverse lock order and risks deadlock against
	// UpdateReplicaSet / Sync paths that hold v.mu and call into
	// peers.
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

// ConfigurePeerLifecycleHook registers callbacks invoked from
// UpdateReplicaSet when peers are added (initial admit OR lineage-
// bump recreate) or removed (set diff OR lineage-bump teardown).
//
// onAdded fires AFTER the new *ReplicaPeer is installed in v.peers
// so callbacks can call peer.Target() / peer.Executor() without
// racing the peer set. onRemoved fires AFTER peer.Close() and
// delete-from-map for the SAME reason.
//
// Configure-once contract: a second call returns an error rather
// than silently replacing the prior hook (matches ConfigureProbeLoop
// discipline). Caller must Close() and reconstruct a new
// ReplicationVolume to swap hooks.
//
// Lock-order discipline: hooks are called UNDER v.mu (the same lock
// UpdateReplicaSet holds). Callbacks MUST NOT re-enter
// ReplicationVolume methods that take v.mu (UpdateReplicaSet, Sync,
// Close, ConfigureProbeLoop, StartProbeLoop, etc.) — that would
// self-deadlock. Callbacks may safely take their own internal locks
// (e.g., PeerAdapterRegistry.mu).
//
// Pinned by:
//   - INV-G5-5C-PER-PEER-ADAPTER-PER-PEER-ENGINE
//
// Called by: host composition root after constructing
// ReplicationVolume + PeerAdapterRegistry.
// Owns: onPeerAdded / onPeerRemoved fields; nothing else.
// Borrows: callbacks — caller retains.
func (v *ReplicationVolume) ConfigurePeerLifecycleHook(onAdded func(*ReplicaPeer), onRemoved func(string)) error {
	if onAdded == nil || onRemoved == nil {
		return fmt.Errorf("replication: ConfigurePeerLifecycleHook: callbacks must be non-nil")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return fmt.Errorf("replication: ConfigurePeerLifecycleHook: volume %s closed", v.volumeID)
	}
	if v.onPeerAdded != nil || v.onPeerRemoved != nil {
		return fmt.Errorf("replication: ConfigurePeerLifecycleHook: volume %s already configured (Configure-once contract)", v.volumeID)
	}
	v.onPeerAdded = onAdded
	v.onPeerRemoved = onRemoved
	// Replay current peer set so the host registry can catch up to
	// any peers that were added BEFORE the hook was configured.
	for _, peer := range v.peers {
		onAdded(peer)
	}
	return nil
}

// ProbeLoopForTest exposes the underlying loop pointer for in-package
// test introspection (lifecycle assertions). Not part of the public
// surface; renamed if exported elsewhere is needed.
func (v *ReplicationVolume) probeLoopForTest() *ProbeLoop {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.probeLoop
}
