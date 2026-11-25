package volume

import (
	"log"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/replication"
)

// PeerAdapterRegistry owns one *adapter.VolumeReplicaAdapter per
// admitted peer of a given volume. The host's own adapter (h.Adapter())
// tracks the primary's OWN slot; the registry tracks each PEER. They
// are independent ReplicaState instances.
//
// G5-5C Batch #7 rationale: without
// per-peer adapter state, engine.checkReplicaID drops probe results
// for peer X as wrong_replica because the host's single adapter
// tracks Identity.ReplicaID = primary's-own-slot. This registry fixes
// that by giving each peer its own engine instance.
//
// Lifecycle: registry is borrowed by the probe loop's router probeFn
// and by ReplicationVolume's peer-lifecycle hook. Constructor stores
// volumeID + selfReplicaID for assignment priming. Add / Remove are
// idempotent. Close tears down all peer adapters.
//
// Pinned by:
//   - INV-G5-5C-PER-PEER-ADAPTER-PER-PEER-ENGINE
type PeerAdapterRegistry struct {
	volumeID       string
	selfReplicaID  string // sanity guard: never register the host's own slot

	// frontier is this primary's local durable frontier (nil = not known).
	// A peer added here has received nothing from this primary at its
	// lineage, so the frontier at the add is the start position above
	// which anything the peer holds is a divergent old-epoch tail.
	frontier func() (uint64, error)

	mu       sync.Mutex
	adapters map[string]*adapter.VolumeReplicaAdapter // keyed by ReplicaID
	// pending: the identity each adapter is primed with on the probe
	// loop's first dispatch for its peer (PrimePending), not at the add.
	pending map[string]adapter.AssignmentInfo
	// peers: the exact peer each adapter was added for. A dispatch acts
	// only for the peer it was admitted for (AdapterForPeer, PrimePendingFor):
	// a lineage bump replaces the peer under the same ReplicaID.
	peers  map[string]*replication.ReplicaPeer
	closed bool
}

// SetFrontier installs the local durable frontier read at each peer add
// (the store's Sync). Call before peers are added.
func (r *PeerAdapterRegistry) SetFrontier(fn func() (uint64, error)) {
	if r != nil {
		r.frontier = fn
	}
}

// NewPeerAdapterRegistry constructs an empty registry for the given
// volume. selfReplicaID is the host's own slot — used as a sanity
// guard so we never accidentally register the host's own slot
// (which already has h.Adapter()).
func NewPeerAdapterRegistry(volumeID, selfReplicaID string) *PeerAdapterRegistry {
	return &PeerAdapterRegistry{
		volumeID:      volumeID,
		selfReplicaID: selfReplicaID,
		adapters:      make(map[string]*adapter.VolumeReplicaAdapter),
		pending:       make(map[string]adapter.AssignmentInfo),
		peers:         make(map[string]*replication.ReplicaPeer),
	}
}

// OnPeerAdded constructs a per-peer adapter wired to a
// PeerCommandExecutor over the peer's transport.BlockExecutor, and
// records the peer's identity to prime it with later (PrimePending).
//
// It does not prime here: priming emits the engine's probe and fence,
// which run on the replica with sessions of their own, while a new peer
// is healthy and owned by the volume's live session (its seed barrier and
// live ship). The replica keeps the newest session, so an adapter acting
// on a healthy peer makes the replica refuse the volume's own barrier.
// The adapter is primed only once the probe loop dispatches for the
// peer, which it does only for a degraded peer (see PrimePending).
//
// Idempotent: repeated calls for the same peer (same ReplicaID + same
// lineage) are a no-op. Lineage-bump recreate (the PeerRemoved →
// PeerAdded sequence in UpdateReplicaSet's tear-down + recreate path)
// gets a fresh adapter with fresh engine state.
//
// Sanity-guarded: refuses to register the host's own ReplicaID
// (would create a duplicate engine state machine for the local slot).
func (r *PeerAdapterRegistry) OnPeerAdded(peer *replication.ReplicaPeer) {
	if r == nil || peer == nil {
		return
	}
	target := peer.Target()
	if target.ReplicaID == "" {
		return
	}
	if target.ReplicaID == r.selfReplicaID {
		log.Printf("PeerAdapterRegistry: refusing to register self ReplicaID=%s", target.ReplicaID)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if _, exists := r.adapters[target.ReplicaID]; exists {
		// Already registered. Idempotent skip; lineage-bump path goes
		// through OnPeerRemoved first and we land here on second add.
		return
	}

	exec := NewPeerCommandExecutor(peer)
	adpt := adapter.NewVolumeReplicaAdapter(exec)

	// Prime with this peer's identity so the engine knows which slot
	// it's tracking. Without this, the engine's checkReplicaID would
	// silently accept the first probe as initial admit (line 122 in
	// apply.go: "Engine not yet initialized — accept"), but having
	// AssignmentObserved fire first matches the production master-
	// driven path semantics: identity comes BEFORE probe.
	var start uint64
	if r.frontier != nil {
		if lsn, err := r.frontier(); err == nil {
			start = lsn
		} else {
			log.Printf("PeerAdapterRegistry: peer=%s frontier unknown (%v): no divergent-tail check", target.ReplicaID, err)
		}
	}
	r.pending[target.ReplicaID] = adapter.AssignmentInfo{
		VolumeID:        r.volumeID,
		ReplicaID:       target.ReplicaID,
		Epoch:           target.Epoch,
		EndpointVersion: target.EndpointVersion,
		DataAddr:        target.DataAddr,
		CtrlAddr:        target.ControlAddr,
		PrimaryStartLSN: start,
	}

	r.adapters[target.ReplicaID] = adpt
	r.peers[target.ReplicaID] = peer
	log.Printf("PeerAdapterRegistry: registered peer=%s epoch=%d EV=%d", target.ReplicaID, target.Epoch, target.EndpointVersion)
}

// OnPeerRemoved drops the per-peer adapter for replicaID. Idempotent —
// missing entry is a no-op. Called from ReplicationVolume's peer-
// removal path AND from lineage-bump recreate (which is REMOVE +
// ADD: this method runs first, then OnPeerAdded for the new lineage).
func (r *PeerAdapterRegistry) OnPeerRemoved(replicaID string) {
	if r == nil || replicaID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.adapters[replicaID]; !exists {
		return
	}
	delete(r.adapters, replicaID)
	delete(r.pending, replicaID)
	delete(r.peers, replicaID)
	log.Printf("PeerAdapterRegistry: unregistered peer=%s", replicaID)
}

// PrimePending primes replicaID's adapter with the identity recorded at
// its add (OnAssignment: the engine's own probe, then fence or recovery),
// once: it reports whether this call primed it. The probe loop calls it
// on its first dispatch for the peer.
//
// Lock graph: the pending entry is taken under r.mu, and OnAssignment runs
// after r.mu is released (its commands run the peer's executor). The probe
// loop dispatches only for a peer ProbeIfDegraded admitted under peer.mu
// (state Degraded, no probe in flight). A peer is created Healthy inside
// the volume's UpdateReplicaSet (under the volume's mu, which also runs its
// seed barrier) and turns Degraded only after its own live session failed
// and was invalidated; a degraded peer is not shipped to. So the adapter's
// first session starts only after the volume's session on that peer has
// ended: there is no window in which both act on the replica. A dispatch
// acts only for the exact peer the loop admitted (PrimePendingFor,
// AdapterForPeer compare the *ReplicaPeer under r.mu): a dispatch admitted
// for an old degraded peer and resumed after a lineage bump replaced it
// under the same ReplicaID finds nothing, so it never primes or probes the
// healthy successor.
func (r *PeerAdapterRegistry) PrimePending(replicaID string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	return r.primeLocked(replicaID)
}

// PrimePendingFor is PrimePending for the exact peer a probe dispatch was
// admitted for: if that peer has since been replaced under its ReplicaID
// (a lineage bump), it primes nothing, so a delayed dispatch for an old
// degraded peer never acts on its healthy successor.
func (r *PeerAdapterRegistry) PrimePendingFor(peer *replication.ReplicaPeer) bool {
	if r == nil || peer == nil {
		return false
	}
	id := peer.Target().ReplicaID
	r.mu.Lock()
	if r.peers[id] != peer {
		r.mu.Unlock()
		return false
	}
	return r.primeLocked(id)
}

// AdapterForPeer is AdapterFor for the exact peer a probe dispatch was
// admitted for (nil once that peer was replaced).
func (r *PeerAdapterRegistry) AdapterForPeer(peer *replication.ReplicaPeer) *adapter.VolumeReplicaAdapter {
	if r == nil || peer == nil {
		return nil
	}
	id := peer.Target().ReplicaID
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.peers[id] != peer {
		return nil
	}
	return r.adapters[id]
}

// primeLocked takes replicaID's pending identity (r.mu held; released
// here) and primes its adapter outside the lock.
func (r *PeerAdapterRegistry) primeLocked(replicaID string) bool {
	info, ok := r.pending[replicaID]
	delete(r.pending, replicaID)
	adpt := r.adapters[replicaID]
	r.mu.Unlock()
	if !ok || adpt == nil {
		return false
	}
	adpt.OnAssignment(info)
	return true
}

// AdapterFor returns the per-peer adapter for replicaID, or nil if
// no adapter is registered. Safe to call from any goroutine.
//
// Used by: the probe loop's router probeFn (ProductionProbeFn) to
// pick the right adapter when forwarding OnProbeResult.
func (r *PeerAdapterRegistry) AdapterFor(replicaID string) *adapter.VolumeReplicaAdapter {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.adapters[replicaID]
}

// Count returns the number of registered peer adapters. Diagnostic
// accessor for tests.
func (r *PeerAdapterRegistry) Count() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.adapters)
}

// Close drops all per-peer adapters and prevents further Add calls.
// Idempotent.
func (r *PeerAdapterRegistry) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.adapters = nil
	r.pending = nil
	r.peers = nil
}
