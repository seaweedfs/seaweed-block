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
// G5-5C Batch #7 rationale (architect ruling 2026-04-27): without
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

	mu       sync.Mutex
	adapters map[string]*adapter.VolumeReplicaAdapter // keyed by ReplicaID
	closed   bool
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
	}
}

// OnPeerAdded constructs a per-peer adapter wired to a
// PeerCommandExecutor over the peer's transport.BlockExecutor, and
// primes the engine with the peer's identity via OnAssignment.
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
	adpt.OnAssignment(adapter.AssignmentInfo{
		VolumeID:        r.volumeID,
		ReplicaID:       target.ReplicaID,
		Epoch:           target.Epoch,
		EndpointVersion: target.EndpointVersion,
		DataAddr:        target.DataAddr,
		CtrlAddr:        target.ControlAddr,
	})

	r.adapters[target.ReplicaID] = adpt
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
	log.Printf("PeerAdapterRegistry: unregistered peer=%s", replicaID)
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
}
