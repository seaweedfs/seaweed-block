package volume

import (
	"fmt"
	"sync"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// storeWait is the primary fact's replica-store wait, read and written as
// one value: the fact's epoch, its counts, and the line still starting from
// that wait.
type storeWait struct {
	mu               sync.Mutex
	epoch            uint64
	awaited, initial uint32
	// starting: the epoch of a line whose assignment awaited replica stores
	// and whose frontend has not opened on it yet (0: none). It outlives the
	// stores-complete assignment until the frontend opens.
	starting uint64
}

// noteReplicaStores keeps the primary fact's count of a new volume's initial
// replicas whose store the master has not recorded yet.
func (h *Host) noteReplicaStores(f *control.AssignmentFact) {
	w := &h.storeWait
	w.mu.Lock()
	defer w.mu.Unlock()
	w.epoch, w.awaited, w.initial = f.GetEpoch(), f.GetReplicaStoresAwaited(), f.GetReplicaStoresInitial()
	switch {
	case w.awaited > 0:
		w.starting = w.epoch
	case w.starting != w.epoch:
		w.starting = 0 // another line: it never started from a wait
	}
}

// replicaStoresAwaitedEpoch is, for the probe answer, the epoch of the line
// that still starts from its creation wait: its assignment awaits replica
// stores, or carried them but the frontend has not opened on it yet. 0
// once the frontend is open (primaryReady), for good on that line.
func (h *Host) replicaStoresAwaitedEpoch(primaryReady bool) uint64 {
	if _, ok := h.getReplication().(storeRequirer); !ok {
		return 0
	}
	w := &h.storeWait
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.starting == 0 || w.starting != w.epoch {
		return 0
	}
	if primaryReady {
		w.starting = 0
		return 0
	}
	return w.starting
}

// noteFrontendServing ends the line's start once its frontend is open: the
// heartbeat calls it, so the start ends without waiting for a probe.
func (h *Host) noteFrontendServing() {
	roles, ok := h.probeRoles.Load().(probeRoleSource)
	if !ok || roles == nil {
		return
	}
	if _, ready := roles(); ready {
		h.replicaStoresAwaitedEpoch(true)
	}
}

// ReplicaStoresAwaited is the reason a new volume's primary serves nothing
// yet: "" once every initial replica's store is recorded (or the volume
// never waited). It applies where the replication slot counts peers only
// with recorded stores; a host without one counts no peer ack at all.
func (h *Host) ReplicaStoresAwaited() string {
	if _, ok := h.getReplication().(storeRequirer); !ok {
		return ""
	}
	w := &h.storeWait
	w.mu.Lock()
	waiting, initial := w.awaited, w.initial
	w.mu.Unlock()
	if waiting == 0 {
		return ""
	}
	return fmt.Sprintf("creating: waiting for replica attestations (%d of %d)", initial-waiting, initial)
}

// INV-WS-STARTUP-RETRY-001: remember the newest assignment observed for us,
// including one whose local readiness admission subsequently refuses.
func (h *Host) recordSelfAssignment(f *control.AssignmentFact) {
	if f.VolumeId != h.cfg.VolumeID {
		return
	}
	h.ownAssignmentMu.Lock()
	defer h.ownAssignmentMu.Unlock()
	if f.Epoch > h.ownAssignmentEpoch || f.Epoch == h.ownAssignmentEpoch && f.EndpointVersion > h.ownAssignmentEV {
		h.ownAssignmentEpoch, h.ownAssignmentEV = f.Epoch, f.EndpointVersion
	}
}

// IsCurrentReplicaSuperseded reuses the authority predicate with the newest
// observed own lineage, zero before any own assignment. Keep that lineage
// stable through the comparison; a historical other line alone cannot mean78.
// This is exit classification only: frontend admission keeps its own lineage.
func (h *Host) IsCurrentReplicaSuperseded() bool {
	h.ownAssignmentMu.RLock()
	defer h.ownAssignmentMu.RUnlock()
	return h.IsSuperseded(h.cfg.ReplicaID, h.ownAssignmentEpoch, h.ownAssignmentEV)
}
