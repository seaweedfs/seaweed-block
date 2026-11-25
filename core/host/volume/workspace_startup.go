package volume

import (
	"fmt"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// noteReplicaStores keeps the primary fact's count of a new volume's initial
// replicas whose store the master has not recorded yet.
func (h *Host) noteReplicaStores(f *control.AssignmentFact) {
	h.storesAwaited.Store(uint64(f.GetReplicaStoresAwaited())<<32 | uint64(f.GetReplicaStoresInitial()))
}

// ReplicaStoresAwaited is the reason a new volume's primary serves nothing
// yet: "" once every initial replica's store is recorded (or the volume
// never waited). It applies where the replication slot counts peers only
// with recorded stores; a host without one counts no peer ack at all.
func (h *Host) ReplicaStoresAwaited() string {
	if _, ok := h.getReplication().(storeRequirer); !ok {
		return ""
	}
	v := h.storesAwaited.Load()
	waiting, initial := v>>32, v&0xffffffff
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
