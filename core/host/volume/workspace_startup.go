package volume

import (
	"fmt"
	"strings"
	"sync"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// storeWait is the primary fact's replica-store wait, read and written as
// one value: the fact's epoch and its counts.
type storeWait struct {
	mu               sync.Mutex
	epoch            uint64
	awaited, initial uint32
}

// noteReplicaStores keeps the primary fact's count of a new volume's initial
// replicas whose store the master has not recorded yet.
func (h *Host) noteReplicaStores(f *control.AssignmentFact) {
	w := &h.storeWait
	w.mu.Lock()
	w.epoch, w.awaited, w.initial = f.GetEpoch(), f.GetReplicaStoresAwaited(), f.GetReplicaStoresInitial()
	w.mu.Unlock()
}

// awaitingStoresEpoch is the epoch of the primary assignment whose creation
// wait holds now (0: none). The frontend gate reads the same value, so once
// it reads 0 the wait no longer holds the frontend back: a not-ready answer
// read after it has another cause.
func (h *Host) awaitingStoresEpoch() uint64 {
	if _, ok := h.getReplication().(storeRequirer); !ok {
		return 0
	}
	w := &h.storeWait
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.awaited == 0 {
		return 0
	}
	return w.epoch
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

// noteAckProfile records whether this primary fact's line may be served: a
// replicated line (one with peers) is served only when its assignment names
// this volume's own ack profile. A later failover reads the writes this
// primary acknowledged by the master's profile, and the primary may not
// answer then; so a primary in another profile never acknowledges at all.
func (h *Host) noteAckProfile(f *control.AssignmentFact) {
	reason := ""
	if len(f.GetPeers()) > 0 {
		want, own := canonicalAckProfile(f.GetAckProfile()), canonicalAckProfile(h.cfg.AckProfile)
		if want != own {
			reason = fmt.Sprintf("ack profile mismatch: this volume runs --replication-ack %s, its replicated line is assigned under %s; restart it with the master's profile", profileName(own), profileName(want))
		}
	}
	if prev, _ := h.ackProfileRefusal.Load().(string); prev != reason && reason != "" {
		h.log.Printf("blockvolume: volume %s primary at epoch=%d does not serve: %s", h.cfg.VolumeID, f.GetEpoch(), reason)
	}
	h.ackProfileRefusal.Store(reason)
}

// AckProfileRefusal is why this primary serves nothing under its current
// line's ack profile, or "".
func (h *Host) AckProfileRefusal() string {
	reason, _ := h.ackProfileRefusal.Load().(string)
	return reason
}

func canonicalAckProfile(p string) string {
	return strings.ToLower(strings.ReplaceAll(p, "-", "_"))
}

func profileName(p string) string {
	if p == "" {
		return "(none)"
	}
	return p
}
