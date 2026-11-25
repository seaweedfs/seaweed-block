package volume

import (
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// primaryEvidenceSource is the local store's epoch table and durable end, as
// a primary line's target checks them before adopting the role
// (replication.ReplicationVolume).
type primaryEvidenceSource interface {
	LocalPrimaryEvidence() (replication.EpochTableStatus, uint64, error)
}

// checkPrimaryEvidence decides whether this node may adopt the primary role a
// line names. "" = adopt; otherwise why it refuses. The target's own store is
// the proof: the master's decision was taken from a probe, and the store may
// have changed since (re-created, restarted lower, a recovery in progress).
//   - a replay of a line this node already adopted (its table has a row for
//     the line's epoch): the table must be valid;
//   - BOOTSTRAP: the node holds no data;
//   - PROMOTION: the table is valid, the durable end is at least the probed
//     durable LSN, and the row at that LSN is the probed (epoch, first LSN);
//     a tail beyond it is this node's own log and becomes the new history;
//   - FORCED: adopted without evidence (the operator's lost range);
//   - a line without a kind to a node holding data: refused.
//
// The rule is for a replicated volume's lines (a non-empty peer set). A
// single-replica workspace move starts from the published image and is
// governed by the layout's own start check and claim id instead. A store
// without an epoch table has no rows: a probed row cannot match it.
func checkPrimaryEvidence(fact *control.AssignmentFact, st replication.EpochTableStatus, end uint64, floor uint64, floorKnown bool, store storage.StoreID) string {
	if len(fact.Peers) == 0 {
		return ""
	}
	// A replay of an epoch this node already holds rows at is idempotent,
	// except a PROMOTION, whose evidence is checked on every delivery: the
	// row this node noted at the epoch starts past the probed LSN, so a
	// genuine replay still matches, and a line naming another store or an
	// end this store does not hold is refused.
	for _, r := range st.Rows {
		if r.Epoch == fact.Epoch && fact.PrimaryKind != control.PrimaryKind_PRIMARY_KIND_PROMOTION {
			if st.Invalid {
				return fmt.Sprintf("epoch %d replayed over an epoch table a recovery left invalid", fact.Epoch)
			}
			return ""
		}
	}
	switch fact.PrimaryKind {
	case control.PrimaryKind_PRIMARY_KIND_BOOTSTRAP:
		if end != 0 || len(st.Rows) != 0 || st.Invalid {
			return fmt.Sprintf("a bootstrap line at epoch %d to a node that holds data (durable end %d)", fact.Epoch, end)
		}
		return ""
	case control.PrimaryKind_PRIMARY_KIND_PROMOTION:
		if st.Invalid {
			return fmt.Sprintf("promoted at epoch %d over an epoch table a recovery left invalid", fact.Epoch)
		}
		if end < fact.EvidenceDurableLsn {
			return fmt.Sprintf("promoted at epoch %d on a probed durable LSN %d, the store's durable end is %d", fact.Epoch, fact.EvidenceDurableLsn, end)
		}
		var row struct{ epoch, first uint64 }
		for _, r := range st.Rows {
			if r.FirstLSN <= fact.EvidenceDurableLsn {
				row.epoch, row.first = r.Epoch, r.FirstLSN
			}
		}
		if len(fact.EvidenceStoreId) != len(store) || string(fact.EvidenceStoreId) != string(store[:]) {
			return fmt.Sprintf("promoted at epoch %d on a probed store %x, this node's store is %x", fact.Epoch, fact.EvidenceStoreId, store[:])
		}
		if row.epoch != fact.EvidenceEpoch || row.first != fact.EvidenceFirstLsn {
			return fmt.Sprintf("promoted at epoch %d on the probed row (epoch %d from %d) at LSN %d, the store's row there is (epoch %d from %d)",
				fact.Epoch, fact.EvidenceEpoch, fact.EvidenceFirstLsn, fact.EvidenceDurableLsn, row.epoch, row.first)
		}
		return ""
	case control.PrimaryKind_PRIMARY_KIND_FORCED:
		return ""
	}
	// Upgrade: a store from before the epoch table (no rows, not invalid)
	// replays the line it held before the upgrade (its durable floor is the
	// line's epoch: a line at one epoch names one primary). Its adoption then
	// notes the row. The master never mints a line without a kind.
	if len(st.Rows) == 0 && !st.Invalid && floorKnown && floor == fact.Epoch {
		return ""
	}
	if end != 0 || len(st.Rows) != 0 {
		return fmt.Sprintf("a line without a primary kind at epoch %d to a node that holds data (durable end %d)", fact.Epoch, end)
	}
	return ""
}

// epochFloorReader is the self fence's durable epoch floor (the replication
// ingress in production).
type epochFloorReader interface {
	EpochFloor() (uint64, error)
}

func (h *Host) selfFenceFloor() (epochFloorReader, bool) {
	h.selfFenceMu.Lock()
	f := h.selfFence
	h.selfFenceMu.Unlock()
	fr, ok := f.(epochFloorReader)
	return fr, ok && f != nil
}

// refusePrimary latches a refused primary role: RoleAdmissionFailure reports
// it (the heartbeat makes the slot ineligible), and the master's next probe
// sees the store's real end and row and decides again at a new epoch.
func (h *Host) refusePrimary(reason string) {
	h.roleAdmissionFailed.Store(reason)
	h.log.Printf("blockvolume: volume %s refuses the primary role: %s — NOT applying to adapter (fail-closed)", h.cfg.VolumeID, reason)
}
