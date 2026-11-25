package volume

import (
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
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
func checkPrimaryEvidence(fact *control.AssignmentFact, st replication.EpochTableStatus, end uint64) string {
	if !st.Present {
		return "" // no epoch table: not a replicated store
	}
	for _, r := range st.Rows {
		if r.Epoch == fact.Epoch {
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
		if row.epoch != fact.EvidenceEpoch || row.first != fact.EvidenceFirstLsn {
			return fmt.Sprintf("promoted at epoch %d on the probed row (epoch %d from %d) at LSN %d, the store's row there is (epoch %d from %d)",
				fact.Epoch, fact.EvidenceEpoch, fact.EvidenceFirstLsn, fact.EvidenceDurableLsn, row.epoch, row.first)
		}
		return ""
	case control.PrimaryKind_PRIMARY_KIND_FORCED:
		return ""
	}
	if end != 0 || len(st.Rows) != 0 {
		return fmt.Sprintf("a line without a primary kind at epoch %d to a node that holds data (durable end %d)", fact.Epoch, end)
	}
	return ""
}

// refusePrimary latches a refused primary role: RoleAdmissionFailure reports
// it (the heartbeat makes the slot ineligible), and the master's next probe
// sees the store's real end and row and decides again at a new epoch.
func (h *Host) refusePrimary(reason string) {
	h.roleAdmissionFailed.Store(reason)
	h.log.Printf("blockvolume: volume %s refuses the primary role: %s — NOT applying to adapter (fail-closed)", h.cfg.VolumeID, reason)
}
