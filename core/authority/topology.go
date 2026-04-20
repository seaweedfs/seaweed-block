package authority

import (
	"fmt"
	"sort"
	"time"
)

// ServerObservation is one server's cluster-visible readiness for
// placement, failover, and rebalance. It is intentionally bounded:
// operator/API surfaces remain P15 work.
type ServerObservation struct {
	ServerID  string
	Reachable bool
	Eligible  bool
}

// ReplicaCandidate is one accepted replica slot in the richer P14
// topology package. Unlike the first S3 slice, the multi-volume
// package carries an explicit evidence score so failover/rebalance
// can be evidence-backed rather than reachability-only.
type ReplicaCandidate struct {
	ReplicaID       string
	ServerID        string
	DataAddr        string
	CtrlAddr        string
	Reachable       bool
	ReadyForPrimary bool
	Eligible        bool
	Withdrawn       bool
	EvidenceScore   uint64
}

// VolumeTopologySnapshot is the complete point-in-time topology and
// authority view for one volume in the accepted full P14 topology
// set: one current primary and exactly three candidate slots on
// distinct servers.
type VolumeTopologySnapshot struct {
	VolumeID  string
	Authority AuthorityBasis
	Slots     []ReplicaCandidate
}

// ClusterSnapshot is the cluster-scoped observation submitted to the
// multi-volume controller. Later snapshots supersede earlier ones.
type ClusterSnapshot struct {
	CollectedRevision uint64
	CollectedAt       time.Time
	Servers           []ServerObservation
	Volumes           []VolumeTopologySnapshot
}

// TopologyControllerConfig pins the accepted richer topology set:
// multi-volume, three slots per volume, and bounded rebalance
// pressure when server load skew exceeds RebalanceSkew.
//
// RetryWindow is the S6 bounded-wait clock: how long a desired
// assignment may remain unconfirmed before it is marked Stuck and
// ConvergenceStuckEvidence is recorded for the volume. S6 retry
// is PASSIVE — this clock governs when Stuck surfaces, not when
// an ask is re-emitted into the publisher. See
// sw-block/design/v3-phase-14-s6-sketch.md §7.
type TopologyControllerConfig struct {
	ExpectedSlotsPerVolume int
	RebalanceSkew          int
	RetryWindow            time.Duration
}

func (c TopologyControllerConfig) withDefaults() TopologyControllerConfig {
	if c.ExpectedSlotsPerVolume == 0 {
		c.ExpectedSlotsPerVolume = 3
	}
	if c.RebalanceSkew == 0 {
		c.RebalanceSkew = 1
	}
	if c.RetryWindow == 0 {
		c.RetryWindow = 5 * time.Second
	}
	return c
}

// DesiredAssignment is the convergence loop's per-volume desired
// state. It intentionally stays at the AssignmentAsk level; the
// publisher remains the sole minter of Epoch/EndpointVersion.
//
// P14 S6 convergence bookkeeping:
//
//   - ProposedBasis is the publisher's current line at the moment
//     this desired ask was decided. Supersede uses it as the basis
//     to compare against any later publisher line: only a publisher
//     epoch STRICTLY greater than ProposedBasis.Epoch that also
//     targets a replica OTHER than Ask.ReplicaID drops the desired
//     (§9 case 1). For a Bind decided from no line, Assigned=false
//     and Epoch=0.
//
//   - DesiredAt is the controller-side activation clock. Set when
//     the desired entry is first recorded (i.e., at enqueue time),
//     NOT when the publisher later applies the ask. The controller
//     has no feedback channel from publisher to observe actual
//     apply time; anchoring the clock at activation is the honest
//     semantic.
//
//   - LastRetryAt records the last time the desired was re-
//     evaluated on a fresh observation. Diagnostic only.
//
//   - Stuck is true once DesiredAt + RetryWindow has elapsed
//     without confirmation. Stuck does NOT re-emit into the
//     publisher; it surfaces ConvergenceStuckEvidence and keeps
//     the desired entry so a later snapshot can still confirm or
//     supersede.
type DesiredAssignment struct {
	Ask           AssignmentAsk
	Reason        string
	Revision      uint64
	Observed      bool
	ProposedBasis AuthorityBasis
	DesiredAt     time.Time
	LastRetryAt   time.Time
	Stuck         bool
}

func sortVolumeSnapshots(vols []VolumeTopologySnapshot) {
	sort.Slice(vols, func(i, j int) bool {
		return vols[i].VolumeID < vols[j].VolumeID
	})
}

func indexServers(servers []ServerObservation) map[string]ServerObservation {
	idx := make(map[string]ServerObservation, len(servers))
	for _, s := range servers {
		idx[s.ServerID] = s
	}
	return idx
}

func validateVolumeTopology(snap VolumeTopologySnapshot, expectedSlots int) error {
	if snap.VolumeID == "" {
		return fmt.Errorf("topology: volume snapshot missing VolumeID")
	}
	if len(snap.Slots) != expectedSlots {
		return fmt.Errorf("topology: volume %s has %d slots, want %d", snap.VolumeID, len(snap.Slots), expectedSlots)
	}
	seenReplica := make(map[string]struct{}, len(snap.Slots))
	seenServer := make(map[string]struct{}, len(snap.Slots))
	for _, slot := range snap.Slots {
		if slot.ReplicaID == "" {
			return fmt.Errorf("topology: volume %s slot missing ReplicaID", snap.VolumeID)
		}
		if slot.ServerID == "" {
			return fmt.Errorf("topology: volume %s slot %s missing ServerID", snap.VolumeID, slot.ReplicaID)
		}
		if slot.DataAddr == "" || slot.CtrlAddr == "" {
			return fmt.Errorf("topology: volume %s slot %s missing addr", snap.VolumeID, slot.ReplicaID)
		}
		if _, ok := seenReplica[slot.ReplicaID]; ok {
			return fmt.Errorf("topology: volume %s duplicate ReplicaID %s", snap.VolumeID, slot.ReplicaID)
		}
		if _, ok := seenServer[slot.ServerID]; ok {
			return fmt.Errorf("topology: volume %s duplicate ServerID %s", snap.VolumeID, slot.ServerID)
		}
		seenReplica[slot.ReplicaID] = struct{}{}
		seenServer[slot.ServerID] = struct{}{}
	}
	return nil
}
