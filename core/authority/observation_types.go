package authority

import "time"

// ============================================================
// P14 S4 — Observation Institution Contracts
//
// Normalized observation contract types consumed by the
// observation store and the snapshot builder. This file defines
// pure data shapes; it has no logic that interprets observations
// as authority.
//
// See sw-block/design/v3-phase-14-s4-sketch.md §5.
// ============================================================

// Observation is one point-in-time fact, supplied by one source,
// about one server and its locally-known slot inventory.
//
// Observation carries raw facts only. It never carries:
//   - an instruction to the authority layer
//   - an epoch or EndpointVersion proposal
//   - a "promote me" flag
// The observation institution records observations; it does not
// interpret them as authority. (Load-bearing: sketch §3.)
type Observation struct {
	ServerID   string
	ObservedAt time.Time
	// ExpiresAt is ObservedAt + FreshnessWindow, computed at
	// ingest. Freshness is checked against wall-clock now against
	// ExpiresAt, never against "heartbeat interval multiples".
	ExpiresAt time.Time
	Server    ServerFact
	Slots     []SlotFact
}

// ServerFact is the cluster-visible state of the reporting server.
type ServerFact struct {
	Reachable bool
	Eligible  bool
}

// SlotFact is one locally-observed replica slot the reporting
// server thinks it hosts for some volume. All fields are raw
// facts; the observation layer does not rank or aggregate them.
type SlotFact struct {
	VolumeID        string
	ReplicaID       string
	DataAddr        string
	CtrlAddr        string
	Reachable       bool
	ReadyForPrimary bool
	Eligible        bool
	Withdrawn       bool
	EvidenceScore   uint64
	// LocalRoleClaim is what the reporting server thinks its own
	// role is right now. Recorded as raw fact only; never
	// interpreted as authority. Two servers both claiming
	// Primary for the same volume is the
	// ReasonConflictingPrimaryClaim trigger.
	LocalRoleClaim LocalRoleClaim
}

// LocalRoleClaim is the raw role claim a server makes about
// itself. Not authority — the authority layer is the only thing
// that mints primary/candidate facts.
type LocalRoleClaim uint8

const (
	LocalRoleNone LocalRoleClaim = iota
	LocalRoleCandidate
	LocalRolePrimary
)

func (r LocalRoleClaim) String() string {
	switch r {
	case LocalRoleNone:
		return "none"
	case LocalRoleCandidate:
		return "candidate"
	case LocalRolePrimary:
		return "primary"
	default:
		return "unknown"
	}
}

// FreshnessConfig pins the freshness and pending-grace windows.
// Both values are explicit configuration; neither is a tick
// multiple nor a hidden constant.
type FreshnessConfig struct {
	// FreshnessWindow is how long an observation remains fresh
	// after its ObservedAt. `ExpiresAt = ObservedAt + FreshnessWindow`.
	FreshnessWindow time.Duration
	// PendingGrace is how long the store tolerates bootstrap /
	// missing observations in the "pending" state before a volume
	// escalates to "unsupported" with an explicit reason. Kept
	// distinct from FreshnessWindow at the API level so future
	// 14A review can tune them independently.
	PendingGrace time.Duration
}

func (c FreshnessConfig) withDefaults() FreshnessConfig {
	out := c
	if out.FreshnessWindow <= 0 {
		out.FreshnessWindow = 10 * time.Second
	}
	if out.PendingGrace <= 0 {
		out.PendingGrace = out.FreshnessWindow
	}
	return out
}

// Supportability is the explicit per-volume supportability state
// computed by the snapshot builder. Kept as a four-value enum
// rather than a boolean so that the API exposes pending and
// unsupported as VISIBLY DISTINCT paths — the 14A reopen will
// review them independently.
type Supportability uint8

const (
	SupportabilityUnknown Supportability = iota
	// SupportabilityPending: the accepted topology set cannot be
	// confirmed for this volume yet, but the pending grace has not
	// elapsed. The volume is omitted from the ClusterSnapshot and
	// omitted from Unsupported; it appears only in the Pending
	// collection of BuildResult.
	SupportabilityPending
	// SupportabilitySupported: all pinned rules pass for this
	// volume; it enters ClusterSnapshot.Volumes.
	SupportabilitySupported
	// SupportabilityUnsupported: one or more pinned rules fail
	// past the pending grace; the volume is omitted from the
	// ClusterSnapshot and appears in Unsupported with evidence.
	SupportabilityUnsupported
)

func (s Supportability) String() string {
	switch s {
	case SupportabilityPending:
		return "pending"
	case SupportabilitySupported:
		return "supported"
	case SupportabilityUnsupported:
		return "unsupported"
	default:
		return "unknown"
	}
}

// Reason codes for VolumeUnsupportedEvidence. Pinned vocabulary
// — see sketch §9. Implementations must not invent synonyms; new
// reasons require an explicit sketch / architect round.
const (
	ReasonPartialInventory         = "PartialInventory"
	ReasonMissingServerObservation = "MissingServerObservation"
	ReasonStaleObservation         = "StaleObservation"
	ReasonConflictingPrimaryClaim  = "ConflictingPrimaryClaim"
	ReasonUnknownReplicaClaim      = "UnknownReplicaClaim"
	ReasonDuplicateServerTopology  = "DuplicateServerTopology"
)

// AcceptedTopology is the operator-configured expected slot layout
// for the accepted P14 topology set. This is static configuration,
// NOT authority: it describes which slot-to-server mapping the
// operator has promised to provide. The observation institution
// compares live observations against this to decide supportability.
type AcceptedTopology struct {
	Volumes []VolumeExpected
}

// VolumeExpected pins the three expected slots of one volume.
type VolumeExpected struct {
	VolumeID string
	Slots    []ExpectedSlot
}

// ExpectedSlot is one accepted (ReplicaID, ServerID) pair.
type ExpectedSlot struct {
	ReplicaID string
	ServerID  string
}

// lookupVolume returns the accepted expected slots for a volume,
// or (_, false) if the volume is not in the accepted topology.
// Callers treat "not found" the same as "no expected topology";
// any observation for an unknown volume is unsupported by reason
// UnknownReplicaClaim.
func (t AcceptedTopology) lookupVolume(volumeID string) (VolumeExpected, bool) {
	for _, v := range t.Volumes {
		if v.VolumeID == volumeID {
			return v, true
		}
	}
	return VolumeExpected{}, false
}

// allVolumeIDs returns every configured volume's ID in a stable
// order. Used by the snapshot builder to iterate volumes
// deterministically.
func (t AcceptedTopology) allVolumeIDs() []string {
	out := make([]string, 0, len(t.Volumes))
	for _, v := range t.Volumes {
		out = append(out, v.VolumeID)
	}
	return out
}
