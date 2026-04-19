package authority

import (
	"fmt"
	"time"
)

// ============================================================
// P14 S4 — V2 Heartbeat Wire Shape (Ported Mechanism Only)
//
// Wire / proto-shape types ported from V2's block-volume
// heartbeat path as MECHANISM only. They carry the same fields
// V2 sends, so an ingest source can translate them into the
// V3 Observation shape without inventing new wire semantics.
//
// What is ported (sketch §10 / §15):
//   - the per-server, per-volume field layout V2 sends
//     (analogue of weed/storage/blockvol/block_heartbeat.go)
//   - the loop-driver shape (analogue of
//     weed/server/block_heartbeat_loop.go) — mechanism only;
//     host wiring owns the loop lifecycle, not the wire types.
//
// What is NOT ported (sketch §10 / §15):
//   - weed/storage/blockvol/promotion.go
//   - HandleAssignment / promote / demote
//   - any path where heartbeat timing or local status becomes
//     failover or authority truth
//
// These types are plain data. They have no authority meaning.
// Translating a HeartbeatMessage into an Observation is a
// field-shape copy; nothing more.
// ============================================================

// HeartbeatMessage is the V2-shaped heartbeat payload one
// reporting server sends. Ported as wire mechanism only.
type HeartbeatMessage struct {
	ServerID   string
	SentAt     time.Time
	Reachable  bool
	Eligible   bool
	Slots      []HeartbeatSlot
}

// HeartbeatSlot is the per-slot portion of HeartbeatMessage.
// Shape mirrors V2's block-volume heartbeat slot subset that
// the observation institution needs; omits V2 fields that
// relate to promotion policy (see denylist).
type HeartbeatSlot struct {
	VolumeID        string
	ReplicaID       string
	DataAddr        string
	CtrlAddr        string
	Reachable       bool
	ReadyForPrimary bool
	Eligible        bool
	Withdrawn       bool
	EvidenceScore   uint64
	// LocalRoleClaim is the V2 "I think I am" role flag, mapped
	// one-to-one into the V3 Observation shape without
	// reinterpretation. Two servers each sending LocalRolePrimary
	// for the same VolumeID triggers
	// ReasonConflictingPrimaryClaim at the builder.
	LocalRoleClaim LocalRoleClaim
}

// HeartbeatToObservation translates a V2-shaped heartbeat into
// the V3 Observation contract. Pure field-level mapping; no
// policy, no filtering, no aggregation.
//
// ExpiresAt is NOT set here — the store always derives it from
// its own FreshnessConfig at Ingest time, to prevent callers
// from fabricating freshness out-of-band (observation_store.go).
func HeartbeatToObservation(msg HeartbeatMessage) (Observation, error) {
	if msg.ServerID == "" {
		return Observation{}, fmt.Errorf("observation_wire: empty ServerID")
	}
	if msg.SentAt.IsZero() {
		return Observation{}, fmt.Errorf("observation_wire: zero SentAt")
	}

	slots := make([]SlotFact, 0, len(msg.Slots))
	for _, s := range msg.Slots {
		slots = append(slots, SlotFact{
			VolumeID:        s.VolumeID,
			ReplicaID:       s.ReplicaID,
			DataAddr:        s.DataAddr,
			CtrlAddr:        s.CtrlAddr,
			Reachable:       s.Reachable,
			ReadyForPrimary: s.ReadyForPrimary,
			Eligible:        s.Eligible,
			Withdrawn:       s.Withdrawn,
			EvidenceScore:   s.EvidenceScore,
			LocalRoleClaim:  s.LocalRoleClaim,
		})
	}

	return Observation{
		ServerID:   msg.ServerID,
		ObservedAt: msg.SentAt,
		Server: ServerFact{
			Reachable: msg.Reachable,
			Eligible:  msg.Eligible,
		},
		Slots: slots,
	}, nil
}
