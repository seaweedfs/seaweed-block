package engine

import "time"

// ProgressSource identifies where a ReplicaProgressFact came from.
// Source is authority, not just metadata: probe facts may classify
// recovery need, but only durable-ack facts may advance WAL recycle pin.
type ProgressSource string

const (
	ProgressFromProbe        ProgressSource = "probe"
	ProgressFromDurableAck   ProgressSource = "durable_ack"
	ProgressFromSessionClose ProgressSource = "session_close"
	ProgressFromStartup      ProgressSource = "startup"
)

// ProgressConfidence records whether the fact was freshly observed on
// the wire or is a cached/missing placeholder.
type ProgressConfidence string

const (
	ProgressLiveWire ProgressConfidence = "live_wire"
	ProgressCached   ProgressConfidence = "cached"
	ProgressMissing  ProgressConfidence = "missing"
)

// ReplicaProgressFact is the common R/S/H observation shape consumed by
// recovery policy. It deliberately has no TargetLSN field: target/frontier
// bands are wire/diagnostic text, not recovery classification input.
type ReplicaProgressFact struct {
	ReplicaID       string
	Epoch           uint64
	EndpointVersion uint64
	ObservedAt      time.Time

	ReplicaR      uint64
	ReplicaRKnown bool
	ReplicaS      uint64
	ReplicaSKnown bool
	ReplicaH      uint64
	ReplicaHKnown bool

	PrimaryS           uint64
	PrimaryH           uint64
	PrimaryBoundsKnown bool

	Source     ProgressSource
	Confidence ProgressConfidence
}

// ProgressFact converts the legacy engine recovery-facts event into the
// canonical fact seam. The historical all-zero event means "no boundaries
// observed" in engine tests, so it remains unknown here; a future explicit
// empty-volume fact should set PrimaryBoundsKnown through a different path.
func (e RecoveryFactsObserved) ProgressFact() ReplicaProgressFact {
	boundsKnown := !(e.R == 0 && e.S == 0 && e.H == 0)
	return ReplicaProgressFact{
		ReplicaID:          e.ReplicaID,
		EndpointVersion:    e.EndpointVersion,
		ReplicaR:           e.R,
		ReplicaRKnown:      boundsKnown,
		PrimaryS:           e.S,
		PrimaryH:           e.H,
		PrimaryBoundsKnown: boundsKnown,
		Source:             ProgressFromProbe,
		Confidence:         ProgressLiveWire,
	}
}

// ProgressFact converts durable recovery feedback into the canonical fact
// seam. Unlike probe facts, durable ack facts are authorized to advance
// WAL recycle pin, but this conversion does not imply health or membership.
func (e DurableAckObserved) ProgressFact() ReplicaProgressFact {
	return ReplicaProgressFact{
		ReplicaID:          e.ReplicaID,
		EndpointVersion:    e.EndpointVersion,
		ReplicaR:           e.DurableLSN,
		ReplicaRKnown:      true,
		PrimaryS:           e.PrimaryTailLSN,
		PrimaryH:           e.PrimaryHeadLSN,
		PrimaryBoundsKnown: true,
		Source:             ProgressFromDurableAck,
		Confidence:         ProgressLiveWire,
	}
}

// ClassifyProgress maps a unified progress fact to the engine's recovery
// class. Numeric R/S/H semantics are source-independent; source authority
// is handled by helpers such as CanAdvanceRecyclePin.
func ClassifyProgress(f ReplicaProgressFact) RecoveryDecision {
	if !f.PrimaryBoundsKnown {
		return DecisionUnknown
	}
	if !f.ReplicaRKnown {
		return DecisionRebuild
	}
	switch {
	case f.ReplicaR >= f.PrimaryH:
		return DecisionNone
	case f.ReplicaR < f.PrimaryS:
		return DecisionRebuild
	default:
		return DecisionCatchUp
	}
}

// CanAdvanceRecyclePin is true only for durable-ack facts. Probe facts can
// improve recovery classification but must not release primary WAL.
func CanAdvanceRecyclePin(f ReplicaProgressFact) bool {
	return f.Source == ProgressFromDurableAck && f.ReplicaRKnown
}
