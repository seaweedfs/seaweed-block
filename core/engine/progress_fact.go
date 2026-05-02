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
