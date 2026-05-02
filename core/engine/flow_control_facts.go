package engine

// FlowControlObservation is the raw fact bundle used to build
// FlowControlFacts. It mirrors the architecture split:
//   - primary storage reports local R/S/H;
//   - replica/session feedback reports durable frontiers;
//   - sync-quorum misses come from the higher-level durability contract.
//
// This type is intentionally not an Event. It does not change engine state,
// start recovery, advance recycle pins, or affect feeder ownership by itself.
type FlowControlObservation struct {
	PrimaryDurableLSN  uint64 // primary local R: synced/durable frontier
	PrimaryTailLSN     uint64 // primary local S: retained WAL tail
	PrimaryHeadLSN     uint64 // primary local H: accepted WAL head
	PrimaryBoundsKnown bool

	SlowestReplicaDurableLSN   uint64
	SlowestReplicaDurableKnown bool
	SessionDurableLSN          uint64
	SessionDurableKnown        bool
	SyncQuorumMisses           uint64
	ExplicitDurability         bool
}

// BuildFlowControlFacts converts raw observations into pressure facts consumed
// by EvaluateFlowControl. Unknown primary bounds deliberately leave LSN-derived
// gauges at zero; callers must not invent pressure from incomplete facts.
func BuildFlowControlFacts(obs FlowControlObservation) FlowControlFacts {
	facts := FlowControlFacts{
		SyncQuorumMisses:   obs.SyncQuorumMisses,
		ExplicitDurability: obs.ExplicitDurability,
	}
	if !obs.PrimaryBoundsKnown {
		return facts
	}

	facts.PrimaryFlushLag = saturatingLag(obs.PrimaryDurableLSN, obs.PrimaryHeadLSN)
	facts.RetentionPressure = saturatingLag(obs.PrimaryTailLSN, obs.PrimaryHeadLSN)
	if obs.SlowestReplicaDurableKnown {
		facts.ReplicaDurableLag = saturatingLag(obs.SlowestReplicaDurableLSN, obs.PrimaryHeadLSN)
	}
	if obs.SessionDurableKnown {
		facts.RecoveryBacklog = saturatingLag(obs.SessionDurableLSN, obs.PrimaryHeadLSN)
	}
	return facts
}

func saturatingLag(frontier, head uint64) uint64 {
	if frontier >= head {
		return 0
	}
	return head - frontier
}
