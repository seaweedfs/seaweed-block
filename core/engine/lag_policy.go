package engine

// LagDecision is the coordinator-facing interpretation of recent progress
// facts. It is intentionally separate from RecoveryDecision: RecoveryDecision
// classifies the latest R/S/H gap, while LagDecision decides whether lag is
// normal live-tail drift, stalled catch-up debt, or unrecoverable from WAL.
type LagDecision string

const (
	LagDecisionUnknown      LagDecision = "unknown"
	LagDecisionInSync       LagDecision = "in_sync"
	LagDecisionKeepFeeding  LagDecision = "keep_feeding"
	LagDecisionStartCatchUp LagDecision = "start_catch_up"
	LagDecisionStartRebuild LagDecision = "start_rebuild"
)

// LagPolicy controls how much evidence the coordinator requires before it
// escalates replica lag into a recovery action.
type LagPolicy struct {
	// MaxLiveTailLag treats a replica as healthy live-tail if H-R is within
	// this distance. Zero disables the threshold and relies on progress.
	MaxLiveTailLag uint64

	// StalledSamples is the number of recent known facts required before a
	// no-progress window escalates to catch-up. Values <= 1 normalize to 3.
	StalledSamples int
}

// EvaluateLagPolicy interprets recent canonical progress facts without using
// target/frontier bands. The latest known fact supplies the current WAL window;
// earlier facts only establish whether the replica is still making progress.
func EvaluateLagPolicy(policy LagPolicy, facts []ReplicaProgressFact) LagDecision {
	known := knownProgressFacts(facts)
	if len(known) == 0 {
		return LagDecisionUnknown
	}

	latest := known[len(known)-1]
	switch ClassifyProgress(latest) {
	case DecisionUnknown:
		return LagDecisionUnknown
	case DecisionRebuild:
		return LagDecisionStartRebuild
	case DecisionNone:
		return LagDecisionInSync
	}

	if policy.MaxLiveTailLag > 0 && progressLag(latest) <= policy.MaxLiveTailLag {
		return LagDecisionKeepFeeding
	}

	window := policy.StalledSamples
	if window <= 1 {
		window = 3
	}
	if len(known) < window {
		return LagDecisionKeepFeeding
	}

	start := known[len(known)-window]
	if latest.ReplicaR > start.ReplicaR {
		return LagDecisionKeepFeeding
	}
	if latest.PrimaryH > start.PrimaryH {
		return LagDecisionStartCatchUp
	}
	return LagDecisionKeepFeeding
}

func knownProgressFacts(facts []ReplicaProgressFact) []ReplicaProgressFact {
	known := make([]ReplicaProgressFact, 0, len(facts))
	for _, f := range facts {
		if f.ReplicaRKnown && f.PrimaryBoundsKnown {
			known = append(known, f)
		}
	}
	return known
}

func progressLag(f ReplicaProgressFact) uint64 {
	if f.ReplicaR >= f.PrimaryH {
		return 0
	}
	return f.PrimaryH - f.ReplicaR
}
