package calibration

import (
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// Scenario is one calibration case. Run sets up the required state,
// exercises the accepted V3 route, and returns Observed — the
// ground-truth record of what happened. Expected is a pure-data
// declaration returned by Expect, so Compare() can produce the
// pass/fail and divergence without the scenario having any chance
// to tweak expectations after observation.
type Scenario interface {
	ID() string
	Family() Family
	Intent() string
	Expect() Expected
	Run() (Observed, error)
}

// AllScenarios returns the first calibration pass exactly as seeded in
// sw-block/design/docs/calibration/scenario-map.md. Order matters only
// for repeatability of the JSON artifact.
func AllScenarios() []Scenario {
	return []Scenario{
		&scenarioC1Healthy{},
		&scenarioC2ShortGap{},
		&scenarioC3LongGap{},
		&scenarioC4TerminalClose{},
		&scenarioC5OrderStable{},
	}
}

// --- C1 ---

type scenarioC1Healthy struct{}

func (*scenarioC1Healthy) ID() string     { return "CAL-C1-healthy-repeat" }
func (*scenarioC1Healthy) Family() Family { return FamilyC1 }
func (*scenarioC1Healthy) Intent() string {
	return "repeated observation on caught-up replica stays healthy"
}
func (*scenarioC1Healthy) Expect() Expected {
	return Expected{
		Decision:            "none",
		CommandPathExcludes: []string{"StartCatchUp", "StartRebuild"},
		FinalMode:           string(engine.ModeHealthy),
	}
}
func (*scenarioC1Healthy) Run() (Observed, error) {
	h, err := newHarness()
	if err != nil {
		return Observed{}, err
	}
	defer h.close()

	h.writeBlocks(10)
	h.mirrorToReplica()

	h.assign(1, 1)
	p := h.waitForDecisionFinal(3 * time.Second)
	if p.Mode != engine.ModeHealthy {
		// Precondition for the repetition proof failed; return what we
		// have so the comparison reports the violation honestly.
		return observed(h, p), nil
	}

	// The actual C1 claim: repeated observation on a caught-up replica
	// stays healthy — no recovery is ever triggered. After reaching
	// healthy, fire N probe results with the same R/S/H facts and
	// assert no StartCatchUp / StartRebuild ever appears.
	//
	// Snapshot command count BEFORE the repetition; after the loop,
	// the per-command totals must be unchanged for recovery kinds.
	const repetitions = 5
	_, _, pH := h.primaryStore.Boundaries()
	rR, rS, _ := h.replicaStore.Boundaries()
	for i := 0; i < repetitions; i++ {
		h.adapter.OnProbeResult(adapter.ProbeResult{
			ReplicaID: "r1", Success: true,
			EndpointVersion:   1,
			TransportEpoch:    1,
			ReplicaFlushedLSN: rR,
			PrimaryTailLSN:    rS,
			PrimaryHeadLSN:    pH,
		})
		// Tiny spacing between probes to exercise the path repeatedly,
		// not to sleep-through-a-race.
		time.Sleep(5 * time.Millisecond)
	}

	// Re-read final projection after the repetition — if any probe had
	// triggered recovery, mode would flip away from healthy.
	p = h.adapter.Projection()
	return observed(h, p), nil
}

// --- C2 ---

type scenarioC2ShortGap struct{}

func (*scenarioC2ShortGap) ID() string     { return "CAL-C2-short-gap-catchup" }
func (*scenarioC2ShortGap) Family() Family { return FamilyC2 }
func (*scenarioC2ShortGap) Intent() string {
	return "short retained gap chooses catch-up; closes to healthy"
}
func (*scenarioC2ShortGap) Expect() Expected {
	return Expected{
		Decision:              "catch_up",
		CommandPathIncludes:   []string{"StartCatchUp"},
		CommandPathExcludes:   []string{"StartRebuild"},
		FinalMode:             string(engine.ModeHealthy),
		TerminalFromClose:     true,
		DataIntegrityExpected: true,
	}
}
func (*scenarioC2ShortGap) Run() (Observed, error) {
	h, err := newHarness()
	if err != nil {
		return Observed{}, err
	}
	defer h.close()

	// Primary has 10 blocks; replica starts empty. WAL tail is at 1
	// (from first write), so R=0, S=1, H=10 and R < S would be rebuild.
	// For catch_up we need R >= S, so mirror enough to pass the S
	// boundary without reaching H.
	h.writeBlocks(10)
	// Copy 3 blocks to replica with an LSN that advances its frontier
	// past S but well below H — classic catch_up territory.
	_, _, pH := h.primaryStore.Boundaries()
	for i := uint32(0); i < 3; i++ {
		data, _ := h.primaryStore.Read(i)
		_ = h.replicaStore.ApplyEntry(i, data, pH/2)
	}
	h.replicaStore.Sync()

	h.assign(1, 1)
	p := h.waitForDecisionFinal(5 * time.Second)

	return observed(h, p), nil
}

// --- C3 ---

type scenarioC3LongGap struct{}

func (*scenarioC3LongGap) ID() string     { return "CAL-C3-long-gap-rebuild" }
func (*scenarioC3LongGap) Family() Family { return FamilyC3 }
func (*scenarioC3LongGap) Intent() string {
	return "long gap beyond retained window chooses rebuild; closes to healthy"
}
func (*scenarioC3LongGap) Expect() Expected {
	return Expected{
		Decision:              "rebuild",
		CommandPathIncludes:   []string{"StartRebuild"},
		CommandPathExcludes:   []string{"StartCatchUp"},
		FinalMode:             string(engine.ModeHealthy),
		TerminalFromClose:     true,
		DataIntegrityExpected: true,
	}
}
func (*scenarioC3LongGap) Run() (Observed, error) {
	h, err := newHarness()
	if err != nil {
		return Observed{}, err
	}
	defer h.close()

	h.writeBlocks(10)
	// Move WAL tail past the current head so R < S holds for any
	// replica that has not yet reached the new tail. Replica is empty
	// (R=0), so the engine must classify this as long-gap rebuild.
	h.advanceWALPastReplica()

	h.assign(1, 1)
	p := h.waitForDecisionFinal(5 * time.Second)

	return observed(h, p), nil
}

// --- C4 ---

// scenarioC4TerminalClose proves that terminal truth (final mode healthy)
// only appears AFTER SessionClosedCompleted in the trace. The test runs
// a normal catch-up scenario end-to-end and then scans the trace: if
// healthy publication preceded the session close event, that would be a
// terminal-truth anti-pattern. Phase 05 engine structurally disallows
// this, so the evidence we collect is order-of-trace-events.
type scenarioC4TerminalClose struct{}

func (*scenarioC4TerminalClose) ID() string     { return "CAL-C4-terminal-close-only" }
func (*scenarioC4TerminalClose) Family() Family { return FamilyC4 }
func (*scenarioC4TerminalClose) Intent() string {
	return "final healthy appears only after SessionClosedCompleted; progress alone does not count"
}
func (*scenarioC4TerminalClose) Expect() Expected {
	return Expected{
		Decision:            "catch_up",
		CommandPathIncludes: []string{"StartCatchUp"},
		FinalMode:           string(engine.ModeHealthy),
		TerminalFromClose:   true,
	}
}
func (*scenarioC4TerminalClose) Run() (Observed, error) {
	h, err := newHarness()
	if err != nil {
		return Observed{}, err
	}
	defer h.close()

	// Same setup as C2: short gap → catch_up → close → healthy.
	h.writeBlocks(10)
	_, _, pH := h.primaryStore.Boundaries()
	for i := uint32(0); i < 3; i++ {
		data, _ := h.primaryStore.Read(i)
		_ = h.replicaStore.ApplyEntry(i, data, pH/2)
	}
	h.replicaStore.Sync()

	h.assign(1, 1)
	p := h.waitForDecisionFinal(5 * time.Second)

	obs := observed(h, p)
	// Overwrite TerminalFromClose with a trace-order check — this is
	// the specific C4 evidence question, and the trace carries the
	// authoritative answer.
	obs.TerminalFromClose = terminalOnlyAfterClose(h.adapter.Trace())
	return obs, nil
}

// terminalOnlyAfterClose proves that a recovery-path scenario reached
// terminal healthy only through a session-close event, not through
// progress or transport success alone.
//
// Evidence (in trace ORDER, not just presence):
//
//  1. "event: SessionClosedCompleted" MUST appear at some index K.
//  2. "decision: none (R >= H)" — the ONLY engine path to Publication.
//     Healthy = true on a recovery path — MUST appear at index >= K.
//     The reducer sets Recovery.Decision to None only when R >= H,
//     and R only advances to H via applySessionCompleted. Any
//     "decision: none (R >= H)" at index < K would mean healthy was
//     declared before close processed — the C4 violation.
//  3. At least one such decision MUST appear. A run that never
//     reached healthy must not vacuously pass this check.
//
// A buggy route that published healthy early AND then later emitted
// the close markers would be caught by rule 2 — its pre-close
// "decision: none (R >= H)" would land before SessionClosedCompleted.
func terminalOnlyAfterClose(trace []engine.TraceEntry) bool {
	closeIdx := -1
	for i, te := range trace {
		if te.Step == "event" && te.Detail == "SessionClosedCompleted" {
			closeIdx = i
			break
		}
	}
	if closeIdx < 0 {
		return false // rule 1 — scenario never closed
	}

	// Scan the whole trace for the healthy-transition decision. Any
	// occurrence before closeIdx is a violation. Require at least one
	// occurrence at or after closeIdx.
	healthyAfterClose := false
	for i, te := range trace {
		if te.Step == "decision" && strings.HasPrefix(te.Detail, "none (R >= H)") {
			if i < closeIdx {
				return false // rule 2 — healthy claimed before close
			}
			healthyAfterClose = true
		}
	}
	return healthyAfterClose // rule 3
}

// --- C5 ---

// scenarioC5OrderStable runs the short-gap case N times with a tiny
// pre-assignment sleep that varies per iteration. Every run must choose
// the same decision and reach the same final mode. The decision string
// recorded in Observed is from the first run; subsequent runs compare
// to it. Any divergence across runs marks the scenario as failed with
// CommandPath mismatch carrying the run where it happened.
type scenarioC5OrderStable struct{}

func (*scenarioC5OrderStable) ID() string     { return "CAL-C5-order-stable" }
func (*scenarioC5OrderStable) Family() Family { return FamilyC5 }
func (*scenarioC5OrderStable) Intent() string {
	return "same facts under small timing/order perturbation still choose the same decision"
}
func (*scenarioC5OrderStable) Expect() Expected {
	return Expected{
		Decision:              "catch_up",
		CommandPathIncludes:   []string{"StartCatchUp"},
		FinalMode:             string(engine.ModeHealthy),
		DataIntegrityExpected: true,
	}
}
// perturbation is one of the concrete interleaving patterns C5 runs.
// Each produces the SAME underlying facts on the SAME replica (r1)
// but delivers events to the adapter in a different order, timing, or
// concurrency shape.
//
// Every perturbation must preserve "same facts" honestly:
//
//  1. No wrong-replica events (checkReplicaID's pre-identity accept
//     path would turn them into real cross-replica side effects).
//  2. No post-assignment probes with pre-captured facts. Those carry
//     STALE R/S/H values; once the engine has converged (R=H), an
//     incoming fact with R<H is correctly interpreted as new truth
//     and triggers a legitimate re-recovery. That's not an A1/A4
//     leak — it's the engine doing its job. Including such events
//     in a "same facts" perturbation is self-sabotage.
//
// Remaining valid perturbations vary goroutine scheduling, pre-assign
// event arrival, and assignment idempotence — the kinds of delivery
// variation an A1/A4 leak would surface on.
type perturbation int

const (
	pertBaseline       perturbation = iota // assign + wait
	pertPreAssignProbe                     // one valid pre-assign probe, wiped when identity arrives
	pertPreAssignStorm                     // 8 concurrent pre-assign probes, then assign
	pertRepeatAssign                       // assign twice with identical epoch/endpointVersion (idempotent)
	pertStaggeredAssign                    // 5 concurrent assigns with identical identity, then wait
)

func (*scenarioC5OrderStable) Run() (Observed, error) {
	perturbations := []perturbation{
		pertBaseline,
		pertPreAssignProbe,
		pertPreAssignStorm,
		pertRepeatAssign,
		pertStaggeredAssign,
	}

	var first Observed
	var firstSig recoverySignature
	sameDecision := true
	sameFinalMode := true
	sameRecoverySig := true
	sameCloseCount := true
	dataOK := true

	for i, p := range perturbations {
		h, err := newHarness()
		if err != nil {
			return Observed{}, err
		}

		// Identical facts across every run — only the delivery shape
		// varies. Short-gap setup: R ~= H/2, S = 1, H = 10.
		h.writeBlocks(10)
		_, _, pH := h.primaryStore.Boundaries()
		for j := uint32(0); j < 3; j++ {
			data, _ := h.primaryStore.Read(j)
			_ = h.replicaStore.ApplyEntry(j, data, pH/2)
		}
		h.replicaStore.Sync()

		applyPerturbation(h, p)
		proj := h.waitForDecisionFinal(5 * time.Second)
		obs := observed(h, proj)
		sig := extractRecoverySignature(obs.CommandPath)

		if i == 0 {
			first = obs
			firstSig = sig
		} else {
			if obs.Decision != first.Decision {
				sameDecision = false
			}
			if obs.FinalMode != first.FinalMode {
				sameFinalMode = false
			}
			if sig != firstSig {
				sameRecoverySig = false
			}
			if obs.CloseEventCount != first.CloseEventCount {
				sameCloseCount = false
			}
			if !obs.DataIntegrityOK {
				dataOK = false
			}
		}
		h.close()
	}

	out := first
	if !sameDecision {
		out.Decision = "DIVERGED_ACROSS_RUNS"
	}
	if !sameFinalMode {
		out.FinalMode = "DIVERGED_ACROSS_RUNS"
	}
	if !sameRecoverySig {
		// Pollute the command path marker so the reader cannot miss
		// that the recovery signature drifted. The baseline first-run
		// path is preserved alongside the marker so divergence-log
		// authors can still see what the first run looked like.
		out.CommandPath = append([]string{"DIVERGED_ACROSS_RUNS_RECOVERY_SIG"}, first.CommandPath...)
	}
	if !sameCloseCount {
		// Set CloseEventCount to a negative sentinel so the JSON
		// artifact makes the drift unambiguous. Negative is never a
		// legitimate count, so tester review cannot miss it.
		out.CloseEventCount = -1
	}
	out.DataIntegrityOK = out.DataIntegrityOK && dataOK
	return out, nil
}

// recoverySignature is the decision-relevant fingerprint of a command
// path — counts of each recovery command kind. Two runs with the same
// facts must produce the same signature even if their non-recovery
// command noise (duplicate PublishHealthy from repeat probes, etc.)
// differs. Comparing full command sequences would false-diverge on
// idempotent re-probes; comparing signatures catches the real A1/A4
// anti-pattern leak.
type recoverySignature struct {
	NCatchUp int
	NRebuild int
}

func extractRecoverySignature(path []string) recoverySignature {
	var sig recoverySignature
	for _, c := range path {
		switch c {
		case "StartCatchUp":
			sig.NCatchUp++
		case "StartRebuild":
			sig.NRebuild++
		}
	}
	return sig
}

// applyPerturbation drives the adapter under one of the C5 event
// orderings. Facts stay the same (same replica r1, same initial
// R/S/H); only the delivery shape — timing, ordering, concurrency,
// assignment idempotence — varies.
//
// All post-assignment probe variations are deliberately omitted: a
// probe fired with pre-captured facts after the engine has converged
// would deliver STALE R to the engine and legitimately trigger
// re-recovery. Treating that as a C5 failure would conflate engine
// correctness with A1/A4. Treating it as a C5 success would require
// gating the engine's fact-processing, which is a route change that
// belongs outside Phase 06.
func applyPerturbation(h *harness, p perturbation) {
	_, _, pH := h.primaryStore.Boundaries()
	rR, rS, _ := h.replicaStore.Boundaries()

	sameFactsProbe := adapter.ProbeResult{
		ReplicaID: "r1", Success: true,
		EndpointVersion:   1,
		TransportEpoch:    1,
		ReplicaFlushedLSN: rR,
		PrimaryTailLSN:    rS,
		PrimaryHeadLSN:    pH,
	}

	switch p {
	case pertBaseline:
		h.assign(1, 1)

	case pertPreAssignProbe:
		// One valid pre-assign probe. Engine accepts via the
		// pre-identity acceptance path; identity then lands and
		// resets Reachability/Recovery, so pre-assign state is
		// wiped. Final decision must match baseline.
		h.adapter.OnProbeResult(sameFactsProbe)
		h.assign(1, 1)

	case pertPreAssignStorm:
		// 8 concurrent pre-assign probes. Same net effect as
		// pertPreAssignProbe but with real mutex contention across
		// the adapter boundary before identity.
		for i := 0; i < 8; i++ {
			go h.adapter.OnProbeResult(sameFactsProbe)
		}
		h.assign(1, 1)

	case pertRepeatAssign:
		// Two identical assignments in sequence. The second's
		// epoch/endpointVersion match the first, so the engine's
		// monotonic identity check accepts without bumping
		// identity — a true idempotent re-assign.
		h.assign(1, 1)
		h.assign(1, 1)

	case pertStaggeredAssign:
		// 5 concurrent identical assignments. Same endpoint/epoch;
		// engine must handle concurrent idempotent re-assignment
		// without producing divergent recovery signatures.
		for i := 0; i < 5; i++ {
			go h.assign(1, 1)
		}
	}
}

// --- shared ---

// observed builds the Observed record from a harness post-run. Central
// so every scenario captures the same fields the same way — scenarios
// never hand-tune Observed outside this function.
//
// Decision is derived from the command log rather than the final
// projection. After a successful recovery run the engine resets
// RecoveryDecision to "none", but the calibration question is "what
// did the engine CHOOSE to do for this setup?" — and that choice is
// visible in which Start* command was emitted. Reading the final
// projection instead would reduce every successful recovery to
// Decision="none", making the catch_up-vs-rebuild distinction
// invisible and falsely diverging from Expected.Decision.
func observed(h *harness, final engine.ReplicaProjection) Observed {
	cmds := h.commandLog()
	trace := h.adapter.Trace()
	return Observed{
		Decision:          decisionFromCommandPath(cmds),
		CommandPath:       cmds,
		CloseEventCount:   countCloseEvents(trace),
		FinalMode:         string(final.Mode),
		TerminalFromClose: terminalOnlyAfterClose(trace),
		DataIntegrityOK:   h.dataMatches(),
	}
}

// countCloseEvents returns the number of SessionClosedCompleted events
// in the adapter trace. This is the truthful "session close" count
// (previously a field named SessionCloseCount in fact counted
// PublishHealthy commands, which was misleading).
func countCloseEvents(trace []engine.TraceEntry) int {
	n := 0
	for _, te := range trace {
		if te.Step == "event" && te.Detail == "SessionClosedCompleted" {
			n++
		}
	}
	return n
}

// decisionFromCommandPath reports which recovery class the engine
// actually picked, regardless of where the projection has since
// settled. If StartRebuild appears, the engine chose rebuild.
// If StartCatchUp appears (and no rebuild), it chose catch_up.
// If neither, no recovery was needed — decision was none.
func decisionFromCommandPath(cmds []string) string {
	hasRebuild := false
	hasCatchUp := false
	for _, c := range cmds {
		if c == "StartRebuild" {
			hasRebuild = true
		}
		if c == "StartCatchUp" {
			hasCatchUp = true
		}
	}
	switch {
	case hasRebuild:
		return "rebuild"
	case hasCatchUp:
		return "catch_up"
	default:
		return "none"
	}
}

// Runner is the small driver that executes a set of scenarios and
// produces one Result per scenario. It is deliberately not parallel:
// the calibration signal is clearer when runs are sequential and a
// shared transport listener per harness is simpler to reason about.
type Runner struct{}

// Run executes every scenario in order, one after another, and returns
// a Report. Any error from a single scenario is recorded on its Result
// but does not stop the remaining scenarios.
func (Runner) Run(scenarios []Scenario) Report {
	results := make([]Result, 0, len(scenarios))
	for _, s := range scenarios {
		r := Result{
			ID:       s.ID(),
			Family:   s.Family(),
			Intent:   s.Intent(),
			Expected: s.Expect(),
		}
		obs, err := s.Run()
		if err != nil {
			r.Error = err.Error()
			r.Pass = false
			r.Divergence = &Mismatch{}
		} else {
			r.Observed = obs
			r.Pass, r.Divergence = Compare(r.Expected, r.Observed)
		}
		results = append(results, r)
	}
	return newReport(results)
}

// Suppress the "adapter imported and not used" linter warning if
// scenarios ever temporarily drop all adapter references during
// development. The adapter IS used via harness; this is a defensive
// import anchor.
var _ = adapter.AssignmentInfo{}
