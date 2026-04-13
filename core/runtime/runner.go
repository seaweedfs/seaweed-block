// Package runtime hosts the V3 mini engine replay runner.
// It is intentionally boring: load case → apply events → collect results.
// No goroutines, no network, no side effects.
package runtime

import (
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/schema"
)

// ReplayResult holds the output of one conformance case replay.
type ReplayResult struct {
	CaseName string
	Passed   bool
	Failures []string

	// Collected outputs from each Apply step.
	Steps []StepResult

	// Final state after all events.
	FinalProjection engine.ReplicaProjection

	// All commands emitted across all steps.
	AllCommands []string

	// All trace entries across all steps.
	AllTrace []engine.TraceEntry
}

// StepResult holds the output of one Apply call within a replay.
type StepResult struct {
	EventKind  string
	Commands   []string
	Projection engine.ReplicaProjection
	Trace      []engine.TraceEntry
}

// Replay executes one conformance case through the engine.
// Returns a ReplayResult with pass/fail status and collected outputs.
func Replay(c schema.ConformanceCase) ReplayResult {
	result := ReplayResult{CaseName: c.Name}
	st := &engine.ReplicaState{}

	for i, ei := range c.Events {
		ev, err := schema.ToEngineEvent(ei)
		if err != nil {
			result.Failures = append(result.Failures,
				fmt.Sprintf("event %d: %v", i, err))
			return result
		}

		ar := engine.Apply(st, ev)

		step := StepResult{
			EventKind:  engine.EventKind(ev),
			Projection: ar.Projection,
			Trace:      ar.Trace,
		}
		for _, cmd := range ar.Commands {
			kind := engine.CommandKind(cmd)
			step.Commands = append(step.Commands, kind)
			result.AllCommands = append(result.AllCommands, kind)
		}
		result.AllTrace = append(result.AllTrace, ar.Trace...)
		result.Steps = append(result.Steps, step)
	}

	if len(result.Steps) == 0 {
		result.Failures = append(result.Failures, "no events to replay")
		return result
	}
	result.FinalProjection = result.Steps[len(result.Steps)-1].Projection

	// Check expectations.
	result.Failures = checkExpectations(c.Expect, result)
	result.Passed = len(result.Failures) == 0
	return result
}

func checkExpectations(exp schema.Expectation, r ReplayResult) []string {
	var failures []string

	if exp.Mode != "" && string(r.FinalProjection.Mode) != exp.Mode {
		failures = append(failures,
			fmt.Sprintf("mode: got %s, want %s", r.FinalProjection.Mode, exp.Mode))
	}
	if exp.RecoveryDecision != "" && string(r.FinalProjection.RecoveryDecision) != exp.RecoveryDecision {
		failures = append(failures,
			fmt.Sprintf("recovery_decision: got %s, want %s", r.FinalProjection.RecoveryDecision, exp.RecoveryDecision))
	}
	if exp.SessionPhase != "" && string(r.FinalProjection.SessionPhase) != exp.SessionPhase {
		failures = append(failures,
			fmt.Sprintf("session_phase: got %s, want %s", r.FinalProjection.SessionPhase, exp.SessionPhase))
	}

	for _, must := range exp.MustEmitCommands {
		if !containsStr(r.AllCommands, must) {
			failures = append(failures,
				fmt.Sprintf("missing command %s (emitted: %v)", must, r.AllCommands))
		}
	}

	for _, mustNot := range exp.MustNotEmitCommands {
		if containsStr(r.AllCommands, mustNot) {
			failures = append(failures,
				fmt.Sprintf("unexpected command %s", mustNot))
		}
	}

	for _, step := range exp.MustHaveTraceSteps {
		found := false
		for _, te := range r.AllTrace {
			if te.Step == step {
				found = true
				break
			}
		}
		if !found {
			failures = append(failures,
				fmt.Sprintf("missing trace step %q", step))
		}
	}

	return failures
}

func containsStr(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
