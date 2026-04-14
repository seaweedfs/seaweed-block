# V3 Calibration Divergence Log

Date: 2026-04-14
Status: no divergences recorded on first pass

This file records all expected-versus-observed mismatches discovered
during Phase 06 calibration. It is seeded from the template in
`sw-block/design/docs/calibration/divergence-log.md`.

## Usage

A divergence is any case where:

1. expected semantic decision differs from observed decision
2. expected command path differs from observed command path
3. expected terminal-close behavior differs from observed closure
4. final state or final data differs from expectation

Do not hide a mismatch by broadening the expectation first.
Record the divergence here, then decide whether to fix or carry it forward.

## Status Labels

Use one of:

1. `open`
2. `investigating`
3. `fixed`
4. `accepted-carry-forward`

## Entry Template

Copy this block for each divergence:

```md
## DIV-XXX

- Scenario ID:
- Scenario family:
- Detected in run / artifact:
- Status:

### Expected

- semantic decision:
- command path:
- terminal close:
- final state / data:

### Observed

- semantic decision:
- command path:
- terminal close:
- final state / data:

### Classification

- semantic / route / terminal-close / data / other:
- anti-pattern pressure involved (`A1`, `A4`, `A7`, other):

### Notes

- short explanation:
- next action:
```

## Current Divergences

No route-level calibration divergences recorded.

First pass on 2026-04-14 ran all five scenarios
(`CAL-C1-healthy-repeat`, `CAL-C2-short-gap-catchup`,
`CAL-C3-long-gap-rebuild`, `CAL-C4-terminal-close-only`,
`CAL-C5-order-stable`) through `sparrow --calibrate` and through
`core/calibration/scenarios_test.go`. All five came back `pass`
with no expected-versus-observed mismatch on the accepted route.

### Harness evidence-attribution issues found and fixed on first pass

These are not route divergences; they are cases where the scenario
claimed stronger evidence than it actually checked. Architect review
on 2026-04-14 caught all three. Recorded here to make the history
honest and to serve as template for future evidence-strength
discipline.

**H-001 (`CAL-C4-terminal-close-only`) — healthy-only-after-close
was presence-only, not order.** The original check asserted that
`SessionClosedCompleted` and `session_completed` both appeared in
the trace, but never verified that no healthy-transition decision
appeared BEFORE them. A buggy route that published healthy early
and then later emitted close markers would still pass. Fixed by
scanning trace in order: any `decision: none (R >= H)` before the
close event is a violation, and at least one must appear at/after
close for the proof to be non-vacuous.

**H-002 (`CAL-C5-order-stable`) — perturbation was a sleep, not an
ordering change.** The original C5 delayed the start of each run by
0-8 ms but then ran the exact same deterministic sequence. This did
not stress A1 or A4 meaningfully. Fixed by replacing the sleep with
five distinct event-ordering patterns: baseline, early duplicate
probes, concurrent wrong-replica storm, mixed valid+stale, and storm
of valid same-fact probes. All five must produce the same decision
and final mode.

**H-003 (`CAL-C1-healthy-repeat`) — scenario did not actually
repeat observation.** The original C1 performed a single
assignment + single wait, then reported "repeated observation stays
healthy". Fixed by firing 5 additional `OnProbeResult` calls with
the same facts after reaching healthy, then re-reading the
projection to confirm mode stayed healthy and no new recovery
commands appeared.

**H-004 (`CAL-C5-order-stable`) — full command path was not
compared; only `Decision` and `FinalMode` were.** The original C5
loop compared three fields across runs but never looked at
`Observed.CommandPath`, and the report reused the first run's path
verbatim. A run whose command log drifted (e.g., extra `StartCatchUp`
from stale-fact re-probes) would pass silently. Fixed by adding a
`recoverySignature` comparison: counts of `StartCatchUp` and
`StartRebuild` must be identical across runs. A signature mismatch
prepends `DIVERGED_ACROSS_RUNS_RECOVERY_SIG` to the reported
CommandPath so divergence is visible in the artifact.

**H-005 (`CAL-C5-order-stable`) — wrong-replica perturbations had
real side effects.** The original C5 included two patterns that
fired `ProbeFailed` events for a different `ReplicaID` ("r-other"),
documented as "rejected by engine `checkReplicaID`". That comment
was wrong: `checkReplicaID` at `apply.go:108-111` accepts events for
ANY replica when `st.Identity.ReplicaID == ""` (pre-identity path).
Those events reached `applyProbeFailed` and emitted real
`PublishDegraded{r-other}` commands, observable in executor logs as
`publish degraded for r-other`. That contaminated the command-path
comparison and contradicted the "same facts under reordering" claim.
Fixed by dropping all wrong-replica perturbations. The remaining
five perturbations are all same-replica: baseline, pre-assign probe,
pre-assign concurrent storm, repeat assign, staggered concurrent
assign. Each one preserves facts while varying delivery.

After this fix a real engine observation was exposed: any
post-convergence probe firing with pre-captured (now stale) facts
triggers the engine to re-enter `catch_up` via a legitimate
`RecoveryFactsObserved → decide()` path. The re-emit is guarded by
`!hasActiveSession(st)` in `decide()` (apply.go:287) but fires once
the first catch-up has completed and the session is in
`PhaseCompleted`. This is not an A1/A4 leak — it's the engine
correctly processing newly-arriving facts. Including such probes in
a "same facts" perturbation set would force C5 to either pass
vacuously or misattribute a legitimate re-recovery as a violation.
Perturbation set narrowed accordingly.

**H-006 — `Observed.SessionCloseCount` was counting
`PublishHealthy`.** The field named `SessionCloseCount` was derived
from `countCommand(cmds, "PublishHealthy")`, which counts healthy
publications, not session-close events. Renamed to
`CloseEventCount` and re-derived from the adapter trace's
`event: SessionClosedCompleted` entries. The JSON artifact field
`close_event_count` now matches its name.

**H-007 (`CAL-C5-order-stable`) — doc claimed identical
`close_event_count` across runs; code did not enforce it.** After
H-006 corrected the field's meaning, the scenario map was updated to
claim "identical close-event count across runs" but the cross-run
compare loop in `scenarioC5OrderStable.Run` did not compare the new
field. A drift in CloseEventCount would have passed silently. Fixed
by extending the compare loop: when counts differ across runs,
`Observed.CloseEventCount` is set to `-1` as a sentinel (a legitimate
count is always >= 0), so the JSON artifact and the per-scenario
test both surface the drift. The scenario test also pins the
expected steady-state count to 1 (one catch-up session per run for
the short-gap setup), so any future refactor that changes session
emission shape is caught.

### Harness decision (retained from prior note)

`Observed.Decision` is derived from the command log
(`StartCatchUp` / `StartRebuild` markers), not from the final
projection. After successful recovery the engine resets
`RecoveryDecision` to `"none"`, which would collapse every
successful recovery to `"none"` and falsely diverge every
`catch_up`/`rebuild` case. The command-log view captures what the
engine actually chose. Not a route mismatch.

Add entries below this line as soon as the first calibration
route divergence appears.

## Recording Rules

When adding a divergence:

1. create the entry immediately
2. link the matching scenario id from `scenario-map.md`
3. classify the mismatch before proposing the fix
4. keep the original expected/observed wording even after the fix lands

## Closure Rule

A divergence may be marked `fixed` only when:

1. the underlying mismatch is corrected
2. the affected calibration case is rerun
3. the new evidence artifact is linked in the entry

A divergence may be marked `accepted-carry-forward` only when:

1. architect and tester agree it is outside the current calibration closure bar
2. the carry-forward reason is written explicitly
