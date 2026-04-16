# V3 Calibration Scenario Map

Date: 2026-04-14
Status: first-pass wired, all scenarios green on 2026-04-14

This file is the working map for the Phase 06 first calibration pass,
seeded from `sw-block/design/docs/calibration/scenario-map.md` in the
planning repo. The authoritative test artifact is the JSON Report
emitted by `go run ./cmd/sparrow --calibrate --json`.

## Usage

For each scenario, record:

1. stable scenario id
2. scenario family (`C1` to `C5`)
3. one-sentence intent
4. expected semantic decision
5. expected command path
6. expected terminal close result
7. final-state/data expectation
8. evidence artifact location
9. current status

Do not delete rows when a case changes. Update status and add notes instead.

## Status Labels

Use one of:

1. `planned`
2. `wired`
3. `running`
4. `passed`
5. `diverged`
6. `accepted-carry-forward`

## First Calibration Set

| Scenario ID | Family | Intent | Expected Decision | Expected Command Path | Expected Terminal Close | Final State / Data Expectation | Evidence Artifact | Status | Notes |
|---|---|---|---|---|---|---|---|---|---|
| `CAL-C1-healthy-repeat` | `C1` | Repeated observation on caught-up replica stays healthy | `none` | no `StartCatchUp`, no `StartRebuild` before OR after 5 extra probe cycles | not required | final mode `healthy` after 5 additional same-fact probes | `core/calibration/scenarios_test.go::TestC1_HealthyNoRecovery` + `sparrow --calibrate --json` | `passed` | proves stable healthy absorbs repeated observation without triggering recovery |
| `CAL-C2-short-gap-catchup` | `C2` | Short retained gap chooses catch-up | `catch_up` | `StartCatchUp` yes, `StartRebuild` no | session close success | final mode `healthy`; replica data matches primary | `TestC2_ShortGapCatchUp` + `--calibrate --json` | `passed` | first calibration for short-gap route |
| `CAL-C3-long-gap-rebuild` | `C3` | Long gap beyond retained window chooses rebuild | `rebuild` | `StartRebuild` yes, `StartCatchUp` no | session close success | final mode `healthy`; rebuilt data matches primary | `TestC3_LongGapRebuild` + `--calibrate --json` | `passed` | first calibration for long-gap route |
| `CAL-C4-terminal-close-only` | `C4` | Progress alone does not count as terminal success | `catch_up` | `StartCatchUp` yes | ordered trace check: `decision: none (R >= H)` appears AFTER `event: SessionClosedCompleted` and at least once | final mode `healthy` only via close | `TestC4_TerminalOnlyFromSessionClose` + `--calibrate --json` | `passed` | ordered evidence, not just presence; catches a route that publishes healthy early then emits close markers late |
| `CAL-C5-order-stable` | `C5` | Same facts under 5 same-replica delivery perturbations keep identical recovery signature | `catch_up` across all runs | identical `(NCatchUp, NRebuild)` recovery signature across runs | identical close-event count across runs | identical final mode and data integrity across runs | `TestC5_OrderStableAcrossRuns` + `--calibrate --json` | `passed` | five patterns: baseline, pre-assign probe, pre-assign storm (8 concurrent), repeat assign, staggered assign (5 concurrent); A1/A4 pressure within the limits of the "same facts" boundary |

## C4 Evidence Shape

The C4 check is in trace ORDER, not presence:

1. `event: SessionClosedCompleted` appears at some index K.
2. `decision: none (R >= H)` — the ONLY engine path to Publication.Healthy — appears at index ≥ K. No occurrence before K is permitted; such an occurrence would prove healthy was declared before close processing.
3. At least one such decision must appear, otherwise the run never reached healthy and the proof is vacuous.

A buggy route that published healthy early and then later emitted the close markers still fails rule 2 deterministically.

## C5 Perturbation Patterns

Each C5 run uses identical facts (same R/S/H on both stores, same
identity) but varies delivery shape. All perturbations are
same-replica on purpose; wrong-replica events would land through
`checkReplicaID`'s pre-identity acceptance path and emit real
`PublishDegraded{r-other}` commands, which would contaminate the
comparison.

Post-assignment probes with pre-captured facts are also deliberately
omitted: after the engine converges, an incoming probe with the
original R (now stale) correctly triggers re-recovery — that's engine
correctness, not an A1/A4 leak, and including it would force C5 to
either pass vacuously or misattribute an engine change.

| Run | Pattern | What it stresses |
|---|---|---|
| 0 | baseline (assign + wait) | reference decision, signature, final mode |
| 1 | one valid pre-assign probe | pre-identity event acceptance and reset on identity arrival |
| 2 | 8 concurrent pre-assign probes | adapter-mutex contention under real concurrency before identity |
| 3 | two identical assignments in sequence | engine's monotonic identity check on equal-value re-assign |
| 4 | 5 concurrent identical assignments | concurrent idempotent re-assignment without signature drift |

All five runs must produce identical `Decision`, `FinalMode`,
`DataIntegrityOK`, AND `recoverySignature` (counts of `StartCatchUp`
and `StartRebuild` commands). Divergence on any of these is signaled
explicitly in the report — `Decision` or `FinalMode` flips to
`DIVERGED_ACROSS_RUNS`, or `CommandPath` gets a
`DIVERGED_ACROSS_RUNS_RECOVERY_SIG` sentinel element prepended.

Verified green under `-count=10` stress as of 2026-04-14.

## Evidence: Decision Attribution

The harness derives `Observed.Decision` from the command log, not from
the final projection. After a successful recovery the engine resets
`RecoveryDecision` to `none`; the calibration question is what the
engine chose to do, which is visible in whether `StartCatchUp` or
`StartRebuild` was emitted. Reading the final projection instead
would collapse every recovery to `none` and silently make the catch-up
vs rebuild distinction disappear. This is recorded here because it
is a real choice, not an accident.

## Anti-Pattern Evidence Map

First-pass coverage of the anti-patterns Phase 06 must calibrate:

| Anti-pattern | Primary scenario | Evidence strength | Status |
|---|---|---|---|
| `A1` (heartbeat timing defines recovery) | `CAL-C5-order-stable` | 5 same-replica delivery patterns with real concurrent goroutine contention; same `Decision` + `FinalMode` + `recoverySignature` across all | first-pass covered within the "same facts" boundary; real-topology/real-heartbeat pressure belongs to later phases |
| `A4` (event ordering defines semantics) | `CAL-C5-order-stable` | same 5 patterns include pre-assign events, concurrent assigns, and idempotent re-delivery; identical recovery signature across all | first-pass covered within the "same facts" boundary |
| `A3` (ack/progress defines terminal success) | `CAL-C4-terminal-close-only` | ordered trace check: `decision: none (R >= H)` only appears after `event: SessionClosedCompleted` | first-pass covered |
| `A7` (transport mechanics leak into engine) | `CAL-C1`-`CAL-C4` all go through real transport; no transport-shaped divergence observed | structural, not adversarial | first-pass supported; a dedicated adversarial A7 scenario (injected transport failure, concurrent shutdown, etc.) remains an open next-step if a divergence suggests one |

Evidence honesty notes:

- `A1`/`A4` coverage is bounded by the "same facts" constraint. Probes that arrive post-convergence with pre-captured (now stale) facts are NOT part of this set, because the engine legitimately re-enters recovery when fact-carrying events refresh R/S/H. That is engine correctness, not an A1/A4 leak, and including it would either loosen the pass criterion to vacuity or misattribute engine behavior.
- `A7` evidence remains structural (all C1-C4 use the real transport stack end-to-end) rather than adversarial. A future calibration that injects transport failures mid-session would strengthen this.

## Post-Closure Adversarial Proofs

After the first-pass calibration set closed, the deliver repo added
adversarial protocol proofs that are narrower than a new calibration
family but stronger than the original structural evidence:

| Proof | Location | What it locks in |
|---|---|---|
| stale callback after reassignment is ignored | `core/adapter/adapter_test.go::TestStaleCallback_OldSessionIgnoredAfterNewAssignment` | old session close cannot become current semantic truth after a newer assignment creates a newer session |
| invalidated rebuild session does not callback success | `core/transport/transport_test.go::TestTransport_Rebuild_InvalidatedSessionStopsWithoutCallback` | execution lag does not re-enter the semantic route after invalidation |
| replica rejects stale mutation lineage | `core/transport/transport_test.go::TestTransport_ReplicaRejectsStaleMutationLineage` | old primary / old session data-plane writes cannot overwrite newer accepted lineage |
| stale transport epoch probe failure is rejected | `core/conformance/cases.yaml` + `core/engine/apply_test.go` | stale reachability loss does not degrade current truth |
| recovery facts before probe success still converge | `core/conformance/cases.yaml` + `core/engine/apply_test.go` | same fact set under reorder still yields the same bounded recovery decision |

These do not replace C1-C5. They carry forward the calibration package
with more explicit fail-closed proof around handoff, stale lineage, and
same-facts reorder behavior.

## Notes By Family

### C1

Focus:

1. no unnecessary recovery session
2. repeated probe/observe cycle remains semantically stable

### C2

Focus:

1. retained-window short gap remains `catch_up`
2. no silent widening into `rebuild`

### C3

Focus:

1. beyond-window long gap remains `rebuild`
2. no accidental downgrade into `catch_up`

### C4

Focus:

1. terminal truth still comes only from explicit session close
2. progress and transport success do not redefine final truth

### C5

Focus:

1. same facts still mean same decision
2. timing/order variation does not become hidden semantic input

## Update Rule

When a scenario changes state:

1. update the `Status` column
2. fill `Evidence Artifact`
3. add a short note if expectation changed or divergence appeared

If expected and observed results differ, also create or update the
matching entry in `divergence-log.md`.
