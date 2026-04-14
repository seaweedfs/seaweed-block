# Engine Surface

This doc is a taxonomy reference for the semantic core. It names every
event, command, and state domain the engine currently exposes, so that
reviewers and contributors can see the full surface without re-reading
the code.

It deliberately does NOT try to draw one unified state diagram. The
reason is in [Why No Single Diagram](#why-no-single-diagram).

## Surface Summary

| Dimension | Count | Values |
|---|---|---|
| Events | 12 | `AssignmentObserved`, `EndpointObserved`, `ReplicaRemoved`, `ProbeSucceeded`, `ProbeFailed`, `RecoveryFactsObserved`, `SessionPrepared`, `SessionStarted`, `SessionProgressObserved`, `SessionClosedCompleted`, `SessionClosedFailed`, `SessionInvalidated` |
| Commands | 6 | `ProbeReplica`, `StartCatchUp`, `StartRebuild`, `InvalidateSession`, `PublishHealthy`, `PublishDegraded` |
| Truth domains | 5 | `Identity`, `Reachability`, `Recovery`, `Session`, `Publication` |
| Operator `Mode` | 4 | `healthy`, `degraded`, `recovering`, `idle` |
| `RecoveryDecision` | 4 | `unknown`, `none`, `catch_up`, `rebuild` |
| `SessionKind` | 3 | `""` (none), `catch_up`, `rebuild` |
| `SessionPhase` | 5 | `""` (none), `starting`, `running`, `completed`, `failed` |
| `ProbeStatus` | 4 | `unknown`, `probing`, `reachable`, `unreachable` |
| `ContactKind` | 5 | `none`, `ship`, `barrier`, `probe`, `ctrl_ack` |

Source files:
- Events: `core/engine/events.go`
- Commands: `core/engine/commands.go`
- Truth domains and enums: `core/engine/state.go`
- Operator projection and `Mode`: `core/engine/projection.go`

## Runtime Layer (not part of engine surface)

The engine is deliberately free of transport and runtime state. The
current `seaweedfs/V2` runtime that will eventually sit beneath this
engine has its own orthogonal state machine — included here for
context, NOT because it is part of the engine contract:

| Layer | State machine | Values |
|---|---|---|
| V2 runtime shipper | `ReplicaState` | `Connecting`, `InSync`, `CatchingUp`, `Degraded`, `NeedsRebuild`, `Rebuilding`, `Disconnected` |

The engine does not read these, does not decide based on them, and
does not return to them. They are runtime execution state. If a future
V3 runtime is written, it will have its own analogous states, and the
engine will stay unaware of them.

## Why No Single Diagram

A "one big state diagram" view does not fit, and should not be drawn:

1. **Combinatorics.** The full reachable system state is the product of
   5 truth domains. Even if each domain has only 4–5 distinguishable
   values, the Cartesian product is in the hundreds. Most of those
   combinations are unreachable by construction, but they're not
   unreachable by any explicit rule — they're unreachable because the
   reducer never produces them from legal event sequences.

2. **It invites anti-patterns.** Historically in the V2 lineage,
   attempts to draw one unified diagram led to two bad habits:
   - treating the diagram as the source of truth and adding code to
     "fix" mismatches, rather than fixing the diagram — this is how
     transport mechanics leak into semantics (A7)
   - using timer/ordering tricks to force desired transitions that
     the diagram showed but the reducer didn't naturally produce —
     this is how timer firings become semantic authority (A1) and
     ordering becomes policy (A4)

3. **The engine is a reducer, not an FSM.** `Apply(state, event) →
   (state, commands, projection)` is a pure function. The legal state
   space is the set of states reachable from the zero value through
   any sequence of legal events. That set is defined by the reducer,
   not by a diagram.

## What Replaces The Diagram

Small per-domain diagrams are fine and do fit — they live in review
comments and local sketches when needed. The durable source of truth
for "what states can this system be in" is not a diagram. It is:

1. **The reducer itself** — `core/engine/apply.go` is the full
   specification of legal transitions. If a state is unreachable, it
   is unreachable because no event sequence produces it, and that's
   visible in the code.

2. **Invariants** — constraint tests live beside the code:
   - `C8` same facts → same decision
   - `C9` timers only trigger observation, never decisions
   - `C10` engine state contains no transport/runtime types
   - `C12` executors cannot return decisions to the engine
   - Same-observation batch rule: multi-fact observations enter
     through `applyBatchAndExecute` atomically
     (see `core/adapter/adapter.go` ingress header)

3. **Conformance cases** — YAML event sequences with expected
   projections under `core/conformance/`. Each case pins one
   legal trajectory.

4. **Trace output** — every `Apply` emits a trace entry per decision
   step. For any concrete run, the trace is the diagram for that run.

## Sub-Diagrams That Do Fit

When a local view helps review, these per-dimension diagrams fit on
one page and are welcome:

- `Mode` transitions (4 nodes)
- `SessionPhase` lifecycle (linear: `none` → `starting` → `running` → `completed`/`failed`)
- `ProbeStatus` transitions (4 nodes)
- `RecoveryDecision` flowchart (R/S/H → one of 4 decisions)
- Event → command trigger matrix (12 × 6)

If you draw one of these for a review, drop it in a PR comment or an
adjacent short `.md` near the code. Don't try to extend it into the
full system — the full system belongs to the reducer.

## Extending The Surface

When adding an event, command, enum value, or truth domain:

1. Update the type in the corresponding `core/engine/*.go` file.
2. Update the count in the table above.
3. Update the reducer in `core/engine/apply.go`.
4. Add a conformance case covering the new transition.
5. If the new ingress produces multiple same-observation facts, route
   them through `applyBatchAndExecute` and add a contention test
   (see the adapter ingress rule).

Do not add a type without a reducer path. Do not add a reducer path
without a conformance case. The surface grows only when all three
move together.
