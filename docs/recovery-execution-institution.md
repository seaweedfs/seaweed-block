# Recovery Execution Institution

The recovery-execution institution is the bounded layer that turns
engine-issued recovery commands into real runtime lifecycle facts.
It owns session preparation, real execution start, cancellation,
timeout wiring, and terminal close callbacks. It does not own
recovery classification, failover decisions, topology authority,
or operator-facing publication.

This doc names what the institution owns, what it does not, and
what is explicitly carried forward to later phases.

## Files

| File | Role |
|---|---|
| `core/adapter/adapter.go` | Command preparation, lifecycle callback ingress, single route from engine commands to runtime execution |
| `core/adapter/executor.go` | `CommandExecutor` contract, including `SetOnSessionStart` and `SetOnSessionClose` |
| `core/adapter/normalize.go` | Runtime lifecycle facts to engine events: `SessionPrepared`, `SessionStarted`, `SessionClosed*` |
| `core/engine/apply.go` | Semantic lifecycle reducer for prepare / start / fail / complete / invalidate |
| `core/transport/executor.go` | Runtime session registry, conn ownership, invalidation, start/close signaling |
| `core/transport/catchup_sender.go` | Catch-up execution path; emits start only after real execution path begins |
| `core/transport/rebuild_sender.go` | Rebuild execution path; same lifecycle rule as catch-up |

## What the Institution Owns

1. **Command admission into execution** — engine-issued `StartCatchUp`,
   `StartRebuild`, and `InvalidateSession` become runtime actions
   through one adapter route.
2. **Prepared versus running split** — `SessionPrepared` is emitted
   when the adapter binds lineage and queues runtime work; `SessionStarted`
   is emitted only when the runtime actually begins execution.
3. **Bounded start timeout** — a prepared session that never reaches
   real execution is failed by the adapter's bounded start-timeout
   watchdog instead of remaining indefinitely in `starting`.
4. **Lifecycle callbacks** — the executor reports real start and
   terminal close through `SetOnSessionStart` and `SetOnSessionClose`.
5. **Session invalidation** — `InvalidateSession` removes the active
   session, closes its conn, and prevents stale delayed callbacks from
   becoming current truth.
6. **Fail-closed start / close behavior** — start failure, timeout,
   or invalidation must not fabricate `running` or `healthy`.
7. **Runtime-local ownership only** — the executor may own conn state,
   deadlines, and cancellation wiring, but not recovery policy.

## What the Institution Does NOT Own

The following belong to later phases. The recovery-execution institution
must not silently absorb them:

| Concern | Owner | Why not here |
|---|---|---|
| Choosing catch-up vs rebuild | engine | semantic policy, not execution lifecycle |
| Widening `targetLSN` or rewriting recovery meaning | engine | execution must honor the frozen command contract |
| Cross-session retry policy and backoff strategy | later P10 / P12 work | broader lifecycle policy than the current bounded institution |
| Live WAL streaming during rebuild | P10 / P12 | needs richer lifecycle orchestration than the current single-run route |
| Multi-replica coordination | P12 | replicated contract, not one replica's runtime lifecycle |
| Promotion / failover selection | P14 | topology authority |
| Epoch minting / endpoint authority | P14 | topology authority produced above the executor |
| Operator-facing health / repair controls | P15 | governance surface |

## Fail-Closed Contract

The current recovery-execution institution proves these boundaries:

| Scenario | Property proven | Proof |
|---|---|---|
| Session prepared but not yet started | projection stays `starting`; no fabricated `running` without a real start callback | `TestSessionStarted_ComesFromExecutorStartCallback` |
| Prepared session exceeds bounded start timeout | adapter emits `SessionClosedFailed(start_timeout)`; prepared does not hang forever | `TestPreparedNeverStarted_TimesOutAndFailsClosed` |
| Immediate executor start error | adapter converts the failure into `SessionClosedFailed`; lifecycle fails closed without a `started` event | `TestPreparedNeverStarted_FailsClosedOnImmediateStartError` |
| Delayed `SessionStarted` after failure | same-session `SessionStarted` is ignored once the session has already failed | `TestV3_Terminal_DelayedStartAfterFailureIgnored` |
| Delayed `SessionClosedCompleted` after failure | same-session completion is ignored; late success cannot revive a failed session | `TestV3_Terminal_DelayedCompleteAfterFailureIgnored` |
| Delayed `SessionClosedFailed` after completion | same-session late failure cannot un-complete an already-completed session | `TestV3_Terminal_DelayedFailureAfterCompletionIgnored` |
| Duplicate terminal close (completed twice) | second `SessionClosedCompleted` does not re-fire healthy commands or alter state | `TestV3_Terminal_DuplicateCompletedIgnored` |
| Duplicate terminal close (failed twice) | second `SessionClosedFailed` does not re-fire degraded commands or alter state | `TestV3_Terminal_DuplicateFailedIgnored` |
| Illegal phase transitions after terminal | every illegal event after `PhaseFailed` / `PhaseCompleted` leaves `st.Session` byte-for-byte unchanged | `TestV3_Terminal_InvalidPhaseTransitionsLeaveStateUnchanged` |
| Delayed stale start callback after newer assignment | old `SessionStarted` is rejected; it cannot advance the new session | `TestStaleStartCallback_OldSessionIgnoredAfterNewAssignment` |
| Delayed stale close callback after newer assignment | old `SessionClosed*` is rejected; it cannot make the new session healthy or failed | `TestStaleCallback_OldSessionIgnoredAfterNewAssignment` |
| Transport dial failure | executor emits failed close but no `SessionStarted`; command issuance alone does not count as running | `TestTransport_CatchUp_DialFailureDoesNotEmitStarted` |
| Session invalidation mid-run | invalidated session does not emit terminal close; runtime cancellation is semantically dead | `TestTransport_Rebuild_InvalidatedSessionStopsWithoutCallback` |
| Session invalidation during blocked ack read | invalidation unblocks `ReadMsg` by closing the conn; no spurious callback | `TestTransport_Rebuild_InvalidateInterruptsBlockedAckRead` |
| Late completion racing invalidate | a session that completes in parallel with invalidation still produces no callback — `finishSession` drops it | `TestTransport_Lifecycle_LateCompletionAfterInvalidateDropped` |

### Watchdog contract (P10)

The start-timeout watchdog lives in the adapter, not the transport.
Its rules are enumerated and separately proven:

| Rule | Property proven | Proof |
|---|---|---|
| Timer cleared by real start | a watchdog armed for session N is cleared when `OnSessionStart(N)` arrives; a later `AfterFunc` fire is a no-op | `TestWatchdog_ClearedByRealStart` |
| Timer cleared by immediate close | a session that closes before the timeout records `clear_closed`, not `fire` | `TestWatchdog_ClearedByImmediateClose` |
| Old timer cannot fail new session | a timer armed for session N, firing after session N+M is prepared, sees the mismatched active session and records `fire_noop` | `TestWatchdog_OldTimerCannotFailNewSession` |
| Invalidate before start — no false success | invalidating a prepared session (e.g., identity change) does not fabricate `SessionStarted` or `SessionClosedCompleted`; late callbacks from the stale session are traced as stale and rejected | `TestWatchdog_InvalidateBeforeStart_NoFalseStartNoFalseSuccess` |

Watchdog evidence is exposed via `adapter.WatchdogLog()` which
records `arm / clear_started / clear_closed / supersede / fire /
fire_noop` events per sessionID. This is execution evidence for
tests and diagnostic dumps, not an operator product surface.

### The truth split

- `SessionPrepared` means the engine accepted one bounded recovery contract.
- `SessionStarted` means runtime execution actually began.
- `SessionClosedCompleted` / `SessionClosedFailed` are the only terminal lifecycle truth.
- The watchdog is the adapter's sole bounded means of escaping a
  never-starting prepared session; once a session is past `PhaseStarting`
  the watchdog can no longer act on it.

## Reading Order

For someone new to this institution:

1. `core/adapter/executor.go` — lifecycle contract surface
2. `core/adapter/adapter.go` — command preparation and callback ingress
3. `core/engine/apply.go` — semantic lifecycle reducer
4. `core/transport/executor.go` — session registry and invalidation
5. `core/transport/catchup_sender.go` and
   `core/transport/rebuild_sender.go` — where real execution start is admitted
6. `core/adapter/adapter_test.go` and
   `core/transport/transport_test.go` — lifecycle proof set

## What is NOT proven here

P10 does not claim any of the following, and its tests must not be
read as evidence for them:

- **Who decides to retry** — the engine may re-emit a `Start*`
  command when new facts arrive; P10 does not define when those
  new facts arrive, nor any cross-session retry schedule.
- **Who decides `catch_up` vs `rebuild`** — this is engine policy,
  not an execution-lifecycle concern.
- **`targetLSN` widening or rewriting** — the executor must honor
  the frozen command; no P10 rule changes this.
- **Multi-replica lifecycle coordination** — a second replica's
  session is a separate adapter instance; P10 rules apply per
  adapter, not across a replica set.
- **Failover-policy-driven session kills** — the engine's
  `InvalidateSession` today is driven only by identity change
  (assignment / removal). Policy-driven invalidation (e.g., a
  placement decision) belongs to P14.
- **Operator-visible retry counters or repair surfaces** —
  watchdog evidence is internal; no operator surface exists yet.

## Carry-Forward

Items the recovery-execution institution leaves explicitly to later phases:

- **Later P10 / operational hardening** — per-session timeout
  classification (dial vs handshake vs ship vs barrier), cross-session
  retry/backoff policy, and live-WAL-during-rebuild orchestration.
- **P12 (bounded replicated failover contract)** — rejoin contract,
  multi-replica lifecycle coordination, and bounded takeover guarantees.
- **P14 (topology / failover policy)** — epoch authority, promotion,
  failover selection, and replica placement truth. Policy-driven
  invalidation (non-identity) enters here.
- **P15 (operator-facing governance surface)** — operator controls for
  retire / repair / rebalance and their publication semantics, plus
  any operator-visible view of the watchdog evidence log.
