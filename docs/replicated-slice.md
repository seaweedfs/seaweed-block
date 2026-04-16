# Bounded Replicated Durable Slice

The replicated-durable-slice institution is the first bounded
product capability above single-node operation. It proves one
old-primary → new-primary → rejoin path that converges, rejects
stale lineage at every layer, and leaves the replica consistent
with the new primary's data.

This is mechanism, not policy. P12 does not decide who should be
the new primary or when failover should trigger — it proves the
handoff works when those decisions arrive from above.

## The bounded route

One statement:

> Given an external reassignment `(replicaID, epoch+1, new addresses)`,
> the engine clears any in-flight session, invalidates stale lineage,
> re-probes the replica, decides catch-up or rebuild based on R/S/H
> facts, and converges to healthy. When recovery is needed (R < H),
> the new primary's mutating traffic establishes the new epoch on the
> replica, and the old primary's stale execution is rejected by the
> lineage gate. When the replica is already caught up (R ≥ H), no
> mutating traffic is sent and the replica's lineage gate is NOT
> advanced — see "Known limitation" below.

### Steps

| Step | What happens | Who acts |
|---|---|---|
| 1. Master reassigns | sends `AssignmentObserved(epoch+1)` to the new primary's adapter | external authority (P14 scope) |
| 2. Old primary demoted | receives explicit `ReplicaRemoved` from external authority | external authority |
| 3. Engine clears | epoch bump → `InvalidateSession` + clear session/reachability/recovery | engine (deterministic) |
| 4. Probe | new primary probes replica to discover R/S/H | executor (data-sync institution) |
| 5. Decide | engine's `decide()` classifies catch-up or rebuild | engine (semantic authority) |
| 6. Execute | catch-up or rebuild ships blocks at new epoch | executor (recovery-execution institution) |
| 7. Converge | session completes, replica in-sync with new primary | all layers |

### Authority boundary

| Decision | Owner | Notes |
|---|---|---|
| Who becomes new primary | **P14** (not this phase) | P12 consumes `(replicaID, epoch+1)` as input |
| When to trigger failover | **P14** (not this phase) | P12 executes, doesn't decide |
| What recovery class to use | **engine** | R/S/H → catch-up or rebuild |
| How to move bytes | **data-sync institution** | lineage-gated, barrier-verified |
| How to manage session lifecycle | **recovery-execution institution** | prepared/start/timeout/close |
| How to reject stale old-primary traffic | **lineage gate** (transport layer) | epoch comparison at replica ingress |

## What this proves

Four handoff tests in `core/adapter/handoff_test.go`:

| Test | What it proves |
|---|---|
| `TestHandoff_OldPrimaryToNewPrimary_Converges` | Full route: A (epoch=1) reaches healthy; B (epoch=2) probes, recovers, converges. Replica data matches B. |
| `TestHandoff_StaleOldPrimaryTraffic_Rejected` | After B's epoch=2 **recovery traffic** establishes lineage on the replica, stale epoch=1 frames are rejected at the data plane. Requires B to have sent at least one mutating message — does NOT cover the R≥H (no-recovery) branch. |
| `TestHandoff_RejoinAfterNewPrimary_DataConsistent` | After handoff, every LBA the new primary wrote is byte-identical on the replica. |
| `TestHandoff_OldPrimaryDemoted_NoResurrection` | During an active session, `ReplicaRemoved` clears the session; late `OnSessionStart` and `OnSessionClose` callbacks from the old executor cannot resurrect a healthy projection or re-enter a running phase. |

These tests use two real adapters + two real transport executors
sharing one replica listener — no mocks. They prove the lower
institutions (lineage gate, session lifecycle, data sync) compose
into a working handoff at the product level.

## Durability claim

**Bounded:** all writes that the new primary successfully replicates
(barrier ack'd by the replica after catch-up or rebuild) are durable
on the replica's store. The replica converges to the new primary's
complete data set.

**Not claimed:** writes that were in-flight on the old primary at the
moment of reassignment and had not yet been barrier-ack'd by the
replica may be lost. The new primary's truth is authoritative after
handoff.

## Known limitation

**If the new primary's probe finds the replica already caught up
(R ≥ H), no mutating traffic is sent.** The replica's lineage gate
stays at the old epoch because probes are lineage-free. Stale
old-epoch traffic with a higher sessionID could still land.

In practice this is low risk because:
- The old primary's adapter has been demoted (removal clears sessions)
- SessionIDs are monotonic within one process, so a truly "old" session
  has a lower sessionID than anything the new primary would mint
- The scenario requires a delayed old-epoch frame arriving AFTER the
  old primary has been told to stop but BEFORE the new primary sends
  any mutating traffic — a narrow window

If this becomes a real concern (e.g., in a multi-replica or
cross-datacenter topology), the fix is straightforward: have the
new primary emit a no-op barrier at its epoch even when no recovery
is needed, so the replica's lineage gate advances proactively.
This is deferred as a P14 concern because it requires knowing when
to fence beyond identity-driven invalidation.

## What this does NOT prove

| Concern | Owner | Why not here |
|---|---|---|
| Who decides to reassign | P14 | topology authority |
| When failover should trigger | P14 | failover policy |
| Epoch minting / authority governance | P14 | topology authority |
| Multi-replica (RF ≥ 3) coordination | P14 | broader topology |
| Launch readiness / release criteria | P13 | production hardening |
| Operator controls (retire / repair / rebalance) | P15 | governance surface |

## Carry-forward

- **P13 (production hardening)** — release criteria for the
  replicated slice: timeout tuning, error-budget gates, operator
  runbook for manual failover.
- **P14 (topology / failover policy)** — epoch authority, automated
  failover trigger, proactive lineage fence on caught-up probe,
  placement/rebalance decisions, multi-replica coordination.
- **P15 (operator-facing governance surface)** — operator commands
  for failover, rejoin, retire, repair; production observability
  beyond what the single-node surface provides.
