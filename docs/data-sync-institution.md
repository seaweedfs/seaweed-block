# Data Sync Institution

The data-sync institution is the bounded layer that moves bytes
between primary and replica during catch-up and rebuild. It owns
the wire protocol, the lineage gate, and the achieved-frontier
report. It does not own recovery classification, session lifecycle
policy, failover decisions, or operator-facing publication.

This doc names what the institution owns, what it does not, and
what is explicitly carried forward to later phases.

## Files

| File | Role |
|---|---|
| `core/transport/protocol.go` | Wire format: message types, `RecoveryLineage`, `ShipEntry`, `ProbeResponse`, encoders / decoders |
| `core/transport/barrier.go` | Typed `BarrierResponse{AchievedLSN}`, `sendBarrierReq`, `recvBarrierResp` |
| `core/transport/executor.go` | Session lifecycle (register / attach conn / invalidate / finish), probe path |
| `core/transport/catchup_sender.go` | Primary-side catch-up byte movement |
| `core/transport/rebuild_sender.go` | Primary-side rebuild byte movement |
| `core/transport/replica.go` | Replica-side per-message-type handlers, lineage gate |

The split is deliberate: each sender owns its own ship loop so the
two protocols stay distinct on the page, even though their inner
loops look similar today.

## What the Institution Owns

1. **Byte movement** — sending blocks from primary store to replica
   store, in deterministic LBA order.
2. **Wire protocol** — frame types, encoders, decoders, length
   prefixes, lineage tuple serialization.
3. **Lineage gate** — `acceptMutationLineage` rejects any mutating
   frame whose lineage is less authoritative than the currently
   active one, or whose required fields are zero.
4. **Achieved-frontier report** — every barrier exchange returns a
   typed `BarrierResponse{AchievedLSN}`. The replica reports what
   `Sync()` actually returned; the primary returns that value
   verbatim to the engine.
5. **Per-conn deadlines** — every recovery write and read sets a
   bounded deadline so a silent peer cannot park the sender.
6. **Cancel / invalidation** — `InvalidateSession` closes the conn
   directly so a parked `ReadMsg` unblocks immediately rather than
   waiting for TCP timeout.
7. **Probe** — observation-only request/response that returns the
   replica's R/S/H boundaries. Carries no lineage, never mutates.

## What the Institution Does NOT Own

The following belong to later phases. The data-sync institution
must not silently absorb them:

| Concern | Owner | Why not here |
|---|---|---|
| Choosing catch-up vs rebuild | engine (P9 inherits) | semantic policy, not byte movement |
| Session start / cancel / complete lifecycle as semantic events | P10 | execution lifecycle, not data plane |
| Timeout / retry policy across sessions | P10 | lifecycle decision, not per-write deadline |
| Live WAL streaming during rebuild | P10 / P12 | requires lifecycle states the data plane has no business knowing |
| Resumable rebuild with bitmap progress | P10 / P12 | progress state outlives a single byte-movement run |
| Fan-out to N replicas (RF≥3) | P12 | replicated topology, not single-replica byte movement |
| Promotion / demotion / failover selection | P14 | topology authority |
| Epoch minting | P14 | topology authority — engine consumes, P14 produces |
| Operator-facing health publication | P15 | governance surface |

## Fail-Closed Contract

The institution's fail-closed contract — proven in
`core/transport/failclosed_test.go`:

| Scenario | Property proven |
|---|---|
| Overlapping sessions | A higher-lineage session bumps `activeLineage`; subsequent frames from the older session are rejected at the data plane |
| Primary crash mid-rebuild | A subsequent higher-lineage session converges; achieved frontier reflects the new session, data matches the new primary |
| Replica disconnects before BarrierResp | Primary surfaces `Success=false` with `AchievedLSN=0`; never fabricates a frontier |
| Stale Ship after newer Rebuild | Cross-message-type lineage check rejects the stale Ship even though Rebuild was the lineage source |
| Probe during rebuild | Probe is observation only; does not disturb `activeLineage`; subsequent rebuild frames continue to be accepted |

The barrier wire contract — proven in
`core/transport/barrier_test.go`:

- `BarrierResponse` roundtrip preserves `AchievedLSN` exactly.
- A short barrier payload returns an error; the primary cannot
  fabricate a frontier on a malformed reply.

## Reading Order

For someone new to this institution:

1. `core/transport/protocol.go` — wire shapes
2. `core/transport/barrier.go` — the typed achieved-frontier seam
3. `core/transport/replica.go` — `acceptMutationLineage` and the
   per-message handlers
4. `core/transport/catchup_sender.go` and
   `core/transport/rebuild_sender.go` — primary-side flows
5. `core/transport/failclosed_test.go` — the institution's
   negative-space proof

## Carry-Forward

Items the data-sync institution leaves explicitly to later phases:

- **P10 (recovery execution lifecycle)** — multi-attempt retries,
  per-session timeout policy, structured session phases
  (accepted / running / completed / failed) beyond what the wire
  needs, live-WAL-during-rebuild orchestration.
- **P12 (bounded replicated failover contract)** — fan-out to N
  replicas, rejoin contract, bitmap-based resumable rebuild.
- **P14 (topology / failover policy)** — epoch authority, who
  promotes, when to fail over, replica placement.
- **P15 (operator-facing governance surface)** — operator commands
  for retire / repair / rebalance.
