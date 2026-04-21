# T0 Port Audit

Date: 2026-04-20
Status: tracked; updated with every T0 patch that touches a V2 file
Purpose: satisfy PG4 (`v3-phase-15-qa-system.md`) — every V2 file referenced by T0 must have explicit ported-function vs rejected-function lists

## 1. Rule

For every V2 source file that the T0 patch touches, consults, or claims to port, this doc lists:

1. **Ported functions** — V2 functions whose structure is copied (as mechanism) into the target file.
2. **Consulted functions** — V2 functions read for shape/idea but NOT copied.
3. **Rejected functions** — V2 functions explicitly NOT ported; the rejection reason must name a boundary rule.

If a V2 file appears in T0 commit diffs (even indirectly referenced) and is not listed here, T0 close fails PG4.

## 2. `weed/server/block_heartbeat_loop.go` (177 LOC)

Target: port loop shape into `core/host/volume/heartbeat_loop.go`.

**Ported**:
- `BlockHeartbeatLoop.Run` outer cadence (ticker-driven periodic collection + send).
- `BlockHeartbeatLoop.collectAndSend` shape (gather local slot facts, translate to wire, send unary).

**Consulted (not copied)**:
- Shutdown/context-cancellation pattern.
- Backoff-on-error shape.

**Rejected**:
- Any V2 path that writes back authority / role / assignment from the heartbeat-ack side. heartbeats carry observation only (sketch §3 load-bearing).
- V2's coupled "heartbeat timing → failover" shortcuts.

## 3. `weed/storage/blockvol/block_heartbeat.go`

Target: port local-collector shape into `core/host/volume/collect.go`.

**Ported**:
- Replica-inventory walk → per-slot `HeartbeatSlot` fill pattern.

**Consulted (not copied)**:
- V2 `LocalRoleClaim` populating convention (the block system preserves the field but the `LocalRolePrimary` value NEVER writes authority — already enforced at the observation-layer `ReasonConflictingPrimaryClaim` rule from P14 S4).

**Rejected**:
- V2 paths that consult local role state to DECIDE primary. the block system consults local role state only to REPORT it raw (observation fact, never authority).

## 4. `weed/storage/blockvol/block_heartbeat_proto.go`

Target: cross-check that `authority.HeartbeatMessage` field set matches V2 proto.

**Ported**:
- Field names + types already ported into `core/authority/observation_wire.go` in P14 S4. T0 adds no new fields; T0 only serializes the existing `HeartbeatMessage` over the T0 HTTP/JSON transport.

**Rejected**:
- V2 extension fields that carry role-as-authority semantics. None of those fields exist in `authority.HeartbeatMessage` — already filtered at S4 port time.

## 5. `weed/server/master_grpc_server_block.go` (600 LOC)

Target: port the heartbeat-ingress RPC handler SHAPE into `core/host/master/observation_handler.go`.

**Ported**:
- Handler pattern: parse request → translate to `authority.HeartbeatMessage` → call `ObservationHost.Ingest` → return ack.

**Consulted (not copied)**:
- Sender-verification pattern (matches expected `ServerID`). T0 ships a minimal form; full authn is T4.

**Rejected**:
- V2's `HandleAssignment` endpoint — NEVER ported. The block master never receives an assignment-mutation RPC; the one-way flow is observation in, assignment out.
- V2 `reportBlockVolume` paths that invoke `promote` / `demote` / local-role-based failover.
- Any RPC endpoint that accepts `Epoch` / `EndpointVersion` from a volume-side caller.

## 6. `weed/server/master_block_observability.go` / `master_block_evidence.go`

Target: consulted for `EvidenceService.QueryVolumeStatus` response shape.

**Ported**:
- Projection-fact + evidence-reason response shape (read-only).

**Rejected**:
- V2 fields that reference V2 role semantics (`Primary`, `Candidate`, promotion timers). Evidence is adapter projection + controller unsupported-evidence + convergence-stuck — all P14-era shapes only.

## 7. Files NOT ported in T0 first patch

Listed per assignment §4 denylist + sketch §7.3. None of these enter the T0 diff:

- `weed/storage/blockvol/promotion.go`
- V2 master paths implementing `HandleAssignment` / `promote` / `demote`
- V2 paths where heartbeat timing writes authority
- V2 paths where a volume-local role claim becomes authority

## 8. V2 files referenced by T0 design docs but NOT ported at all

These appear in the T0 sketch §7.1–7.2 or this audit's cross-refs but carry no ported functions. Listing for traceability:

- `weed/server/volume_server_block.go` (1891 LOC) — consulted only as reference for volume-side state machine shape; no functions ported in the first T0 patch. A later T0-follow-up may port specific subsets if L2/L3 tests surface a gap.
- `weed/server/volume_grpc_block.go` (207 LOC) — consulted only as reference for volume-side RPC client conventions.

## 9. Audit change log

| Date | V2 file | Action | T0 target |
|---|---|---|---|
| 2026-04-20 | `weed/server/block_heartbeat_loop.go` | loop-shape ported | `core/host/volume/heartbeat_loop.go` |
| 2026-04-20 | `weed/storage/blockvol/block_heartbeat.go` | collector shape ported | `core/host/volume/collect.go` |
| 2026-04-20 | `weed/server/master_grpc_server_block.go` | handler shape ported (heartbeat only) | `core/host/master/observation_handler.go` |
| 2026-04-20 | all others in §7 denylist | REJECTED | — |

Future T0 follow-ups update this table before the patch lands.
