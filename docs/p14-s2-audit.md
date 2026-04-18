# P14 S2 — Scoped Audit (Authority Path)

This is the audit note for P14 S2. It is pure observation of the V3
repo at the time of writing. No design here, no code. The design note
is `p14-s2-design.md`.

The audit answers five questions about how `assignment` / `epoch` /
`endpointVersion` travel through the system today, plus a sixth about
which V2 pieces are mechanically portable to V3.

## Sharpened constraints honored in the audit

1. S2 is *authoritative publication*, not policy source. Findings are
   tagged mechanism vs policy-shaped.
2. Authority must be a system-owned route. Findings explicitly flag
   hidden-operator-path smells (renamed test helper, CLI/admin manual
   entry, volume-local convenience path).
3. The architect's owner-placement bias is thin-control-plane layer
   outside the volume body — NOT inside adapter or volume server.
4. V2 port inventory splits `portable-mechanism` vs `policy-tangled`.

## Q1 — Producers of `assignment` / `epoch` / `endpointVersion`

**Real-system code.** None today. There is no production path in V3
that mints or observes an assignment from outside the process.

- `engine.applyAssignment` mutates identity at `core/engine/apply.go:171-174`
  but only after the adapter has already normalized an `AssignmentInfo`
  coming from *some* caller. Today, the callers are all tests.
- `engine.applyProbeSucceeded` binds observed endpoint/transport epoch
  at `core/engine/apply.go:258-259`. This observes what the transport
  reports, not a minted truth.
- `engine.applyFenceCompleted` bumps internal `FencedEpoch` at
  `core/engine/apply.go:509`. Internal gate, not authority.

**Test / scenario code.**

- `harness.assign(epoch, endpointVersion)` in
  `core/calibration/harness.go:87-92`.
- Scenario driver in `core/calibration/scenarios.go:64`.
- Declarative conformance via `core/conformance/cases.yaml`, decoded
  by `core/schema/convert.go:13-20`.
- Adapter unit tests at `core/adapter/adapter_test.go` (many call
  sites) construct `AssignmentInfo` inline.

**Hidden-operator-path candidates.** None. The ops HTTP surface is
read-only: mutation verbs rejected at `core/ops/server.go:46-63`. There
is no CLI flag, admin endpoint, or debug hook in `cmd/sparrow/main.go`
that mints or modifies epoch / assignment.

## Q2 — Publishers

No multi-subscriber publisher exists. Every site calls
`VolumeReplicaAdapter.OnAssignment(...)` directly
(`core/adapter/adapter.go:103-105`). No registry, bus, store, or
watcher. This is greenfield for the publication path.

## Q3 — Consumers beyond the adapter

- Transport executor embeds epoch in outbound `RecoveryLineage`:
  `core/transport/executor.go:75-86`.
- Transport replica validates lineage epoch on inbound:
  `core/transport/replica.go:186`.
- Ops `/projection` surfaces epoch / endpointVersion read-only:
  `core/ops/handlers.go:45-48`, `core/engine/projection.go:20-26`.
- Storage does NOT embed assignment in persisted state. (V2
  `weed/storage/blockvol/blockvol.go:71-72` persists epoch for fencing;
  V3 does not.)

No policy-adjacent reader exists.

## Q4 — Test-injection / manual-driving inventory

All three recognized paths:

- Calibration scenarios (`harness.assign`).
- Conformance YAML cases.
- Adapter and mock tests.

Hidden-operator-path screen: clean.

- No test helper wears a production name.
- No CLI / admin API accepts an `AssignmentInfo`.
- No `vol.SetEpoch()` convenience shortcut exists inside the adapter
  or engine that bypasses the assignment ingress.

Because there is no hidden-operator-path today, S2 does *not* have a
demotion problem. It has a **build-the-real-route** problem.

## Q5 — Existing authority-adjacent surfaces

None in V3. The categorization below applies the owner-placement bias:

| Existing surface | Category | Why |
|---|---|---|
| `engine` | Not an authority | Pure reducer. Accepts events. Not a decision owner. |
| `adapter` | Volume-local-shaped affordance | Sits inside sparrow volume body; owns local ingress. Using it as the authority owner would conflate topology authority with replica-local execution — explicitly NOT preferred. |
| `ops` (read-only) | Read-only inspection | Rejects mutations. Not a control path. |
| (new thin control-plane layer) | Control-plane-shaped gap | Preferred S2 landing spot. Greenfield. |

S2 must create a new thin layer outside the volume body.

## Q6 — V2 port inventory

Source: `C:\work\seaweedfs`.

| V2 piece | File | Category | Note |
|---|---|---|---|
| `BlockVolumeHeartbeatCollector` | `weed/server/block_heartbeat_loop.go:16-34` | portable-mechanism | Status aggregation + assignment fetch loop. No policy. |
| `SetAssignmentSource()` | `weed/server/block_heartbeat_loop.go:67-71` | portable-mechanism | Callback wire-up. Pure mechanism. |
| `ProcessBlockVolumeAssignments()` | `weed/storage/store_blockvol.go:129-145` | portable-mechanism | Per-volume apply. Mechanism only. |
| `BlockVolumeAssignment` struct | `weed/storage/blockvol/block_heartbeat.go:49-61` | portable-mechanism | Data shape (epoch, role, replica addrs). |
| `BlockVolumeInfoMessage` struct | `weed/storage/blockvol/block_heartbeat.go:10-38` | portable-mechanism | Heartbeat message shape. |
| `vol.SetEpoch()` / `vol.SetRole()` | `weed/storage/blockvol/blockvol.go:71-74` | portable-mechanism | Low-level bind. Mechanism only if authority is outside. |
| `HandleAssignment()` | `weed/storage/blockvol/promotion.go:19-69` | policy-tangled | Inline promotion/demotion decision, shipper stop, lease revoke. Volume-local convenience ownership. **Do not port.** |
| `promote()` / `demote()` | `weed/storage/blockvol/promotion.go:71-137` | policy-tangled | Role state machine with drain semantics. **Do not port.** |

## Summary

- **V3 is additive-greenfield for authority.** No pre-existing path to
  dismantle, no hidden operator surface to demote.
- **Publication is also greenfield.** S2 must invent a real
  publication route — a system-owned layer that calls
  `adapter.OnAssignment(...)` on subscribers, replacing today's
  test-only call sites with a genuine producer.
- **V2 port scope is limited to shapes and low-level bind
  machinery.** The V2 promotion/demotion state machine is
  policy-tangled and does not belong in S2.
- **Owner lands outside the volume body.** The architect's bias maps
  cleanly onto the greenfield shape.

S2 builds exactly one new thing and does not disturb any existing
path: a thin authority publisher that adapters subscribe to and
consume via the existing `OnAssignment` ingress.
