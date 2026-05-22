# QA Close - Phase 22 ManagedVolume Operations Model

Verdict: PASS for Phase 22 scope. Recommended close after review.

Progress at close: about 86%.

This close covers the read-side ManagedVolume model, protocol ledger stamp,
operations/report integration, artifact replay, and action-contract seed. It
does not claim new HA behavior, mutating operator lifecycle, repair/rebuild, or
dashboard controls.

## Product Claim

Seaweed Block can now represent a Kubernetes PVC-backed block volume as one
internal product entity:

```text
PVC/PV/StorageClass intent
-> ManagedVolume typed facts
-> Kubernetes, CSI, authority, host-path, recovery, workload projections
-> ops cluster/report/explain/dashboard-ready JSON
-> read-only/dry-run action contract
```

This is a read model and orchestration context. It does not replace Kubernetes
PVC/PV ownership, CSI publish/stage ownership, blockmaster authority/promotion
ownership, launcher/operator workload ownership, or host/kernel path ownership.

## Hard Gate Table

| Gate | Requirement | Result |
|---|---|---|
| HG-0 | Protocol design reference exists under `internal/docs/protocol/` | PASS |
| HG-1 | ManagedVolume has typed facts for PVC/K8s/CSI/authority/replica/host/workload | PASS |
| HG-2 | Projection has stable multi-state dimensions and status priority | PASS |
| HG-3 | Existing live-gate cases are represented by model tests | PASS |
| HG-4 | Blocked K8s states are modeled without minting authority | PASS |
| HG-5 | iSCSI ALUA/multipath transparent recovery requires host-path + workload evidence | PASS |
| HG-6 | NVMe ANA is a schema seam only; no NVMe recovery claim inferred | PASS |
| HG-7 | Non-claims are derived when evidence is insufficient | PASS |
| HG-8 | Action hints are read-only or dry-run and name owner executor / invariant refs | PASS |
| HG-9 | `sw-block ops cluster/report/explain` share the ManagedVolume projection | PASS |
| HG-10 | Bundle artifacts replay into ManagedVolume facts | PASS |
| HG-11 | Protocol ledger rows promoted from STUB to ACTIVE with concrete tests | PASS |
| HG-12 | Scope regression passes for ops/CSI/launcher/master surfaces | PASS |

## TDD Evidence

Tests added or extended:

- `core/ops/managed_volume_model_test.go`
- `core/ops/managed_volume_evidence_test.go`
- `core/ops/managed_volume_artifact_test.go`
- `core/ops/observation_report_test.go`
- `core/ops/observation_bundle_test.go`
- `cmd/sw-block/main_test.go`

Covered cases:

- healthy Helm first-volume projection,
- loopback cross-node attach blocked,
- PVC Pending blocked,
- writer mount failure blocked,
- CSI node image-pull blocked,
- RF3 CSI/pod-recreate node-loss recovery,
- Stage 2 iSCSI ALUA/multipath transparent recovery,
- missing multipath blocks transparent claim,
- NVMe ANA schema seam without recovery claim,
- fact-order independence,
- action invariant refs and executor boundary,
- transparent-failover non-claim without same-pod workload proof,
- product bundle artifact replay for node-loss and Stage 2 summaries,
- report/explain/JSON output containing `managed_volumes`.

## Internal Review

Truth owner boundary:

- Kubernetes facts remain observations of PVC/PV/Pod/Node state.
- CSI facts remain observations of publish/stage/reattach.
- Master facts remain authority, epoch, endpoint version, and publish target.
- Host-path facts remain iSCSI/multipath/ALUA observations.
- Workload facts remain writer/reader checksum evidence.
- ManagedVolume correlates and projects; it does not mint authority.

Action boundary:

- Phase 22 emits only `read_only` or `dry_run` actions.
- Actions include side-effect class, owner executor, preconditions, invariant
  refs, and evidence refs.
- No kubectl write, host mutation, promotion RPC, repair, cleanup, or operator
  reconciliation path was added.

Claim boundary:

- Transparent host-path recovery requires same-pod workload checksum,
  multipath/ALUA host-path evidence, and stale-primary fencing.
- Node-loss recovery requires CSI reattach and reader checksum evidence.
- Host-path evidence alone emits a non-claim rather than a recovery claim.

## Regression

Scope regression passed:

```text
go test ./cmd/sw-block ./core/ops -count=1
go test ./cmd/sw-block ./core/ops ./core/csi ./core/launcher ./core/host/master -count=1
```

Broader repository regression was also attempted:

```text
go test ./... -count=1
```

That broader run failed in packages not touched by Phase 22:

- `cmd/sparrow`: `TestRunSparrow_AllThreePathsPass`
- `core/frontend/iscsi`: `TestT2Process_ISCSI_ReopenAfterMove_ServesNewLineage`

Targeted reruns reproduced both failures. They are not introduced by the
ManagedVolume changes because Phase 22 does not modify `cmd/sparrow` or
`core/frontend/iscsi`. Track them separately before using full-repo green as a
release gate.

## Non-Claims

Phase 22 does not claim:

- new failover behavior,
- new Kubernetes operator lifecycle,
- mutating admin actions,
- repair/rebuild/failback,
- backup/snapshot/restore,
- hosted dashboard,
- production SLOs,
- NVMe ANA parity.

## Verdict

PASS for Phase 22 scope. The product now has a tested internal ManagedVolume
read model and protocol discipline strong enough for Phase 23 Operations /
dashboard / operator-conditions work to depend on it.

Recommended next action: move Phase 22 to `finished-plans/` after review, then
start Phase 23 Operations Surface / Dashboard / Operator-readiness work.
