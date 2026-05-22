# Finished Plan: Phase 22 - ManagedVolume Operations Model

Status: closed for Phase 22 scope after the ManagedVolume model, operations
surface alignment, protocol ledger stamp, and close report passed their scoped
regression.

Close report:

- `internal/docs/qa-assignments/managed-volume-operations-model-close-report.md`

Previous closed capability:

- `finished-plans/phase20_finishedplan_activation_day1_ops_mvp.md`
- Helm activation gates were green before this model-hardening follow-on began;
  Phase 21 should still get its own close artifact if release packaging needs a
  separate Helm install milestone.

## Product Question

Can Seaweed Block represent a Kubernetes PVC-backed block volume as one product
entity, so operations, dashboard, failover, CSI, iSCSI, and future NVMe logic do
not keep spreading across scripts, listeners, and unrelated small state
machines?

## Answer

Yes for the read-side product model.

Phase 22 introduced `ManagedVolume` as the internal projection for the volume a
Kubernetes user thinks they created:

```text
PVC/PV/StorageClass intent
-> ManagedVolume facts
-> Kubernetes, CSI, authority, replica, host-path, recovery, workload state
-> ops/report/explain/dashboard-ready output
-> read-only/dry-run action contract
```

This is not a lifecycle owner. Ownership remains:

- Kubernetes owns PVC/PV lifecycle.
- CSI owns publish/stage lifecycle.
- launcher/future operator owns generated `blockvolume` workload lifecycle.
- blockmaster owns authority, epoch, endpoint version, promotion, and
  fail-closed decisions.
- host/kernel owns iSCSI/NVMe path mechanics.
- ManagedVolume owns correlation, projection, reason codes, evidence refs, and
  user-facing explanation.

## Delivered

- `internal/docs/protocol/` now records protocol design principles, engine
  guidelines, anti-patterns, invariant ledger, and the Phase 22 model plan.
- `core/ops/managed_volume_model.go` defines typed facts, multi-state
  projection, status priority, non-claims, and read-only/dry-run action
  contracts.
- `ClusterEvidence` includes additive `managed_volumes` JSON output.
- `sw-block ops cluster`, `ops report`, and `ops explain` consume the shared
  projection instead of separately recomposing product state.
- Existing bundle artifacts can replay into facts:
  - node-loss recovery summary -> CSI reattach recovery projection,
  - Stage 2 primary-failure summary -> transparent host-path recovery
    projection.
- Protocol ledger rows were promoted to `ACTIVE`:
  - `INV-MANAGED-VOLUME-READMODEL-001`,
  - `INV-CONTROL-CONTEXT-001`,
  - `INV-K8S-ADAPTOR-FACTS-001`,
  - `INV-HOSTPATH-FACTS-001`.

## Covered States

Healthy and recovered:

- first-volume writer/reader verified,
- RF3 CSI/pod-recreate node-loss recovery,
- Stage 2 same-pod iSCSI ALUA/dm-multipath transparent recovery.

Blocked and unsafe:

- loopback publish target across Kubernetes nodes,
- PVC Pending,
- writer mount failure,
- CSI node image-pull failure,
- host path not multipathed when transparent recovery is expected,
- multiple primary replicas observed.

Non-claims:

- transparent failover is not claimed from host-path evidence alone; same-pod
  workload verification is required.
- NVMe ANA fields are a schema seam only; no NVMe recovery claim is inferred.

## Validation

Scoped regression:

```text
go test ./cmd/sw-block ./core/ops -count=1
go test ./cmd/sw-block ./core/ops ./core/csi ./core/launcher ./core/host/master -count=1
```

TDD files:

- `core/ops/managed_volume_model_test.go`
- `core/ops/managed_volume_evidence_test.go`
- `core/ops/managed_volume_artifact_test.go`
- `core/ops/observation_report_test.go`
- `core/ops/observation_bundle_test.go`
- `cmd/sw-block/main_test.go`

Full repository regression was attempted and exposed two unrelated failures in
untouched packages:

- `cmd/sparrow`: `TestRunSparrow_AllThreePathsPass`
- `core/frontend/iscsi`: `TestT2Process_ISCSI_ReopenAfterMove_ServesNewLineage`

Those should be tracked separately before treating full-repo green as a release
gate.

## Non-Claims

Phase 22 does not deliver:

- new failover behavior,
- mutating operator lifecycle,
- repair/rebuild/failback,
- backup/snapshot/restore,
- hosted dashboard,
- production SLOs,
- NVMe ANA parity.

## Next

Recommended Phase 23: Operations Surface / Dashboard / Operator-readiness.

Use ManagedVolume as the semantic core for:

- `kubectl get`-readable Conditions,
- read-only dashboard,
- product timeline and support bundles,
- operator Events,
- future safe mutating workflows after separate gates.
