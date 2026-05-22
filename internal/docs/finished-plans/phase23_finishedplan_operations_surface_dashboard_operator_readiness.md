# Finished Plan: Phase 23 - Operations Surface / Dashboard / Operator-Readiness

Status: closed for Phase 23 scope after Conditions, report/explain alignment,
operator-readiness contract, replay gate, and scope regression passed.

Close report:

- `internal/docs/qa-assignments/operations-surface-dashboard-operator-readiness-close-report.md`

Previous closed capability:

- `finished-plans/phase22_finishedplan_managed_volume_operations_model.md`

## Product Question

Can a Kubernetes user or operator understand volume readiness, blockers,
recovery state, host-path state, and next safe action from product-owned
surfaces without SSHing into every node or reading raw logs?

## Answer

Yes for the read-only operations surface.

Phase 23 took the Phase 22 ManagedVolume model and made it visible in the
surfaces users, support, dashboard, AI, and a future operator will consume:

```text
ManagedVolumeProjection
-> Conditions
-> report summary / HTML
-> ops explain text
-> operator-readiness contract
-> replayable support-bundle evidence
```

## Delivered

- `ManagedVolumeProjection.conditions`
- additive `ObservationCondition.evidence_refs`
- report summary lines:
  - `managed_volume_condition=...`
- report HTML section:
  - `Managed Volume Conditions`
- explain text lines:
  - `managed_volume_condition ...`
  - `managed_volume_condition_evidence ...`
  - `managed_volume_action_preconditions ...`
  - `managed_volume_action_invariants ...`
  - `managed_volume_action_evidence ...`
  - `managed_volume_non_claim ...`
- `ManagedVolumeOperatorContractFromProjection`
- `internal/docs/protocol/operator-readiness-contract.md`

## Operator Boundary

This plan does not implement an operator. It defines the contract a future
operator should consume.

The future operator may publish Conditions and Events derived from
ManagedVolume. It must not:

- recompute primary/recovery/host-path state from raw pod logs,
- mint authority,
- decide promotion,
- execute dry-run actions without a separate product gate,
- bypass RBAC/audit/policy requirements.

All Phase 23 actions have:

```text
mutation_allowed=false
mode=read_only | dry_run
```

## Validation

Scope regression:

```text
go test ./cmd/sw-block ./core/ops ./core/csi ./core/launcher ./core/host/master -count=1
```

Replay gate:

- first-volume bundle replay,
- blocked image-pull bundle replay,
- Stage 2 recovery bundle replay.

TDD files:

- `core/ops/managed_volume_conditions_test.go`
- `core/ops/managed_volume_operator_contract_test.go`
- `core/ops/observation_report_test.go`
- `core/ops/observation_bundle_test.go`

## Non-Claims

Phase 23 does not deliver:

- hosted dashboard service,
- CRDs,
- operator reconciliation,
- mutating admin actions,
- promote/repair/rebuild/failback,
- backup/snapshot/restore,
- production SLOs,
- NVMe ANA parity.

## Next

Recommended Phase 24 choice:

1. Hosted read-only dashboard over the existing observation/report model, or
2. Operator scaffolding with CRDs/Conditions/Events but still no mutating admin
   workflows.

Do not start mutating admin controls until a separate spec gates RBAC, audit,
preconditions, rollback, and data-safety invariants.
