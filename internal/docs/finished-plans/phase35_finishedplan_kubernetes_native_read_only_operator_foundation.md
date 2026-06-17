# Finished Plan: Phase 35 - Kubernetes-Native Read-Only Operator Foundation

Closed: 2026-06-04

Verdict: PASS.

## Delivered Claim

Phase 35 turns the existing ManagedVolume read model into a Kubernetes-native,
read-only operator foundation:

```text
SwBlockCluster CRD
SwBlockVolume CRD
status-only operator-status controller
CRD Conditions
Kubernetes Events
read-only/status-only RBAC
```

This is not a mutating operator lifecycle. The controller may read observation
evidence, patch CRD `.status`, and create Kubernetes Events. It must not mutate
storage, workloads, PVCs, PVs, Secrets, StorageClasses, Helm releases, iSCSI
sessions, multipath maps, hostPath data, or CRD spec.

## Evidence

| Gate | Evidence | Result |
|---|---|---|
| D1 CRD/RBAC contract | `internal/docs/qa-assignments/phase35-d1-operator-crd-qa-signoff.md` | PASS |
| D2 dry-run packaged controller | `internal/docs/qa-assignments/phase35-d2-operator-status-qa-signoff.md` | PASS |
| D3 happy-path status writer | `internal/docs/qa-assignments/phase35-d3-operator-status-writer-qa-signoff.md` | PASS |
| D4 blocked status + Events | `internal/docs/qa-assignments/phase35-d4-operator-status-events-qa-signoff.md` | PASS |
| D5 stale/unreachable projections | `internal/docs/qa-assignments/phase35-d5-stale-status-projections-qa-signoff.md` | PASS |
| D6 stable Event identity | `internal/docs/qa-assignments/phase35-d6-stable-event-identity-qa-signoff.md` | PASS |
| D7 read-only boundary | `internal/docs/qa-assignments/phase35-d7-read-only-boundary-qa-signoff.md` | PASS |

Final scoped checks:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block \
  --namespace kube-system \
  --include-crds \
  --set operatorStatus.create=true \
  --set operatorStatus.dryRun=false
```

## User-Facing Impact

- Users can inspect Seaweed Block state through Kubernetes-native objects:
  `SwBlockCluster` and `SwBlockVolume`.
- Existing ManagedVolume reason codes and Conditions project into CRD
  `.status`.
- Known blocked evidence becomes `Ready=False` / `Blocked=True`; it does not
  become false ready.
- Unreachable or stale evidence becomes `Ready=Unknown` /
  `EvidenceStale=True`; it does not become an inappropriate hard block.
- WAL integrity faults remain non-ready and surface
  `reason=wal_integrity_fault`.
- Kubernetes Events provide normal cluster breadcrumbs with stable
  object/type/reason identity. Persistent blockers do not mint unbounded new
  Event objects on every reconcile.
- The operator-status ServiceAccount is constrained to CRD read, CRD status
  write, and Event create.

## Important Non-Claims

- Not production-ready.
- No mutating Kubernetes operator lifecycle.
- No automatic CR object creation or ownership.
- No finalizers or delete safety.
- No automatic cleanup.
- No promote, repair, rebuild, failback, delete, backup, restore, or cleanup
  mutation through UI/API/operator.
- No upgrade execution.
- No backup/snapshot/restore workflow.
- No returned-replica rebuild or automated failback.
- No NVMe ANA parity expansion.
- No production hosted dashboard.
- No performance, RTO, RPO, or SLO claim.

## Followups

- Add server-side dry-run or envtest validation for CRD status payloads.
- Optionally patch existing Events on conflict to update `count` and
  `lastTimestamp`.
- Project cleanup residue evidence into `CleanupRequired` status and Events.
- Add CR object ownership/creation only after the status-only contract remains
  stable.
- Start any mutating operator lifecycle work as a separate phase with explicit
  gates and rollback boundaries.

