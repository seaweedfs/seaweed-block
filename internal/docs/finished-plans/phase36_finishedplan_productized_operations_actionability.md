# Finished Plan: Phase 36 - Productized Operations Actionability

Closed: 2026-06-06

Verdict: PASS.

## Delivered Claim

Phase 36 turns the Phase 35 Kubernetes-native read-only status foundation into
an actionable operations surface:

```text
SwBlockCluster.status.nodes[]
support bundle and evidence refs
cleanup visibility and CleanupRequired conditions
safe read-only or scripted next-step hints
cross-surface agreement across CRD status, Events, report, dashboard,
operator-snapshot, and ops explain
```

This is still read-only. The operator-status controller may read Kubernetes and
Seaweed Block observation APIs, patch CRD `.status`, and create Kubernetes
Events. It must not mutate storage, workloads, PVCs, PVs, Secrets,
StorageClasses, Helm releases, iSCSI sessions, multipath maps, hostPath data,
or CRD spec.

## Evidence

| Gate | Evidence | Result |
|---|---|---|
| D1 operations model contract | internal review + scoped tests | PASS |
| D2 node readiness / preflight status | `internal/docs/qa-assignments/phase36-d2-node-readiness-qa-signoff.md` | PASS |
| D3 support evidence refs | `internal/docs/qa-assignments/phase36-d3-support-evidence-qa-signoff.md` | PASS |
| D4 cleanup visibility | `internal/docs/qa-assignments/phase36-d4-cleanup-visibility-qa-signoff.md` | PASS |
| D5 surface agreement and negative-first gates | `internal/docs/qa-assignments/phase36-d5-surface-agreement-qa-signoff.md` | PASS |

Final scoped checks:

```text
go test ./scripts
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block \
  --namespace kube-system \
  --include-crds \
  --set operatorStatus.create=true \
  --set operatorStatus.dryRun=false
git diff --check
```

## User-Facing Impact

- Users can inspect node-level readiness and preflight status through
  `SwBlockCluster.status.nodes[]`.
- Blocked or unknown status can point to support bundle evidence refs.
- Cleanup verifier evidence projects `CleanupRequired=True|False`, residue
  counters, and safe scripted verification hints.
- Safe next steps are visible as `read_only`, `dry_run`, or `scripted`; they do
  not grant the operator mutation authority.
- CRD status, Kubernetes Events, report artifacts, local dashboard,
  `operator-snapshot.json`, and `sw-block ops explain` agree across healthy,
  blocked, stale, cleanup-required, and multi-volume paths.
- Negative-first behavior holds: blocked, unknown, stale, and cleanup-required
  evidence does not become false `Ready=True`.

## Important Non-Claims

- Not production-ready.
- No mutating Kubernetes operator lifecycle.
- No automatic CR object creation or ownership.
- No finalizers or delete safety.
- No automatic cleanup.
- No automatic support-bundle collection or upload.
- No promote, repair, rebuild, failback, delete, backup, restore, or cleanup
  mutation through UI/API/operator.
- No upgrade execution.
- No backup/snapshot/restore workflow.
- No returned-replica rebuild or automated failback.
- No NVMe ANA parity expansion.
- No production hosted dashboard.
- No performance, RTO, RPO, or SLO claim.

## Follow-ups

- Populate live `NodeEvidence` from real Kubernetes node readiness,
  schedulability, image presence, and CSI driver registration. The negative
  missing-image node path is currently replay-only.
- Tighten local-image build/import evidence so the build host proves both
  `sw-block:local` and `sw-block-csi:local` are present before local-image
  gates claim node readiness.
- Document loopback publish targets as single-node/local-consumer only.
- Keep cleanup verifier strict for force-delete residue such as stale iSCSI
  node DB records.
- Start any mutating operator lifecycle work as a separate phase with explicit
  gates, rollback boundaries, and PM-visible non-claims.

