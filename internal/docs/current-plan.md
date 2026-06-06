# Current Plan

Status: no active phase. Phase 36 closed on 2026-06-06.

Branch: `phase33-testops-failure-hardening`

Most recent finished plan:

- `internal/docs/finished-plans/phase36_finishedplan_productized_operations_actionability.md`

## Closed Phase 36 Summary

Phase 36 delivered actionable read-only operations over the Phase 35
Kubernetes-native status foundation:

- node readiness under `SwBlockCluster.status.nodes[]`,
- support-bundle and evidence refs,
- cleanup visibility and `CleanupRequired` projection,
- safe read-only/scripted next-step hints,
- cross-surface agreement across CRD status, Events, report, dashboard,
  operator-snapshot, and `ops explain`.

This phase did not add mutating operator lifecycle.

## Open Follow-ups

- Populate live node evidence from real Kubernetes node readiness,
  schedulability, image presence, and CSI driver registration.
- Tighten local-image build/import evidence for the build host's k3s.
- Document loopback publish targets as single-node/local-consumer only.
- Keep cleanup verifier strict for force-delete residue such as stale iSCSI node
  DB records.

## Next Plan Candidates

Pick one as a separate gated phase:

- mutating lifecycle foundation: finalizers and delete safety,
- live node-evidence hardening,
- upgrade/rollback drift status before upgrade execution,
- returned-replica rebuild/failback,
- backup/snapshot/restore,
- NVMe ANA parity over the same CRD/Condition/Event model.

Do not extend Phase 36 for mutating workflows.
