# Current Plan: Phase 30 - Pending Next Work Selection

Status: pending, 0% complete. Phase 29 closed on 2026-05-24.

## Context

Phase 29 closed lifecycle/cleanup reliability:

- cleanup ownership matrix,
- helper TOCTOU cleanup fix,
- cleanup evidence parity across report/dashboard/operator snapshot,
- deterministic RF3 cleanup QA replay,
- finished plan:
  `internal/docs/finished-plans/phase29_finishedplan_lifecycle_cleanup_reliability.md`.

## Candidate Next Directions

Do not start implementation until the next direction is selected.

| Candidate | Type | Why |
|---|---|---|
| Control-model / ManagedVolume hardening | Core Stability + Operational | Operator-grade operations need stable state, action, and evidence ownership before mutating workflows. |
| Returned-replica rebuild / reintegration / failback | Functional + Core Stability | Required for credible sustained HA after recovery. |
| TestOps runner cleanup/wait action hardening | Operational | Reduces shell-helper orchestration and improves reproducibility. |
| NVMe ANA Kubernetes parity | Functional | Protocol parity after iSCSI multipath path is stable. |
| Backup/snapshot/restore planning | Functional + Operational | Enterprise expectation, but depends on stable lifecycle model. |

## Recommendation

Pick **control-model / ManagedVolume hardening** first if operator-grade
operations are the business priority. Pick **rebuild/reintegration/failback**
first if the next release must improve functional HA behavior.
