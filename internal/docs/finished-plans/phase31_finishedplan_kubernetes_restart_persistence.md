# Finished Plan: Phase 31 - Kubernetes Restart Persistence

Status: **PASS, 100%**.

Dates: started and closed on 2026-05-25.

## Goal

Prove that the Kubernetes block product can restart without forgetting data,
authority, promoted primary, publish target, epoch, lifecycle registration, or
ManagedVolume status.

## What Shipped

- Durable Helm restart mode via `sw-block ops generate-helm-values`:
  - `--restart-persistence ephemeral|hostpath`,
  - `--state-hostpath`,
  - generated `blockmaster.stateHostPath`,
  - generated `restartPersistence.mode=hostpath`,
  - generated `restartPersistence.stateHostPath`.
- Helm chart restart-persistence values and schema.
- Blockmaster hostPath permission initContainer.
- Restart persistence TestOps gates:
  - single-node k3s restart,
  - RF3 sync-quorum promotion then restart,
  - 3-PVC RF3 multi-volume restart smoke.
- Scenario hardening:
  - port-forward after restart uses a selected Running/Ready blockmaster pod,
  - product `ops cluster` is the readiness probe,
  - pre-clean rejects dirty generated blockvolume Deployment residue.

## Validation

| Scope | Run | Result |
|---|---:|---|
| D3 single-node restart | `20260525-104016-f3d4` | 40/40 PASS |
| D4 RF3 promotion restart | `20260525-122723-f7ed` | 34/34 PASS |
| D5 multi-volume RF3 restart | `20260525-123233-541b` | 36/36 PASS |

Supporting dev runs:

- D3 dev baseline `20260525-103441-2e9c`, 39/39 PASS.
- D4 original dev baseline `20260525-104247-d6da`, 34/34 PASS.
- D4 hardened dev rerun `20260525-122104-60c3`, 34/34 PASS.
- D5 dev baseline `20260525-110800-4f19`, 36/36 PASS.

## Product Claim Now Supported

In durable Helm hostPath mode, Seaweed Block can preserve:

- blockvolume data,
- master authority state,
- current primary,
- publish target,
- epoch,
- ManagedVolume readiness,
- per-volume identity across a k3s/product restart.

This was validated for:

- one PVC on one node,
- one RF3 sync-quorum PVC after promotion,
- three RF3 sync-quorum PVCs concurrently.

## Explicit Non-Claims

- No fresh-cluster restore.
- No backup/snapshot/restore.
- No host disk loss survival.
- No returned-replica rebuild/failback.
- No broad production SLO.
- No claim that restart persistence replaces Phase 27 stale-primary fencing
  evidence.

## Open Follow-Ups

- Add user-facing docs for durable restart mode.
- Decide release packaging/tagging for the restart-persistence bits.
- Resume Phase 32 read-only operator/status surface now that it can rely on
  restart-stable state.

## Close Artifacts

- `internal/docs/qa-assignments/phase31-kubernetes-restart-persistence-close-report.md`
- `internal/docs/qa-assignments/phase31-restart-persistence-d3-qa-signoff.md`
- `internal/docs/qa-assignments/phase31-restart-persistence-d4-qa-signoff.md`
- `internal/docs/qa-assignments/phase31-restart-persistence-d5-qa-signoff.md`
