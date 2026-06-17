# Finished Plan: Phase 33 - TestOps Failure Hardening

Closed: 2026-05-29

Verdict: PASS.

## Delivered Claim

Phase 33 hardens the existing Helm/PVC/read-only-ops alpha product against
failure-path ambiguity:

```text
blocked or unreachable evidence
-> no false Ready=True
-> stable reason code
-> cold-readable support evidence
-> deterministic cleanup
```

This phase does not introduce a new storage feature. It strengthens release
confidence for the current product loop.

## Evidence

- F1 live negative support bundle:
  - run `20260528-190738-51a2`
  - PASS, 49/49 actions
  - `reason=csi_node_image_pull_failed`
  - `support_bundle_status=ok`
  - `failure_snapshot_status=ok`
  - cleanup residue zero
- F2 unreachable status replay:
  - run `20260529-155016-e9a5`
  - PASS, 17/17 actions
  - `status=unknown reason=status_endpoint_unreachable`
  - report, explain, dashboard, and operator snapshot agree
- F5 cleanup residue:
  - run `20260529-155040-4519`
  - PASS, 13/13 actions
  - all residue counters zero
- Minimal new-user regression:
  - run `20260529-155216-0d9d`
  - PASS, 34/34 actions
  - Helm values, install, first PVC, writer/reader, report, cleanup all pass
- Scoped tests:
  - `go test ./scripts ./core/ops ./cmd/sw-block` PASS

Close report:

- `internal/docs/qa-assignments/phase33-testops-failure-hardening-close-report.md`

## User-Facing Impact

- Support bundles and failure snapshots are less noisy: expected empty
  diagnostics no longer make the bundle look failed.
- A status endpoint unreachable condition becomes `Ready=Unknown`, not false
  ready.
- Corrupt evidence no longer prevents replay when a newer valid snapshot exists.
- Cleanup residue vocabulary remains locked across Kubernetes, iSCSI session,
  iSCSI node DB, multipath, dmsetup, process, and hostPath dimensions.

## Important Non-Claims

- Not production-ready.
- No new HA claim.
- No rebuild/failback implementation.
- No backup/snapshot/restore.
- No mutating operator/admin/dashboard action.
- No NVMe ANA parity expansion.
- No broad performance, RTO, RPO, or SLO claim.

## Followups

- Promote more failure matrix P1 entries to hard gates when useful:
  multi-volume cross-interference, restart-during-promotion, and loopback
  publish-target refusal.
- Continue reducing shell helper complexity with runner-native primitives.
- Use this as release-hardening evidence if cutting `v0.3.5-alpha`.
