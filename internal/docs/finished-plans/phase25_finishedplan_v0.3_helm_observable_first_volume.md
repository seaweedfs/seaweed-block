# Finished Plan: Phase 25 - v0.3 Helm Observable First-Volume Release

Closed: 2026-05-22

Verdict: PASS.

## Delivered Claim

v0.3 alpha packages the current Kubernetes block product as a user-runnable
Helm-first first-volume loop:

```text
generate values.day1.yaml
-> helm install
-> create first PVC
-> writer checksum
-> reader checksum
-> read-only report/dashboard artifacts
-> helm uninstall and clean residue
```

## Evidence

- Single-node Helm first-volume gate:
  - run `20260522-031019-ef25`
  - PASS, 34/34 actions
  - selected node `m02`, loopback mode, writer/reader verified
- Multi-node Helm first-volume gate:
  - run `20260522-031124-0a44`
  - PASS, 51/51 actions
  - external iSCSI/status, CHAP, writer/reader verified
- Close report:
  - `internal/docs/qa-assignments/v0.3-helm-observable-first-volume-close-report.md`

## User-Facing Artifacts

- `README.md`
- `docs/quickstart-kubernetes.md`
- `docs/releases/v0.3-alpha.md`
- `docs/releases/README.md`

## Important Non-Claims

- Not production-ready.
- No operator/CRD lifecycle.
- No mutating dashboard/admin action.
- No backup/snapshot/restore.
- No upgrade/rollback safety.
- No broad performance/SLO claim.
- No new recovery claim beyond prior closed recovery phases.

## Followups

- Operator/CRD lifecycle should be a later release, not part of v0.3.
- Continue using immutable `sha-<commit>` images for QA/PM release proof.
- Keep the dashboard/report read-only until mutating actions have their own
  hard gates.
