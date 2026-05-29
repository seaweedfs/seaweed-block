# Phase 33 Close Report - TestOps Failure Hardening

Closed: 2026-05-29

Verdict: PASS.

Source branch: `phase33-testops-failure-hardening`

Base release: PR #50 / merge `8102cf3` (`v0.3.4-alpha` baseline).

## Product Rule Validated

```text
If the product cannot prove a volume is ready, it must not claim Ready=True.
It must surface a stable reason, collect useful evidence, and clean up
deterministically.
```

Phase 33 is a reliability and evidence-hardening phase. It does not add a new
HA, rebuild, NVMe, backup, or mutating operator claim.

## Gate Summary

| Gate | Evidence | Result |
|---|---|---|
| D1 failure matrix | `phase33-failure-matrix.md` | PASS |
| D2 helper/status hardening | `go test ./scripts ./core/ops ./cmd/sw-block` | PASS |
| D3/F1 live negative status | `helm-support-bundle-diagnostics-chain.yaml` run `20260528-190738-51a2` | PASS, 49/49 |
| D3/F2 unreachable status replay | `status-endpoint-unreachable-replay-chain.yaml` run `20260529-155016-e9a5` | PASS, 17/17 |
| D4 cleanup residue | `cleanup-residue-chain.yaml` run `20260529-155040-4519` | PASS, 13/13 |
| D5 minimal new-user regression | `helm-first-volume-via-sw-block-cli-chain.yaml` run `20260529-155216-0d9d` | PASS, 34/34 |

## Key Evidence

F1 CSI image-pull blocked path:

```text
volume pvc-blocked status=blocked rf=3 reason=csi_node_image_pull_failed
managed_volume_condition Ready status=False reason=csi_node_image_pull_failed severity=warning
managed_volume_condition Blocked status=True reason=csi_node_image_pull_failed severity=warning
managed_volume_action safe_k8s.import_csi_image mode=dry_run
support_bundle_status=ok
failure_snapshot_status=ok
capture_failure_count=0
```

F2 status endpoint unreachable replay:

```text
managed_volume=pvc-unreachable status=unknown reason=status_endpoint_unreachable
managed_volume_condition Ready status=Unknown reason=status_endpoint_unreachable
dashboard/operator-snapshot reason_code=status_endpoint_unreachable
mutation_allowed=false
```

Cleanup residue:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

Minimal user path regression:

```text
generate Helm values
-> helm install
-> first PVC
-> writer_verified=true
-> reader_verified=true
-> read-only report/operator snapshot
-> helm uninstall cleanup
```

## Changes Landed

- `collect-helm-support-bundle.sh` now distinguishes required captures from
  optional diagnostics. Expected states such as no active iSCSI session no
  longer fail the support bundle.
- `collect-k8s-failure-snapshot.sh` applies the same required/optional split
  for previous logs, iSCSI state, multipath/dmsetup, and kubelet mount
  diagnostics.
- ManagedVolume projection treats `status_endpoint_unreachable` as
  `status=unknown` and `Ready=Unknown`, not `Ready=True`.
- Bundle replay skips corrupt evidence candidates and selects the newest valid
  snapshot.
- Cleanup verifier contract tests lock the residue counters and failure reason
  vocabulary.

## Blocking Findings

None.

## Non-Blocking Follow-Ups

- Convert more negative cases from replay-only to live failure injection when
  the lab orchestration supports it cleanly.
- Add runner-native actions for JSONPath waits and structured cleanup
  assertions so fewer shell helpers are needed.
- Keep F6-F8 from the failure matrix as future hardening candidates:
  multi-volume cross-interference, restart-during-promotion timing, and
  loopback publish target refusal.

## Release Boundary

If cut as `v0.3.5-alpha`, the release claim should be:

```text
Failure-path evidence and cleanup reliability hardening for the existing
Helm/PVC/read-only-ops alpha product.
```

Do not claim:

- new HA behavior,
- production readiness,
- mutating operator/admin workflows,
- rebuild/failback,
- backup/restore,
- NVMe ANA parity,
- broad performance/SLO guarantees.
