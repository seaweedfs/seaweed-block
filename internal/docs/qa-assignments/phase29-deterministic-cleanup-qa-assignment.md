# Phase 29 Deterministic Cleanup QA Assignment

Date: 2026-05-24

Purpose: independently validate Phase 29 D4 after D1-D3 landed. This QA pass
must prove that cleanup evidence is deterministic across the active RF3
multi-volume alpha loops.

## Source State

Use the branch containing these commits or newer:

- `1d4e53c` - helper cleanup TOCTOU fix
- `f0f57ec` - cleanup ownership matrix
- `102fc74` - cleanup evidence in report/dashboard/operator snapshot

## Required Runs

Run from a clean 3-node lab:

```powershell
$results='C:/work/seaweed_block/results/phase29-d4-deterministic-cleanup-qa'
New-Item -ItemType Directory -Force -Path $results | Out-Null
$scenarios=@(
  'testops/scenarios/helm-multi-volume-rf3-readiness-chain.yaml',
  'testops/scenarios/helm-multi-volume-rf3-reattach-recovery-chain.yaml',
  'testops/scenarios/helm-multi-volume-rf3-mounted-failover-chain.yaml',
  'testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml',
  'testops/scenarios/cleanup-residue-chain.yaml'
)
foreach ($scenario in $scenarios) {
  & C:/work/swblock.exe run --results-dir $results (Join-Path (Get-Location) $scenario)
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
```

## Pass Criteria

All required scenarios must pass:

| Scenario | Required result |
|---|---|
| `helm-multi-volume-rf3-readiness-chain.yaml` | PASS, all actions green |
| `helm-multi-volume-rf3-reattach-recovery-chain.yaml` | PASS, all actions green |
| `helm-multi-volume-rf3-mounted-failover-chain.yaml` | PASS, all actions green |
| `helm-multi-volume-rf3-interleaved-failover-chain.yaml` | PASS, all actions green |
| `cleanup-residue-chain.yaml` | PASS, all actions green |

Residue requirements:

- no active iSCSI sessions matching `io.seaweedfs`,
- no iSCSI node records matching `io.seaweedfs`,
- no Seaweed Block dm-multipath or dmsetup residue,
- no sw-block pods, deployments, Helm release, StorageClass, CSI driver, or RBAC residue,
- no `blockmaster`, `blockvolume`, `blockcsi`, or `iscsi-target` host processes.

Evidence requirements:

- Scenario summaries must not report ambiguous cleanup status.
- Any `cleanup-summary.txt` present must use the Phase 29 field vocabulary:
  `cleanup_status`, `k8s_residue_count`, `iscsi_residue_count`,
  `multipath_residue_count`, `process_residue_count`,
  `hostpath_residue_count`, and `failure_count`.
- If a report bundle is produced, `summary.txt`, dashboard HTML, and
  `operator-snapshot.json` must carry the same cleanup evidence.

## Dev Baseline

Dev rerun on 2026-05-24 passed all D4 scenarios:

| Scenario | Run ID | Result |
|---|---|---|
| readiness | `20260524-144856-f4b3` | PASS, 35/35 |
| reattach recovery | `20260524-145058-6289` | PASS, 29/29 |
| mounted failover | `20260524-145513-41d0` | PASS, 48/48 |
| interleaved failover | `20260524-145901-6d11` | PASS, 56/56 |
| cleanup residue | `20260524-150146-f4e5` | PASS, 13/13 |

Total dev baseline: 181/181 actions.

## QA Report

Write the close or blocker report to:

```text
internal/docs/qa-assignments/phase29-deterministic-cleanup-qa-validation.md
```

If any scenario fails, classify the failure as one of:

- product cleanup gap,
- scenario/helper race,
- lab residue or prerequisite issue,
- evidence/report mismatch.

