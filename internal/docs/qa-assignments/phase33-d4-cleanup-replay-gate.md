# Phase 33 D4 Cleanup And Replay Gate

Status: PASS on 2026-05-29.

Source branch: `phase33-testops-failure-hardening`

Purpose: consolidate Phase 33 cleanup and replay evidence. This gate does not
add a new product claim; it proves failed or degraded evidence paths remain
cold-readable and leave the lab clean.

## Gate Results

| Gate | Scenario / Test | Run / Evidence | Result |
|---|---|---|---|
| F1 live negative support bundle | `helm-support-bundle-diagnostics-chain.yaml` | `20260528-190738-51a2` | PASS, 49/49 |
| F2 unreachable status replay | `status-endpoint-unreachable-replay-chain.yaml` | `20260529-155016-e9a5` | PASS, 17/17 |
| F4 corrupt evidence replay | `go test ./cmd/sw-block` | `TestOpsReportFromBundleSkipsCorruptClusterEvidenceCandidate` | PASS |
| F5 cleanup residue | `cleanup-residue-chain.yaml` | `20260529-155040-4519` | PASS, 13/13 |

## F1: Failed Path Evidence Remains Useful

The live negative gate proves the CSI image-pull blocked path is explainable and
read-only:

```text
support_bundle_status=ok
failure_snapshot_status=ok
capture_failure_count=0
volume pvc-blocked status=blocked rf=3 reason=csi_node_image_pull_failed
managed_volume_condition Ready status=False reason=csi_node_image_pull_failed severity=warning
managed_volume_action safe_k8s.import_csi_image mode=dry_run
cleanup_status=ok
```

This closes the support-bundle quality requirement for a real blocked install
path.

## F2: Unreachable Evidence Does Not Become Ready

The replay-only gate synthesizes status endpoint unreachability and verifies
all read-only surfaces agree:

```text
summary: managed_volume=pvc-unreachable status=unknown reason=status_endpoint_unreachable
operator-snapshot.json: reason_code=status_endpoint_unreachable
dashboard /operator-snapshot.json: reason_code=status_endpoint_unreachable
explain: managed_volume_condition Ready status=Unknown reason=status_endpoint_unreachable
```

The scenario includes a JSON assertion that no per-volume `Ready=True` condition
appears in the report or dashboard snapshot.

## F4: Corrupt Evidence Is Skipped

The CLI replay unit gate verifies `sw-block ops report --from-bundle` can skip a
corrupt `cluster-evidence.json` candidate and select the newest valid snapshot.
That protects support-bundle replay from one bad evidence file masking a later
valid one.

## F5: Cleanup Residue Is Zero

The cleanup gate verifies residue dimensions after cleanup:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

The verifier contract test also locks the residue counters and failure reason
vocabulary so future edits cannot silently drop iSCSI node DB, multipath,
process, Kubernetes, or hostPath dimensions.

## Verdict

D4 is closed. Phase 33 now has evidence that blocked paths, unreachable evidence
replay, corrupt evidence replay, and cleanup residue checks all follow the
negative-first rule:

```text
No false Ready=True.
Stable reason code.
Useful evidence.
Deterministic cleanup.
```

Remaining Phase 33 work is D5 release-close packaging: compact close report,
minimal new-user validation decision, and release-note/addendum wording if this
branch is cut as `v0.3.5-alpha`.
