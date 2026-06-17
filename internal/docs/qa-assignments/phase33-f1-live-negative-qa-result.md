# Phase 33 F1 Live Negative QA Result

Status: PASS on 2026-05-28.

Source branch: `phase33-testops-failure-hardening`

Scenario:

- `testops/scenarios/helm-support-bundle-diagnostics-chain.yaml`

Run:

- `20260528-190738-51a2`
- Result bundle: `C:\work\seaweed_block\results\phase33-f1-live-negative-r3\20260528-190738-51a2`
- Demo artifacts: `V:\share\g15d-k8s\20260528-190738-51a2-helm-support-bundle\`

## Result

49/49 actions passed across all 8 phases:

- `pre_clean`
- `build_and_generate_values`
- `helm_install_stack`
- `first_volume_kept_for_bundle`
- `collect_support_bundle`
- `blocked_bundle_asserts`
- `failure_snapshot`
- `helm_uninstall_cleanup`

## Evidence

Support bundle:

```text
support_bundle_status=ok
report_status=ok
explain_status=ok
timeline_status=ok
capture_status=ok
read_only=true
```

Blocked-path explanation:

```text
volume pvc-blocked status=blocked rf=3 reason=csi_node_image_pull_failed
managed_volume pvc-blocked status=blocked reason=csi_node_image_pull_failed
managed_volume_condition Ready status=False reason=csi_node_image_pull_failed severity=warning
managed_volume_condition Blocked status=True reason=csi_node_image_pull_failed severity=warning
managed_volume_action safe_k8s.import_csi_image mode=dry_run side_effect=safe_k8s executor=installer_or_operator
```

Failure snapshot:

```text
failure_snapshot_status=ok
capture_failure_count=0
read_only=true
k8s_snapshot=k8s
logs=logs
host_snapshot=host
```

Cleanup:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Fixes Verified

- `collect-helm-support-bundle.sh` no longer treats expected optional diagnostics
  such as no active iSCSI sessions as support-bundle failure.
- `collect-k8s-failure-snapshot.sh` no longer treats expected optional
  diagnostics such as missing previous logs or no active iSCSI sessions as
  failure-snapshot failure.

## Verdict

F1 is release-gate green. The blocked CSI image-pull path does not claim
`Ready=True`, carries stable reason `csi_node_image_pull_failed`, exports
cold-readable support evidence, and cleans up with zero residue.
