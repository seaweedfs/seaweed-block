# Phase 40 D6 QA: Release Candidate Gate

## Goal

Prove the Phase 40 release candidate from both a local build perspective and a
user/lab perspective.

The release claim is narrow:

```text
Helm + PVC + read-only/status-only operator status + Events + diagnostics +
delete-safety/install-drift visibility. No lifecycle mutation.
```

## Local Gate

Run from the product repo:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-phase40-release-candidate-local.ps1
```

Required summary:

```text
phase40_release_candidate_local_status=ok
go_test_release_scope=ok
helm_lint=ok
helm_operator_status_template=ok
helm_published_image_compat_template=ok
status_api_conformance_gate=ok
git_diff_check=ok
```

The local gate does **not** prove the Kubernetes user path. It only proves the
release-candidate code, chart render, and CRD/RBAC conformance checks.

## Required Lab Gates

Use the release image digest if available. If no digest is published yet, use a
fresh local build and state that clearly in the report.

### G1 — Minimal New-User Helm Path

Run the documented CLI values path:

```text
swblock run testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml \
  -env product_root=/path/to/seaweed_block \
  -env sw_block_image=<release-or-local-image> \
  -env sw_block_csi_image=<release-or-local-csi-image>
```

Pass criteria:

- Helm install succeeds.
- First PVC binds.
- Writer verifies data.
- Replacement reader verifies data.
- Report has `index.html`, `summary.txt`, `cluster-evidence.json`,
  `timeline.jsonl`, and `operator-snapshot.json`.
- Cleanup summary is `cleanup_status=ok`.

### G2 — Operator-Status CRD And Event Path

Install with operator-status write mode enabled:

```text
helm install sw-block charts/seaweed-block \
  --namespace kube-system \
  --create-namespace \
  -f values.day1.yaml \
  --set operatorStatus.create=true \
  --set operatorStatus.dryRun=false \
  --wait --timeout 10m
```

Create the minimal CR stubs if the chart/user path did not create them:

```yaml
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockCluster
metadata:
  name: sw-block
  namespace: kube-system
spec: {}
---
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: <pvc-or-volume-name>
  namespace: kube-system
spec: {}
```

Pass criteria:

- `SwBlockCluster.status.readyVolumeCount >= 1`.
- At least one `SwBlockVolume.status.status=ready`.
- Ready condition has `reasonCode=first_volume_verified`.
- Kubernetes Events exist for the ready volume.
- `kubectl auth can-i` confirms status/events only:
  - `patch swblockvolumes --subresource=status`: yes
  - `create events`: yes
  - `patch swblockvolumes`: no
  - `patch pods`: no
  - `patch pvc`: no
  - `update storageclasses`: no

### G3 — Negative Status

Run a blocked-path validation. Preferred path:

```text
swblock run testops/scenarios/helm-support-bundle-diagnostics-chain.yaml \
  -env product_root=/path/to/seaweed_block \
  -env sw_block_image=<release-or-local-image> \
  -env sw_block_csi_image=<release-or-local-csi-image>
```

Pass criteria:

- Blocked evidence shows `reason=csi_node_image_pull_failed` or another stable
  blocked reason intentionally induced by the scenario.
- No CRD/report/dashboard/operator-snapshot surface shows false `Ready=True`.
- Suggested actions are `read_only`, `dry_run`, or `scripted` with
  `mutationAllowed=false`.

### G4 — Status API Conformance TestOps Gate

Run:

```text
swblock run testops/scenarios/operator-status-api-conformance-chain.yaml \
  -env product_root=/path/to/seaweed_block
```

Pass criteria:

```text
phase40_status_api_conformance_status=ok
casing_drift_gate=ok
enum_drift_gate=ok
wrong_endpoint_gate=ok
rbac_boundary_gate=ok
delete_safety_status_gate=ok
finalizer_mutation_allowed=false
```

### G5 — Final Cleanup

Run:

```text
swblock run testops/scenarios/cleanup-residue-chain.yaml \
  -env product_root=/path/to/seaweed_block
```

Pass criteria:

- no Helm release,
- no Seaweed Block pods/deployments/CRDs left unless intentionally kept for
  follow-up inspection,
- no iSCSI sessions,
- no Seaweed Block iSCSI node DB records,
- no multipath/dmsetup residue,
- no product processes,
- no test hostPath residue.

## Optional Extended Gates

Run only if lab time is available:

- RF=3 multi-volume smoke,
- restart persistence,
- WAL corruption no-false-ready gate,
- loopback cross-node negative gate.

## Blocking Criteria

Block D6 if any of these happen:

- first-volume writer/reader fails,
- operator-status cannot publish CRD status,
- operator-status gains any storage/workload/spec/finalizer mutation power,
- blocked/stale/corrupt evidence becomes false `Ready=True`,
- cleanup leaves residue,
- docs/release note claim a capability not covered by the tested image.

## Close Report Template

```text
Phase 40 D6 Release Candidate QA — PASS/FAIL

Source commit:
Image(s):
Runner:

Local gate:
- run/artifact:
- result:

G1 first-volume:
- run:
- result:

G2 operator-status CRD/Event:
- evidence:
- result:

G3 negative status:
- run:
- result:

G4 status API conformance:
- run:
- result:

G5 cleanup:
- run:
- result:

Blocking findings:
- ...

Non-blocking findings:
- ...

Release recommendation:
- release / hold
```
