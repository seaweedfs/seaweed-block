# Phase 32 Negative-Status Evidence Review

Date: 2026-05-25

Owner: QA. Source for D1a Workstream B.

This review picks **one** blocked path that already has measured QA evidence
from Phase 28 G3 and walks the negative-first contract end to end. Goal: prove
the existing surfaces never claim `Ready=True` on a known blocker, and name
exactly which surface must carry which field.

## Reviewed Blocker

Scenario: `testops/scenarios/helm-support-bundle-diagnostics-chain.yaml`,
blocked bundle phase.

Failure mode injected: **CSI node image pull failure**. The chart values
reference an image SHA the k3s lab cannot pull, causing `sw-block-csi-node`
DaemonSet pods to enter `ImagePullBackOff`.

Evidence reused: Phase 28 G3 cycle 2 rerun run `20260524-103350-901d`
artifact tree under
`/v/share/g15d-k8s/20260524-103350-901d-helm-support-bundle/`.

## Negative-First Contract Walk

### Q1: Which user-visible status should be false?

`Ready` must be `False` (never `True`).

The product **must not** silently report a `volumes=N status=ok` aggregate
when one or more volumes are actually blocked on CSI publish. Aggregate
should reflect the worst per-volume status.

### Q2: Which Condition should be true?

`Blocked=True` (with `Ready=False` as the corollary).

From the blocked bundle's `explain.txt`:

```text
managed_volume pvc-blocked status=blocked reason=csi_node_image_pull_failed
managed_volume_condition Ready status=False reason=csi_node_image_pull_failed severity=warning
managed_volume_condition Blocked status=True reason=csi_node_image_pull_failed severity=warning message="a documented blocker prevents the expected user path"
```

Both Conditions present, both carry the same `reason=` code.

### Q3: Which reason code should appear?

`reason=csi_node_image_pull_failed`.

This is a stable reason code drawn from the cleanup-ownership / status-reason
registry. It appears identically in:

- `managed_volume_condition Ready status=False reason=csi_node_image_pull_failed`
- `managed_volume_condition Blocked status=True reason=csi_node_image_pull_failed`
- per-volume `reason=` field on the ManagedVolume projection
- supporting Kubernetes Event `Warning Failed ... ErrImagePull` (raw)
- ManagedVolume Action precondition: `csi_node_not_ready,image_pull_failed`

### Q4: Which evidence file proves it?

| Evidence | Layer | Path |
|---|---|---|
| Kubernetes events showing `ImagePullBackOff` | k8s | `blocked-bundle/demo/kube-system-pods-deploys.txt` |
| Pod state showing `waiting=ImagePullBackOff on node m02 image sw-block-csi:local` | k8s | same as above |
| ManagedVolume Condition + reason | product | `blocked-bundle/explain.txt` |
| Operator-snapshot fragment (when generated against this bundle) | product | `operator-snapshot.json` Conditions array |
| Dry-run remediation: `safe_k8s.import_csi_image` with preconditions + invariants | product | `blocked-bundle/explain.txt` |

The evidence chain links **raw Kubernetes signal** (ImagePullBackOff Event)
to **product reason code** (`csi_node_image_pull_failed`) to **dry-run next
action** (`safe_k8s.import_csi_image mode=dry_run side_effect=safe_k8s
executor=installer_or_operator`).

### Q5: Which surfaces must agree?

All five read-only surfaces must carry the same Condition type + reason
code:

| Surface | Required field | Evidence from G3 |
|---|---|---|
| `sw-block ops report` `summary.txt` | `managed_volume_condition Blocked status=True reason=csi_node_image_pull_failed` | PRESENT |
| `sw-block ops explain` text | `condition Blocked severity=error reason=csi_node_image_pull_failed` | PRESENT |
| Dashboard `index.html` ManagedVolume row | Blocked badge + reason cell | PRESENT (Phase 28 audit) |
| `operator-snapshot.json` | `conditions[].type=Blocked status=True reason=csi_node_image_pull_failed` | PRESENT (Phase 28 G4 audit) |
| Support bundle (cold replay) | All of the above must reproduce from `--from-bundle` | PRESENT (G3 cold replay PASS) |
| Future CRD `SwBlockVolume.status` | Same Condition + reason | PENDING D2 |

## What Would FAIL This Review

Any of these patterns would fail the negative-first contract:

1. **Silent timeout**: surface returns `status=unknown` or `reason=timeout`
   without naming the blocker class. Today's evidence shows
   `csi_node_image_pull_failed` directly, so PASS.

2. **`Ready=True` with degraded reason**: surface claims Ready while a
   Blocked Condition is also True. Today: `Ready=False` is enforced
   alongside `Blocked=True`. PASS.

3. **Volume in `managed_volume_count=N` aggregate but zero per-volume
   evidence**: aggregate hides per-volume blocker. Today the per-volume
   row carries the Condition. PASS.

4. **Action with `mode=mutate` or no `executor=` qualifier**: blocker would
   suggest a mutating remediation. Today the action is `mode=dry_run
   side_effect=safe_k8s executor=installer_or_operator`. PASS.

5. **Reason code not in registry**: surface invents a one-off reason
   string. Today `csi_node_image_pull_failed` is in the documented
   cleanup-ownership/status reason set. PASS.

## Surface-Agreement Audit Methodology

Repeatable steps for any future Phase 32 negative case:

1. Identify failure-mode injection point in the scenario (image override,
   loopback target, deliberate pod-kill, etc).
2. Capture the bundle root path on the SMB share
   (`/v/share/g15d-k8s/<run-id>-<scenario>/`).
3. Locate `explain.txt` (cold-reader's first artifact). Extract:
   - per-volume `status=blocked reason=<code>`,
   - all `managed_volume_condition` lines.
4. Run `sw-block ops report --from-bundle <bundle>` against the saved
   bundle. Compare summary.txt and operator-snapshot.json Condition arrays
   to step 3. They must match.
5. Probe dashboard URL paths: `/`, `/operator-snapshot.json`,
   `/summary.txt` must all return 200 and carry the same Condition.
6. Confirm `mode=read_only` or `mode=dry_run` on every action attached to
   the blocked volume; never `mode=mutate` or missing.

## Verdict for the Reviewed Blocker

The CSI node image-pull blocker class passes the Phase 32 D4 negative-first
contract on every reachable surface today. This is direct evidence that the
read-only operations surface can be truthful under at least one common
failure class, without inventing Ready or suggesting mutating remediation.

## Recommended D4 Coverage Expansion

The other blocker classes named in the Phase 32 D4 plan still need the same
walk-through. Suggested order:

1. **publish_target_loopback_cross_node** at runtime (not chart-config-time).
   No existing scenario covers this; needs a new chain.
2. **writer_pod_mount_failure** (PVC bound but mount fails on a different
   node). `csi-rf1-durable-restart-failure-chain.yaml` may already cover
   this; needs reason-code audit.
3. **blockmaster_unreachable** as a status-surface blocker. Phase 27 D6 saw
   the port-forward race incidentally; needs a deliberate scenario.
4. **cleanup_residue_present_blocking_promotion**. `cleanup-residue-chain`
   proves clean; need a chain where promotion is held off because cleanup
   isn't complete.
5. **stale_evidence_blockmaster_quiet** (D7 territory).

Each needs the same Q1-Q5 walk plus a recorded `explain.txt` snippet to
prove the reason code is stable across surfaces.
