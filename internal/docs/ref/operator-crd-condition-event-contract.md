# Operator CRD / Condition / Event Contract

Status: Phase 28 D10 working contract.

Purpose: define the Kubernetes-native status surface before implementing a
full operator.

This is a read-only/status-first contract. It does not implement an operator,
and it does not authorize mutating storage actions.

## Source

Code contract:

- `core/ops/managed_volume_crd_contract.go`
- `core/ops/managed_volume_crd_contract_test.go`
- `core/ops/managed_volume_operator_contract.go`

Model input:

- `ManagedVolumeProjection`
- `ManagedVolumeFactContract`

The future operator must consume product-owned ManagedVolume state. It must not
recompute primary, recovery, host-path, or workload status from raw pod logs.

## API Group

```text
group: block.seaweedfs.com
version: v1alpha1
```

## Resources

### SwBlockCluster

Scope: namespaced.

Purpose: summarize one installed sw-block control plane.

Spec fields are deployment intent, not authority:

- `image`
- `csiImage`
- `storageClass`
- `blockNodes`
- `ackProfile`
- `protocol`

Status comes from ManagedVolume aggregate and install/readiness evidence:

- `status.conditions`
- `status.nodeCount`
- `status.volumeCount`
- `status.readyVolumeCount`
- `status.blockedVolumeCount`
- `status.observedGeneration`

Non-claims:

- no mutating storage actions,
- no repair/rebuild/failback,
- no backup/restore.

### SwBlockVolume

Scope: namespaced.

Purpose: expose one PVC-backed ManagedVolume status.

Spec fields are correlation hints, not a second volume API:

- `pvcName`
- `storageClass`

Status comes from `ManagedVolumeProjection`:

- `status.volumeID`
- `status.pvcName`
- `status.status`
- `status.reasonCode`
- `status.conditions`
- `status.nonClaims`
- `status.evidenceRefs`
- `status.allowedActions`

Non-claims:

- status only,
- no primary selection,
- no promote/repair/rebuild/delete.

## Condition Vocabulary

Allowed condition types:

| Condition | Meaning |
|---|---|
| `Ready` | Volume or cluster satisfies the documented claim boundary. |
| `Recovered` | Recovery completed with evidence. |
| `Recovering` | Recovery in progress; readiness not yet claimed. |
| `Blocked` | A documented blocker prevents the user path. |
| `Invalid` | Safety invariant violation or contradictory state. |
| `CleanupRequired` | Functional path may be done, but residue remains. |

Mapping from ManagedVolume projection:

| ManagedVolume state | Conditions |
|---|---|
| `ready` | `Ready=True` |
| `recovered` | `Ready=True`, `Recovered=True` |
| `recovering` | `Ready=False`, `Recovering=True` |
| `blocked` | `Ready=False`, `Blocked=True` |
| `invalid` / `unsafe` | `Ready=False`, `Invalid=True` |
| `unknown` | `Ready=Unknown` |

Every condition must carry:

- stable reason code,
- message,
- severity,
- evidence refs when available.

## Event Mapping

Kubernetes Events are derived from Conditions:

| Condition severity | Kubernetes Event type |
|---|---|
| `info` | `Normal` |
| `warning` | `Warning` |
| `error` | `Warning` |

Events are explanatory. They are not authority decisions.

## Allowed Actions

The CRD status may expose `allowedActions`, but Phase 28 keeps them read-only or
dry-run:

```text
mutation_allowed=false
mode=read_only | dry_run
```

Examples:

- `observe.collect_bundle`
- `observe.wait_for_pvc_bound`
- `observe.inspect_mount_failure`
- `observe.inspect_host_path`
- `safe_k8s.import_csi_image` as dry-run only
- `safe_k8s.reinstall_external_iscsi` as dry-run only

No Phase 28 CRD action may execute:

- promote,
- repair,
- rebuild,
- failback,
- delete data,
- cleanup live state.

## Example: Ready Volume

```yaml
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: demo-pvc
  namespace: default
spec:
  pvcName: demo-pvc
  storageClass: sw-block-dynamic
status:
  volumeID: pvc-123
  pvcName: demo-pvc
  status: ready
  reasonCode: first_volume_verified
  conditions:
    - type: Ready
      status: "True"
      reason: first_volume_verified
      severity: info
      message: managed volume is ready for the documented path
```

## Example: Blocked Volume

```yaml
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: demo-pvc
  namespace: default
spec:
  pvcName: demo-pvc
  storageClass: sw-block-dynamic
status:
  volumeID: pvc-456
  pvcName: demo-pvc
  status: blocked
  reasonCode: publish_target_loopback_cross_node
  conditions:
    - type: Ready
      status: "False"
      reason: publish_target_loopback_cross_node
      severity: warning
      message: managed volume is blocked; inspect dry-run actions and evidence refs
    - type: Blocked
      status: "True"
      reason: publish_target_loopback_cross_node
      severity: warning
      message: a documented blocker prevents the expected user path
  allowedActions:
    - type: observe.collect_bundle
      mode: read_only
      mutationAllowed: false
    - type: safe_k8s.reinstall_external_iscsi
      mode: dry_run
      mutationAllowed: false
```

## Gate Requirement

D10 is dev-complete when:

- CRD contract code exists,
- condition vocabulary is tested against ManagedVolume projection,
- event severity mapping is tested,
- docs include ready and blocked examples,
- no mutating action is exposed.

D10 is close-ready only after QA/PM review confirms the CRD wording is
understandable and does not over-claim operator functionality.
