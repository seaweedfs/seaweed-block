# Phase 37 D2 QA Assignment: Live Node And CSI Evidence

Status: assigned.

## Goal

Validate that `sw-block ops operator-status` publishes live Kubernetes node and
CSI registration facts into `SwBlockCluster.status.nodes[]`.

This is not a lifecycle or mutation test. The operator-status controller must
remain read-only except CRD status writes and Events.

## Source Under Test

- Branch: `phase33-testops-failure-hardening`
- Minimum commit: includes `phase37: define live node evidence contract` plus
  D2 live node evidence implementation.

## Required Gates

### G1: Healthy Live Node Path

Install with:

```bash
helm install sw-block charts/seaweed-block \
  --namespace kube-system \
  --create-namespace \
  -f values.day1.yaml \
  --set operatorStatus.create=true \
  --set operatorStatus.dryRun=false \
  --wait --timeout 10m
```

Pass criteria:

```text
SwBlockCluster.status.nodes has one entry per Kubernetes node observed by the lab.
Each healthy node has status=ready reasonCode=node_ready.
ready=true and schedulable=true reflect the Kubernetes Node object.
CRD status and operator-snapshot.json agree on node names, ready, schedulable,
status, and reasonCode.
```

### G2: SchedulingDisabled Node

Temporarily cordon one non-critical node, wait for one operator-status
iteration, then uncordon during cleanup.

Pass criteria:

```text
affected node has status=blocked reasonCode=node_scheduling_disabled.
Ready condition is False with reason node_scheduling_disabled.
Blocked condition is True with reason node_scheduling_disabled.
No workload, PVC, PV, storage, image, or host mutation is performed by
operator-status.
```

### G3: NotReady Node

Use the safest lab-approved method to make one node NotReady, or mark this gate
blocked if lab policy does not allow node readiness disruption.

Pass criteria:

```text
affected node has status=unknown reasonCode=node_not_ready.
Ready condition is Unknown/False with reason node_not_ready.
No false node_ready is shown for the affected node.
```

### G4: CSI Registration Blocker

Use a controlled CSI node pod / CSINode registration fault, preferably by
blocking the CSI node DaemonSet image or selecting one node whose CSINode lacks
`block.csi.seaweedfs.com`.

Pass criteria:

```text
missing CSIDriver or missing CSINode driver registration projects
reasonCode=csi_driver_not_registered.
CSI node pod not Ready projects reasonCode=csi_node_pod_not_ready unless a more
specific image blocker is present.
CRD status, report, dashboard/operator-snapshot, and explain agree.
No Ready=True is shown for the blocked node.
```

### G5: RBAC Boundary

Run `kubectl auth can-i` as the operator-status service account.

Pass criteria:

```text
Allowed:
- get/list/watch nodes
- get/list/watch pods
- get/list/watch csidrivers.storage.k8s.io
- get/list/watch csinodes.storage.k8s.io
- get/list/watch swblockclusters,swblockvolumes
- patch/update swblockclusters/status,swblockvolumes/status
- create events

Forbidden:
- create/update/patch/delete pods
- create/update/patch/delete PVC/PV
- create/update/patch/delete deployments
- create/update/patch/delete storageclasses
- create/update/patch/delete secrets
- update/patch/delete swblockclusters or swblockvolumes spec
```

## Cleanup

Run the standard Helm uninstall and cleanup verifier. Final pass requires:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
multipath_residue_count=0
process_residue_count=0
hostpath_residue_count=0
```

## Report

Write sign-off to:

```text
internal/docs/qa-assignments/phase37-d2-live-node-csi-evidence-qa-signoff.md
```
