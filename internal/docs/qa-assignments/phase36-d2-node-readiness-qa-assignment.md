# Phase 36 D2 QA Assignment - Node Readiness Status

Status: ready for QA.

Source scope:

- `SwBlockCluster.status.nodes[]`
- `operator-snapshot.json.cluster.nodes[]`
- read-only/status-only operator-status controller

## Goal

Verify that existing product-owned node evidence is visible through
Kubernetes-native read-only status and agrees with the support/report surface.

This gate must not mutate storage or workloads.

## Required Checks

### G1: Healthy 1-Node Lab

Run the documented Helm first-volume path with operator-status enabled in
write mode.

Expected:

```text
SwBlockCluster.status.nodeCount=1
SwBlockCluster.status.nodes[0].status=ready
SwBlockCluster.status.nodes[0].reasonCode=node_ready
SwBlockCluster.status.nodes[0].ready=true
SwBlockCluster.status.nodes[0].schedulable=true
operator-snapshot.json cluster.node_count=1
operator-snapshot.json cluster.nodes[0].status=ready
no storage/workload mutation permission beyond CRD status and Events
```

### G2: Healthy 3-Node Lab

Run the same path on the 3-node k3s lab.

Expected:

```text
SwBlockCluster.status.nodeCount=3
SwBlockCluster.status.nodes has one entry per observed k3s node
all ready/schedulable nodes show status=ready reasonCode=node_ready
operator-snapshot.json has the same node names and node statuses
first-volume still verifies writer/reader
```

### G3: Missing Image / Node Blocker

Use an existing blocked-image path or a controlled missing-image setup.

Expected:

```text
affected node status=blocked
affected node reasonCode=image_missing_on_node
affected node conditions include Ready=False and Blocked=True
missingImages[] names the missing image
cluster/volume status must not show false Ready=True because of node evidence
```

If a live missing-image setup is too expensive, QA may start with a
bundle-backed replay, but mark the result as replay-only.

### G4: Boundary

Verify the operator-status ServiceAccount still has only:

```text
get/list/watch swblockclusters, swblockvolumes
get/update/patch swblockclusters/status, swblockvolumes/status
create events
```

And still cannot mutate:

```text
pods, deployments, PVCs, PVs, secrets, storageclasses, CRD spec
```

## Pass Criteria

```text
G1 PASS
G2 PASS
G3 PASS or replay-only PASS with live follow-up filed
G4 PASS
cleanup residue clean
```

## Non-Claims

- No automatic CR object creation.
- No node repair.
- No image import.
- No cleanup mutation.
- No scheduler mutation.
- No storage/workload mutation.
