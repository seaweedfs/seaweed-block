# QA Sign-off - Phase 36 D2 Node Readiness Status

Verdict: **PASS.** Node evidence projects correctly into
`SwBlockCluster.status.nodes[]` and agrees with `operator-snapshot.json`:
G1 (1-node) and G2 (3-node) verified live, G4 boundary intact, G3 (missing-image
node blocker) verified as **replay-only PASS** with a live follow-up filed (the
live evidence source cannot yet produce unhealthy nodes — see below).

Date: 2026-06-05

Source commit: `9900be5 phase36: project node readiness status`
(branch `phase33-testops-failure-hardening`)

Environment: 3-node k3s `v1.34.4+k3s1` (m01/m02/tp01), write-mode operator-status
(`operatorStatus.create=true, dryRun=false`), fresh `9900be5` images on all nodes.

## G1 — Healthy 1-Node Lab — PASS

Default chart (single `blockNodes` entry, m02). Deployed controller patched
`SwBlockCluster.status` live from blockmaster (`--master-api`):

```text
SwBlockCluster.status.nodeCount=1
node name=m02 k8s=m02 status=ready reason=node_ready ready=true schedulable=true
operator-snapshot.json: node_count=1, nodes[0]={name:m02, status:ready, reason_code:node_ready, Ready=True}
```

CRD status and operator-snapshot agree. PASS.

## G2 — Healthy 3-Node Lab — PASS

Installed with `sw-block ops generate-helm-values` (discovered 3 Ready k3s
nodes, real InternalIPs). All three launchers registered with blockmaster:

```text
SwBlockCluster.status.nodeCount=3
node=m01 k8s=m01 status=ready reason=node_ready ready=true
node=m02 k8s=m02 status=ready reason=node_ready ready=true
node=tp01 k8s=tp01 status=ready reason=node_ready ready=true
operator-snapshot.json: node_count=3, names m01/m02/tp01 all status=ready
```

One entry per observed k3s node; all ready/schedulable show
`status=ready reason=node_ready`; CRD and snapshot node sets match.

**First-volume writer/reader** (sw-block-dynamic PVC + consuming pod):

```text
PVC g2-vol -> Bound (pvc-16bf06f0-…)
pod g2-rw  -> 1/1 Running
pod log    -> SW_BLOCK_G2_DATA   (wrote /data/test.txt and read it back)
```

First volume provisions, attaches, and the workload writes+reads on the 3-node
cluster. PASS.

## G3 — Missing Image / Node Blocker — REPLAY-ONLY PASS (live follow-up filed)

Driven via a crafted `cluster-evidence.json` node with
`missing_images:["sw-block-csi:local"]` (separate `--cluster-name g3-cluster`
so the live controller's `sw-block` writes don't race it):

```text
node=m02 status=blocked reason=image_missing_on_node ready=true
         missingImages=["sw-block-csi:local"]
node conditions: Ready=False (image_missing_on_node), Blocked=True (image_missing_on_node)
cluster: readyVolumeCount=0, blockedVolumeCount=0, volumeCount=0, Ready=True conditions=0
```

All G3 assertions hold: node `status=blocked`, `reasonCode=image_missing_on_node`,
`Ready=False` + `Blocked=True`, `missingImages[]` names the image, and the
cluster shows **no** false `Ready=True`.

### Why this is replay-only (and the live follow-up)

This is not just a convenience choice — the **live `--master-api` path cannot
currently produce an unhealthy node.** Blockmaster's node evidence builder
(`core/host/master/observation_snapshot.go:observationNodes`) hardcodes
`Schedulable: true`, `Ready: true`, and never populates `MissingImages`:

```go
out = append(out, ops.NodeEvidence{
    NodeName: node.ServerID, KubernetesNode: ...,
    Schedulable: true,   // hardcoded
    Ready:       true,   // hardcoded
    // MissingImages never set
})
```

So on the live path, every registered node classifies as `ready/node_ready`. The
three negative node reasons added in D2 — `node_not_ready`,
`node_scheduling_disabled`, `image_missing_on_node` — are correct and unit-tested
in the projection, but are **unreachable live** until blockmaster carries real
node facts (k8s NodeReady/Unschedulable, and an image-presence check) into
`NodeEvidence`.

**Follow-up to file:** populate live `NodeEvidence.Ready` / `.Schedulable` from
the k8s node status and `.MissingImages` from a node image-presence check, so the
D2 negative node reasons fire on the live `--master-api` surface. Until then, G3
is necessarily replay-only and the live node surface always shows nodes healthy.

## G4 — Boundary — PASS

operator-status SA, live `auth can-i`:

```text
ALLOWED:  get swblockclusters: yes   patch swblockclusters --subresource=status: yes   create events: yes
FORBIDDEN: patch swblockclusters (spec): no   create pods: no   patch deployments.apps: no
           create persistentvolumeclaims: no  create persistentvolumes: no
           create secrets: no                 create storageclasses.storage.k8s.io: no
```

Node-readiness projection added no new mutation power. PASS. (Reminder from D7:
use `--subresource=status`, not `…/status`, on kubectl v1.34 — the latter
false-negatives.)

## Minor Observation (non-blocking)

On a blocked (missing-image) node, the `.ready` boolean field is `true` while
`.status=blocked` and the `Ready` *condition* is `False`. `.ready` carries the
raw kubelet/node readiness (the node IS a Ready k8s node) while the status +
condition carry the block-readiness judgment (it cannot serve because the image
is missing). Both are individually correct, but a consumer reading `.ready=true`
alongside `status=blocked` could be momentarily confused. Consider documenting
that `.ready`/`.schedulable` are raw node facts and `.status`/conditions are the
block-readiness verdict — or deriving `.ready=false` when the node is blocked.
Not a blocker.

## Non-Claims Verified

No CR objects auto-created (stubs were pre-created by QA), no node repair, no
image import, no scheduler/cleanup/storage/workload mutation — consistent with
G4 and the controller's status-only surface.

## Lab State

Clean — first-volume pod/PVC deleted, both `SwBlockCluster` stubs deleted, helm
uninstalled, both CRDs deleted; 0 sw-block pods, 0 CRDs, 0 PVCs, 0 iSCSI sessions.

## Bottom Line

- **D2 PASS.** Node readiness projects into `SwBlockCluster.status.nodes[]` and
  matches `operator-snapshot.json`: G1 (1 node) and G2 (3 nodes) live, all
  healthy nodes `ready/node_ready`, first-volume writer/reader works on the
  3-node cluster, and the missing-image node correctly projects
  `blocked/image_missing_on_node` with `Ready=False` + `Blocked=True` and no
  false cluster `Ready=True`. Boundary unchanged.
- **G3 is replay-only** because blockmaster's live node evidence hardcodes
  ready/schedulable and omits missing-images. **File the live follow-up**:
  populate real `Ready`/`Schedulable`/`MissingImages` in `observationNodes` so
  the D2 negative node reasons are reachable on the live path. Until then the
  live node surface always shows healthy nodes.
- **One non-blocking polish:** clarify/reconcile `.ready=true` vs
  `status=blocked` on a blocked node.
- **D2 can close** per the assignment's "G3 PASS or replay-only PASS with live
  follow-up filed" criterion.
