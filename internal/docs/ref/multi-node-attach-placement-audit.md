# Multi-Node Attach Placement Audit

Date: 2026-05-12

Plan: `current-plan.md` / Multi-Node Attach And Placement MVP

## Summary

The current product has useful placement primitives, but the alpha Kubernetes
attach path is still same-node only.

The master/lifecycle code can register multiple nodes, choose placement slots,
materialize blockvolume workloads, and allocate node-local ports per server.
However, the rendered Kubernetes blockvolume frontend and status endpoints are
loopback addresses (`127.0.0.1`). CSI publishes those observed frontend facts
directly to the node plugin, and the publish lookup does not filter by the
requesting CSI node. That is safe only when the app pod's CSI node and the
blockvolume process are on the same Kubernetes node.

## Lab State

Observed lab:

- `m02` is the only node currently in the k3s cluster.
- `m02` is labeled with `topology.block.csi.seaweedfs.com/node=m02`.
- SSH to the presumed second lab node `192.168.1.183` timed out during this
  audit, so a true two-node live gate cannot be run yet from the current lab.

Implication: D4 needs an explicit two-node lab prerequisite before it can claim
live multi-node behavior. Until then, code and scenario work should prove the
placement contract with fast tests and single-node safety gates.

## Code Findings

### Placement And Workload Planning

What exists:

- `core/lifecycle.PlanPlacement` selects placement candidates from node
  inventory and desired RF.
- `core/lifecycle.PlanBlockVolumeWorkloads` converts placement slots into
  blockvolume workload plans.
- `core/host/master.RunLifecycleWorkloadPlanTick` allocates node-local ports per
  `ServerID`, so multiple volumes on the same node do not collide.
- `cmd/blockmaster` can import a cluster spec containing multiple nodes and
  placement slots.

This is enough substrate for multi-node placement accounting.

### Kubernetes Rendering

`core/launcher.RenderBlockVolumeDeployments` renders each blockvolume
Deployment with:

- `hostNetwork: true`
- `nodeSelector: kubernetes.io/hostname=<replica.ServerID>`
- `--data-addr=<node data addr>`
- `--ctrl-addr=<node ctrl addr>`
- `--status-addr=127.0.0.1:<derived port>`
- `--iscsi-listen=127.0.0.1:<allocated port>` for iSCSI
- `--nvme-listen=127.0.0.1:<allocated port>` for NVMe

The node selector is useful. The loopback frontend/status endpoints are the
limiting factor for remote-node attach and host-side inventory collection.

### CSI Publish Lookup

`core/csi.ControlStatusLookup.LookupPublishTarget(ctx, volumeID, nodeID)` reads
master status and returns the first observed frontend target. It accepts
`nodeID`, but currently does not use it to select or reject targets.

The node plugin then uses the published `iscsiAddr` directly for `iscsiadm`.
If that address is `127.0.0.1:<port>`, it targets the app node's loopback, not a
remote blockvolume node.

### Alpha Scripts

`scripts/install-k8s-alpha.sh` and `scripts/run-alpha-app-demo.sh` select the
first Kubernetes node:

```bash
NODE_NAME="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
```

`deploy/k8s/alpha/block-stack.yaml` then renders the cluster spec for that one
node with loopback data/control addresses.

This matches the existing single-node proof, but it is not a complete
multi-node user model.

## Supported Model For This Plan

For the remainder of this plan, the supported alpha model should be:

```text
same-node RF=1 attach on a multi-node-capable Kubernetes cluster
```

Meaning:

- The blockvolume workload is placed on a known Kubernetes node.
- The app pod is scheduled onto that same node for the attach/write/read path.
- CSI publishes loopback iSCSI endpoints only for same-node attach.
- Inventory must show PVC, PV, app node, blockvolume node, server ID, frontend,
  status endpoint, lifecycle owner, desired/observed count, and support bundle.
- If app node and blockvolume node diverge while the endpoint is loopback,
  inventory or the negative fixture must name the unsupported placement.

Explicit non-claim:

```text
remote-node attach to a loopback-published blockvolume is not supported
```

To claim remote-node attach later, the product needs a routable frontend
strategy, for example a Service-backed iSCSI endpoint, host IP frontend
publishing, or an in-cluster attach/proxy model.

## Recommended Next Slice

D2 should define the same-node placement and endpoint contract:

- how the alpha installer chooses or accepts the target blockvolume node,
- how the app pod is pinned or guided to the same node,
- what exact inventory fields prove app-node/blockvolume-node alignment,
- how unsupported cross-node placement is reported,
- what future work is needed for routable remote-node attach.

QA needed now: no. QA should be engaged after D2/D3 produce a concrete
contract plus tests, or after a true two-node lab is available.
