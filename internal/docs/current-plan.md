# Current Plan: Phase 100 Kubernetes CSI NVMe Multipath Attach

Status: closed; D1/D2 component slice PASS, live Kubernetes gate PASS.

## Goal

Phase 99 pinned the current NVMe baseline:

- ANA Identify/Get Log Page and provider-backed ANA state exist.
- Direct-host ANA/multipath gates exist.
- CSI can select and stage a single NVMe publish target.

Phase 100 starts the Kubernetes CSI parity gap that remains:

```text
multiple NVMe frontend paths for one NQN/NSID
  -> master status groups them as one multipath publish target
  -> CSI publish context preserves all path addresses
  -> NodeStage connects every path
  -> app pod sees one mounted namespace
  -> cleanup proves no stale NVMe subsystem residue
```

This phase is separate from the Operation Layer release-readiness gate. The
operation milestone release remains blocked until matching published
`seaweed-block` and `seaweed-block-csi` images exist and pass the pinned-image
smoke. Development is continuing on NVMe in parallel.

## D1/D2 Component Slice

Implemented component-level support for the first half of the path:

- `PublishTarget` can carry `NVMeAddrs` in addition to the legacy first
  `NVMeAddr`.
- master status lookup groups multiple NVMe frontends only when they share the
  same `NQN` and `NSID`.
- CSI publish context emits:

  ```text
  protocol=nvme
  nvmeAddr=<first path>
  nvmeAddrs=<comma-separated paths>
  nqn=<subsystem NQN>
  ```

- NodeStage reads `nvmeAddrs`, connects each address for the same NQN, records
  staged multipath metadata, and preserves the existing single-path fallback.

## Gates

Added:

```text
scripts/run-phase100-nvme-csi-multipath-component-gate.sh
testops/scenarios/nvme-csi-multipath-component-chain.yaml
scripts/run-phase100-nvme-csi-multipath-live-gate.sh
testops/scenarios/nvme-csi-multipath-live-chain.yaml
```

The component gate proves:

- same-NQN/NSID NVMe frontends are grouped into a single CSI multipath target;
- different NQN frontends are not silently merged;
- NodeStage connects all NVMe addresses in `nvmeAddrs`;
- mount failure still cleans up the NVMe connection state;
- live Kubernetes NVMe multipath attach remains required before release claim.

Verification:

```text
local component gate: PASS
swblock validate nvme-csi-multipath-component-chain.yaml: PASS
swblock run 20260627-013844-4a23: PASS, 10/10 actions
go test ./core/frontend/nvme ./cmd/blockvolume ./core/csi ./core/launcher -count=1: PASS
```

## D3/D4 Live Kubernetes Slice

Implemented the live attach close gate:

- dynamic PVC can request `protocol=nvme`, `replicationFactor=2`, and
  `stage2_multipath=true`;
- CSI `CreateVolumeResponse.VolumeContext` carries the safe multipath attach
  parameter forward into PV volume attributes;
- `ControllerPublish` preserves the multipath request in publish context;
- `NodeStage` performs a bounded refresh until `nvmeAddrs` contains at least two
  portals, then connects each portal for the same NQN;
- dynamic lifecycle status can merge fresh observed replica IDs for read-only
  publish-target aggregation when lifecycle records do not persist placement
  slots;
- the live runner imports fresh local images to every schedulable node and
  removes stale containerd tags before import.

Verification:

```text
swblock run testops/scenarios/nvme-csi-multipath-live-chain.yaml
run: 20260627-024451-2ee8
result: PASS, 18/18 actions

phase100_nvme_csi_multipath_live_status=ok
generated_nvme_listen_count=2
generated_nqn_unique_count=1
generated_nsid_unique_count=1
node_stage_nvme_multipath_count=1
node_stage_two_portals_count=1
nvme_residue_count=0
```

## Non-Claims

- no RoCE, performance, broad host compatibility, or production HA claim;
- no automatic release claim for the operation milestone;
- no backup/snapshot/restore.

## Next

Phase 100 is closed for the supported lab path. The next NVMe work should not
expand the claim broadly yet; use a larger follow-up milestone for soak,
failure-path behavior, or ANA/CSI status surfacing if needed.
