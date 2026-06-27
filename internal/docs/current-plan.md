# Current Plan: Phase 100 Kubernetes CSI NVMe Multipath Attach

Status: active; D1/D2 component slice PASS.

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

## Non-Claims

- no live Kubernetes app pod NVMe multipath claim yet;
- no RoCE, performance, broad host compatibility, or production HA claim;
- no automatic release claim for the operation milestone;
- no backup/snapshot/restore.

## Next

After the component gate passes, continue Phase 100 with a live Kubernetes gate:

```text
NVMe multipath frontend deployment
  -> CSI dynamic PVC with protocol=nvme
  -> NodeStage connects multiple paths for one NQN/NSID
  -> writer/reader verifies mounted data
  -> cleanup checks nvme subsystem/controller residue
```

The live gate should be the release-quality proof for the NVMe multipath attach
claim.
