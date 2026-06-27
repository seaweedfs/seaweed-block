# Finished Plan: Phase 100 NVMe CSI Multipath Component

Status: closed for the component slice; live Kubernetes attach remains next.

## Problem

Phase 99 showed that NVMe ANA and CSI single-path NVMe staging already exist,
but Kubernetes CSI did not yet preserve multiple NVMe frontend paths for a
single namespace. Without that, the product cannot claim CSI NVMe multipath
attach even if the lower NVMe/ANA pieces exist.

## Delivered

- `PublishTarget` now carries `NVMeAddrs`.
- CSI publish context emits `nvmeAddrs` while preserving legacy `nvmeAddr`.
- master status lookup groups multiple NVMe frontends only when they share the
  same `NQN` and `NSID`.
- NodeStage reads `nvmeAddrs` and connects every NVMe address for the same NQN.
- Component tests cover grouping, no cross-NQN merge, multi-address NodeStage,
  and cleanup on mount failure.
- A swblock runner scenario records the component evidence and blocks false
  release claims by asserting that live k8s NVMe multipath remains required.

## Gates

```text
bash scripts/run-phase100-nvme-csi-multipath-component-gate.sh .
C:\work\swblock.exe validate testops\scenarios\nvme-csi-multipath-component-chain.yaml
C:\work\swblock.exe run testops\scenarios\nvme-csi-multipath-component-chain.yaml
go test ./core/frontend/nvme ./cmd/blockvolume ./core/csi ./core/launcher -count=1
```

Runner evidence:

```text
20260627-013844-4a23
nvme-csi-multipath-component-chain PASS
10/10 actions
```

## Non-Claims

- no live Kubernetes dynamic PVC NVMe multipath attach;
- no app writer/reader mounted through multiple NVMe paths;
- no NVMe cleanup/residue proof;
- no release claim for operation milestone images.

## Next

Continue Phase 100 with the live Kubernetes attach close gate:

```text
NVMe multipath frontends
  -> CSI dynamic PVC protocol=nvme
  -> NodeStage connects all paths
  -> app writer/reader verifies mounted data
  -> cleanup proves no stale NVMe subsystem/controller residue
```
