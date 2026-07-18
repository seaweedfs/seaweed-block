# Phase 165 Finished Plan: NVMe/RDMA Kubernetes Publish And Attach

Status: closed on 2026-07-18.

## Outcome

Seaweed Block now has an explicit, opt-in Kubernetes NVMe/RDMA single-path in
the supported RoCE lab:

```text
StorageClass nvmeTransport=rdma
-> lifecycle and frontend publication transport=rdma
-> RDMA blockvolume target on m02/10.0.0.3
-> CSI NodeStage nvme connect -t rdma on m01
-> mounted Pod writer/reader
-> CRD and host-controller transport evidence
-> exact delete/uninstall cleanup
```

NVMe/TCP remains the compatibility default for old or empty transport records.
Invalid transport combinations fail closed, and RDMA does not fall back to TCP.

## Delivered

- Added typed NVMe transport to lifecycle RPCs, stored volume intent, frontend
  facts, CSI publish context, observation evidence, and CRD status.
- Added `storageClass.nvmeTransport` and generator
  `--nvme-transport tcp|rdma`; RDMA requires protocol NVMe, a complete external
  frontend map, and `100gbe_roce`.
- Made CSI NodeStage and mounted reconnect issue transport-aware
  `nvme connect -t rdma`.
- Rendered privileged `/dev`, configfs, and module mounts only for RDMA target
  Pods; TCP and iSCSI remain unchanged.
- Added `kmod` to the runtime image so product-owned module preflight can call
  `modprobe` rather than silently skipping it.
- Added a TestOps close gate using fresh matching product and CSI images on m02
  and m01, with scoped failure cleanup.

## Live Findings

The gate caught two product defects before close:

1. The target image lacked `modprobe`, so the real `nvmet-rdma` target could not
   start even though unit tests passed.
2. `ReplicaEvidence` protobuf omitted the transport, so a real RDMA mount was
   projected as TCP in `SwBlockVolume.status`.

Both were fixed at their owning boundaries and covered by regression tests.
The gate also fixed its own namespace lookup, verifier output filename, status
CR cleanup ownership, and PV/provisioner ordering instead of converting those
failures into false product evidence.

## QA Evidence

TestOps run `20260718-025048-9d6a` passed 14/14 actions. See
`internal/docs/qa-assignments/phase165-nvme-rdma-kubernetes-publish-attach-qa-signoff.md`.

## Claim Boundary

This phase supports a source-gated supported-lab claim for one Kubernetes
NVMe/RDMA publish/attach path with mounted I/O and zero residue. It does not
claim RDMA multipath, reconnect after target movement, failover, performance
improvement, broad kernel/NIC/distro compatibility, or production SLOs.

The next protocol milestone should own reconnect/failover semantics explicitly
before making any transparent-HA claim.
