# Current Plan: Phase 165 NVMe/RDMA Kubernetes Publish And Attach

Status: planning.

Phase 164 closed the standalone Linux NVMe/RDMA correctness and lifecycle gate.
The transport remains invisible to Kubernetes: RDMA targets are not published
by the volume process, the control RPC has no explicit NVMe transport field,
and CSI node attach assumes NVMe/TCP.

## Goal

Add one opt-in Kubernetes NVMe/RDMA path from StorageClass intent through
launcher publication and CSI node attach to mounted Pod I/O. Keep NVMe/TCP as
the default and preserve the existing three-way control ownership boundaries.

## Deliverables

### D1. Typed Publish Contract

- Add an explicit NVMe transport value to frontend publication and CSI volume
  context; do not infer transport from port or address.
- Accept only `tcp` or `rdma`, defaulting old/empty records to `tcp`.
- Preserve compatibility for existing iSCSI and NVMe/TCP volumes.

### D2. Launcher And Chart Wiring

- Add an opt-in StorageClass/Helm value for NVMe/RDMA.
- Select a non-loopback node data-plane address and pass
  `--nvme-transport=rdma` to the target volume only when requested.
- Project module, RDMA device, bind-address, NBD, and configfs blockers before
  claiming a publishable target.

### D3. CSI Node Lifecycle

- Use transport-aware `nvme connect -t rdma` for an RDMA publish context.
- Make NodeStage/NodeUnstage idempotent and preserve foreign NVMe controllers.
- Refuse missing host prerequisites without falling back silently to TCP.

### D4. Mounted Workload Gate

- Create a PVC from the opt-in RDMA StorageClass.
- Prove the published target uses the RoCE address and RDMA transport.
- Mount the volume into a Pod and verify writer/reader checksums through the
  real CSI node path.

### D5. Negative And Cleanup Boundary

- Verify unsupported nodes surface a stable non-Ready reason with no false
  `Ready=True` and no TCP fallback.
- Delete Pod/PVC, uninstall, and return host controllers, configfs, NBD,
  iSCSI/multipath, CRDs, and product processes to baseline.
- Keep failover, multipath, performance, broad compatibility, and SLOs as
  explicit non-claims.

### D6. Close Gate

- Package D1-D5 into one TestOps scenario using fresh matching product and CSI
  images.
- Require control-plane status, CSI evidence, mounted I/O, and independent host
  cleanup evidence in the same run bundle.

## Exit Criteria

Phase 165 closes only when a normal Kubernetes user can select the opt-in
NVMe/RDMA class, mount a PVC, read back written data, and delete it with zero
residue while NVMe/TCP behavior remains unchanged. The next phase may then own
NVMe/RDMA reconnect/failover; performance work remains later and separate.
