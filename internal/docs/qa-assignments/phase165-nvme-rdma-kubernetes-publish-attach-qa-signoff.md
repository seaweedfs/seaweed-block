# Phase 165 QA Sign-Off: NVMe/RDMA Kubernetes Publish And Attach

Status: PASS on 2026-07-18.

## Run

- Scenario: `testops/scenarios/nvme-rdma-k8s-publish-attach-chain.yaml`
- Run: `20260718-025048-9d6a`
- Result: 14/14 actions PASS
- Target: m02, RoCE `10.0.0.3`
- CSI application node: m01, RoCE `10.0.0.1`
- Images: fresh no-cache matching `sw-block:local` and
  `sw-block-csi:local`, imported to the participating k3s nodes

## Evidence

```text
phase165_nvme_rdma_k8s_publish_attach_status=ok
rdma_host_preflight=ok
generated_nvme_transport=rdma
generated_frontend_network_class=100gbe_roce
csi_publish_context_transport=rdma
active_host_controller_transport=rdma
active_host_controller_traddr=10.0.0.3
swblockvolume_status_transport=rdma
writer_verified=true
reader_verified=true
tcp_fallback_observed=false
target_configfs_residue_count=0
target_nbd_residue_count=0
app_nvme_controller_residue_count=0
kubernetes_product_residue_count=0
cleanup_status=ok
```

The live initiator evidence identified `controller=nvme1`, `transport=rdma`,
and `address=traddr=10.0.0.3,trsvcid=4420`. The CRD independently reported the
same RDMA transport and endpoint while the PVC was mounted.

## Gate Coverage

- Typed StorageClass, lifecycle RPC, frontend fact, CSI publish-context, and
  CRD-status transport agreement: PASS.
- Dynamic PVC, writer, reader, and held mounted Pod on the real CSI node path:
  PASS.
- No implicit TCP fallback: PASS.
- RDMA-only target privilege and host mounts: PASS.
- Exact NQN controller, configfs, NBD, Kubernetes resources, CR instances, and
  Helm-retained CRD cleanup: PASS.
- Existing NVMe/TCP compatibility tests: PASS.

## Findings Resolved During QA

- Runtime image missing `modprobe`: fixed by installing `kmod`.
- Cluster-evidence RPC dropped frontend transport and projected false TCP:
  fixed with an additive protobuf field plus server/client round-trip tests.
- Gate Deployment namespace, verifier filename, status-CR teardown, and PV
  deletion ordering: corrected and kept scoped to gate-owned resources.

## Non-Claims

This sign-off does not cover RDMA multipath, reconnect/failover, performance or
SLO improvement, broad compatibility, or published release images.
