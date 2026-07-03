# Phase 122 Finished Plan: NVMe/TCP 100GbE Live Baseline

Status: closed, QA PASS on 2026-07-03.

## Problem

Phase 120 measured the current Kubernetes NVMe/TCP path on the default
management/InternalIP network. Phase 121 then added explicit frontend/data-plane
address configuration. Phase 122 had to answer the next concrete question:

```text
Can the current Block NVMe/TCP Kubernetes path publish on the intended 100GbE
TCP frontend address, and what baseline throughput does it show today?
```

It also had to avoid a false RDMA claim. The current target is still NVMe/TCP;
RoCE/NVMe-RDMA remains unsupported until a separate live RDMA I/O gate passes.

## What Changed

- Added `scripts/run-phase122-nvme-tcp-100gbe-baseline-gate.sh`, a wrapper
  around the Phase 120 performance gate that requires:

  ```text
  publish_target=<10.0.0.x>:4420
  publish_target_network_class=100gbe_tcp
  publish_target_route_dev=enp1s0np0
  frontend_transport=tcp
  nvme_rdma_supported=false
  ```

- Extended the Phase 120 baseline gate to accept optional
  `SW_BLOCK_FRONTEND_IP_MAP`, `SW_BLOCK_FRONTEND_NETWORK_CLASS`, and
  `SW_BLOCK_EXPECTED_FRONTEND_ROUTE_DEV`.

- Added route evidence for the live publish target.

- Added the runner scenario
  `testops/scenarios/nvme-tcp-100gbe-baseline-chain.yaml`.

- Fixed the ClusterEvidence gRPC wire model so node frontend address evidence
  survives `blockmaster -> sw-block ops`:

  ```text
  NodeEvidence.frontend_ip
  NodeEvidence.frontend_network_class
  ```

## Verification

Local checks:

```text
go test ./cmd/sw-block ./core/host/master ./core/ops ./cmd/blockvolume
bash -n scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh scripts/run-phase122-nvme-tcp-100gbe-baseline-gate.sh
swblock validate testops/scenarios/nvme-tcp-100gbe-baseline-chain.yaml
```

Live gate:

```text
phase122_nvme_tcp_100gbe_baseline_status=ok
publish_target=10.0.0.1:4420
management_ip=192.168.1.181
frontend_ip=10.0.0.1
publish_target_network_class=100gbe_tcp
publish_target_source=configured_data_plane
publish_target_route_dev=enp1s0np0
internal_ip_not_reused_as_performance_target=true
managed_volume_status=ready
managed_volume_reason=first_volume_verified
seq_write_mibps=115.11
seq_read_mibps=250.98
small_write_iops=606.64
final_data_verified=true
cleanup_status=ok
```

QA sign-off:

```text
internal/docs/qa-assignments/phase122-nvme-tcp-100gbe-baseline-qa-signoff.md
```

## Product Meaning

The 100GbE TCP frontend address path is now real and observable, but the
baseline is not high enough to support a performance claim. The useful next
step is bottleneck attribution: determine whether the limit is in fio shape,
container networking, blockvolume CPU path, NVMe/TCP implementation, durable
store, or host/network configuration.

## Non-Claims

Phase 122 does not implement or validate NVMe/RDMA, RoCE, GPU Direct,
cuFile/cuObject, NIXL, performance SLOs, production HA, or broad host
compatibility.
