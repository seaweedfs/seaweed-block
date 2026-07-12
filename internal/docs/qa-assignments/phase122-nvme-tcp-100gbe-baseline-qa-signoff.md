# Phase 122 NVMe/TCP 100GbE Live Baseline QA Sign-off

Verdict: PASS.

Validated on: 2026-07-03.

Environment: m02 k3s lab, fresh local `sw-block:local` and
`sw-block-csi:local` images built from the Phase 122 working tree and imported
to m01/m02. The gate used the configured frontend/data-plane map
`m01=10.0.0.1,m02=10.0.0.3`.

## Gate

Command:

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/manual-phase122-nvme-100gbe-pass \
SW_BLOCK_PHASE122_FRONTEND_IP_MAP=m01=10.0.0.1,m02=10.0.0.3 \
SW_BLOCK_PHASE122_EXPECTED_ROUTE_DEV=enp1s0np0 \
SW_BLOCK_IMPORT_K3S_SSH_KEY=/opt/work/testdev_key \
KUBECONFIG=/etc/rancher/k3s/k3s.yaml \
  bash scripts/run-phase122-nvme-tcp-100gbe-baseline-gate.sh /tmp/seaweed_block
```

Summary:

```text
phase122_nvme_tcp_100gbe_baseline_status=ok
frontend_transport=tcp
frontend_ip_map=m01=10.0.0.1,m02=10.0.0.3
frontend_network_class=100gbe_tcp
nvme_rdma_supported=false
roce_claim_allowed=false
performance_slo_claim_allowed=false
management_ip=192.168.1.181
frontend_ip=10.0.0.1
publish_target=10.0.0.1:4420
publish_target_network_class=100gbe_tcp
publish_target_source=configured_data_plane
publish_target_route_dev=enp1s0np0
internal_ip_not_reused_as_performance_target=true
managed_volume_status=ready
managed_volume_reason=first_volume_verified
seq_size_mib=64
seq_write_duration_ms=556
seq_write_mibps=115.11
seq_read_duration_ms=255
seq_read_mibps=250.98
small_write_ops=256
small_write_block_bytes=4096
small_write_duration_ms=422
small_write_iops=606.64
small_write_mibps=2.37
final_data_verified=true
cleanup_status=ok
```

Final explicit cleanup verifier:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## What This Proves

- The generated Helm values and rendered blockvolume cluster spec can publish
  the NVMe/TCP target on the configured data-plane IP instead of the Kubernetes
  management/InternalIP.
- The live publish target was `10.0.0.1:4420`.
- The app node route to the target used `enp1s0np0`, matching the configured
  100GbE TCP fabric path.
- The read-only status path preserved management IP, frontend IP, and
  `frontendNetworkClass=100gbe_tcp`.
- The current Kubernetes NVMe/TCP path completed marker write/read, sequential
  write/read, small-write, final verification, and cleanup.

## Fixes Found By The Gate

The first live run proved the target had already moved to `10.0.0.1:4420`, but
`ops cluster --master-api` dropped `frontend_ip` and `frontend_network_class`.
Root cause: `core/rpc/proto/control.proto` lacked those fields on
`NodeEvidence`, so the gRPC observation boundary truncated evidence that was
present in the in-process model.

Fix:

- add `frontend_ip` and `frontend_network_class` to `NodeEvidence`;
- map the fields in `nodeEvidenceToWire`;
- map the fields back in `nodeEvidenceFromWire`;
- add server-side and CLI-side regression tests.

A second issue was in the Phase 122 wrapper only: it read the first
`phase120_nvme_tcp_performance_baseline_status=running` line instead of the
last `status=ok` line from the inner Phase 120 summary. The wrapper now uses
the last value for repeated summary keys.

## Non-Claims

This is not RDMA performance evidence. It does not claim RoCE, NVMe/RDMA,
GPU Direct, cuFile/cuObject, NIXL, production HA, broad host compatibility, or
any performance SLO. The measured throughput is a baseline for the current
NVMe/TCP implementation over the configured 100GbE TCP path.
