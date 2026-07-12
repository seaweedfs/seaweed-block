# Phase 124 NVMe/TCP Target / Backend / Shape Split QA Sign-off

Verdict: PASS.

Validated on: 2026-07-03.

Environment: m02 k3s lab. The gate used the configured frontend/data-plane map
`m01=10.0.0.1,m02=10.0.0.3`, reused the Phase 123 network + Block NVMe/TCP
path, then compared a same-shape `local-path` PVC on the same app node.

## Runner Gate

Command:

```powershell
C:\work\swblock.exe run `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  testops\scenarios\nvme-tcp-target-backend-shape-split-chain.yaml
```

Runner result:

```text
=== nvme-tcp-target-backend-shape-split-chain === PASS (4m5.661s)
28 actions: 28 passed, 0 failed
run bundle: results\20260702-235727-bcfd
```

Summary:

```text
phase124_nvme_tcp_target_backend_shape_split_status=ok
frontend_transport=tcp
frontend_ip_map=m01=10.0.0.1,m02=10.0.0.3
frontend_network_class=100gbe_tcp
nvme_rdma_supported=false
roce_claim_allowed=false
performance_slo_claim_allowed=false
test_shape=dd_exec_baseline
network_baseline_mibps=3769.28
publish_target=10.0.0.1:4420
route_dev=enp1s0np0
publish_target_network_class=100gbe_tcp
management_ip=192.168.1.181
frontend_ip=10.0.0.1
block_app_node=m02
local_path_storageclass=local-path
block_nvme_seq_write_mibps=118.74
block_nvme_seq_read_mibps=273.50
block_nvme_small_write_iops=721.13
local_path_seq_write_mibps=324.87
local_path_seq_read_mibps=235.29
local_path_seq_write_nofsync_mibps=383.23
local_path_small_write_iops=630.54
block_vs_local_read_ratio=1.162
block_vs_local_write_ratio=0.366
local_vs_network_read_ratio=0.062
local_vs_network_write_ratio=0.086
shape_fsync_penalty=1.180
top_bottleneck=block_target_or_backend
next_recommendation=phase125_blockvolume_target_cpu_profile
cleanup_status=ok
```

Final cleanup verifier:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Interpretation

The network is still far ahead of both mounted paths:

```text
network_baseline_mibps=3769.28
local_path_seq_write_mibps=324.87
block_nvme_seq_write_mibps=118.74
```

The important split is asymmetric:

- Block read is not below local-path read: `273.50 MiB/s` vs `235.29 MiB/s`.
- Block write is much lower than local-path write: `118.74 MiB/s` vs
  `324.87 MiB/s`.
- The local-path no-fsync shape improves only modestly:
  `shape_fsync_penalty=1.180`, so this is not simply a `conv=fsync` artifact.

The next work should profile the Block write path first: target CPU, per-write
copying, durable/backend write path, and sync behavior. Starting NVMe/RDMA now
would not address the demonstrated write-side gap.

## Harness Notes

- The gate runs the Block NVMe/TCP path through Phase 123, then tears it down
  before creating the local-path PVC. This keeps the comparison in one clean
  runner bundle while avoiding storageclass/PVC interference.
- The local-path storageclass is auto-detected; the validated run used
  `local-path`.
- The same app node was used for both paths: `m02`.

## Non-Claims

Phase 124 does not implement or validate NVMe/RDMA, RoCE, GPU Direct,
cuFile/cuObject, NIXL, production HA, or a performance SLO. It is a diagnostic
split that makes Phase 125 a write-side Block target/backend profiling task.
