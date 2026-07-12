# Phase 125 Block NVMe/TCP Write-Path Profile QA Sign-off

Verdict: PASS.

Validated on: 2026-07-03.

Environment: m02 k3s lab. The gate used the configured frontend/data-plane map
`m01=10.0.0.1,m02=10.0.0.3`, ran a 512MiB profiled Block NVMe/TCP write, then
ran a same-node 512MiB `local-path` write comparator.

## Runner Gate

Command:

```powershell
C:\work\swblock.exe run `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  testops\scenarios\nvme-tcp-write-path-profile-chain.yaml
```

Runner result:

```text
=== nvme-tcp-write-path-profile-chain === PASS (4m7.455s)
22 actions: 22 passed, 0 failed
run bundle: results\20260703-005137-2fc9
```

Summary:

```text
phase125_block_nvme_tcp_write_path_profile_status=ok
frontend_transport=tcp
frontend_ip_map=m01=10.0.0.1,m02=10.0.0.3
frontend_network_class=100gbe_tcp
nvme_rdma_supported=false
roce_claim_allowed=false
performance_slo_claim_allowed=false
test_shape=dd_exec_profiled_seq_write
network_baseline_mibps=3836.30
publish_target=10.0.0.1:4420
route_dev=enp1s0np0
publish_target_network_class=100gbe_tcp
block_seq_size_mib=512
block_write_duration_ms=2937
block_nvme_seq_write_mibps=174.33
block_nvme_seq_read_mibps=544.10
local_path_seq_write_duration_ms=446
local_path_seq_write_mibps=1147.98
local_path_seq_read_mibps=513.54
block_vs_local_write_ratio=0.152
block_vs_local_read_ratio=1.060
blockvolume_cpu_sample_count=3
blockvolume_cpu_peak_percent=0.80
blockvolume_cpu_avg_percent=0.80
write_path_observation=backend_sync
top_bottleneck=backend_sync
next_recommendation=phase126_durable_backend_write_optimization
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

The 512MiB shape makes the write gap clearer:

```text
local_path_seq_write_mibps=1147.98
block_nvme_seq_write_mibps=174.33
block_vs_local_write_ratio=0.152
```

The read path still does not show the same gap:

```text
local_path_seq_read_mibps=513.54
block_nvme_seq_read_mibps=544.10
block_vs_local_read_ratio=1.060
```

During the profiled Block write, `kubectl top pods -A --containers` captured
three blockvolume samples and did not show target CPU saturation:

```text
blockvolume_cpu_peak_percent=0.80
blockvolume_cpu_avg_percent=0.80
```

This points away from a simple CPU-bound target-copy loop and toward a
write-side wait/sync/backend path. The evidence is still coarse because
metrics-server samples are low-frequency and only three samples landed during
the 2.9s write. Phase 126 should instrument the write path closer to code:
target receive/write timing, backend/durable write timing, and sync boundaries.

## Harness Findings

- Initial run used 2Gi PVCs and timed out before the profiled pod became Ready.
  The gate was narrowed to 1Gi PVCs with a 512MiB test file, which is sufficient
  for the profile and avoids unnecessary provisioning/attach risk.
- Phase 120 gained an opt-in profiling hook:
  `SW_BLOCK_PHASE120_PROFILE_WRITE=true`. It is default-off, so existing
  Phase 120/122/123/124 gates keep their previous behavior unless explicitly
  enabled.

## Non-Claims

Phase 125 does not implement or validate NVMe/RDMA, RoCE, GPU Direct,
cuFile/cuObject, NIXL, production HA, broad host compatibility, or a
performance SLO. It only narrows the current write-side bottleneck direction
and recommends Phase 126 backend/write instrumentation.
