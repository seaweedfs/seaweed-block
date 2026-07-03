# Phase 123 NVMe/TCP Performance Bottleneck Triage QA Sign-off

Verdict: PASS.

Validated on: 2026-07-03.

Environment: m02 k3s lab. The runner synced a clean Phase 122 tree to
`/tmp/seaweed_block` and overlaid only the Phase 123 gate/scenario. The gate
used the configured frontend/data-plane map `m01=10.0.0.1,m02=10.0.0.3`.

## Runner Gate

Command:

```powershell
C:\work\swblock.exe run `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  testops\scenarios\nvme-tcp-bottleneck-triage-chain.yaml
```

Runner result:

```text
=== nvme-tcp-bottleneck-triage-chain === PASS (3m28.042s)
20 actions: 20 passed, 0 failed
run bundle: results\20260702-225104-ef04
```

Summary:

```text
phase123_nvme_tcp_bottleneck_triage_status=ok
frontend_transport=tcp
frontend_ip_map=m01=10.0.0.1,m02=10.0.0.3
frontend_network_class=100gbe_tcp
nvme_rdma_supported=false
roce_claim_allowed=false
performance_slo_claim_allowed=false
test_shape=dd_exec_baseline
network_baseline_server_node=m01
network_baseline_client_node=m02
network_baseline_server_ip=10.0.0.1
network_baseline_client_ip=10.0.0.3
network_baseline_tool=iperf3
network_baseline_status=ok
network_baseline_mibps=4106.55
publish_target=10.0.0.1:4420
route_dev=enp1s0np0
publish_target_network_class=100gbe_tcp
publish_target_source=configured_data_plane
management_ip=192.168.1.181
frontend_ip=10.0.0.1
host_local_nvme_tcp_status=unavailable
host_local_nvme_tcp_reason=not_safe_in_status_only_gate
host_local_nvme_tcp_mibps=unavailable
k8s_mounted_nvme_tcp_mibps=248.06
k8s_mounted_seq_write_mibps=127.74
k8s_mounted_seq_read_mibps=248.06
k8s_mounted_small_write_iops=755.16
runtime_diagnostics_collected=true
top_bottleneck=unknown
next_recommendation=phase124_target_backend_shape_split
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

## Interpretation

The independent network comparator is much higher than the mounted Block path:

```text
network_baseline_mibps=4106.55
k8s_mounted_seq_read_mibps=248.06
k8s_mounted_seq_write_mibps=127.74
```

This means the 10.0.0.x data-plane route itself is not the immediate bottleneck.
Phase 123 deliberately does not over-classify the bottleneck because it still
cannot separate target CPU path, backend/durable store, Kubernetes/filesystem
overhead, and the current `dd` test shape. The correct next step is the
Phase 124 target/backend/test-shape split.

## Harness Findings

- Initial script bug: `map_value` used awk in a way that blocked on stdin. Fixed
  to pure bash parsing.
- Initial iperf start bug: remote `pkill -f` matched and killed its own SSH
  command. Fixed to `pkill -x iperf3`.
- Initial scenario bug: runner `grep_log` did not treat `+` and alternation as
  ERE. Fixed target/bottleneck assertions to basic-regex-compatible patterns.

## Non-Claims

Phase 123 does not implement or validate NVMe/RDMA, RoCE, GPU Direct,
cuFile/cuObject, NIXL, production HA, or a performance SLO. It only proves that
the configured TCP data-plane network has far more headroom than the current
mounted Block NVMe/TCP path, and that the next bottleneck split must be inside
target/backend/Kubernetes/test-shape.
