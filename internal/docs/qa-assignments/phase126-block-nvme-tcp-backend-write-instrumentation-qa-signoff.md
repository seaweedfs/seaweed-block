# Phase 126 QA Sign-Off: Block NVMe/TCP Backend Write Instrumentation

Status: PASS on 2026-07-03.

## Scope

Phase 126 adds product-owned write-path evidence to the supported-lab
NVMe/TCP performance triage. The gate must prove writes reached both the target
handler and durable backend counters, then classify the next bottleneck without
making a RoCE, NVMe/RDMA, or performance/SLO claim.

Scenario:

```text
testops/scenarios/nvme-tcp-backend-write-instrumentation-chain.yaml
```

Run:

```text
run_id=20260703-015034-187b
runner_status=PASS
remote_bundle_path=/mnt/smb/work/share/g15d-k8s/20260703-015034-187b-phase126-write-instrumentation
collected_remote_bundle=C:\work\seaweed_block\results\20260703-015034-187b\artifacts\remote-phases.tgz
```

## Evidence

```text
phase126_block_nvme_tcp_backend_write_instrumentation_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
instrumentation_surface=status_durable_write_profile
network_baseline_mibps=4180.60
block_nvme_seq_write_mibps=177.72
block_nvme_seq_read_mibps=520.85
local_path_seq_write_mibps=1115.47
local_path_seq_read_mibps=536.13
block_vs_local_write_ratio=0.159
block_vs_local_read_ratio=0.971
target_write_observed=true
target_write_bytes=588075008
target_write_ops=17972
target_write_duration_ms=34233
backend_write_bytes=588075008
backend_write_ops=17972
backend_write_duration_ms=33186
backend_sync_ops=9
backend_sync_duration_ms=73
write_path_observation=backend_write
top_bottleneck=backend_write
next_recommendation=phase127_durable_backend_write_batching
cleanup_status=ok
```

Duration values are cumulative per-operation time from product counters, not
wall-clock elapsed time. They are bottleneck evidence only.

## Verdict

PASS.

- Target-side write evidence landed: `target_write_observed=true`,
  `target_write_bytes=588075008`, `target_write_ops=17972`.
- Backend-side write evidence landed with matching bytes:
  `backend_write_bytes=588075008`.
- Sync/flush time is not the observed dominant cost:
  `backend_sync_duration_ms=73`.
- The gate classifies the next bottleneck as `backend_write`, not network,
  target CPU, or RDMA.
- Cleanup is clean: `cleanup_status=ok`.

## Process Note

The first run (`20260703-014531-50ec`) failed before Phase126 completion because
the profile collector looked for `status_addr` only in cluster evidence. The
cluster managed-volume projection does not include replica status addresses.
The collector was fixed to fall back to `ops inventory` evidence, where
`status_address=10.0.0.1:24420` is present. The rerun passed.

This was a gate-script issue, not a product write-path failure.

## Non-Claims

This sign-off does not claim NVMe/RDMA, RoCE, GPU Direct, cuFile/cuObject,
NIXL, production HA, broad host compatibility, or a throughput/SLO guarantee.
