# Phase 126 Finished Plan: Block NVMe/TCP Backend Write Instrumentation

Status: closed, QA PASS on 2026-07-03.

## Problem

Phase 125 proved the Block NVMe/TCP write path was much slower than same-node
local-path, while read throughput was comparable. Its CPU samples were coarse:
useful enough to avoid jumping to NVMe/RDMA, but not precise enough to pick a
code optimization.

Phase 126 needed product-owned evidence from the write path itself.

## What Changed

- Added append-only write profile counters to the durable backend status:

  ```text
  TargetWriteOps
  TargetWriteBytes
  TargetWriteDurationNanos
  BackendWriteOps
  BackendWriteBytes
  BackendWriteDurationNanos
  BackendSyncOps
  BackendSyncDurationNanos
  ```

- Recorded backend write and sync durations inside
  `durable.StorageBackend.Write` and `durable.StorageBackend.Sync`.
- Recorded NVMe target-handler write duration after successful backend writes.
- Extended the opt-in Phase 120 profile hook to collect
  `/status/durable?volume=<id>` after the sequential write.
- Added the Phase 126 gate and TestOps scenario:

  ```text
  scripts/run-phase126-block-nvme-tcp-backend-write-instrumentation-gate.sh
  testops/scenarios/nvme-tcp-backend-write-instrumentation-chain.yaml
  ```

The counters are diagnostic evidence, not a public performance API.

## Verification

Local checks:

```text
go test ./core/frontend/durable ./core/frontend/nvme ./core/host/volume ./cmd/blockvolume ./cmd/sw-block
bash -n scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh \
  scripts/run-phase125-block-nvme-tcp-write-path-profile-gate.sh \
  scripts/run-phase126-block-nvme-tcp-backend-write-instrumentation-gate.sh
swblock validate testops/scenarios/nvme-tcp-backend-write-instrumentation-chain.yaml
```

Runner gate:

```text
nvme-tcp-backend-write-instrumentation-chain PASS
run_id=20260703-015034-187b
remote_bundle_path=/mnt/smb/work/share/g15d-k8s/20260703-015034-187b-phase126-write-instrumentation
cleanup_status=ok
```

Key evidence:

```text
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
next_recommendation=phase127_durable_backend_write_batching
cleanup_status=ok
```

The duration fields are cumulative per-operation time, not wall-clock elapsed
time.

QA sign-off:

```text
internal/docs/qa-assignments/phase126-block-nvme-tcp-backend-write-instrumentation-qa-signoff.md
```

## Product Meaning

The write bottleneck is now localized to the durable backend write path. The
network is not limiting this gate, Block read is not behind local-path, and
sync time is not the dominant measured cost. The next phase should optimize or
batch durable backend writes before spending effort on NVMe/RDMA.

## Next Step

Phase 127 should focus on durable backend large-write batching:

```text
reduce per-block write fan-out
measure logical storage write op count
preserve partial-block semantics
rerun the Phase 126 evidence shape before/after
```

## Non-Claims

Phase 126 does not implement or validate NVMe/RDMA, RoCE, GPU Direct,
cuFile/cuObject, NIXL, production HA, broad host compatibility, or a
performance SLO.
