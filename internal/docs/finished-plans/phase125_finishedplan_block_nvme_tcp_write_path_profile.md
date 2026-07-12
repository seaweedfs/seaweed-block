# Phase 125 Finished Plan: Block NVMe/TCP Write-Path Profile

Status: closed, QA PASS on 2026-07-03.

## Problem

Phase 124 showed an asymmetric performance gap: Block NVMe/TCP read was not
behind the local-path comparator, but Block write was much slower than
same-node local-path write. That made NVMe/RDMA premature: the next question was
whether Block write looked CPU-bound, benchmark-shape-bound, or wait/sync bound.

## What Changed

- Added `scripts/run-phase125-block-nvme-tcp-write-path-profile-gate.sh`.
- Added `testops/scenarios/nvme-tcp-write-path-profile-chain.yaml`.
- Added an opt-in Phase 120 profiling hook:

  ```text
  SW_BLOCK_PHASE120_PROFILE_WRITE=true
  ```

  The hook records `kubectl top pods -A --containers`, node top, and process
  snapshots while the seq-write command runs. It is default-off and does not
  change previous gates unless explicitly enabled.

## Verification

Local checks:

```text
bash -n scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh
bash -n scripts/run-phase125-block-nvme-tcp-write-path-profile-gate.sh
swblock validate testops/scenarios/nvme-tcp-write-path-profile-chain.yaml
```

Runner gate:

```text
nvme-tcp-write-path-profile-chain PASS
22 actions: 22 passed, 0 failed
```

Key evidence:

```text
network_baseline_mibps=3836.30
block_seq_size_mib=512
block_nvme_seq_write_mibps=174.33
block_nvme_seq_read_mibps=544.10
local_path_seq_write_mibps=1147.98
local_path_seq_read_mibps=513.54
block_vs_local_write_ratio=0.152
block_vs_local_read_ratio=1.060
blockvolume_cpu_sample_count=3
blockvolume_cpu_peak_percent=0.80
write_path_observation=backend_sync
next_recommendation=phase126_durable_backend_write_optimization
cleanup_status=ok
```

QA sign-off:

```text
internal/docs/qa-assignments/phase125-block-nvme-tcp-write-path-profile-qa-signoff.md
```

## Product Meaning

The current Block NVMe/TCP write gap is not explained by network capacity or by
read-path behavior. The profiled write did not show target CPU saturation in
the coarse pod-level samples, so the next work should instrument the write-side
backend/sync path rather than start a new RDMA target.

## Next Step

Phase 126 should add targeted write-path instrumentation:

```text
target receive timing
target-to-backend write timing
durable/sync timing
bytes written and flush boundaries
read/write asymmetry evidence
```

The output should choose between backend write optimization, target copy
optimization with stronger evidence, or a corrected benchmark shape.

## Non-Claims

Phase 125 does not implement or validate NVMe/RDMA, RoCE, GPU Direct,
cuFile/cuObject, NIXL, production HA, broad host compatibility, or a
performance SLO.
