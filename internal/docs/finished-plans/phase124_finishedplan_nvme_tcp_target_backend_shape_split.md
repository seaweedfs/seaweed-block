# Phase 124 Finished Plan: NVMe/TCP Target / Backend / Shape Split

Status: closed, QA PASS on 2026-07-03.

## Problem

Phase 123 proved the configured `10.0.0.x` network was not the immediate
bottleneck, but it still could not say whether the low mounted NVMe/TCP numbers
came from Kubernetes mounted filesystem behavior, the current `dd` shape, the
blockvolume target, or the durable/backend path.

## What Changed

- Added `scripts/run-phase124-nvme-tcp-target-backend-shape-split-gate.sh`.
- Added `testops/scenarios/nvme-tcp-target-backend-shape-split-chain.yaml`.
- The gate now compares:

  ```text
  Phase123 network baseline
  Phase123 Block NVMe/TCP mounted PVC baseline
  same-node Kubernetes local-path PVC baseline
  same dd seq write/read/small-write shape
  optional local no-fsync write shape
  cleanup verifier
  ```

## Verification

Local checks:

```text
bash -n scripts/run-phase124-nvme-tcp-target-backend-shape-split-gate.sh
swblock validate testops/scenarios/nvme-tcp-target-backend-shape-split-chain.yaml
```

Runner gate:

```text
nvme-tcp-target-backend-shape-split-chain PASS
28 actions: 28 passed, 0 failed
```

Key evidence:

```text
network_baseline_mibps=3769.28
block_nvme_seq_write_mibps=118.74
block_nvme_seq_read_mibps=273.50
local_path_seq_write_mibps=324.87
local_path_seq_read_mibps=235.29
block_vs_local_read_ratio=1.162
block_vs_local_write_ratio=0.366
shape_fsync_penalty=1.180
top_bottleneck=block_target_or_backend
next_recommendation=phase125_blockvolume_target_cpu_profile
cleanup_status=ok
```

QA sign-off:

```text
internal/docs/qa-assignments/phase124-nvme-tcp-target-backend-shape-split-qa-signoff.md
```

## Product Meaning

The result narrows the next engineering question. The read path is not the
first suspect because Block read was slightly above local-path read in this
shape. The write path is the gap: local-path wrote about 2.7x faster than Block
NVMe/TCP using the same app node and same `dd conv=fsync` shape.

This means the next phase should profile Block write-side execution before any
RDMA target work. A faster transport is unlikely to fix a target/backend write
path that is already below the local Kubernetes storage comparator.

## Next Step

Phase 125 should profile the Block write path:

```text
blockvolume CPU during seq write
per-write target/copy/backend timing
durable/backend fsync cost
read-vs-write asymmetry
```

The expected output is a concrete choice between target CPU/copy optimization,
durable backend write optimization, or a corrected benchmark shape.

## Non-Claims

Phase 124 does not implement or validate NVMe/RDMA, RoCE, GPU Direct,
cuFile/cuObject, NIXL, production HA, broad host compatibility, or a
performance SLO.
