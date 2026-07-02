# Phase 120 QA: NVMe/TCP Performance Baseline

Purpose: collect a repeatable supported-lab NVMe/TCP performance baseline before
deciding whether RoCE/NVMe-RDMA is the next protocol investment.

This is a baseline gate, not an SLO gate. Numeric throughput and IOPS are
evidence rows; they are not pass/fail floors.

## Gate

Scenario:

```text
testops/scenarios/nvme-tcp-performance-baseline-chain.yaml
```

Script:

```text
scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh
```

## PASS Criteria

The summary must contain:

```text
phase120_nvme_tcp_performance_baseline_status=ok
protocol=nvme
frontend_transport=tcp
performance_claim_allowed=false
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
pvc_bound=true
perf_pod_ready=true
managed_volume_status=ready
managed_volume_reason=first_volume_verified
publish_target_loopback=false
marker_verified=true
final_data_verified=true
seq_write_mibps=<number>
seq_read_mibps=<number>
small_write_iops=<number>
cleanup_status=ok
```

## Required Verdict Shape

Report:

- branch and commit tested;
- scenario run id and bundle path;
- full summary `key=value` rows;
- whether the baseline ran on local images or published images;
- residue status from `verify-helm-cleanup.sh`;
- any lab caveat such as image import, node availability, disk pressure, or
  noisy neighbor.

## Non-Claims

PASS does not claim:

- RoCE or NVMe/RDMA;
- performance SLO;
- production tuning;
- broad kernel/distro compatibility;
- GPU/NIXL/cuObject.
