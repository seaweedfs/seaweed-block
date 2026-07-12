# Phase 146 Finished Plan: WAL Record Materialization Effectiveness Profile

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 145 removed one safe local allocation seam in `WALStore.WriteBatch`, but
that did not automatically prove a meaningful backend improvement. The next
step was to measure whether the change was visible enough to keep or whether
future work must move to deeper WAL format/write-path design.

## Work

Phase 146 added:

- `scripts/run-phase146-wal-record-materialization-effectiveness-gate.sh`;
- `testops/scenarios/nvme-tcp-wal-record-materialization-effectiveness-chain.yaml`.

The gate reuses the product-owned Phase 126/120 NVMe/TCP profile path under the
64KiB H2C opt-in and compares encode+append time against the Phase 144 baseline
(`297ms + 295ms`).

## Evidence

```text
phase146_wal_record_materialization_effectiveness_status=ok
wal_record_materialization_change=writebatch_value_entries
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_duration_ms=281
wal_append_duration_ms=280
phase146_baseline_pair_ms=592
phase146_current_pair_ms=561
phase146_pair_improvement_pct=5.24
phase146_effectiveness=visible
phase146_decision=keep_change
next_recommendation=phase147_wal_multiblock_record_design_gate
cleanup_status=ok
```

Run bundle:

```text
results\20260706-154244-4e69
34 actions: 34 passed, 0 failed
```

## Conclusion

The Phase 145 change can stay. It is a visible local reduction in this lab
profile, but it is not a throughput/SLO claim and does not change the public
NVMe/TCP supported-lab boundary.

The next backend work should stop doing small local materialization tweaks and
open a deeper design gate for fewer WAL records or fewer write calls:
multi-block WAL records and/or vectored write-at.
