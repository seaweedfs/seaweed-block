# Current Plan: Phase 145 WAL Record Materialization Reduction

Status: planning.

Phase 144 closed the encode/append pair profile:

```text
phase144_wal_encode_append_pair_profile_status=ok
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=143573
wal_encode_bytes=593530782
wal_encode_duration_ms=297
wal_append_ops=9009
wal_append_bytes=593543918
wal_append_duration_ms=295
wal_append_writeat_calls=9009
wal_append_writeat_avg_bytes=65883
phase144_pair_shape=encode_append_tied
phase144_decision=continue_backend_work
next_recommendation=phase145_wal_record_materialization_reduction
cleanup_status=ok
```

## Goal

Reduce one concrete WAL record materialization cost without changing WAL
durability semantics.

```text
identify a local encode/copy allocation or data-copy seam
-> add a narrow unit regression for record bytes and recovery compatibility
-> implement the smallest safe reduction
-> keep 64KiB H2C request-shape assertions
-> live profile shows mounted writer/reader still passes
-> cleanup remains clean
```

## Required Evidence

```text
phase145_wal_record_materialization_reduction_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
wal_record_materialization_change=<specific change>
unit_record_compatibility=pass
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_duration_ms=<ms>
wal_append_duration_ms=<ms>
writer_verified=true
reader_verified=true
phase145_decision=<keep_change|revert|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not change WAL on-disk format unless a compatibility test proves old data
  can still recover.
- Do not weaken CRC/checksum or SmartWAL integrity behavior.
- Do not claim performance/SLO, RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject,
  or NIXL.
- Do not raise the default H2C size.

## Candidate Work

1. Inspect WAL encode/append code for a single redundant allocation/copy seam.
2. Prefer a local helper change with before/after unit coverage.
3. If no safe seam exists, close as `blocked` with explicit code evidence
   rather than speculative optimization.
4. Re-run the large-H2C profile to ensure request shape and mounted I/O remain
   correct.

## Exit Criteria

Phase 145 can close when a narrow materialization reduction is implemented and
validated, or when the phase proves no safe local seam exists without deeper
WAL format work.
