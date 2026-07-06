# Phase 144 Finished Plan: WAL Encode/Append Pair Profile

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 143 showed WAL append and WAL encode were nearly equal under the 64KiB
H2C opt-in. The phase needed to decide whether to optimize encode, append, or a
shared materialization path.

## Work

Phase 144 added:

- `scripts/run-phase144-wal-encode-append-pair-profile-gate.sh`;
- `testops/scenarios/nvme-tcp-wal-encode-append-pair-profile-chain.yaml`.

The gate pins the 64KiB request shape and compares WAL encode and append
counters from the existing durable write profile.

## Evidence

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

Run bundle:

```text
results\20260706-151902-2826
44 actions: 44 passed, 0 failed
```

## Conclusion

Encode and append are tied enough that a single-bucket optimization is likely
to be misleading. Phase 145 should reduce WAL record materialization cost in a
narrow, testable way.
