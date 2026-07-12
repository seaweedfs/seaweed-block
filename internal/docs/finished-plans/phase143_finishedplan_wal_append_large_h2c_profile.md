# Phase 143 Finished Plan: WAL Append Large-H2C Profile

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 142 named WAL append as the top remaining product-owned cost under the
64KiB NVMe/TCP H2C opt-in. The risk was to treat "WAL append" as one blob and
start optimizing the wrong thing.

## Work

Phase 143 added:

- `scripts/run-phase143-wal-append-large-h2c-profile-gate.sh`;
- `testops/scenarios/nvme-tcp-wal-append-large-h2c-profile-chain.yaml`.

The gate keeps the 64KiB H2C shape pinned, then records append duration,
write-at count/bytes/max/avg, wrap count, padding bytes, and WAL encode
duration.

No WAL implementation semantics were changed.

## Evidence

```text
phase143_wal_append_large_h2c_profile_status=ok
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
backend_full_block_batch_max=16
wal_append_duration_ms=290
wal_append_writeat_calls=9009
wal_append_writeat_bytes=593543918
wal_append_writeat_max_bytes=66144
wal_append_writeat_avg_bytes=65883
wal_append_wrap_count=8
wal_append_padding_bytes=13136
wal_encode_duration_ms=285
phase143_append_shape=encode_close_second
phase143_decision=continue_backend_work
next_recommendation=phase144_wal_encode_append_pair_profile
cleanup_status=ok
```

Run bundle:

```text
results\20260706-151054-d34d
46 actions: 46 passed, 0 failed
```

## Conclusion

The append path is not dominated by wrap/padding under 64KiB H2C. Encode is
nearly tied with append duration, so the next useful backend phase is an
encode+append pair profile rather than a blind append rewrite.
