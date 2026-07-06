# Phase 143 QA Sign-Off: WAL Append Large-H2C Profile

Status: **PASS**.

Validated source tree: `phase143-wal-append-large-h2c-profile`, synced to
`/tmp/seaweed_block` on m02 as clean Phase 142 HEAD plus the Phase 143 gate
script overlay.

Run command:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-wal-append-large-h2c-profile-chain.yaml `
  -output results\phase143-wal-append-run2.json `
  -html results\phase143-wal-append-run2.html
```

Run bundle:

```text
results\20260706-151054-d34d
46 actions: 46 passed, 0 failed
```

## Evidence

```text
phase143_wal_append_large_h2c_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
phase143_contract_tests=pass
helm_candidate_max_h2c_data_length=65536
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

## Verdict

The 64KiB request shape remains intact:

```text
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
backend_full_block_batch_max=16
```

WAL append is still the top bucket, but Phase 143 shows it is not a
wrap/padding problem:

```text
wal_append_padding_bytes=13136
wal_append_writeat_bytes=593543918
```

The close secondary cost is WAL encode:

```text
wal_append_duration_ms=290
wal_encode_duration_ms=285
phase143_append_shape=encode_close_second
```

The next backend work should profile the encode+append pair before changing
WAL append semantics.

## Notes

The first Phase 143 run, `results\20260706-150619-cf53`, passed the live gate
but classified any non-zero wrap/padding as `wrap_padding`. That was too coarse
because the observed padding was only 13KiB across ~566MiB written. The
classifier was tightened to require padding at least one average write-at
record or 1% of write-at bytes before naming wrap/padding as the shape.
