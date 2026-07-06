# Phase 144 QA Sign-Off: WAL Encode/Append Pair Profile

Status: **PASS**.

Validated source tree: `phase144-wal-encode-append-pair-profile`, synced to
`/tmp/seaweed_block` on m02 as clean Phase 143 HEAD plus the Phase 144 gate
script overlay.

Run command:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-wal-encode-append-pair-profile-chain.yaml `
  -output results\phase144-wal-pair-run1.json `
  -html results\phase144-wal-pair-run1.html
```

Run bundle:

```text
results\20260706-151902-2826
44 actions: 44 passed, 0 failed
```

## Evidence

```text
phase144_wal_encode_append_pair_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
phase144_contract_tests=pass
helm_candidate_max_h2c_data_length=65536
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

## Verdict

Under the 64KiB H2C opt-in, the write path is not blocked by target/backend
request size:

```text
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
```

WAL encode and append are effectively tied:

```text
wal_encode_duration_ms=297
wal_append_duration_ms=295
phase144_pair_shape=encode_append_tied
```

The next implementation phase should reduce WAL record materialization cost,
not tune a single isolated bucket.
