# Phase 142 QA Sign-Off: NVMe/TCP Large-H2C Retriage

Status: **PASS**.

Validated source tree: `phase142-nvme-tcp-large-h2c-retriage`, synced to
`/tmp/seaweed_block` on m02 as clean Phase 141 HEAD plus the Phase 142 gate
script overlay.

Run command:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-large-h2c-retriage-chain.yaml `
  -output results\phase142-large-h2c-run2.json `
  -html results\phase142-large-h2c-run2.html
```

Run bundle:

```text
results\20260706-145742-ddf3
50 actions: 50 passed, 0 failed
```

## Evidence

```text
phase142_nvme_tcp_large_h2c_retriage_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
phase142_contract_tests=pass
helm_candidate_max_h2c_data_length=65536
host_connects_candidate=true
writer_verified=true
reader_verified=true
seq_write_mibps=201.89
seq_read_mibps=504.43
target_write_observed=true
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
backend_full_block_batch_max=16
wal_copy_duration_ms=97
wal_append_writeat_max_bytes=66144
wal_append_duration_ms=300
wal_encode_duration_ms=289
wal_checksum_duration_ms=116
dirty_map_update_duration_ms=66
phase142_bottleneck=wal_append
phase142_decision=continue_backend_work
next_recommendation=phase143_wal_append_large_h2c_profile
cleanup_status=ok
```

## Verdict

The 64KiB H2C opt-in still passes the mounted NVMe/TCP writer/reader path, and
the request-size evidence remains at 64KiB through both target and backend:

```text
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
backend_full_block_batch_max=16
```

That closes the immediate "did larger H2C just move the boundary?" question.
The remaining top product-owned write-path cost is now WAL append, with WAL
encode close behind:

```text
wal_append_duration_ms=300
wal_encode_duration_ms=289
```

This is not a throughput/SLO, RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject, or
NIXL claim. The decision is to keep the 64KiB path as an explicit opt-in and
continue backend work in Phase 143.

## Notes

The first run, `results\20260706-145249-c023`, had a passing product gate but a
scenario assertion bug: the runner uses basic grep regex, so `[0-9]+` did not
match numeric counters. The scenario was corrected to `[0-9][0-9]*` and
re-run as the passing bundle above.
