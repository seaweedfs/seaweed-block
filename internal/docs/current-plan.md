# Current Plan: Phase 143 NVMe/TCP WAL Append Large-H2C Profile

Status: planning.

Phase 142 closed the post-64KiB-H2C retriage:

```text
phase142_nvme_tcp_large_h2c_retriage_status=ok
candidate_max_h2c_bytes=65536
host_connects_candidate=true
writer_verified=true
reader_verified=true
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

## Goal

```text
64KiB H2C opt-in remains enabled
-> mounted NVMe/TCP writer/reader still passes
-> target/backend request max stays 64KiB
-> WAL append is decomposed into write-at syscall shape, append bookkeeping,
   wrap/padding, and record-size evidence
-> close secondary WAL encode cost is preserved in the report
-> next backend change is named, or the phase explicitly blocks on missing
   instrumentation
-> cleanup remains clean
```

## Required Evidence

```text
phase143_wal_append_large_h2c_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
backend_full_block_batch_max=<blocks>
wal_append_duration_ms=<ms>
wal_append_writeat_calls=<calls>
wal_append_writeat_bytes=<bytes>
wal_append_writeat_max_bytes=<bytes>
wal_append_writeat_avg_bytes=<bytes>
wal_append_wrap_count=<count>
wal_append_padding_bytes=<bytes>
wal_encode_duration_ms=<ms>
phase143_append_shape=<writeat_latency|writeat_count|encode_close_second|wrap_padding|unknown>
phase143_decision=<continue_backend_work|add_instrumentation|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not claim performance/SLO, RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject,
  or NIXL.
- Do not raise the default H2C size.
- Do not change failover, reconnect, CSI publish, authority, or WAL recovery
  semantics.
- Do not optimize WAL append until the phase names which part of append is
  actually dominant under the 64KiB request shape.

## Candidate Work

1. Add a Phase 143 wrapper over the Phase 142/126 profile.
2. Preserve 64KiB H2C request-shape assertions.
3. Classify WAL append shape using write-at call count, max/avg bytes,
   wrap/padding, append duration, and encode duration.
4. If existing counters are insufficient, add the narrowest WAL append
   instrumentation needed and gate it locally before another live run.

## Exit Criteria

Phase 143 can close when the live supported-lab gate explains why WAL append is
the top cost under the 64KiB opt-in and names the next backend change without
broadening the product claim.
