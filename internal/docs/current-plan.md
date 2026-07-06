# Current Plan: Phase 144 WAL Encode/Append Pair Profile

Status: planning.

Phase 143 closed the large-H2C WAL append profile:

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

## Goal

```text
64KiB H2C opt-in remains enabled
-> target/backend request max remains 64KiB
-> WAL encode and WAL append are measured as a pair
-> encode record-size/copy behavior and append write-at behavior are reported
-> the next change is either a narrow encode/copy reduction, append write path
   reduction, or explicit instrumentation gap
-> cleanup remains clean
```

## Required Evidence

```text
phase144_wal_encode_append_pair_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=<ops>
wal_encode_bytes=<bytes>
wal_encode_duration_ms=<ms>
wal_append_ops=<ops>
wal_append_bytes=<bytes>
wal_append_duration_ms=<ms>
wal_append_writeat_calls=<calls>
wal_append_writeat_avg_bytes=<bytes>
phase144_pair_shape=<encode_dominant|append_dominant|encode_append_tied|instrumentation_gap|unknown>
phase144_decision=<continue_backend_work|add_instrumentation|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not claim performance/SLO, RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject,
  or NIXL.
- Do not raise the default H2C size.
- Do not change failover, reconnect, CSI publish, authority, or WAL recovery
  semantics.
- Do not optimize until the encode-vs-append split is explicit enough to name
  a small implementation change.

## Candidate Work

1. Add a Phase 144 wrapper over Phase 126/143 profile evidence.
2. If current counters are enough, classify encode-vs-append directly.
3. If current counters are insufficient, add minimal encode allocation/copy
   counters and keep the phase a profiling gate.
4. Update roadmap and release docs with the narrow result only.

## Exit Criteria

Phase 144 can close when the live supported-lab gate explains whether encode,
append, or their tie is the next backend bottleneck under the 64KiB opt-in, and
names one concrete next implementation step.
