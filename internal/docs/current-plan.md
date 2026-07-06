# Current Plan: Phase 142 NVMe/TCP Large-H2C Retriage

Status: planning.

Phase 141 proved the 64KiB NVMe/TCP H2C candidate works as an explicit opt-in:

```text
phase141_nvme_tcp_max_h2c_boundary_status=ok
baseline_max_h2c_bytes=32768
candidate_max_h2c_bytes=65536
icresp_max_h2c_matches_candidate=true
identify_ioccsz_matches_candidate=true
host_connects_candidate=true
writer_verified=true
reader_verified=true
seq_write_mibps=208.21
seq_read_mibps=489.95
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
request_size_increase_observed=true
phase141_decision=add_opt_in
next_recommendation=phase142_nvme_tcp_large_h2c_retriage
cleanup_status=ok
```

The next useful work is to classify the new bottleneck under the 64KiB opt-in
shape. Do not turn the opt-in into a default from one lab pass.

## Goal

```text
64KiB H2C opt-in is enabled
-> mounted NVMe/TCP writer/reader still passes
-> target/backend request max stays 64KiB
-> backend full-block batch shape is observed
-> WAL append/copy/encode/checksum/dirty-map counters are captured
-> next bottleneck is named from the 64KiB profile
-> decision remains explicit: document opt-in, run broader compatibility, or continue backend work
-> cleanup remains clean
```

## Required Evidence

```text
phase142_nvme_tcp_large_h2c_retriage_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
host_connects_candidate=true
writer_verified=true
reader_verified=true
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
backend_full_block_batch_max=<blocks>
wal_append_writeat_max_bytes=<bytes>
wal_append_duration_ms=<ms>
wal_encode_duration_ms=<ms>
wal_checksum_duration_ms=<ms>
dirty_map_update_duration_ms=<ms>
phase142_bottleneck=<wal_append|wal_encode|wal_checksum|dirty_map|frontend_request_size|unknown>
phase142_decision=<document_opt_in|broader_compat_gate|continue_backend_work|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not claim performance/SLO, RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject,
  or NIXL.
- Do not raise the default H2C size in this phase.
- Do not change failover, reconnect, CSI publish, authority, or WAL recovery
  semantics.
- Keep the opt-in explicit and source-gated until broader host/distro evidence
  exists.

## Candidate Work

1. Add a Phase 142 wrapper over the Phase 126 profile with
   `SW_BLOCK_NVME_MAX_H2C_DATA_LENGTH=65536`.
2. Compare 64KiB request shape against Phase 141 and previous 32KiB evidence.
3. Name the largest remaining write-path cost from product-owned counters.
4. Update docs to describe the opt-in as supported-lab evidence only.

## Exit Criteria

Phase 142 can close when the live supported-lab gate names the post-64KiB
write-path bottleneck and gives a concrete next action without broadening the
release claim beyond explicit NVMe/TCP opt-in evidence.
