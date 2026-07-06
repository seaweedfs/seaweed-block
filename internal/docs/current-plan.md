# Current Plan: Phase 151 WAL Multi-Block Mounted NVMe Profile

Status: planning.

Phase 150 closed the runtime opt-in:

```text
phase150_wal_multiblock_runtime_opt_in_status=ok
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_default=false
explicit_opt_in_reaches_walstore=true
helm_default_omits_opt_in=true
helm_explicit_renders_opt_in=true
single_block_compatibility=pass
current_recovery_compatibility=pass
phase150_decision=mounted_profile_next
next_recommendation=phase151_wal_multiblock_mounted_nvme_profile
cleanup_status=ok
```

## Goal

Run a mounted NVMe/TCP profile with the multi-block WAL runtime opt-in enabled
and compare it to the current 64KiB H2C baseline. This is still lab evidence,
not a public performance/SLO claim.

## Required Evidence

```text
phase151_wal_multiblock_mounted_nvme_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=<n>
wal_append_ops=<n>
seq_write_mibps=<value>
seq_read_mibps=<value>
writer_verified=true
reader_verified=true
cleanup_status=ok
phase151_decision=<keep_opt_in|defer|blocked>
next_recommendation=<specific next phase>
```

## Boundaries

- Do not enable multi-block records by default.
- Do not claim throughput/SLO from this phase.
- Do not raise the default H2C size.
- Do not claim RoCE/NVMe-RDMA.

## Candidate Work

1. Extend the Phase 126/146 profile wrapper to set
   `blockmaster.durableWALMultiBlockRecords=true`.
2. Confirm the rendered blockvolume args include
   `--durable-wal-multiblock-records`.
3. Run the mounted writer/reader NVMe/TCP profile.
4. Compare WAL encode/append counters and record whether the opt-in is worth
   keeping.

## Exit Criteria

Phase 151 can close when the mounted opt-in profile passes or is explicitly
blocked by live evidence. Default must remain off either way.
