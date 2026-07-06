# Current Plan: Phase 146 WAL Record Materialization Effectiveness Profile

Status: planning.

Phase 145 closed a narrow WAL record materialization reduction:

```text
phase145_wal_record_materialization_reduction_status=ok
wal_record_materialization_change=writebatch_value_entries
unit_record_compatibility=pass
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_duration_ms=285
wal_append_duration_ms=293
writer_verified=true
reader_verified=true
phase145_decision=keep_change
next_recommendation=phase146_wal_record_materialization_effectiveness_profile
cleanup_status=ok
```

## Goal

Measure whether the Phase 145 allocation reduction is visible enough to keep as
a meaningful backend optimization, or whether the next work must move to deeper
WAL format/vectored-I/O design.

## Required Evidence

```text
phase146_wal_record_materialization_effectiveness_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_record_materialization_change=writebatch_value_entries
unit_record_compatibility=pass
wal_encode_duration_ms=<ms>
wal_append_duration_ms=<ms>
phase146_effectiveness=<visible|not_visible|inconclusive>
phase146_decision=<keep_change|needs_deeper_design|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not claim throughput/SLO from one noisy lab profile.
- Do not change WAL on-disk format or use platform-specific vectored I/O in
  this phase.
- Do not raise the default H2C size.
- Keep the 64KiB H2C path as explicit opt-in lab evidence.

## Candidate Work

1. Re-run the large-H2C profile and compare against Phase 144/145 evidence.
2. Classify the change as visible, not visible, or inconclusive.
3. If not visible/inconclusive, name the next design gate explicitly:
   multi-block WAL records or vectored pwrite.

## Exit Criteria

Phase 146 can close when it either justifies keeping the local allocation
reduction as visible, or redirects future work to a deeper WAL design gate
without making a performance claim.
