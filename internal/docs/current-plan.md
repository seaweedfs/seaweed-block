# Current Plan: Phase 150 WAL Multi-Block Runtime Opt-In

Status: planning.

Phase 149 closed the local multi-block WAL profile gate:

```text
phase149_wal_multiblock_record_profile_status=ok
default_wal_format_unchanged=true
feature_gate_default=false
single_block_compatibility=pass
current_recovery_compatibility=pass
profile_scope=local_storage
single_block_wal_encode_ops=2048
multiblock_wal_encode_ops=128
single_block_wal_append_ops=128
multiblock_wal_append_ops=128
single_block_wal_writeat_calls=128
multiblock_wal_writeat_calls=128
record_count_reduction_visible=true
phase149_decision=wire_runtime_opt_in
next_recommendation=phase150_wal_multiblock_runtime_opt_in
cleanup_status=ok
```

## Goal

Wire the multi-block WAL record prototype as an explicit disabled-by-default
runtime opt-in so a later mounted NVMe/TCP profile can exercise it. The default
path must remain single-block WAL records.

## Required Evidence

```text
phase150_wal_multiblock_runtime_opt_in_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=<name>
runtime_opt_in_default=false
helm_default_omits_opt_in=true
explicit_opt_in_reaches_walstore=true
single_block_compatibility=pass
current_recovery_compatibility=pass
phase150_decision=<mounted_profile_next|defer|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not enable multi-block records by default.
- Do not claim throughput/SLO from this phase.
- Do not raise the default H2C size.
- Do not treat the opt-in as release-supported until a mounted NVMe/TCP gate
  passes.

## Candidate Work

1. Add a WALStore option field and plumb it from durable/blockvolume only behind
   an explicit flag or config.
2. Keep default Helm/render path unchanged.
3. Add tests proving default false and explicit opt-in true.
4. Keep all Phase 148 correctness and compatibility tests green.

## Exit Criteria

Phase 150 can close when the runtime opt-in is wired and default-off, with no
public performance claim. Phase 151 should run the mounted NVMe/TCP profile.
