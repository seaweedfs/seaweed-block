# Phase 151 Finished Plan: WAL Multi-Block Mounted NVMe Profile

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 150 wired multi-block WAL records behind an explicit runtime opt-in, but
only local tests had proven the record shape. The next question was whether the
opt-in survives the real Kubernetes mounted NVMe/TCP path without changing
defaults or making a performance claim.

## Work

Phase 151 added:

- a narrow Phase 120 Helm-values append hook for profile gates;
- `scripts/run-phase151-wal-multiblock-mounted-nvme-profile-gate.sh`;
- `testops/scenarios/nvme-tcp-wal-multiblock-mounted-profile-chain.yaml`;
- a mounted runtime-shape assertion that compares WAL encode ops to storage
  write calls and written block count.

## Evidence

```text
phase151_wal_multiblock_mounted_nvme_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=9002
wal_append_ops=9002
backend_storage_write_calls=9002
backend_storage_write_blocks=143570
backend_storage_batch_calls=8982
backend_full_block_batch_blocks=143550
multiblock_record_shape_observed=true
seq_write_mibps=204.96
seq_read_mibps=519.80
writer_verified=true
reader_verified=true
cleanup_status=ok
phase151_decision=keep_opt_in
next_recommendation=phase152_wal_multiblock_recovery_compatibility_gate
```

## Conclusion

The opt-in is worth keeping for the next gate. It remains default-off and not a
performance/SLO claim. Before considering a release or default change, the
multi-block WAL entry type needs a mounted restart/recovery compatibility gate.
