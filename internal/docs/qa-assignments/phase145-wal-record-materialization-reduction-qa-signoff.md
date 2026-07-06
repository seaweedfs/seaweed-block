# Phase 145 QA Sign-Off: WAL Record Materialization Reduction

Status: **PASS**.

Validated source tree: `phase145-wal-record-materialization-reduction`, synced
to `/tmp/seaweed_block` on m02 as clean Phase 144 HEAD plus the Phase 145 code
and gate overlay.

Run command:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-wal-record-materialization-reduction-chain.yaml `
  -output results\phase145-materialization-run1.json `
  -html results\phase145-materialization-run1.html
```

Run bundle:

```text
results\20260706-152811-0a23
36 actions: 36 passed, 0 failed
```

## Change

`WALStore.WriteBatch` now builds WAL records as a `[]walEntry` value slice
instead of allocating one `*walEntry` per block. The WAL writer consumes the
values by address while encoding the batch.

This removes per-record pointer/object allocation in the batch materialization
path. It does not change:

- WAL on-disk record layout;
- CRC/checksum coverage;
- caller-buffer copy semantics;
- recovery semantics;
- pwrite batching behavior.

## Evidence

```text
phase145_wal_record_materialization_reduction_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
candidate_max_h2c_bytes=65536
wal_record_materialization_change=writebatch_value_entries
unit_record_compatibility=pass
helm_candidate_max_h2c_data_length=65536
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

## Verdict

The narrow materialization reduction is safe to keep. The live gate proves the
large-H2C mounted path still works and the request shape remains 64KiB. The
phase does not claim throughput improvement; Phase 146 should measure whether
the allocation reduction is visible or whether deeper WAL format/writev work is
needed.
