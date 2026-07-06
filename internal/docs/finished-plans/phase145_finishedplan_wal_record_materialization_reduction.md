# Phase 145 Finished Plan: WAL Record Materialization Reduction

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 144 showed WAL encode and append were tied under the 64KiB H2C opt-in.
The next implementation step needed to be narrow and safe: reduce a local
materialization cost without changing WAL durability semantics.

## Work

Phase 145 changed the WAL batch materialization path:

- before: `WALStore.WriteBatch` allocated a `*walEntry` per block;
- after: it creates one `[]walEntry` value slice and the WAL writer consumes
  each entry by address while encoding.

This avoids per-record pointer/object allocation while preserving the same WAL
record bytes and write-at batching.

The phase added:

- `scripts/run-phase145-wal-record-materialization-reduction-gate.sh`;
- `testops/scenarios/nvme-tcp-wal-record-materialization-reduction-chain.yaml`.

## Evidence

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

Run bundle:

```text
results\20260706-152811-0a23
36 actions: 36 passed, 0 failed
```

## Conclusion

The safe local allocation reduction can stay. It should not be marketed as a
performance win until Phase 146 measures before/after effectiveness. Larger
reductions probably require either WAL format work or vectored I/O, both of
which need separate design gates.
