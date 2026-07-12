# Phase 137 Finished Plan: Reduce WAL Record Encode / Copy Cost

Status: **closed 2026-07-05, live gate PASS**.

## Problem

Phase 136 named `wal_encode` as the largest backend-internal cost and WAL copy
as the next closest cost. The hot path still copied each block before building
the WAL record, then copied the same payload again into the encoded WAL record.
The batch path also encoded each record into its own temporary slice before
copying it into the coalesced pending write buffer.

## Work

Phase 137 kept the WAL on-disk format and per-record LSN semantics unchanged,
but reduced transient copies:

- WALStore no longer performs a separate pre-encode block copy.
- WAL payload copy is performed once while encoding the final WAL record bytes.
- Batch append encodes each record directly into the coalesced pending buffer.
- Added regression tests proving caller-buffer mutation after `Write` and
  `WriteBatch` does not affect immediate reads or recovered bytes.

## Gate

Phase 137 added:

- `scripts/run-phase137-reduce-wal-record-encode-copy-gate.sh`
- `testops/scenarios/nvme-tcp-reduce-wal-record-encode-copy-chain.yaml`

The gate reruns the 512MiB mounted NVMe/TCP profile, requires active backend
batching, compares encode+copy duration against the Phase 136 baseline, and
names the next bottleneck.

## Evidence

```text
phase137_reduce_wal_record_encode_copy_status=ok
phase136_wal_encode_copy_duration_ms=1346
wal_encode_copy_duration_ms=363
wal_encode_copy_reduced_vs_phase136=true
target_write_observed=true
backend_write_bytes=588075008
backend_storage_batch_calls=17953
backend_storage_batch_blocks=143555
wal_copy_duration_ms=93
wal_encode_duration_ms=270
wal_checksum_duration_ms=110
wal_append_duration_ms=375
dirty_map_update_duration_ms=74
post_phase137_bottleneck=wal_append
next_recommendation=phase138_wal_writeat_shape_profile
cleanup_status=ok
```

Run bundle:

```text
results\20260705-090453-2584
24 actions: 24 passed, 0 failed
```

## Conclusion

The encode/copy seam is no longer the largest measured backend-internal cost.
The next evidence-backed phase should profile WAL append/write-at shape:
pwrite count, coalescing, wrap behavior, and whether append time is dominated
by syscall count or write size. This remains a source/lab profile, not a
throughput/SLO, RoCE, or NVMe/RDMA claim.
