# Phase 172 D3 Shared-Record Materialization QA

Validate the bounded shared-record comparison path without changing the
shipped default or the D2 single-read semantics.

## Source And Command

Use the exact assigned commit from a clean Linux worktree:

```bash
bash scripts/run-phase172-shared-record-correctness-gate.sh
```

Do not reduce the 20 focused repetitions, 20 race repetitions, 1024-logical-
block probe, or exact-file `strace` filter.

## Required Evidence

```text
phase172_shared_record_correctness_status=ok
focused_repeat_20=pass
race_repeat_20=pass
storage_regression=pass
storage_vet=pass
default_two_read_path_unchanged=pass
d2_single_read_path_unchanged=pass
ordinary_and_multiblock_shared_reuse=pass
legacy_range_trim_shared_reuse=pass
concurrent_partial_overwrite=pass
legal_ring_wrap_recovery_reuse=pass
malformed_shared_record_fails_closed=pass
scoped_probe_shared_record=true
scoped_probe_snapshot_entries=1024
scoped_probe_unique_records=64
scoped_probe_reuse_candidates=960
scoped_probe_validated_records=1024
scoped_probe_header_reads=0
scoped_probe_record_reads=64
scoped_probe_product_materialization_reads=64
scoped_probe_reuse_hits=960
scoped_probe_strace_pread64_calls=64
scoped_strace_matches_product_counter=true
cache_scope=single_flush_cycle
cache_bound=one_decoded_record
external_selector_added=false
d4_equivalence_gate_eligible=true
```

Confirm that exact record identity is `(WALOffset, RecordSize)`, every reused
logical entry still validates its own LSN, LBA, length, and data offset, and a
malformed shared record leaves checkpoint, logical/physical WAL tail, and all
dirty entries unchanged.

Confirm that the legal ring-wrap fixture reopens and recovers the shared
multi-block record at physical offset zero, then reads it once, reuses it twice,
and advances the physical tail exactly to the recovered head.

For the concurrent partial-overwrite case, confirm that still-current blocks
from the old shared record reach extent, the newer same-LBA write remains
dirty, and the next flush publishes that newer LSN. The first cycle may write
other current blocks but must not delete the newer dirty entry.

The scoped probe must corroborate 64 product materialization reads with exactly
64 `pread64` calls for 1024 logical blocks stored in 64 physical records. A
counter-only result is not sufficient.

## Deliverable

Write:

```text
internal/docs/qa-assignments/phase172-d3-shared-record-materialization-qa-signoff.md
```

Include the exact commit, all summary keys, artifact SHA-256, findings, and
cleanup status. Do not patch product code during QA.
