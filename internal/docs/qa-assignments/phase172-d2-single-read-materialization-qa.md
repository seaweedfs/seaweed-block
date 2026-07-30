# Phase 172 D2 Single-Read Materialization QA

Validate the disabled single-read comparison path without enabling shared
record reuse or changing the shipped default.

## Source And Command

Use the exact assigned commit from a clean Linux worktree:

```bash
bash scripts/run-phase172-single-read-correctness-gate.sh
```

Do not reduce the 20 focused repetitions, 20 race repetitions, 1024-record
probe, or exact-file `strace` filter.

## Required Evidence

```text
phase172_single_read_correctness_status=ok
focused_repeat_20=pass
race_repeat_20=pass
storage_regression=pass
storage_vet=pass
default_two_read_path=pass
ordinary_single_read=pass
legacy_range_trim_single_read=pass
multiblock_single_read_without_reuse=pass
invalid_geometry_fails_closed=pass
short_read_fails_closed=pass
stale_corrupt_flags_unsupported_fail_closed=pass
ordinary_batch_trim_semantic_mismatches_fail_closed=pass
failed_cycle_retains_wal_tail=pass
physical_record_failure_positions=pass
reverse_gapped_wrap=pass
scoped_probe_single_read=true
scoped_probe_validated_records=1024
scoped_probe_header_reads=0
scoped_probe_record_reads=1024
scoped_probe_product_materialization_reads=1024
scoped_probe_strace_pread64_calls=1024
scoped_strace_matches_product_counter=true
default_materialization_unchanged=true
external_selector_added=false
d3_shared_record_reuse_eligible=true
```

Review all failure subtests and confirm that no invalid geometry, short read,
stale LSN, corrupt header/payload, unsupported type, or flags mismatch advances
checkpoint or deletes any dirty entry. Partial extent writes before a later
record failure are allowed only because checkpoint publication and dirty
deletion remain all-or-nothing.

Confirm that multi-block records use one physical read per logical block in D2
and report zero actual reuse hits. Reading one shared record once belongs to
D3; do not claim it here.

## Deliverable

Write:

```text
internal/docs/qa-assignments/phase172-d2-single-read-materialization-qa-signoff.md
```

Include the exact commit, all summary keys, artifact SHA-256, findings, and
cleanup status. Do not patch product code during QA.
