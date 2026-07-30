# Phase 169 D4 Segment Performance Pre-Admission QA Sign-Off

Verdict: evidence mechanism PASS and segmented candidate REJECT at exact commit
`ddd69e95a38a9432f6eb48df7c7be63ee35a6757`.

## Environment

```text
environment=m02
go_version=go1.25.0 linux/amd64
cgo_enabled=1
archive_sha256=1f8308f063c8099edcf9f6f5b1a9131318f7109c5989a42be3fe03ee42fe86fd
gate_exit=0
gate_duration_seconds=79.3
```

## Final Evidence

```text
benchmark_time=1s
repetitions=5
writers=1,4
sync_cadence=one_final_sync_per_sample
comparison_scope=optimistic_wal_append_core_upper_bound
legacy_background_flush_enabled=true
positioned_checkpoint_recycle_expected=false
segmented_writers_1_mibps_median=101.98
segmented_writers_4_mibps_median=78.68
positioned_writers_1_mibps_median=91.80
positioned_writers_4_mibps_median=79.51
legacy_writers_1_mibps_median=45.57
legacy_writers_4_mibps_median=43.06
segmented_writers_4_entries_per_segment_median=1.348
segmented_single_vs_legacy_ratio=2.238
segmented_four_writer_scaling_ratio=0.772
segmented_four_vs_positioned_ratio=0.990
segmented_four_vs_legacy_ratio=1.827
single_writer_threshold_pass=true
four_writer_scaling_threshold_pass=false
absolute_four_writer_gain_threshold_pass=false
concurrency_gain_threshold_pass=false
positioned_threshold_pass=false
grouping_threshold_pass=true
d4_full_engine_admitted=false
next_recommendation=stop_before_full_engine_integration
phase169_segment_pre_admission_performance_status=ok
```

All 15 raw logs completed and contained both writer rows. All 30 benchmark rows
reported one final logical Sync. Every positioned sample reported zero
checkpoint and recycle operations. No capacity, WAL-full, panic, or benchmark
failure occurred.

The format did group concurrent requests, but the one owner still serialized
segment encoding, CRC, `WriteAt`, and publication. Four writers therefore
reached only `0.772x` the candidate's one-writer throughput and `0.990x` the
positioned four-writer control. This result was measured before the candidate
paid mature dirty-map, checkpoint, retention, rebuild, or replication costs,
so full D4 integration could only add work. The stop rule applies.

The earlier `f752119` run failed because the positioned control exhausted its
test capacity. The intermediate `3d130bb` revision was discarded because
stopping the legacy flusher made its bounded WAL unsustainable. Neither run is
candidate performance evidence. Only `ddd69e9` produced the valid decision.

QA removed its scoped m02 artifacts and did not modify the shared worktree.
