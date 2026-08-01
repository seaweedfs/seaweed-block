# Phase 170 D1 Default WALStore Headroom QA

Validate the exact committed D1 gate in an isolated m02 source tree. Follow
`QA-AGENT-RUNBOOK.md`; do not edit or commit the shared worktree.

```bash
cd /tmp/seaweed_block
bash scripts/run-phase170-walstore-headroom-gate.sh /tmp/seaweed_block
cat results/phase170-walstore-headroom-gate/phase170-walstore-headroom-summary.txt
```

## Mechanism Gate

Require:

- exact one-second samples and five repetitions;
- ordinary and 16-block batch controls at 1/2/4/8 writers;
- all 40 benchmark rows present;
- `explicit_sync_calls=1.000` on every row;
- every row settles all flusher debt: `dirty_entries=0`,
  `checkpoint_lsn=head_lsn`, and `checkpoint_coverage=1`;
- no WAL-full, queue, panic, or benchmark failure;
- complete MB/s median/min/max, p50/p95/p99, allocation, stage, WriteAt, flush,
  and checkpoint evidence;
- a readable CPU top profile;
- benchmark-only strace summary from the prebuilt test binary when the tool is
  available.

The evidence mechanism passes only when:

```text
phase170_walstore_headroom_status=ok
```

## D2 Decision

D2 is admitted only when all of these are true:

```text
batch_throughput_headroom=true
ordinary_one_writeat_per_entry=true
batch_writeat_coalescing=true
paired_batch_gain_stable=true
writeat_shape_stable=true
ordinary_range_bounded=true
batch_range_bounded=true
existing_format_headroom=true
d2_owner_admitted=true
next_recommendation=implement_bounded_existing_format_commit_owner
```

The batch control is not a prediction that ordinary writes will reach the same
throughput. It proves only that the current record format and `appendBatch`
path have measurable coalescing headroom worth one bounded owner experiment.
Latency and Go allocation metrics are per API call: one 4 KiB Write for
ordinary and one 16-block WriteBatch for batch. Record-stage and
`writeat_calls/entry` metrics use the explicit denominator in their key. Do not
compare unlike denominators. Strace covers the whole prebuilt benchmark
process, including calibration/setup/cleanup, and is qualitative only.

If D2 is not admitted, report mechanism PASS and candidate direction STOP.
Do not weaken the thresholds or disable the normal flusher.

Write the result to
`internal/docs/qa-assignments/phase170-d1-walstore-headroom-qa-signoff.md`.
