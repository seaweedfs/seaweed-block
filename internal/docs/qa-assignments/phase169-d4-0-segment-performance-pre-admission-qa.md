# Phase 169 D4-0 Segment Performance Pre-Admission QA

Run this evidence gate before implementing the full segmented checkpoint,
retention, rebuild, and replication path. A failed performance admission is a
valid stop result and must prevent speculative full-engine integration.

Use an isolated exact commit on m02:

```bash
cd /tmp/seaweed_block
bash scripts/run-phase169-segment-pre-admission-performance-gate.sh /tmp/seaweed_block
cat results/phase169-segment-pre-admission-performance-gate/phase169-segment-pre-admission-performance-summary.txt
```

The gate must use one-second time-driven samples, five repetitions, rotated
mode order, one final Sync per sample, and writers 1 and 4.

The evidence gate passes when
`phase169_segment_pre_admission_performance_status=ok`. The candidate is
admitted to full D4 only when all of these are also true:

```text
single_writer_threshold_pass=true
concurrency_gain_threshold_pass=true
positioned_threshold_pass=true
grouping_threshold_pass=true
d4_full_engine_admitted=true
next_recommendation=implement_checkpoint_rebuild_equivalence
```

`concurrency_gain_threshold_pass=true` requires either 1.5x candidate
four-writer scaling or at least 1.25x four-writer throughput over both controls.
The positioned control must report zero checkpoint and recycle operations. The
legacy control retains its mature dirty-map and background-flush path, while
the incomplete candidate does not yet pay equivalent costs. This is therefore
an optimistic candidate upper bound: admission only justifies full D4
implementation and is not a product performance claim.

If any threshold is false, report PASS for the evidence mechanism and REJECT
for the candidate. Do not reinterpret a batch/grouping gain as an ordinary
write capability. Confirm every raw log contains both writer rows and the
segmented, positioned, and legacy rows each report exactly one final logical
Sync. Treat any capacity exhaustion as a gate failure, not a candidate result.

Write the result to
`internal/docs/qa-assignments/phase169-d4-0-segment-performance-pre-admission-qa-signoff.md`.
