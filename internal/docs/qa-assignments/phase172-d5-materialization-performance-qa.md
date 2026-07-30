# Phase 172 D5 Materialization Performance QA

## Goal

Run one exact-commit Linux decision gate comparing the unchanged default
materialization path with the complete disabled `shared-record` candidate.
This is an admission decision, not a benchmark demonstration. Do not change
thresholds after observing results.

## Source

Use the exact assigned commit in a clean isolated worktree on `m02`. Follow
`QA-AGENT-RUNBOOK.md`. Do not patch product, benchmark, or gate code.

## Command

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/<run-id> \
  bash scripts/run-phase172-materialization-performance-gate.sh \
  <exact-clean-worktree>
```

The gate requires Linux `strace`, five one-second repetitions, and the default
100 ms flusher. It rotates default/candidate order and workload order in one
run. Do not shorten the run.

## Required Evidence

The gate must cover ordinary sequential 4 KiB, scattered 4 KiB, explicit
16-block `WriteBatch`, and opt-in 16-block physical records at 1/2/4/8 writers.
For every sample confirm:

- the selected materialization mode is exact;
- one final explicit `Sync` and flusher drain completes;
- checkpoint equals head, dirty entries are zero, and checkpoint coverage is
  one;
- all validation, read, extent, sync, metadata, and cycle failure counters are
  zero;
- foreground p50/p95/p99, throughput, allocations, final Sync/drain,
  materialization counters, CPU profile, memory profile, and exact-path
  `strace` evidence are present.

The pre-declared admission checks are:

```text
ordinary materialization reads per validated entry: candidate/default <= 0.55
ordinary one-writer throughput: candidate/default >= 0.95
ordinary four-writer throughput: candidate/default >= 1.15
candidate ordinary four-writer max/min range: <= 1.50
ordinary four-writer p99: candidate/default <= 1.10
multi-block physical materialization reads: candidate/default <= 0.55
multi-block shared-record reuse: > 0
default and candidate ordinary and multi-block exact-path product reads:
equal strace pread64 calls
```

## Verdict

- `PASS, ADMIT` only when the gate exits zero and
  `d5_materialization_candidate_admitted=true`.
- `PASS, REJECT` when the gate is internally valid and complete but reports
  `d5_materialization_candidate_admitted=false`. This is an expected,
  non-product-failure outcome; D6 must not run and the candidate must be
  removed.
- `FAIL` for missing/invalid evidence, nonzero correctness counters, incomplete
  checkpoint/drain, wrong mode, dirty source, or gate execution failure.

Copy the artifact tarball, SHA-256, complete summary, and sign-off to the SMB
QA directory. Clean the isolated worktree and temporary files. Do not touch
the shared Windows tree.
