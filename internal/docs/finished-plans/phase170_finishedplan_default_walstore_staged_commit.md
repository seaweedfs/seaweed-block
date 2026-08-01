# Finished Plan: Phase 170 Default WALStore Staged Commit Pipeline

Status: complete as an evidence-backed pre-implementation rejection. D1
validated the benchmark mechanism and denied admission to D2. No staged owner
or product selector was added; D2-D6 were intentionally skipped.

## Final Outcome

Phase 170 tested whether the shipped `walstore` could gain concurrent ordinary
Write throughput by routing several existing-format records through the
already implemented `walWriter.appendBatch` seam.

The exact m02 gate used the normal background flusher, five rotated one-second
samples, 1/2/4/8 writers, one explicit final Sync per sample, and an untimed
final flusher drain that had to settle checkpoint to head:

```text
ordinary writers=1                  87.97 MiB/s
ordinary writers=4                  64.09 MiB/s
ordinary writers=8                  50.12 MiB/s
explicit batch writers=1           137.48 MiB/s
explicit batch writers=4            47.22 MiB/s
explicit batch writers=8            50.51 MiB/s
ordinary four/single                 0.729x
batch four/ordinary four             0.737x
ordinary WriteAt calls/entry         1.000
batch WriteAt calls/entry            0.06250
paired batch gains                   2/5
ordinary four-writer range           2.936x
batch four-writer range              3.500x
```

The batch primitive did exactly what its implementation promises: it preserved
independent current-format records while reducing adjacent WAL writes to about
one positioned write per 16 entries. That syscall reduction did not produce a
stable four-writer gain. Four-writer batch throughput was lower than ordinary
Write, only two paired runs improved, and both paths had unacceptable run
range.

## What Was Proved

- The default product path can be measured with normal flusher/checkpoint work
  included rather than with a non-sustainable disabled-flusher control.
- Benchmark timing and accounting distinguish API-call latency/allocation
  metrics from per-record encode/copy/checksum metrics.
- Every accepted sample performs one explicit Sync and finishes with no dirty
  entries and complete checkpoint coverage.
- Existing-format `appendBatch` reliably reduces WAL `WriteAt` calls.
- The current ordinary path has a concurrency deficit: four-writer throughput
  is `0.729x` one writer and commit-lock wait rises from about `87 ns` to
  `29 us` per API call.
- Coalescing only the append syscall is not a sufficient answer under the
  complete WAL-to-extent workload.

The exact evidence is recorded in
`internal/docs/qa-assignments/phase170-d1-walstore-headroom-qa-signoff.md`.

## Why D2-D6 Were Skipped

The proposed owner would have added queueing, owned payload copies, LSN-range
assignment, Sync fencing, terminal drain, and new failure coordination around
a primitive whose concurrent advantage was not reproducible. It would also
have had to avoid a pressure deadlock between WAL admission and the flusher's
checkpoint lock.

Implementing those semantics after the D1 denial would repeat the pattern from
Phases 168 and 169: substantial correctness machinery around a performance
hypothesis that already failed its cheapest valid control. The stop rule is
the engineering result, not an incomplete implementation.

## Surviving Product State

- `walstore` remains the default backend and its synchronous Write path is
  unchanged.
- Phase 167 `parallel-walstore` remains explicit and opt-in.
- No staged owner, queue, new disk format, selector, or recovery branch was
  added.
- The Phase 170 benchmark and instrumentation remain as reusable evidence for
  later default-path work.
- Existing frontend, replication, recovery, and Operation Layer contracts are
  unchanged.

## Next Direction

The gate's large run-to-run range and syscall-heavy profile point below the
append batching seam. The default flusher currently reads WAL records
individually, writes each dirty extent block separately, and competes with
foreground append and fsync on the same file. Phase 171 will measure that
writeback amplification directly, then may port the bounded contiguous
checkpoint-write mechanism already proven in Phase 167 into the shipped
`walstore` flusher.

That work optimizes the full persistent pipeline rather than adding another
append owner.
