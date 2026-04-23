# T3c Durable Backend Perf Baseline

**Date**: 2026-04-22
**Status**: characterization only — no pass/fail gate
**Purpose**: Per T3c mini plan §1.3 + audit §11 G-int.6. Publish
first-light numbers for both `LogicalStorage` impls so future
changes have a comparison point. No threshold; regressions become
visible but aren't auto-blocking.

---

## Workload

| Parameter | Value |
|---|---|
| Operation | 4 KiB random write |
| Queue depth | 8 |
| Duration | 60 seconds |
| Volume size | 256 MiB (65536 × 4 KiB blocks) |
| Host | sw dev machine (Windows 11; localhost loopback) |
| Driver | Go benchmark (`BenchmarkT3c_DurablePerf`) — not fio |

The originally-specified workload was a fio run (mini plan §1.3).
Because fio is not available in this dev environment, the
baseline is captured via Go benchmarks against the in-process
`StorageBackend` → `LogicalStorage` path. This captures the
adapter + storage cost without wire-layer overhead; m01 fio runs
are a post-sign activity that will overlay true network + kernel
cost.

---

## Numbers — first-light (2026-04-22, sw dev Windows 11, loopback)

Capture via `BenchmarkT3c_DurablePerf` at 8000 iterations, Sync
every 64 writes, 4 KiB random write, 256 MiB volume.

### Matrix comparison

| Metric | walstore | smartwal |
|---|---|---|
| ns/op | 197,365 | 303,436 |
| Throughput (mean) | 20.75 MB/s | 13.50 MB/s |
| Approx ops/s | ~5,067 | ~3,295 |

### Observations

- walstore is ~1.5× faster than smartwal on this specific
  workload (4 KiB random write with periodic Sync every 64 ops).
  Not a general claim — different workloads (sequential, larger
  block, longer sync intervals) may flip the ordering.
- Both impls are well within the order-of-magnitude we'd expect
  for single-threaded in-process loopback writes with periodic
  fsync. Numbers are useful as a comparison point, not as a
  published performance claim.
- p50 / p99 latency percentiles are NOT captured by this benchmark
  (standard Go bench only reports mean ns/op). A later fio run
  will fill that gap.

Numbers are first-light. Trends matter more than absolute values
until the m01 fio run replaces these with real hardware + network
cost.

---

## Reproduction

```bash
cd seaweed_block
go test -run '^$' -bench 'BenchmarkT3c_DurablePerf' ./core/frontend/durable/ -benchtime=60s -count=1
```

The benchmark iterates over `{walstore, smartwal}` impls per the
shared `logicalStorageFactories()` helper (Addendum A #1 matrix).

---

## Scope + non-claims

- Characterization ONLY. No regression gate; no pass/fail.
- Single workload (4 KiB random write). Read + mixed + sequential
  workloads land post-G4.
- No cross-node / network / kernel overhead — that's the m01 fio
  run scheduled for post-sign verification.
- First-light numbers above captured 2026-04-22 on sw dev
  Windows 11 loopback. m01 fio run scheduled post-sign is the
  production-representative measurement; dev-loopback numbers
  are the internal baseline only.

---

## Change log

| Date | Change | Author |
|---|---|---|
| 2026-04-22 | T3c initial baseline shape; numbers TBD | sw |
