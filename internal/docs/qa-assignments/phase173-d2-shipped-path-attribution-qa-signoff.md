# Phase 173 D2 Shipped-Path Attribution QA Sign-Off

Verdict: **PASS** at `f780cc4` on the dedicated m02 NVMe filesystem.

## Source And Run

```text
commit=f780cc4
branch=phase173-storage-execution-architecture
host=M02
kernel=Linux 6.17.0-23-generic x86_64
go=go1.25.0 linux/amd64
store_source=/dev/nvme0n1p1
store_filesystem=ext4 rw,noatime
shape=mounted_mixed
writers=4
```

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/20260801T074011Z-phase173-d2-close \
SW_BLOCK_PHASE173_STORE_DIR=/data/nvme/block/20260801T074011Z-phase173-d2-close-stores \
  bash scripts/run-phase173-shipped-path-attribution-gate.sh \
  /tmp/seaweed-block-phase173-d2-f780cc4
```

The source directory was a `git archive` of exactly `f780cc4`. The gate used
one preconditioned persistent store, then reopened it for independent plain,
CPU/memory-profile, exact-strace, and exact-perf runs.

## Terminal Evidence

```text
phase173_shipped_path_attribution_status=ok
scope=walstore_engine_checkpoint_path
fixed_work_runs_reconciled=4
product_counter_reconciliation=true
cpu_profile_scope_exact=true
memory_delta_scope_exact=true
heap_profile_scope=post_window_after_gc
allocs_profile_scope=process_cumulative_reference
strace_scope_exact=true
strace_product_counter_reconciliation=true
perf_scope_exact=true
perf_required_events_present=true
iostat_device_observed=true
checkpoint_frontiers_equal=true
complete_drain=true
architecture_candidate_selected=false
optimization_code_present=false
store_residue_count=0
```

The CPU profile covers foreground through final drain. Exact memory deltas use
`runtime.MemStats` over that same window. The heap profile is post-window and
post-GC. Go's allocs profile is process-cumulative and is retained only as a
function-level reference; it is not mislabeled as exact-window evidence.

## Fixed Work And Foreground

```text
logical_operations=2560
logical_blocks=16000
logical_bytes=65536000
foreground_ns=254058894
foreground_mib_per_second=246.006
final_sync_ns=8011919
final_drain_ns=41494323
measured_alloc_bytes=223983976
measured_mallocs=49196
measured_frees=49036
measured_heap_alloc_start=2515616
measured_heap_alloc_end=3764560
```

The three non-strace runs were 235.174-246.006 MiB/s. Exact per-syscall tracing
reduced its own run to 121.445 MiB/s, so the trace is used for operation-count
reconciliation, not throughput admission.

Foreground stage counters from the plain run:

```text
stage                     operations/bytes             cumulative ns
copy                      16000 / 65536000              7173141
encode                    16000 / 66144000             26635621
checksum                  16000 / 66016000             12190529
WAL physical append       2561 / 66145642              26779673
WAL append lock wait      2561 calls implied             275727
volume commit lock wait   2560                         203396679
dirty-map update          16000                          6492432
```

Encode includes its nested copy and checksum work, so those values must not be
added as independent wall time. Commit-lock wait is accumulated across four
concurrent writers and overlaps elapsed wall time; D2 does not call it a wall
share or select an owner/queue redesign from that number alone.

## Flusher And Checkpoint

```text
stage                     operations/bytes             cumulative ns
complete cycles           3                            241180476
snapshot                  16000 entries                  3229490
opportunity analysis      3 cycles plus written sets     7858511
WAL header pread          16000 / 608000                22757747
WAL record pread          16000 / 66144000              32655078
WAL decode and CRC        16000 / 66144000              40527294
extent pwrite             16000 / 65536000              50519084
extent fsync              3                             53255381
checkpoint pwrite         3                                46548
checkpoint fsync          3                              3456795
unattributed remainder                                  26874548
```

All 16,000 records were validated and written, with zero decode/validation
failure, superseded entry, dirty entry, or incomplete checkpoint frontier.

## Independent OS Evidence

The controlled test blocked after warmup. `strace` attached before foreground
work and detached after final Sync, complete drain, and ten correctness reads,
but before `Close` metadata I/O.

```text
pread64=32010 = 16000 header + 16000 record + 10 correctness
pwrite64=18564 = 2561 WAL + 16000 extent + 3 checkpoint
sync=7 = 3 extent + 3 checkpoint + 1 final Sync
```

Perf stat ran over the same controlled window with sudo because
`perf_event_paranoid=4`:

```text
task_clock=495298674
cycles=1147474971
instructions=1275404137
cache_misses=7667121
context_switches=4650
cpu_migrations=89
page_faults=2379
```

The exact CPU profile had syscall execution at 36.59% flat,
`flusher.flushOnceInternal` at 46.34% cumulative, WALStore foreground writes at
about 14.6% cumulative, and `readDirtyRecord` at 26.83% cumulative. No single
CPU function establishes an architecture boundary by itself.

## D2 Decision

D2 closes the local shipped WALStore engine/checkpoint attribution. It proves
the complete logical-to-physical I/O accounting and names three facts for D3:

- four-writer commit-lock wait is large but concurrent/overlapping;
- same-file pread/pwrite/fsync and syscall CPU are material;
- the measured window allocates about 224 MB for 62.5 MiB logical data while
  retaining only about 3.8 MB at the endpoint.

These are control hypotheses, not selected optimizations. D3 must compare a
no-contention writer, deferred-writeback foreground ceiling, prefilled
flusher-only drain, same-device split-file scratch path, and the engine versus
frontend/RF1/RF3 boundaries before selecting at most one direction.

## Artifact And Cleanup

```text
/mnt/smb/work/share/g15d-k8s/20260801T074011Z-phase173-d2-close.tar.gz
sha256=7647cf7f075afaf1d06a48c1b9ef3256ff864626b70633b000ff40246b6e4e9a
source_dirs_remaining=0
store_files_remaining=0
processes_remaining=0
```

This gate is engine/filesystem-only and does not require Kubernetes.
