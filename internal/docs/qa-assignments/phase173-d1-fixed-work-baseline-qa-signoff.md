# Phase 173 D1 Fixed-Work Baseline QA Sign-Off

Verdict: **PASS** at `29897cc` on the dedicated m02 NVMe filesystem.

## Source And Run

```text
commit=29897cc
branch=phase173-storage-execution-architecture
host=M02
kernel=Linux 6.17.0-23-generic x86_64
go=go1.25.0 linux/amd64
store_source=/dev/nvme0n1p1
store_filesystem=ext4 rw,noatime
cpu_affinity=0-15
scheduler=SCHED_OTHER priority 0
thermal_celsius=34.0,27.8,34.0
```

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/20260801T071605Z-phase173-d1-fixed-work-nvme \
SW_BLOCK_PHASE173_STORE_DIR=/data/nvme/block/phase173-d1-stores-29897cc \
  bash scripts/run-phase173-fixed-work-baseline-gate.sh \
  /tmp/seaweed-block-qa-phase173-d1-29897cc
```

The gate used fixed operations rather than Go benchmark auto-calibration. Each
matrix point ran one full precondition pass against a persistent store. The
measured runs reopened and recovered that same store. The second set used
separate files and reversed shape/writer order.

## Terminal Evidence

```text
contract=phase173-fixed-work-v1
sets=2
four_writer_runs_per_set=5
diagnostic_runs_per_set=1
fixed_work_result_count=64
precondition_runs=32
measured_store_reuse_required=true
flusher_interval_ms=100
flusher_phase_policy=restart_after_warmup_with_start_signal
go_benchmark_autocalibration_allowed=false
fixed_work_counter_reconciliation=true
fixed_work_complete_drain=true
fixed_work_correctness_samples=true
four_writer_stability_gate=pass
architecture_candidate_admission_allowed=true
phase173_fixed_work_baseline_status=ok
```

Every result had one final `Sync`, zero dirty entries, equal
checkpoint/head/synced frontiers, `store_reused=true`, and
`flusher_phase_reset=true`. Logical blocks reconciled with encoded WAL records;
API calls reconciled with commit-lock observations; physical appends, wrap
padding, and `WriteAt` calls reconciled separately.

## Stability Result

```text
shape           set1    set2    combined   median MiB/s   median p99 ns
sequential_4k   1.065   1.143   1.143      208.383        1,144,308
scattered_4k    1.042   1.129   1.129      214.366        1,147,132
batch_16        1.047   1.076   1.078      277.290        5,336,886
mounted_mixed   1.143   1.048   1.143      250.480        3,359,970
limit                                    <=1.250
```

These are engine fixed-work baseline values. They are not mounted NVMe,
frontend, RF3, or release performance claims.

## Harness Findings Retained

Earlier runs correctly held architecture admission and were not relabeled as
passes:

- short 32 MiB samples could finish before one 100 ms flusher period;
- `/tmp` selected the shared OS SATA SSD rather than the dedicated NVMe;
- 256 MiB samples exhausted a different consumer-NVMe write/GC state across
  the full matrix;
- recreating sparse store files measured allocator/device churn rather than a
  long-lived block volume;
- the flusher ticker started at store creation, so post-warmup foreground runs
  inherited a random periodic phase and could report false-fast samples.

The final gate fixes those measurement defects without changing the shipped
WALStore algorithm, durability boundary, default interval, or threshold.

## Additional Validation And Cleanup

```text
linux_storage_tests=pass
linux_storage_race=pass
linux_storage_vet=pass
store_residue_count=0
```

Artifact:

```text
/mnt/smb/work/share/g15d-k8s/20260801T071605Z-phase173-d1-fixed-work-nvme.tar.gz
sha256=760805ac218b0b5e112d56976623669335c593fd1af88cfb3ae0ac190da07db1
```

m02's k3s API was unavailable during preflight. D1 is an engine/filesystem
gate and did not use Kubernetes; this does not weaken its result, but it must
be repaired before any later mounted or cluster gate.
