# Phase 168 D5 Native WAL Performance Decision

Status: evidence gate PASS; candidate REJECTED at exact commit `6cf852c`.

## Final Gate

The final comparison corrected the initial fixed-iteration instability:

- one-second time-driven samples crossing repeated flush intervals;
- five repetitions with rotated native/positioned/legacy order;
- raw per-sample admitted/SQE/CQE/failure validation;
- 4 KiB Write at 1/2/4/8 writers;
- 16-block WriteBatch at 1/4 writers;
- median, range, p99, allocation, CPU, and Sync cadence evidence;
- full wait path in strace, including eventfd, read, and ppoll;
- an explicit native-versus-positioned admission condition.

One profile-justified optimization was allowed before the final decision:
owner-confined per-lane WAL encode buffers reserve one bounded capacity and are
reused after terminal CQE consumption. The gate proved four allocations for
four active lanes rather than per-round buffer growth.

## Exact Evidence

```text
native_writers_1_mibps_median=33.71
positioned_writers_1_mibps_median=36.44
legacy_writers_1_mibps_median=35.13
native_writers_4_mibps_median=32.47
positioned_writers_4_mibps_median=34.48
legacy_writers_4_mibps_median=34.45
native_batch_writers_4_mibps_median=37.01
positioned_batch_writers_4_mibps_median=38.52
candidate_single_writer_vs_legacy_ratio=0.960
candidate_four_writer_scaling_ratio=0.963
candidate_four_writer_vs_legacy_ratio=0.943
candidate_four_writer_vs_positioned_ratio=0.942
candidate_batch_four_writer_vs_positioned_ratio=0.961
native_all_writer_counts_stable=true
native_all_writer_p99_bounded=true
fallback_count=0
queue_full_rejects=0
short_completions=0
native_writers_4_selected_syscalls=1553
positioned_writers_4_selected_syscalls=1052
performance_claim_allowed=false
next_recommendation=remove_native_candidate
phase168_native_wal_performance_status=ok
```

The native path was correct but did not scale: four writers were `0.963x` its
own one-writer throughput, below the required `1.5x`. It was also slower than
positioned I/O for ordinary and batch writes, had higher p99, and used more
selected syscalls once completion waiting was included.

## Independent QA Rerun

Independent QA reran exact commit `6cf852c` on m02 and parsed all 90 samples:
18 benchmark combinations times five repetitions. Every native sample had
matching admitted/SQE/CQE counts, four reusable lane-buffer allocations, and
zero fallback, queue-full rejection, or short completion.

The independent medians differed from the author run:

```text
native_writers_1_mibps_median=30.37
native_writers_4_mibps_median=31.92
positioned_writers_4_mibps_median=36.27
legacy_writers_4_mibps_median=37.89
native_batch_writers_4_mibps_median=38.16
positioned_batch_writers_4_mibps_median=35.61
candidate_single_writer_vs_legacy_ratio=0.871
candidate_four_writer_scaling_ratio=1.051
candidate_four_writer_vs_positioned_ratio=0.880
candidate_batch_four_writer_vs_positioned_ratio=1.072
performance_claim_allowed=false
next_recommendation=remove_native_candidate
```

The batch ratio moved above one, but the ordinary single-writer, four-writer
scaling, and four-writer positioned-control thresholds still failed
independently. The candidate rejection is therefore robust to the observed run
variance.

QA also independently observed the full native wait path
(`IORING_REGISTER_EVENTFD`, `io_uring_enter`, `ppoll`, and successful eventfd
reads). The historical gate did not hard-assert every wait-path/profile detail;
that limitation is recorded here rather than repaired after removal of the
candidate.

## Decision

The D5 stop rule applies. Further slice pooling, registered buffers, or ring
tuning is not justified because the bounded obvious allocation experiment did
not establish an architectural advantage. The native candidate was removed at
`954231f`, D6 was skipped, `walstore` remains default, and Phase 167's
positioned `parallel-walstore` remains opt-in.
