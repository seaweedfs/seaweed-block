# Phase 174 D2 NVMe/TCP Fixed-Work QA Sign-off

Verdict: **PASS for NVMe/TCP RF1 attribution; all-shapes stability remains
HOLD.** No architecture candidate is selected.

## Source And Evidence

```text
source_commit=706b17381b6d57cbaa7d6823b0f8bc726dd0f3ae
host=m02
store_source=/dev/nvme0n1p1
store_filesystem=ext4
control_cpuset=0,2,4,6
control_gomaxprocs=4
artifact=/mnt/smb/work/share/g15d-k8s/20260801T104958Z-phase174-d2-nvme-fixed.tar.gz
sha256=93cefd739f39d91ed0b9a8f54df44dd3fe6c13051fccbf3ad97be0d63024a4be
```

The bundle contains 30 measured JSON rows, per-run logs, five merged
four-queue CPU/block/mutex profiles, environment evidence, and the summary.

## Contract And Correctness

Each row used the same Phase 174 logical work:

```text
16,384 writes x 4 KiB = 67,108,864 bytes
writers/IO queues=1,4,8
sets=2
runs per set=5
ack_profile=local_durable
transport=real loopback NVMe/TCP R2T/H2C
```

All 30 rows reconciled exactly:

```text
nvme_write_commands=16384
nvme_r2t_write_commands=16384
nvme_h2c_data_pdus=16384
target_write_ops=16384
adapter_request_ops=16384
adapter_write_ops=16384
adapter_storage_write_calls=16384
adapter_storage_write_blocks=16384
primary_wal_write_ops=16384
all byte counters=67108864
```

Final NVMe Flush, close/reopen, WAL recovery, `R == H`, and five recovered
data samples passed for every row. Store cleanup also passed:

```text
nvme_tcp_rf1_counter_reconciliation=true
nvme_tcp_rf1_close_recover_verified=true
store_residue_count=0
```

## Performance And Stability

| IO queues | Median MiB/s | Overall max/min | Set 1 | Set 2 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 103.243 | 1.551 | 1.018 | 1.530 |
| 4 | 260.787 | 1.035 | 1.035 | 1.018 |
| 8 | 298.731 | 1.038 | 1.029 | 1.038 |

The four-writer admission shape is stable, but all-shapes stability is HOLD.
The one-queue set-2 slow rows were `81.25`, `74.57`, and `67.73 MiB/s`.
Their foreground flusher-cycle time rose to `434-690 ms`, versus roughly
`152-165 ms` in the stable set-1 rows. The one-queue foreground/flusher
correlation is `0.995`. This is strong evidence that the instability moved
with background checkpoint work, not with a deterministic target CPU cost.

```text
nvme_tcp_rf1_four_writer_stability_gate=pass
nvme_tcp_rf1_all_shapes_stability_gate=hold
```

## Boundary Attribution

The stable four-queue median accumulated time per operation was:

```text
client write round trip              59.564 us/op
NVMe/TCP round-trip non-backend      52.255 us/op
target backend call                   7.357 us/op
adapter write                         6.995 us/op
write commit-lock wait                2.836 us/op
WAL encode                            0.731 us/op
WAL append                            1.492 us/op
dirty-map update                      0.273 us/op
```

About 88% of accumulated client-visible latency is outside the target's
backend call. This aggregate includes the test initiator, loopback TCP,
R2T/H2C receive, dispatch/scheduling, and completion. It is not yet evidence
that any one target function dominates.

Merged CPU profiles likewise do not identify a product Go hotspot:

```text
internal/runtime/syscall.Syscall6=54.76% flat
runtime.memmove=3.67% flat
```

The profile covers the whole test process, including payload generation and
recovery, so it is supporting evidence rather than a foreground-only CPU
breakdown.

## Decision

- D2's NVMe/TCP RF1 operation/byte/recovery attribution slice passes.
- D1 remains HOLD because not every required shape is stable across the
  broader Phase 174 evidence.
- No architecture candidate is selected. The dominant measured bucket is too
  broad to justify changing queue, completion, or protocol ownership.
- The next bounded step is to split NVMe receive/data collection,
  dispatch/queue, and completion publication inside the target while retaining
  the same fixed-work contract.
- Historical mounted fio/dd results remain diagnostic:
  `mounted_shape_comparable=false` and
  `mounted_throughput_ratio_allowed=false`.

No shipped product code or default changed in this slice.
