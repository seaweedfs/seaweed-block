# Fsync latency breakdown

**Use this when:** you need to find where `fsync` time goes on a walstore or
walstore-chunk volume, stage by stage, from the volume host's status endpoint.

Read the volume-host endpoint:

```sh
curl -fsS 'http://127.0.0.1:PORT/status/durable?volume=VOLUME'
```

Find `WriteProfile.DurabilityLatency` in the returned volume status. Instrumentation is on by default, cumulative for this open backend/store, bounded in memory, and does not change batching, Sync dedup, write acknowledgement, replication or checkpoint ordering. A missing `WAL` object means that backend does not expose walstore timing; it does not mean zero latency. Restart/reopen starts a new series. Capture before/after bucket counters for an isolated workload, then calculate interval quantiles from bucket differences; do not subtract cumulative p99 values.

| Field | Sample and boundary |
|---|---|
| `Frontend.ReceiveToBackend` | Each write/FUA/Flush that enters StorageBackend: handler dispatch start to first backend call, before admission/fence checks. No socket/uring/kernel/guest scheduling before the handler. |
| `WAL.WALAppend` | Each local single/grouped append attempt (including errors): encode, WAL lock, wrap/padding and file writes inside the append call. A batch is one sample, not one per block. Excludes publication/admission wait before that call. |
| `WAL.GroupCommitWait` | Each waiter selected for dispatch: insertion into the committer queue to batch dispatch, before physical Sync. Includes scheduling/batching delay, excludes syscall time. Shutdown-drained requests have no dispatch sample. |
| `WAL.FileSync` | Each attempted physical WAL file `Sync` call, including errors; one per shared batch. This is Go's file Sync/fsync path, not a claim that fdatasync is used. Deduplicated Sync requests add no physical sample. |
| `BackendSync` | Every adapter Sync attempt, including gate refusal and errors: admission, local/group commit, direct-frontier metadata and any replication-observer barrier. Encloses other stages; it is not an additional summand. |
| `Frontend.AckDispatch` | Last backend Write/Sync return to handler completion/result construction, for requests that entered a backend. Includes failure-result dispatch. Ends before actual CQE/socket completion and guest receipt. |
| `Frontend.WriteTotal`, `FlushTotal`, `FUATotal` | Handler-dispatch-to-result latency separately for ordinary writes, Flush and FUA. Includes backend refusals. ublk range/buffer validation precedes backend tracing. `RequestTotal` pools all three; use the separate Flush/FUA distributions for fsync diagnosis. |

Each distribution exposes `Count`, `SumNanos`, 64 non-cumulative `Buckets`, `P50UpperNanos` and `P99UpperNanos`. Bucket 0 holds 0 ns; bucket i>0 holds [2^(i-1), 2^i) ns. Quantiles report conservative power-of-two upper bounds, not exact samples. Zero samples have zero quantiles. Concurrent snapshots are best-effort, not transactional; quiesce the workload for exact counts. Atomic storage is fixed size: no per-volume unbounded labels, request logs or payloads.

`Frontend.Writes` includes FUA, `FUAWrites` is its subset, `Flushes` counts Flush commands and `Failed` counts unsuccessful handler results. Invalid ublk range/buffer requests are rejected before the timing hook or provider lookup: they remain counted by the device-level error/request counters, but add no backend-owned frontend sample. NVMe handler refusal counts retain their existing instrumentation. Non-durable backends retain their existing dispatch behavior. ublk and NVMe/TCP use the same optional backend timing seam; no guest filesystem or hardware is required for accounting tests.

A guest fsync may produce two Flush commands; compare command counts and physical Sync samples before attributing two disk syncs. Shared physical Sync and per-request queue samples have different populations. Do not add p50/p99 values, or subtract one stage's p99 from another's. Large queue time suggests batching/scheduling; large FileSync suggests local file/disk latency; large BackendSync with small local stages can include replication or metadata work. This diagnostic does not separately time each replication/checkpoint operation. Kernel transport completion and guest scheduling are outside these histograms and need an external measurement.

## Verification and overhead scope

Unit tests cover known histogram quantiles, concurrent counters, failed/refused requests, both frontend dispatchers, batch waiter-versus-physical counts and dedup.

Default-on overhead was checked with an interleaved before-versus-after comparison of the file-backed TCP paired-Flush proxy (`SW_BLOCK_SYNC_PERF=1`, `TestNVMePairedFlushLatency`). That measures request-path overhead only; it is not mounted fio, a guest fsync result, or a device-specific latency promise.

The ublk Target forwards the optional timing hook through its lazy providerBackend to the actual durable backend. Initial provider lookup is included in ReceiveToBackend. If provider open fails before a durable backend is available, the request returns that error without a second open attempt, and no backend-owned frontend histogram exists for that failure. The device-level error counters remain the source for such failures. A later request may retry normally. Caller tests start the real Target with only the kernel device constructor replaced; NVMe uses a real TCP target and FUA capsule.
