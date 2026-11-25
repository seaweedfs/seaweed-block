# Status schema

The blockvolume status listener (`--status-addr`) serves JSON on the routes below. Each table lists every field a route returns: its JSON path (`[]` marks a list, `{}` a map), its type, and its meaning as the field's Go comment states it. A field marked `—` has no comment in the code yet.

The tables are generated from the Go types, so they list exactly the fields that exist. `go test ./core/host/volume -run TestStatusSchemaDoc` fails when the code and this page disagree; `STATUS_SCHEMA_UPDATE=1` rewrites the generated block.

All routes take `?volume=<id>` and answer 404 for a volume this host does not serve.

<!-- BEGIN GENERATED: go test ./core/host/volume -run TestStatusSchemaDoc (STATUS_SCHEMA_UPDATE=1 rewrites) -->

### `GET /status`

The volume's authority and health projection. Go type `volume.StatusProjection`.

| Field | Type | Meaning |
|---|---|---|
| `VolumeID` | string | the volume this host serves |
| `ReplicaID` | string | this host's replica of it |
| `Epoch` | uint64 | the authority epoch of the current assignment |
| `EndpointVersion` | uint64 | the assignment's endpoint version within the epoch |
| `Healthy` | bool | the replica may serve I/O at this epoch |
| `Memory` | object | Memory is the process's memory use at the time of the request. |
| `Memory.HeapAllocBytes` | uint64 | Go heap bytes in live objects (runtime.MemStats.HeapAlloc) |
| `Memory.HeapInuseBytes` | uint64 | Go heap bytes in in-use spans (HeapInuse) |
| `Memory.HeapSysBytes` | uint64 | Go heap bytes obtained from the OS (HeapSys) |
| `Memory.HeapReleasedBytes` | uint64 | Go heap bytes returned to the OS (HeapReleased) |
| `Memory.SoftLimitBytes` | int64 | the Go soft memory limit in force (--memory-limit-mib or GOMEMLIMIT) |
| `Memory.RSSBytes` | uint64 | resident set size from /proc/self/status (VmRSS); 0 off Linux |
| `Memory.AnonymousRSSBytes` | uint64 | anonymous resident memory (RssAnon); 0 off Linux |
| `FrontendPrimaryReady` | bool | FrontendPrimaryReady: this replica is the healthy, non-superseded primary, its local durable backend admits I/O, and it may serve guest I/O (INV-FE-PRIMARY-READY-OPERATIONAL-001). |
| `NotReadyReason` | string | NotReadyReason says why FrontendPrimaryReady is false, first match: a workspace primary holds no granted lease now (NotReadyLeaseNotHeld); the local durable backend admits no I/O (its evidence: attach pending, fenced, detached) or has no status for the volume; the frontend target is not listening yet; this replica is not the healthy primary. Empty when ready. (omitted when empty) |
| `AuthorityRole` | string | AuthorityRole is primary, superseded or unknown. |
| `ReplicationRole` | string | ReplicationRole is none (a primary), recovering, not_ready, replica_ready or unknown. |
| `RoleAdmissionFailure` | string | RoleAdmissionFailure (G1) explains a not-Healthy projection caused by a failed role call on the replica listener; empty otherwise. (omitted when empty) |
| `UnhealthyReason` | string | UnhealthyReason is the slot's explicit unhealthy report (#128 rule 6), the value its heartbeat carries; empty when healthy. (omitted when empty) |
| `CatchUp` | object | CatchUp shows a supporting replica's catch-up progress while it is not yet replica_ready; it is visible without being an alert. (omitted when empty) |
| `CatchUp.ElapsedMs` | int64 | ElapsedMs is the time since this replica was admitted as a supporting replica at its current epoch. |
| `CatchUp.LagLSN` | uint64 | LagLSN is the engine's H-R (primary head minus this replica's durable frontier) as last observed; 0 when unknown or caught up. |
| `Warnings[]` | string | Warnings include routing disagreements and typed recovery warnings (e.g. wal_hole_suspected, issue #51). Visible, not an alert: they do not change Healthy or the frontend's readiness. (omitted when empty) |
| `MasterWait` | object | MasterWait is why a workspace primary holds no granted lease, present only while NotReadyReason is lease_not_held: Reason (unreachable, credentials, "identity refused: <kind>" with kind unregistered or wrong_host, barrier, no_assignment, refused, moved, deleted), since when, and the startup wait's one bound or the master's barrier time (UntilMs, absent while there is no bound). Detail carries the master's diagnostic text for a typed final refusal, including a moved holder and epoch. (omitted when empty) |
| `MasterWait.Reason` | string | — |
| `MasterWait.Detail` | string | — (omitted when empty) |
| `MasterWait.SinceMs` | int64 | — |
| `MasterWait.UntilMs` | int64 | — (omitted when empty) |

### `GET /status/peers`

Replication peers of the served volume. Go type `volume.peersStatusBody`.

| Field | Type | Meaning |
|---|---|---|
| `VolumeID` | string | the served volume |
| `ReplicaID` | string | this host's replica |
| `PeerCount` | int | number of entries in Peers |
| `Peers[]` | object | this primary's replication peers |
| `Peers[].ReplicaID` | string | the peer replica |
| `Peers[].State` | string | the peer state (the replica peer table, M3) |
| `Peers[].Epoch` | uint64 | the epoch the peer was assigned at |
| `Peers[].EndpointVersion` | uint64 | the peer's endpoint version within the epoch |
| `Peers[].DataAddr` | string | the peer's data-path address |
| `Peers[].CtrlAddr` | string | the peer's control-path address |
| `Peers[].SessionID` | uint64 | the current recovery or shipping session; 0 when none |
| `Peers[].ProbeInFlight` | bool | a reachability probe of the peer is running |
| `Peers[].Closed` | bool | the peer handle was closed (it no longer ships) |

### `GET /status/durable`

Durable store state of the served volume. Go type `volume.durableStatusBody`.

| Field | Type | Meaning |
|---|---|---|
| `VolumeID` | string | the served volume |
| `ReplicaID` | string | this host's replica |
| `VolumeCount` | int | number of entries in Volumes |
| `Volumes[]` | object | the durable store state of the served volume |
| `Volumes[].VolumeID` | string | the volume |
| `Volumes[].Path` | string | its store path under --durable-root |
| `Volumes[].Impl` | string | the durable implementation (walstore, smartwal, walstore-chunk, ...) |
| `Volumes[].ReplicaID` | string | the replica this store holds |
| `Volumes[].StoreID` | string | the store's superblock UUID (hex); empty when it carries none (omitted when empty) |
| `Volumes[].Epoch` | uint64 | the epoch the store is latched to; 0 before an assignment |
| `Volumes[].EndpointVersion` | uint64 | the endpoint version it is latched to |
| `Volumes[].Latched` | bool | an assignment latched the store (epoch and endpoint version set) |
| `Volumes[].Operational` | bool | the backend admits I/O now |
| `Volumes[].FrontierKnown` | bool | DurableLSN, RetainedLSN and HeadLSN are the store's current boundaries |
| `Volumes[].DurableLSN` | uint64 | R: the newest LSN made durable |
| `Volumes[].RetainedLSN` | uint64 | S: the oldest LSN the WAL still retains |
| `Volumes[].HeadLSN` | uint64 | H: the newest LSN written |
| `Volumes[].Evidence` | string | why the backend is or is not operational |
| `Volumes[].Closed` | bool | the backend was closed |
| `Volumes[].WriteProfile` | object | write-path counters since start |
| `Volumes[].WriteProfile.DurabilityLatency` | object | latency distributions of writes, flushes and syncs |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend` | object | per-request latency at the frontend, by stage |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal` | object | end-to-end latency of writes |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal` | object | end-to-end latency of flushes |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal` | object | end-to-end latency of forced-unit-access writes |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend` | object | from request receipt to the backend call |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch` | object | from backend completion to the acknowledgement sent |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal` | object | end-to-end latency of every request |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.Writes` | uint64 | writes observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.Flushes` | uint64 | flushes observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUAWrites` | uint64 | forced-unit-access writes observed |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.Failed` | uint64 | requests that completed with an error |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync` | object | latency of backend syncs |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.WAL` | object | the WAL's own sync latency, where the store reports it (omitted when empty) |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend` | object | WAL append latency |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait` | object | time a sync request waited in the group commit queue |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync` | object | physical file syncs (one sample per sync, however many waiters) |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.Count` | uint64 | samples observed |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.SumNanos` | uint64 | their total nanoseconds |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.P50UpperNanos` | uint64 | upper bound of the bucket holding the median |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.P99UpperNanos` | uint64 | upper bound of the bucket holding the 99th percentile |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.Buckets[]` | uint64 | bucket i counts samples of i significant bits in ns, i.e. [2^(i-1), 2^i) ns |
| `Volumes[].WriteProfile.TargetWriteOps` | uint64 | writes the protocol target handed to the volume |
| `Volumes[].WriteProfile.TargetWriteBytes` | uint64 | their bytes |
| `Volumes[].WriteProfile.TargetWriteRequestMaxBytes` | uint64 | the largest of them |
| `Volumes[].WriteProfile.TargetWriteDurationNanos` | uint64 | their total time at the target |
| `Volumes[].WriteProfile.BackendWriteRequestOps` | uint64 | write requests that entered the durable backend |
| `Volumes[].WriteProfile.BackendWriteRequestBytes` | uint64 | their bytes |
| `Volumes[].WriteProfile.BackendWriteRequestMaxBytes` | uint64 | the largest of them |
| `Volumes[].WriteProfile.BackendWriteOps` | uint64 | backend writes completed |
| `Volumes[].WriteProfile.BackendWriteBytes` | uint64 | their bytes |
| `Volumes[].WriteProfile.BackendWriteDurationNanos` | uint64 | their total time |
| `Volumes[].WriteProfile.BackendStorageWriteCalls` | uint64 | single-write calls into the store |
| `Volumes[].WriteProfile.BackendStorageWriteBlocks` | uint64 | blocks they wrote |
| `Volumes[].WriteProfile.BackendStorageBatchCalls` | uint64 | batch-write calls into the store |
| `Volumes[].WriteProfile.BackendStorageBatchBlocks` | uint64 | blocks they wrote |
| `Volumes[].WriteProfile.BackendFullBlockBatchCalls` | uint64 | batch calls made only of whole blocks |
| `Volumes[].WriteProfile.BackendFullBlockBatchBlocks` | uint64 | blocks they wrote |
| `Volumes[].WriteProfile.BackendFullBlockBatchMax` | uint64 | the largest such batch, in blocks |
| `Volumes[].WriteProfile.WALCopyOps` | uint64 | WAL record payload copies |
| `Volumes[].WriteProfile.WALCopyBytes` | uint64 | their bytes |
| `Volumes[].WriteProfile.WALCopyDurationNanos` | uint64 | their total time |
| `Volumes[].WriteProfile.WALEncodeOps` | uint64 | WAL record encodes |
| `Volumes[].WriteProfile.WALEncodeBytes` | uint64 | their bytes |
| `Volumes[].WriteProfile.WALEncodeDurationNanos` | uint64 | their total time |
| `Volumes[].WriteProfile.WALChecksumOps` | uint64 | WAL record checksums |
| `Volumes[].WriteProfile.WALChecksumBytes` | uint64 | their bytes |
| `Volumes[].WriteProfile.WALChecksumDurationNanos` | uint64 | their total time |
| `Volumes[].WriteProfile.WALAppendOps` | uint64 | WAL appends |
| `Volumes[].WriteProfile.WALAppendBytes` | uint64 | their bytes |
| `Volumes[].WriteProfile.WALAppendDurationNanos` | uint64 | their total time |
| `Volumes[].WriteProfile.WALAppendLockWaitOps` | uint64 | appends that waited for the WAL append lock |
| `Volumes[].WriteProfile.WALAppendLockWaitNanos` | uint64 | their total wait |
| `Volumes[].WriteProfile.WriteCommitLockWaitOps` | uint64 | writes that waited for the commit lock |
| `Volumes[].WriteProfile.WriteCommitLockWaitNanos` | uint64 | their total wait |
| `Volumes[].WriteProfile.WALAppendWriteAtCalls` | uint64 | file WriteAt calls made by WAL appends |
| `Volumes[].WriteProfile.WALAppendWriteAtBytes` | uint64 | their bytes |
| `Volumes[].WriteProfile.WALAppendWriteAtMaxBytes` | uint64 | the largest of them |
| `Volumes[].WriteProfile.WALAppendWrapCount` | uint64 | times the WAL append wrapped to the start of the log |
| `Volumes[].WriteProfile.WALAppendPaddingBytes` | uint64 | padding bytes written at those wraps |
| `Volumes[].WriteProfile.DirtyMapUpdateOps` | uint64 | dirty-map updates after writes |
| `Volumes[].WriteProfile.DirtyMapUpdateDurationNanos` | uint64 | their total time |
| `Volumes[].WriteProfile.BackendSyncOps` | uint64 | backend syncs (flush, FUA, explicit sync) |
| `Volumes[].WriteProfile.BackendSyncDurationNanos` | uint64 | their total time |
| `Volumes[].DataSync` | object | data-file sync state, where the store has one (omitted when empty) |
| `Volumes[].DataSync.Mode` | string | the data-file sync call in use (fsync or fdatasync) |
| `Volumes[].DataSync.Reason` | string | why it was chosen (an explicit SWBLOCK_SYNC, or the automatic choice) |
| `Volumes[].CheckpointPersistence` | object | checkpoint persistence state, where the store has one (omitted when empty) |
| `Volumes[].CheckpointPersistence.Successes` | uint64 | checkpoint calls that succeeded (including unchanged ones) |
| `Volumes[].CheckpointPersistence.Refusals` | uint64 | checkpoint calls refused because the map coverage was stale |
| `Volumes[].CheckpointPersistence.Failures` | uint64 | checkpoint calls that failed with an error |
| `Volumes[].CheckpointPersistence.CheckpointLSN` | uint64 | the persisted checkpoint LSN |
| `Volumes[].CheckpointPersistence.Advances` | uint64 | successes that moved CheckpointLSN forward |
| `Volumes[].CheckpointPersistence.LastAdvance` | time (RFC 3339) | when the latest advance happened; zero if none in this open |
| `Volumes[].Chunk` | object | Chunk is the chunk layer's state and cut counters; walstore-chunk volumes only. |
| `Volumes[].Chunk.Attached` | bool | the chunk layer is attached to the remote |
| `Volumes[].Chunk.AwaitingAuthority` | bool | recovered; the layer waits for master authority to attach |
| `Volumes[].Chunk.Fenced` | bool | a newer holder fenced this layer: it publishes nothing |
| `Volumes[].Chunk.RemoteLSN` | uint64 | the LSN of the remote HEAD (what the remote holds durably) |
| `Volumes[].Chunk.Stats` | object | the layer's cut counters; zero until attached |
| `Volumes[].Chunk.Stats.Hydration` | object | lazy fetches of absent chunks from the remote |
| `Volumes[].Chunk.Stats.Hydration.WorkerLimit` | int | fetch workers allowed (--chunk-hydration-workers) |
| `Volumes[].Chunk.Stats.Hydration.QueueDepth` | int | fetch queue capacity (--chunk-hydration-queue-depth) |
| `Volumes[].Chunk.Stats.Hydration.Active` | int | fetches running now |
| `Volumes[].Chunk.Stats.Hydration.Queued` | int | fetches waiting for a worker now |
| `Volumes[].Chunk.Stats.Hydration.WaitCalls` | uint64 | reads that waited for a chunk to arrive |
| `Volumes[].Chunk.Stats.Hydration.Joined` | uint64 | of those, reads that joined a fetch already running |
| `Volumes[].Chunk.Stats.Hydration.WaitRefusals` | uint64 | waits that ended without the chunk (the caller cancelled, or the fetch was not admitted) |
| `Volumes[].Chunk.Stats.Hydration.Fetches` | uint64 | fetches started |
| `Volumes[].Chunk.Stats.Hydration.Completed` | uint64 | fetches that installed the chunk |
| `Volumes[].Chunk.Stats.Hydration.FetchErrors` | uint64 | fetches that failed |
| `Volumes[].Chunk.Stats.CutsOK` | uint64 | cuts that installed a HEAD |
| `Volumes[].Chunk.Stats.CutsNoop` | uint64 | cuts with nothing new (no CAS) |
| `Volumes[].Chunk.Stats.CutsFailed` | uint64 | every other cut failure |
| `Volumes[].Chunk.Stats.CutsRefused{}` | uint64 | refusals by reason: admission, fenced, detached |
| `Volumes[].Chunk.Stats.CutsAborted{}` | uint64 | saver_capacity, cow_capacity, no_room_to_cut, free_space_unreadable, budget_revoked, upload |
| `Volumes[].Chunk.Stats.CASConflicts` | uint64 | HEAD CAS precondition failures |
| `Volumes[].Chunk.Stats.CASUnknown` | uint64 | HEAD CAS results still unresolved after the re-read |
| `Volumes[].Chunk.Stats.PendingUnresolved` | bool | an unresolved HEAD proposal is held now |
| `Volumes[].Chunk.Stats.DirtyChunks` | int | chunks dirty since the last HEAD (not counting those awaiting reconciliation): candidates, not progress |
| `Volumes[].Chunk.Stats.StagedChunks` | uint64 | chunks staged by every cut attempt, failed ones included (in-process) |
| `Volumes[].Chunk.Stats.KnownZeroChunks` | uint64 | chunks this layer's fresh attach certified known-zero (RULE-FRESH-KNOWN-ZERO); 0 otherwise |
| `Volumes[].Chunk.Stats.Reconciling` | bool | the dirty set is being reconciled against HEAD; no cut publishes meanwhile |
| `Volumes[].Chunk.Stats.ReconcileDone` | int | chunks compared so far |
| `Volumes[].Chunk.Stats.ReconcileTotal` | int | chunks to compare |
| `Volumes[].Chunk.Stats.LastCut` | object | the last successful cut's measurements |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases` | object | the parts of SyncNs (never added to it) |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases.lifecycleWaitNs` | int64 | waiting for the store's lifecycle lock |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases.storeWaitNs` | int64 | waiting for the store lock |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases.groupCommitNs` | int64 | in the group commit that made the data durable |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases.completionWaitNs` | int64 | waiting for the sync leader role to validate the completion |
| `Volumes[].Chunk.Stats.LastCut.CandidateNs` | int64 | Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.WindowSetupNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.PublishPrepareNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.ManifestNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.CommitStateNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.NonPayloadCopyNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.PayloadNs` | int64 | disjoint streaming/copy/upload wall intervals |
| `Volumes[].Chunk.Stats.LastCut.COWWALReadAttempts` | uint64 | Local validated WAL attempts and local copy duration, including savers. |
| `Volumes[].Chunk.Stats.LastCut.COWLocalReadNs` | int64 | (group of `COWWALReadAttempts`) Local validated WAL attempts and local copy duration, including savers. |
| `Volumes[].Chunk.Stats.LastCut.COWSavedChunks` | int | (group of `COWWALReadAttempts`) Local validated WAL attempts and local copy duration, including savers. |
| `Volumes[].Chunk.Stats.LastCut.COWSaveNs` | int64 | (group of `COWWALReadAttempts`) Local validated WAL attempts and local copy duration, including savers. |
| `Volumes[].Chunk.Stats.LastCut.COWWaitNs` | int64 | (group of `COWWALReadAttempts`) Local validated WAL attempts and local copy duration, including savers. |
| `Volumes[].Chunk.Stats.LastCut.BarrierPauseNs` | int64 | BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.LockWaitNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.DrainNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.CASWriteNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.HeadReadbackNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.SyncNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.CopyNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.CandidateChunks` | int | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.UploadedChunks` | int | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.UploadedBytes` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.ZeroedChunks` | int | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.UnchangedChunks` | int | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.UploadNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.CASNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.MapPersistNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.CASResult` | string | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.TotalNs` | int64 | (group of `BarrierPauseNs`) BarrierPauseNs is the guest pause: from the capture barrier's Lock request until it is released. LockWaitNs is its first part, the wait for writers already holding the capture barrier shared. |
| `Volumes[].Chunk.Stats.LastCut.StageRounds` | int | Pre-copy (staging by the copy-on-write side budget, M5 Stage): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier. BarrierOverBound: more than the legacy fixture BarrierBound; always false in production, where that bound is zero. |
| `Volumes[].Chunk.Stats.LastCut.StagedChunks` | int | (group of `StageRounds`) Pre-copy (staging by the copy-on-write side budget, M5 Stage): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier. BarrierOverBound: more than the legacy fixture BarrierBound; always false in production, where that bound is zero. |
| `Volumes[].Chunk.Stats.LastCut.StagedChunksUsed` | int | (group of `StageRounds`) Pre-copy (staging by the copy-on-write side budget, M5 Stage): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier. BarrierOverBound: more than the legacy fixture BarrierBound; always false in production, where that bound is zero. |
| `Volumes[].Chunk.Stats.LastCut.BarrierCopyChunks` | int | (group of `StageRounds`) Pre-copy (staging by the copy-on-write side budget, M5 Stage): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier. BarrierOverBound: more than the legacy fixture BarrierBound; always false in production, where that bound is zero. |
| `Volumes[].Chunk.Stats.LastCut.BarrierOverBound` | bool | (group of `StageRounds`) Pre-copy (staging by the copy-on-write side budget, M5 Stage): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier. BarrierOverBound: more than the legacy fixture BarrierBound; always false in production, where that bound is zero. |
| `Volumes[].Chunk.Stats.LastCutH` | uint64 | the LSN the last successful cut published |
| `Volumes[].Chunk.Stats.LastCutNs` | int64 | when it completed (wall clock, Unix ns) |
| `Volumes[].Chunk.Stats.CutSeq` | uint64 | successful cuts of this layer so far (in-process); the latest cut's identity |
| `Volumes[].Chunk.Stats.BarrierOverBound` | uint64 | BarrierOverBound counts cuts whose barrier copied more than the bound after the staging rounds. |
| `Volumes[].Chunk.Stats.AbsentChunks` | int | AbsentChunks is the number of HEAD-named chunks still absent with a block not local (what HydrateAll would still fetch); 0 = fully local. |
| `Volumes[].Chunk.Stats.COWSpillCap` | int64 | COWSpillCap is the last cut's side-file cap in bytes, read once per cut; COWSpillCapReason is why it is 0, when the free space made it so: no_room_to_cut (half the free space is within the 1 GiB reserve) or free_space_unreadable (the free space could not be read). |
| `Volumes[].Chunk.Stats.COWSpillCapReason` | string | (group of `COWSpillCap`) COWSpillCap is the last cut's side-file cap in bytes, read once per cut; COWSpillCapReason is why it is 0, when the free space made it so: no_room_to_cut (half the free space is within the 1 GiB reserve) or free_space_unreadable (the free space could not be read). (omitted when empty) |
| `Volumes[].Chunk.HostBudget` | interface | the host background-cut budget state, with --host-limit-dir (omitted when empty) |
| `Volumes[].Chunk.ScheduledSnapshot` | object | the scheduled-snapshot state; absent when the schedule is off or its loop has not started (omitted when empty) |
| `Volumes[].Chunk.ScheduledSnapshot.periodNs` | int64 | PeriodNs is the schedule period; zero means off. |
| `Volumes[].Chunk.ScheduledSnapshot.lastOkNs` | int64 | LastOkNs is when the last scheduled snapshot succeeded; zero: none since the schedule started. |
| `Volumes[].Chunk.ScheduledSnapshot.nextDueNs` | int64 | NextDueNs is when the next attempt is due. |
| `Volumes[].Chunk.ScheduledSnapshot.lastFailure` | object | LastFailure is the last failed attempt since the last success. (omitted when empty) |
| `Volumes[].Chunk.ScheduledSnapshot.lastFailure.code` | string | Code is the failure's code, as the control endpoint reports it. |
| `Volumes[].Chunk.ScheduledSnapshot.lastFailure.atNs` | int64 | AtNs is when the attempt failed. |
| `Volumes[].Chunk.ScheduledSnapshot.attempts` | int | Attempts counts the failed attempts since the last success. |
| `Volumes[].Chunk.ScheduledSnapshot.skipped{}` | int64 | Skipped counts attempts not made, by reason. (omitted when empty) |
| `Volumes[].Chunk.ScheduledSnapshot.snapshot_overdue` | bool | Overdue reports that no scheduled snapshot succeeded within one period plus the retry cap, counted from the last success or, before any, from the start of the schedule. |
| `Volumes[].Chunk.CutSchedule` | object | the automatic-cut timer's state; absent when the timer is off or its loop has not started (omitted when empty) |
| `Volumes[].Chunk.CutSchedule.HostSlotWaits` | uint64 | automatic cuts refused because every host slot was held |
| `Volumes[].Chunk.CutSchedule.HostSlotWaitSinceNs` | int64 | wall time of the first such refusal since the last successful cut; 0 when not waiting |
| `Volumes[].Warnings[]` | string | Warnings are the store's typed recovery warnings (e.g. wal_hole_suspected, issue #51); they do not change readiness. HolesSuspected counts the recoveries that found a WAL hole. (omitted when empty) |
| `Volumes[].HolesSuspected` | uint64 | (group of `Warnings`) Warnings are the store's typed recovery warnings (e.g. wal_hole_suspected, issue #51); they do not change readiness. HolesSuspected counts the recoveries that found a WAL hole. (omitted when empty) |

### `GET /status/recovery`

The engine's replica projection (only with recovery enabled). Go type `engine.ReplicaProjection`.

| Field | Type | Meaning |
|---|---|---|
| `Mode` | Mode (string) | the engine mode (idle, recovering, healthy, degraded) |
| `RecoveryDecision` | RecoveryDecision (string) | the recovery class the R/S/H boundaries call for |
| `SessionKind` | SessionKind (string) | the kind of the current recovery session, if any |
| `SessionPhase` | SessionPhase (string) | its phase |
| `Epoch` | uint64 | the authority epoch |
| `EndpointVersion` | uint64 | the endpoint version within the epoch |
| `R` | uint64 | the replica's durable boundary (LSN) |
| `S` | uint64 | the primary's recoverable start boundary (WAL tail LSN) |
| `H` | uint64 | the primary's target boundary (head LSN) |
| `Reason` | string | why the engine is in this mode or decision |

### `GET /status/chunk`

Chunk store usage of a walstore-chunk volume (only with --chunk-control-endpoint). Go type `chunkstore.Usage`.

| Field | Type | Meaning |
|---|---|---|
| `snapshots` | int | — |
| `dependents` | int | — |
| `own_chunks` | int | — |
| `foreign_chunks` | int | — |

### `GET /status/nvme`

NVMe/TCP frontend counters. Go type `volume.NVMeStatus`.

| Field | Type | Meaning |
|---|---|---|
| `volumeID` | string | the served volume |
| `stats` | object | the NVMe/TCP target's counters since start |
| `stats.sessions_accepted` | uint64 | TCP sessions accepted |
| `stats.admin_connects` | uint64 | admin-queue Connect commands |
| `stats.io_connects` | uint64 | I/O-queue Connect commands |
| `stats.read_commands` | uint64 | Read commands received |
| `stats.write_commands` | uint64 | Write commands received |
| `stats.flush_commands` | uint64 | Flush commands received |
| `stats.inline_write_commands` | uint64 | writes whose data came inline in the command capsule |
| `stats.inline_write_bytes` | uint64 | their data bytes |
| `stats.r2t_write_commands` | uint64 | writes whose data the target requested with R2T |
| `stats.r2t_write_bytes` | uint64 | their data bytes |
| `stats.h2c_data_pdus` | uint64 | host-to-controller data PDUs received |
| `stats.h2c_data_bytes` | uint64 | their bytes |
| `stats.c2h_data_pdus` | uint64 | controller-to-host data PDUs sent (read data) |
| `stats.c2h_data_bytes` | uint64 | their bytes |
| `stats.write_capsule_receive_parse_ops` | uint64 | Write phase attribution: for each phase, the operations timed and their total nanoseconds (overlapping phases can overlap in time). |
| `stats.write_capsule_receive_parse_ns` | uint64 | total nanoseconds of those operations |
| `stats.r2t_data_collection_ops` | uint64 | R2T data collections (R2T send to the last H2CData) |
| `stats.r2t_data_collection_ns` | uint64 | total nanoseconds of those operations |
| `stats.write_dispatch_wait_ops` | uint64 | waits for a write handler to take the command |
| `stats.write_dispatch_wait_ns` | uint64 | total nanoseconds of those operations |
| `stats.write_handler_ops` | uint64 | write handler runs (the backend write) |
| `stats.write_handler_ns` | uint64 | total nanoseconds of those operations |
| `stats.write_completion_queue_wait_ops` | uint64 | waits of a completion for the send queue |
| `stats.write_completion_queue_wait_ns` | uint64 | total nanoseconds of those operations |
| `stats.write_completion_send_ops` | uint64 | completion sends to the host |
| `stats.write_completion_send_ns` | uint64 | total nanoseconds of those operations |

### `GET /status/ublk`

Ublk frontend counters. Go type `volume.UblkStatus`.

| Field | Type | Meaning |
|---|---|---|
| `volumeID` | string | the served volume |
| `stats` | object | the ublk device's request counters since start |
| `stats.read_requests` | uint64 | read requests received |
| `stats.write_requests` | uint64 | write requests received |
| `stats.flush_requests` | uint64 | flush requests received |
| `stats.fua_writes` | uint64 | writes with forced unit access |
| `stats.other_requests` | uint64 | requests of any other operation |
| `stats.errors` | uint64 | requests completed with a negative result |

### `GET /status/frontend-capabilities`

The frontends this host can serve. Go type `volume.FrontendCapabilitiesStatus`.

| Field | Type | Meaning |
|---|---|---|
| `volumeID` | string | the served volume |
| `hostCapabilityIsProductClaim` | bool | always false: host facts, not a supported-product claim |
| `frontendTransports[]` | object | one entry per protocol and transport |
| `frontendTransports[].protocol` | string | the block protocol (iscsi, nvme, ublk) |
| `frontendTransports[].transport` | string | its transport (tcp, rdma, local) |
| `frontendTransports[].supported` | bool | this build and host can serve it |
| `frontendTransports[].listenerImplemented` | bool | a listener exists for it in this binary |
| `frontendTransports[].listenerStarted` | bool | the listener is running now |
| `frontendTransports[].startAllowed` | bool | the preflight checks allow starting it |
| `frontendTransports[].startReason` | string | why it may or may not start (omitted when empty) |
| `frontendTransports[].reason` | string | why it is or is not supported |
| `frontendTransports[].preflight[]` | object | the host facts checked for it (omitted when empty) |
| `frontendTransports[].preflight[].name` | string | the host fact checked (a module, device or privilege) |
| `frontendTransports[].preflight[].available` | bool | the fact holds on this host |
| `frontendTransports[].preflight[].reason` | string | why it holds or not |
| `frontendTransports[].preflight[].detail` | string | extra detail, such as the path probed (omitted when empty) |

<!-- END GENERATED -->
