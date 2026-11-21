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
| `FrontendPrimaryReady` | bool | FrontendPrimaryReady: this replica is the healthy, non-superseded primary and may serve guest I/O. |
| `AuthorityRole` | string | AuthorityRole is primary, superseded or unknown. |
| `ReplicationRole` | string | ReplicationRole is none (a primary), recovering, not_ready, replica_ready or unknown. |
| `RoleAdmissionFailure` | string | RoleAdmissionFailure (G1) explains a not-Healthy projection caused by a failed role call on the replica listener; empty otherwise. (omitted when empty) |
| `UnhealthyReason` | string | UnhealthyReason is the slot's explicit unhealthy report (#128 rule 6), the value its heartbeat carries; empty when healthy. (omitted when empty) |
| `CatchUp` | object | CatchUp shows a supporting replica's catch-up progress while it is not yet replica_ready; it is visible without being an alert. (omitted when empty) |
| `CatchUp.ElapsedMs` | int64 | ElapsedMs is the time since this replica was admitted as a supporting replica at its current epoch. |
| `CatchUp.LagLSN` | uint64 | LagLSN is the engine's H-R (primary head minus this replica's durable frontier) as last observed; 0 when unknown or caught up. |
| `Warnings[]` | string | Warnings are the durable store's typed recovery warnings (e.g. wal_hole_suspected, issue #51). Visible, not an alert: they do not change Healthy or the frontend's readiness. (omitted when empty) |

### `GET /status/peers`

Replication peers of the served volume. Go type `volume.peersStatusBody`.

| Field | Type | Meaning |
|---|---|---|
| `VolumeID` | string | the served volume |
| `ReplicaID` | string | this host's replica |
| `PeerCount` | int | number of entries in Peers |
| `Peers[]` | object | this primary's replication peers |
| `Peers[].ReplicaID` | string | — |
| `Peers[].State` | string | — |
| `Peers[].Epoch` | uint64 | — |
| `Peers[].EndpointVersion` | uint64 | — |
| `Peers[].DataAddr` | string | — |
| `Peers[].CtrlAddr` | string | — |
| `Peers[].SessionID` | uint64 | — |
| `Peers[].ProbeInFlight` | bool | — |
| `Peers[].Closed` | bool | — |

### `GET /status/durable`

Durable store state of the served volume. Go type `volume.durableStatusBody`.

| Field | Type | Meaning |
|---|---|---|
| `VolumeID` | string | the served volume |
| `ReplicaID` | string | this host's replica |
| `VolumeCount` | int | number of entries in Volumes |
| `Volumes[]` | object | the durable store state of the served volume |
| `Volumes[].VolumeID` | string | — |
| `Volumes[].Path` | string | — |
| `Volumes[].Impl` | string | — |
| `Volumes[].ReplicaID` | string | — |
| `Volumes[].Epoch` | uint64 | — |
| `Volumes[].EndpointVersion` | uint64 | — |
| `Volumes[].Latched` | bool | — |
| `Volumes[].Operational` | bool | — |
| `Volumes[].FrontierKnown` | bool | — |
| `Volumes[].DurableLSN` | uint64 | — |
| `Volumes[].RetainedLSN` | uint64 | — |
| `Volumes[].HeadLSN` | uint64 | — |
| `Volumes[].Evidence` | string | — |
| `Volumes[].Closed` | bool | — |
| `Volumes[].WriteProfile` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.WriteTotal.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FlushTotal.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUATotal.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.ReceiveToBackend.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.AckDispatch.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.RequestTotal.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.Writes` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.Flushes` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.FUAWrites` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.Frontend.Failed` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.BackendSync.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL` | object | — (omitted when empty) |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.WALAppend.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.GroupCommitWait.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync` | object | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.Count` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.SumNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.P50UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.P99UpperNanos` | uint64 | — |
| `Volumes[].WriteProfile.DurabilityLatency.WAL.FileSync.Buckets[]` | uint64 | — |
| `Volumes[].WriteProfile.TargetWriteOps` | uint64 | — |
| `Volumes[].WriteProfile.TargetWriteBytes` | uint64 | — |
| `Volumes[].WriteProfile.TargetWriteRequestMaxBytes` | uint64 | — |
| `Volumes[].WriteProfile.TargetWriteDurationNanos` | uint64 | — |
| `Volumes[].WriteProfile.BackendWriteRequestOps` | uint64 | — |
| `Volumes[].WriteProfile.BackendWriteRequestBytes` | uint64 | — |
| `Volumes[].WriteProfile.BackendWriteRequestMaxBytes` | uint64 | — |
| `Volumes[].WriteProfile.BackendWriteOps` | uint64 | — |
| `Volumes[].WriteProfile.BackendWriteBytes` | uint64 | — |
| `Volumes[].WriteProfile.BackendWriteDurationNanos` | uint64 | — |
| `Volumes[].WriteProfile.BackendStorageWriteCalls` | uint64 | — |
| `Volumes[].WriteProfile.BackendStorageWriteBlocks` | uint64 | — |
| `Volumes[].WriteProfile.BackendStorageBatchCalls` | uint64 | — |
| `Volumes[].WriteProfile.BackendStorageBatchBlocks` | uint64 | — |
| `Volumes[].WriteProfile.BackendFullBlockBatchCalls` | uint64 | — |
| `Volumes[].WriteProfile.BackendFullBlockBatchBlocks` | uint64 | — |
| `Volumes[].WriteProfile.BackendFullBlockBatchMax` | uint64 | — |
| `Volumes[].WriteProfile.WALCopyOps` | uint64 | — |
| `Volumes[].WriteProfile.WALCopyBytes` | uint64 | — |
| `Volumes[].WriteProfile.WALCopyDurationNanos` | uint64 | — |
| `Volumes[].WriteProfile.WALEncodeOps` | uint64 | — |
| `Volumes[].WriteProfile.WALEncodeBytes` | uint64 | — |
| `Volumes[].WriteProfile.WALEncodeDurationNanos` | uint64 | — |
| `Volumes[].WriteProfile.WALChecksumOps` | uint64 | — |
| `Volumes[].WriteProfile.WALChecksumBytes` | uint64 | — |
| `Volumes[].WriteProfile.WALChecksumDurationNanos` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendOps` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendBytes` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendDurationNanos` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendLockWaitOps` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendLockWaitNanos` | uint64 | — |
| `Volumes[].WriteProfile.WriteCommitLockWaitOps` | uint64 | — |
| `Volumes[].WriteProfile.WriteCommitLockWaitNanos` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendWriteAtCalls` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendWriteAtBytes` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendWriteAtMaxBytes` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendWrapCount` | uint64 | — |
| `Volumes[].WriteProfile.WALAppendPaddingBytes` | uint64 | — |
| `Volumes[].WriteProfile.DirtyMapUpdateOps` | uint64 | — |
| `Volumes[].WriteProfile.DirtyMapUpdateDurationNanos` | uint64 | — |
| `Volumes[].WriteProfile.BackendSyncOps` | uint64 | — |
| `Volumes[].WriteProfile.BackendSyncDurationNanos` | uint64 | — |
| `Volumes[].DataSync` | object | — (omitted when empty) |
| `Volumes[].DataSync.Mode` | string | — |
| `Volumes[].DataSync.Reason` | string | — |
| `Volumes[].CheckpointPersistence` | object | — (omitted when empty) |
| `Volumes[].CheckpointPersistence.Successes` | uint64 | — |
| `Volumes[].CheckpointPersistence.Refusals` | uint64 | — |
| `Volumes[].CheckpointPersistence.Failures` | uint64 | — |
| `Volumes[].CheckpointPersistence.CheckpointLSN` | uint64 | — |
| `Volumes[].CheckpointPersistence.Advances` | uint64 | — |
| `Volumes[].CheckpointPersistence.LastAdvance` | time (RFC 3339) | — |
| `Volumes[].Chunk` | object | Chunk is the chunk layer's state and cut counters; walstore-chunk volumes only. |
| `Volumes[].Chunk.Attached` | bool | — |
| `Volumes[].Chunk.AwaitingAuthority` | bool | — |
| `Volumes[].Chunk.Fenced` | bool | — |
| `Volumes[].Chunk.RemoteLSN` | uint64 | — |
| `Volumes[].Chunk.Stats` | object | — |
| `Volumes[].Chunk.Stats.Hydration` | object | — |
| `Volumes[].Chunk.Stats.Hydration.WorkerLimit` | int | — |
| `Volumes[].Chunk.Stats.Hydration.QueueDepth` | int | — |
| `Volumes[].Chunk.Stats.Hydration.Active` | int | — |
| `Volumes[].Chunk.Stats.Hydration.Queued` | int | — |
| `Volumes[].Chunk.Stats.Hydration.WaitCalls` | uint64 | — |
| `Volumes[].Chunk.Stats.Hydration.Joined` | uint64 | — |
| `Volumes[].Chunk.Stats.Hydration.WaitRefusals` | uint64 | — |
| `Volumes[].Chunk.Stats.Hydration.Fetches` | uint64 | — |
| `Volumes[].Chunk.Stats.Hydration.Completed` | uint64 | — |
| `Volumes[].Chunk.Stats.Hydration.FetchErrors` | uint64 | — |
| `Volumes[].Chunk.Stats.CutsOK` | uint64 | — |
| `Volumes[].Chunk.Stats.CutsNoop` | uint64 | — |
| `Volumes[].Chunk.Stats.CutsFailed` | uint64 | — |
| `Volumes[].Chunk.Stats.CutsRefused{}` | uint64 | — |
| `Volumes[].Chunk.Stats.CutsAborted{}` | uint64 | saver_capacity, cow_capacity, budget_revoked, upload |
| `Volumes[].Chunk.Stats.CASConflicts` | uint64 | — |
| `Volumes[].Chunk.Stats.CASUnknown` | uint64 | — |
| `Volumes[].Chunk.Stats.PendingUnresolved` | bool | — |
| `Volumes[].Chunk.Stats.DirtyChunks` | int | — |
| `Volumes[].Chunk.Stats.Reconciling` | bool | — |
| `Volumes[].Chunk.Stats.ReconcileDone` | int | — |
| `Volumes[].Chunk.Stats.ReconcileTotal` | int | — |
| `Volumes[].Chunk.Stats.LastCut` | object | — |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases` | object | — |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases.lifecycleWaitNs` | int64 | — |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases.storeWaitNs` | int64 | — |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases.groupCommitNs` | int64 | — |
| `Volumes[].Chunk.Stats.LastCut.SyncPhases.completionWaitNs` | int64 | — |
| `Volumes[].Chunk.Stats.LastCut.CandidateNs` | int64 | Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.WindowSetupNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.PublishPrepareNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.ManifestNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.CommitStateNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
| `Volumes[].Chunk.Stats.LastCut.IntentCleanupNs` | int64 | (group of `CandidateNs`) Disjoint non-payload intervals within barrier-to-settlement fixed cost. |
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
| `Volumes[].Chunk.Stats.LastCut.StageRounds` | int | Pre-copy (BarrierBound > 0): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier; BarrierOverBound: more than the bound (the staging rounds did not converge). |
| `Volumes[].Chunk.Stats.LastCut.StagedChunks` | int | (group of `StageRounds`) Pre-copy (BarrierBound > 0): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier; BarrierOverBound: more than the bound (the staging rounds did not converge). |
| `Volumes[].Chunk.Stats.LastCut.StagedChunksUsed` | int | (group of `StageRounds`) Pre-copy (BarrierBound > 0): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier; BarrierOverBound: more than the bound (the staging rounds did not converge). |
| `Volumes[].Chunk.Stats.LastCut.BarrierCopyChunks` | int | (group of `StageRounds`) Pre-copy (BarrierBound > 0): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier; BarrierOverBound: more than the bound (the staging rounds did not converge). |
| `Volumes[].Chunk.Stats.LastCut.BarrierOverBound` | bool | (group of `StageRounds`) Pre-copy (BarrierBound > 0): StageRounds staging rounds staged StagedChunks chunks outside the barrier; StagedChunksUsed of the uploads in HEAD came from staging. BarrierCopyChunks were copied under the barrier; BarrierOverBound: more than the bound (the staging rounds did not converge). |
| `Volumes[].Chunk.Stats.LastCutH` | uint64 | — |
| `Volumes[].Chunk.Stats.LastCutNs` | int64 | — |
| `Volumes[].Chunk.Stats.BarrierOverBound` | uint64 | BarrierOverBound counts cuts whose barrier copied more than the bound after the staging rounds. |
| `Volumes[].Chunk.Stats.AbsentChunks` | int | AbsentChunks is the number of HEAD-named chunks still absent with a block not local (what HydrateAll would still fetch); 0 = fully local. |
| `Volumes[].Chunk.HostBudget` | interface | — (omitted when empty) |
| `Volumes[].Warnings[]` | string | Warnings are the store's typed recovery warnings (e.g. wal_hole_suspected, issue #51); they do not change readiness. HolesSuspected counts the recoveries that found a WAL hole. (omitted when empty) |
| `Volumes[].HolesSuspected` | uint64 | (group of `Warnings`) Warnings are the store's typed recovery warnings (e.g. wal_hole_suspected, issue #51); they do not change readiness. HolesSuspected counts the recoveries that found a WAL hole. (omitted when empty) |

### `GET /status/recovery`

The engine's replica projection (only with recovery enabled). Go type `engine.ReplicaProjection`.

| Field | Type | Meaning |
|---|---|---|
| `Mode` | Mode (string) | — |
| `RecoveryDecision` | RecoveryDecision (string) | — |
| `SessionKind` | SessionKind (string) | — |
| `SessionPhase` | SessionPhase (string) | — |
| `Epoch` | uint64 | — |
| `EndpointVersion` | uint64 | — |
| `R` | uint64 | — |
| `S` | uint64 | — |
| `H` | uint64 | — |
| `Reason` | string | — |

### `GET /status/chunk`

Chunk store usage of a walstore-chunk volume (only with --chunk-control-endpoint). Go type `chunkstore.VolumeUsage`.

| Field | Type | Meaning |
|---|---|---|
| `volumeID` | string | — |
| `chunkBytes` | uint64 | one full chunk |
| `liveChunks` | int | — |
| `liveBytes` | uint64 | — |
| `snapshots[]` | object | — |
| `snapshots[].name` | string | — |
| `snapshots[].createdNs` | int64 | — |
| `snapshots[].class` | string | — |
| `snapshots[].pinned` | bool | — |
| `snapshots[].chunks` | int | — |
| `snapshots[].exclusiveBytes` | uint64 | — |
| `manifestBindings` | int | — |
| `manifestBytes` | uint64 | — |
| `storedBindings` | int | — |
| `storedBytes` | uint64 | — |
| `storedUpperBound` | bool | — |
| `policy` | object | — |
| `policy.keepNewest` | int | — |
| `policy.keepYoungerNs` | int64 | — |
| `policy.keepHourly` | int | — |
| `policy.keepDaily` | int | — |
| `policy.keepWeekly` | int | — |
| `policySet` | bool | — |

### `GET /status/nvme`

NVMe/TCP frontend counters. Go type `volume.NVMeStatus`.

| Field | Type | Meaning |
|---|---|---|
| `volumeID` | string | the served volume |
| `stats` | object | the NVMe/TCP target's counters since start |
| `stats.sessions_accepted` | uint64 | — |
| `stats.admin_connects` | uint64 | — |
| `stats.io_connects` | uint64 | — |
| `stats.read_commands` | uint64 | — |
| `stats.write_commands` | uint64 | — |
| `stats.flush_commands` | uint64 | — |
| `stats.inline_write_commands` | uint64 | — |
| `stats.inline_write_bytes` | uint64 | — |
| `stats.r2t_write_commands` | uint64 | — |
| `stats.r2t_write_bytes` | uint64 | — |
| `stats.h2c_data_pdus` | uint64 | — |
| `stats.h2c_data_bytes` | uint64 | — |
| `stats.c2h_data_pdus` | uint64 | — |
| `stats.c2h_data_bytes` | uint64 | — |
| `stats.write_capsule_receive_parse_ops` | uint64 | — |
| `stats.write_capsule_receive_parse_ns` | uint64 | — |
| `stats.r2t_data_collection_ops` | uint64 | — |
| `stats.r2t_data_collection_ns` | uint64 | — |
| `stats.write_dispatch_wait_ops` | uint64 | — |
| `stats.write_dispatch_wait_ns` | uint64 | — |
| `stats.write_handler_ops` | uint64 | — |
| `stats.write_handler_ns` | uint64 | — |
| `stats.write_completion_queue_wait_ops` | uint64 | — |
| `stats.write_completion_queue_wait_ns` | uint64 | — |
| `stats.write_completion_send_ops` | uint64 | — |
| `stats.write_completion_send_ns` | uint64 | — |

### `GET /status/ublk`

Ublk frontend counters. Go type `volume.UblkStatus`.

| Field | Type | Meaning |
|---|---|---|
| `volumeID` | string | the served volume |
| `stats` | object | the ublk device's request counters since start |
| `stats.read_requests` | uint64 | — |
| `stats.write_requests` | uint64 | — |
| `stats.flush_requests` | uint64 | — |
| `stats.fua_writes` | uint64 | — |
| `stats.other_requests` | uint64 | — |
| `stats.errors` | uint64 | — |

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
