# Current Plan: Phase 175 Snapshot, Backup, And Restore Milestone

Status: active. Started 2026-08-01 from merged `origin/main` at `7d016ed`.

Progress through the local data layer plus the D4 capture runtime:

- D1 storage cut contract implemented for `BlockStore`, `walstore`, and the
  shipped-default `smartwal`; deterministic tests prove normal writes and
  direct extent installs cannot cross an in-progress cut.
- D2 immutable archive and durable catalog implemented locally with per-block
  CRC, archive SHA-256, fsync/rename publication, restart validation,
  idempotency, corruption refusal, active-reader deletion hold, and owned
  temporary/orphan cleanup.
- D3 restore-to-new implemented locally for `smartwal` and `walstore` with
  normal target writes, durability before atomic publication, restart proof,
  source/snapshot/target isolation, and failed-restore cleanup.
- D1-D3 local data-layer gate passed on exact `11b945f` on m02 Linux with
  `go test -race -count=10 ./core/snapshot ./core/storage/...`; Linux directory
  fsync paths executed and all packages passed. D1-D3 are closed for the local
  data layer. Distributed authority/runtime and CSI claims remain D4+ work.
- D4 capture runtime resolves one positively ready current primary, carries
  the exact replica/epoch/endpoint-version/runtime-endpoint lineage through a
  fresh heartbeat observation, and fences capture before, during, and after
  streaming. The blockvolume endpoint is dedicated HTTPS with mutual TLS, a
  file-backed bearer token, per-block CRC, a source-capacity stream bound, a
  reconciling terminal frame, and no redirect forwarding. Blockmaster now owns
  the durable catalog and the dedicated mTLS plus API-token-protected
  create/get/list/delete SnapshotService RPC; that service is not registered
  on the shared plaintext control listener. Launcher and Helm wiring are
  explicit opt-in and default-disabled, require a hostname-pinned single
  blockmaster replica, and keep the catalog on durable hostPath storage. A real
  mTLS integration test streams a
  cut into the durable catalog; authority-change and partial-stream paths
  publish no ready record. D4 restore orchestration and the live distributed
  gate remain open.

## Product Outcome

Deliver one user-visible data-lifecycle loop rather than a sequence of small
surface patches:

```text
PVC with live writes
-> crash-consistent snapshot
-> durable snapshot identity and catalog
-> restore into a new volume
-> Kubernetes VolumeSnapshot and restored PVC
-> independent writer/read verification
-> delete and uninstall with zero residue
```

The first supported claim is a crash-consistent, single-volume snapshot and
restore to a new volume. It is not an application-consistent snapshot unless
the user freezes the application before requesting it.

## Why This Is Next

Phases 35-98 closed the operation/control-plane loop, Phases 99-166 added and
hardened NVMe paths, and Phases 167-174 evaluated execution architecture
without finding a safe performance candidate. The largest remaining product
capability gap is data lifecycle: the CSI controller does not advertise
`CREATE_DELETE_SNAPSHOT`, there is no durable snapshot catalog, and a new
volume cannot be provisioned from a snapshot.

The existing `LogicalStorage.AllBlocks()` method is rebuild enumeration. It
does not fence concurrent writes and therefore is not a snapshot API. Reusing
it as one would create a torn image that may contain blocks from different
logical times.

## Standards Boundary

Phase 175 follows the CSI controller contract:

- `CreateSnapshot`, `DeleteSnapshot`, and `ListSnapshots` own snapshot
  lifecycle and must be idempotent;
- `CreateVolume.volume_content_source.snapshot` restores into a new volume;
- in-place revert is outside the CSI snapshot contract;
- the plugin advertises `CREATE_DELETE_SNAPSHOT` only after the complete
  runtime path is available;
- Kubernetes `external-snapshotter` calls the CSI RPCs while the cluster
  snapshot-controller owns `VolumeSnapshot` / `VolumeSnapshotContent`
  reconciliation.

Primary references:

- <https://github.com/container-storage-interface/spec/blob/master/spec.md>
- <https://kubernetes-csi.github.io/docs/external-snapshotter.html>
- <https://kubernetes-csi.github.io/docs/snapshot-controller.html>

## Required Invariants

1. A successful snapshot has one atomic cut frontier. Every write ordered
   before the cut is included and every write ordered after it is excluded.
2. Snapshot success is published only after snapshot data and catalog metadata
   are durable. A partial archive is never listed as ready.
3. Repeating `CreateSnapshot` with the same name and source is idempotent;
   reusing the name for another source is rejected.
4. Snapshot identity, source volume identity, geometry, creation time,
   frontier, readiness, and integrity evidence survive restart.
5. Restore creates a new volume identity. It never overwrites the source and
   never implements an implicit in-place rollback.
6. A restored volume is published only after all archive records pass
   integrity checks and the target durability fence succeeds.
7. Source writes after the cut cannot change snapshot bytes. Writes to a
   restored volume cannot change the source or snapshot.
8. Delete is idempotent and cannot remove data still referenced by an active
   restore operation.
9. CSI, lifecycle, data-plane, operator-status, and cleanup ownership remain
   separate. The status controller gains no arbitrary data mutation power.
10. Unsupported backends and incomplete runtime wiring fail closed and do not
    advertise snapshot capability.

## Deliverables

### D1. Consistency And Ownership Contract

- Define the snapshot cut, durability fence, catalog states, restore states,
  idempotency keys, failure reasons, and component ownership.
- Add a storage `SnapshotSource` capability separate from rebuild
  `AllBlocks()`.
- Prove with deterministic concurrency tests that a write cannot cross an
  in-progress cut and that the captured bytes match exactly one frontier.
- Initial implementation may stop writes during capture. Record this as a
  latency limitation, not as an application-consistency or performance claim.

Gate: contract review, race-enabled tests, and at least one rejected torn-scan
control.

### D2. Durable Local Snapshot Catalog

- Persist immutable snapshot archives and one catalog record per snapshot.
- Stream block records with per-record CRC and whole-archive digest; avoid a
  full-volume in-memory copy.
- Publish `ready` only after archive fsync, atomic rename, catalog fsync, and
  directory durability.
- Recover catalog state after process restart; remove only provable temporary
  or unreferenced files.
- Support idempotent get/list/delete and name/source conflict rejection.

Gate: create/reopen/list/delete, interrupted-create cleanup, corrupt archive
refusal, and no ready record for partial data.

### D3. Restore To A New Volume

- Restore an archive into a newly-created empty storage object.
- Verify geometry and archive integrity before publication.
- Give the restored volume its own write frontier and lifecycle identity.
- On failure, discard the unpublished target rather than leaving a partially
  usable volume.
- Prove source/snapshot/restore isolation and restart durability.

Gate: source writes before/after cut, restored bytes, independent post-restore
writes, corrupt input refusal, retry behavior, and clean failure residue.

### D4. Product Runtime And Control RPC

- Add a bounded blockvolume snapshot runtime owner that invokes the local cut
  only for the current primary and current authority lineage.
- Add master/control RPCs for create/get/list/delete snapshot and restore
  orchestration without moving data through operator-status.
- Carry terminal evidence and stable failure reasons; timeout or authority
  change fails closed.
- Preserve RF/ACK semantics and prove the cut came from a current primary.

Gate: current-primary success; stale/non-primary/authority-change refusal;
restart and retry idempotency; no cross-volume identity mix-up.

### D5. CSI Snapshot Contract

- Implement `CreateSnapshot`, `DeleteSnapshot`, and `ListSnapshots`.
- Implement `CreateVolume` from `volume_content_source.snapshot`.
- Map validation and runtime failures to required gRPC status codes.
- Advertise `CREATE_DELETE_SNAPSHOT` only when the runtime is configured.
- Keep the existing non-snapshot controller behavior unchanged when disabled.

Gate: CSI conformance-style unit tests plus a real CSI sidecar call path.

### D6. Kubernetes Packaging And User Path

- Package the external-snapshotter sidecar wiring and a
  `VolumeSnapshotClass`; document cluster snapshot-controller/CRD prerequisite
  versus chart-owned resources explicitly.
- Keep the feature disabled until matching controller and blockvolume images
  contain the complete path.
- Prove PVC -> VolumeSnapshot readyToUse -> restored PVC -> mounted read ->
  independent write/read.

Gate: real Kubernetes API, matching images, no manual snapshot CR stubs, and
cross-surface identity agreement.

### D7. Failure, Isolation, And Lifecycle Safety

- Concurrent source writes, process kill during create, restart during
  restore, duplicate RPCs, source delete, snapshot delete, and target delete.
- Multi-snapshot and multi-volume isolation.
- Refuse deletion while a restore holds a reference; release deterministically
  after terminal evidence.
- Verify no stale catalog, temporary archive, mounted path, session, CRD, PVC,
  PV, or process residue.

Gate: real dirty-failure scenarios, not replay-only summaries.

### D8. Backup Export And Import

- Export an already-ready immutable snapshot to an explicit file/object
  target; do not read a live volume directly.
- Persist backup identity, source snapshot ID, digest, size, state, and
  destination evidence.
- Import and restore only after integrity verification.
- Start with full backup. Incremental/changelog backup is a later capability.

Gate: export, destroy local restore target, import, restore, verify bytes, and
  reject truncated/corrupt backup.

### D9. Operations And Release Close

- Publish snapshot/backup/restore state and failure reasons through CRD status,
  Kubernetes Events, report, dashboard, explain, and support bundle.
- Negative-first: pending, stale, corrupt, or failed evidence never reports
  ready.
- Update README/Helm docs only after the live product path passes.
- Run matching published-image Day-1, snapshot, restore, delete, and cleanup
  gates before claiming the milestone release.

## Explicit Non-Goals

- No application quiesce orchestration or application-consistent claim.
- No group snapshot or cross-volume atomicity.
- No in-place revert.
- No cross-cluster disaster-recovery claim in the first milestone.
- No incremental backup or changed-block tracking.
- No snapshot-based clone performance claim.
- No COW optimization before the stop-the-world cut is correct and measured.

## Stop Rules

- Do not add CSI capability advertisement before D2-D5 are complete.
- Do not call `AllBlocks()` a snapshot or accept a scan that can interleave
  writes.
- Do not publish a snapshot before both data and metadata are durable.
- Do not expose a partially restored target after failure.
- Do not let operator-status execute snapshot, restore, or backup mutations.
- Do not optimize capture latency until crash, restart, corruption, and
  isolation gates pass.

## Exit Criteria

```text
atomic durable cut
-> persistent integrity-checked catalog
-> restore to new durable volume
-> current-primary runtime orchestration
-> CSI snapshot RPCs and capability
-> Kubernetes VolumeSnapshot + restored PVC
-> dirty failure and lifecycle safety
-> backup export/import
-> operations surfaces + matching-image release gate
```
