# Chunk garbage collection (W5, library slice)

`NewCollector(catalog, blobDeleter, config).Run(ctx)` traces every volume returned by the catalog, every HEAD, every snapshot (including deleting snapshots), and every live upload/copy intent. Use a deadline-bound context. Supply `IssuedEpoch` from the authoritative blockmaster reader, not lease expiry or a host-local projection. There is no automatic scheduler or destructive CLI in this slice.

A capture intent can expire only when its age strictly exceeds IntentTTL, the blockmaster has issued a higher epoch, and the actual remote HEAD has advanced to a higher epoch no greater than the issued one. Unknown authority retains it. Snapshot/fork copy intents are retained until the operation settles: their exclusive creates are not fenced by the remote HEAD epoch. This is intentionally conservative pending a shared publication fence for those operations.

An immutable binding becomes a candidate only when its creation time is strictly older than Grace and no root names it. Reclamation requires another successful mark at least Grace after its first observation with the same binding bytes. A rooted candidate is forgotten; failed marks and collector restart discard coverage. Grace must exceed the catalog operation deadline plus the measured mark duration. Backward clock movement refuses the pass.

Before physical deletion the binding receives a conditional `swb.gc.deleted=1` tombstone; after every resolved replica deletion succeeds it becomes `done`. Bindings are never removed or rebound. Failed/uncertain blob deletion returns an error and retains the fid for a conservative retry after fresh marks. The SeaweedFS adapter deletes the exact fid, accepts only HTTP 200/204/404, and propagates transport/other HTTP failures. Authenticated deployments must supply a BlobDeleter with their write authorization; this patch does not add an auth provisioning path.

Unbound needles (upload succeeded but key binding never persisted) are RETAINED. The existing interfaces cannot enumerate/count them, so `UnknownUnbound=true` reports that limitation instead of a fabricated zero. No sweep is attempted.

## Scope and follow-ups

This first Draft implements the GC core and the real Capture/intent/HEAD caller regression. Retention (newest N plus logarithmic age buckets), removal of deleting snapshots, workspace tombstoning, CLI scheduling and a concrete blockmaster client binding remain follow-up W5 slices. Deleting snapshots continue to root their data until the catalog actually removes them. This is safe retention, not complete workspace lifecycle acceptance.

## Lock order

The new Collector mutex serializes its own passes and candidate state; it never enters WAL, publication, shard or chunk-layer locks. Remote calls execute only while holding that collector-local mutex. Producer Capture already uploads after releasing its capture/publication barrier, so GC adds no edge to the storage lock graph. The epoch-reader and BlobDeleter must not re-enter this Collector. Multiple collectors may repeat idempotent physical deletes, but each must independently satisfy the two-mark grace rule.

## Evidence boundary

The assertion RED is an initial GC candidate that already implemented HEAD/snapshot tracing and two marks but omitted intent roots; the base has no GC API to test. It is NOT claimed as a GC bug reproduced on unmodified main. The same test holds the shipping Capture before CAS after real immutable uploads, runs two GC marks, resumes CAS, and reads exact bytes through ChunkRemote. Filer/needle services are deterministic adapters; no SeaweedFS process, hardware, lab or cloud acceptance is claimed.
