# Storage Layer Architecture Learning

Status: reference note.

Purpose: capture the architecture lessons from comparing V3 block storage with
Lakebase/Neon-style database storage, Ceph-style distributed block/object
storage, and SPDK-style high-performance storage stacks. This is not a current
execution plan and does not change `internal/docs/current-plan.md`.

## Main Takeaway

- Modern storage systems increasingly decouple compute/frontends from durable
  storage state.
- The decoupling point determines what optimizations are possible:
  - block systems see LBA, bytes, flushes, and path state,
  - database page systems see page identity, LSN, WAL delta, and checkpoint
    semantics,
  - table/object systems see files, manifests, snapshots, and metadata commits.
- V3 currently operates at the block layer. That is useful and general, but it
  cannot transparently optimize database-internal WAL semantics.

## Lakebase / Neon Lesson

- Lakebase/Neon-style architecture is not a faster iSCSI/NVMe block device.
- It changes the Postgres compute/storage boundary:
  - Postgres compute is stateless,
  - WAL is streamed directly to distributed safekeepers,
  - pageserver/storage reconstructs page images from materialized image plus
    WAL deltas,
  - storage understands Postgres page identity and LSN.
- That is why they can reduce Postgres full-page-write overhead:
  - Postgres full-page writes protect against torn local data pages,
  - in their architecture there is no local data directory page that compute
    depends on,
  - storage takes over page image generation and keeps delta replay bounded.
- A normal block device cannot safely remove Postgres WAL or full-page writes:
  - by the time writes pass through filesystem + block + iSCSI/NVMe, database
    page/LSN semantics are gone.

## What Applies To V3 Block

- We can use the same architectural idea at block granularity:
  - append minimal block deltas on the foreground write path,
  - materialize block images in the background,
  - bound read replay as `latest block image + limited delta chain`,
  - compact/checkpoint independently from frontend protocol handling.
- This can reduce V3's own backend write amplification and read amplification.
- It cannot transparently reduce an application's own WAL unless the application
  talks to a semantic storage protocol.

## Product Boundary

- General block product:
  - frontends: iSCSI, NVMe-oF, CSI,
  - input semantics: block writes and flushes,
  - value: generic PVC/block volume, HA, failover, multipath, snapshots,
    backend durability.
- Semantic storage product:
  - frontends: Postgres page/WAL protocol, table/manifest protocol, KV/log
    protocol,
  - input semantics: application-level objects such as page id, relation id,
    LSN, manifest commit, or transaction boundary,
  - value: application-specific write amplification reduction and stronger
    semantic optimization.
- The semantic product can reuse V3's storage core, authority, replication, and
  placement ideas, but it should not be treated as a small iSCSI/NVMe feature.

## Current V3 Gap

- `blockvolume` currently combines several responsibilities:
  - frontend target,
  - replica role projection,
  - local durable backend,
  - replication/catch-up logic,
  - protocol-visible readiness.
- This was pragmatic for getting iSCSI/NVMe/CSI green quickly.
- It becomes limiting for backend evolution:
  - storage compaction/image generation cannot scale independently,
  - frontend failover and storage ownership are easy to couple accidentally,
  - multiple frontend protocols sharing one storage engine is awkward,
  - DB-aware protocols would inherit blockvolume coupling.

## Industrial Patterns To Study

- Ceph-style distributed storage:
  - clients/frontends use control-plane maps,
  - storage daemons own data, replication, recovery, rebalancing,
  - block/file/object frontends sit above a common storage layer.
- SPDK-style high-performance storage:
  - user-space, polled-mode, lockless NVMe drivers,
  - hardware queue affinity and kernel-bypass data path,
  - useful later for NVMe performance, not a replacement for clean layering.
- Cloud block storage:
  - control plane manages volume placement and lifecycle,
  - data plane is storage fleet plus NIC/SSD/hardware acceleration,
  - hardware acceleration is valuable only after the software boundary is clean.

## Suggested Future Direction

- Short term:
  - keep current frontend plan intact,
  - document backend coupling points,
  - define code-level interfaces for storage engine vs frontend target.
- Medium term:
  - split responsibilities in code while still allowing one binary deployment:
    - `frontend`: iSCSI/NVMe protocol and path state,
    - `replica controller`: authority projection, fencing, peer set,
    - `storage engine`: WAL, block image, delta, compaction, snapshot.
- Long term:
  - optionally split process boundaries,
  - add SPDK/io_uring/RDMA-backed storage paths,
  - consider a DB-aware page/WAL frontend only after the block storage core is
    mature.

## Non-Claims

- This note does not claim V3 can reduce Postgres WAL or full-page writes today.
- This note does not change the active NVMe/iSCSI/CSI roadmap.
- This note does not require an immediate process split.
- This note argues for learning the industrial boundaries before turning backend
  work into a hardware-acceleration project.
