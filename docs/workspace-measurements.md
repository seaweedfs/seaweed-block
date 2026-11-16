# Agent workspace disks: measured results

**Use this when:** you need the full workspace measurements behind the README
summary: the workload comparison, `fsync` behavior, snapshot and fork cost, cut
upload time, and the two-host lifecycle run, with how each was measured.

**Source of truth:** lab runs on Firecracker microVMs and a host-side proxy, as
described below. This text moved from the top-level README; only the `fsync`
bullet was updated to say which frontend the runs used.

The workload and lifecycle numbers come from lab runs with Firecracker
microVMs (4 vCPU, 4 GiB, guest kernel 6.1). The snapshot, fork and cut numbers
come from a host-side proxy, described with their table. Each number is the
median of 3 runs. They are measurements, not SLOs.

**Workspace workloads, Seaweed Block disk vs a local file-backed disk** (one
bare-metal host; NVMe/TCP loopback; seconds):

| Workload | Seaweed Block | Local disk |
|---|---|---|
| `npm ci` (617 packages, offline) | 2.14 | 2.14 |
| `git clone` (Go 1.23 repository, local) | 7.10 | 6.99 |
| Untar 5,000 files | 0.31 | 0.31 |
| Untar a 100,000-file tree | 1.10 | 1.06 |
| `find` + `stat` over 100,000 files (cold) | 0.32 | 0.33 |
| 2,000 × create + write + `fsync` | 14.33 | 6.09 |

- Metadata-heavy agent workloads run at local-disk speed, because the guest
  kernel absorbs metadata and the block device only sees batched writes.
- Every guest `fsync` was verified to reach the backend sync: about 2 NVMe
  flushes per `fsync`, and zero in an unsafe-cache control.
- The cost in these runs is `fsync` latency, about 7 ms against 3 ms local.
  The runs used the NVMe/TCP loopback frontend. The ublk frontend
  (`--ublk-device`) and the per-stage `fsync` histograms (`/status/durable`,
  see [fsync latency](fsync-latency.md)) are on `main` now; reducing the
  latency, including group-commit tuning, is in progress. The numbers here
  record those runs, not a measurement of current `main`.
- The same comparison on an Azure `D16s_v5` nested-virtualization host gives
  the same picture.

**Snapshot and fork cost** (a 10 GiB volume written as 2,560 chunks of 4 MiB;
median of 3). This is a host-side proxy, not a guest or network run: chunks are
local files on a native Linux host, and filer metadata calls use a
deterministic model, so network latency, guest `fsync` and the block frontend
are excluded.

| | Flat manifest | Two-level manifest (current) |
|---|---|---|
| HEAD size | 261,531 B | 2,172 B |
| Snapshot | 5.79 ms | 0.18 ms |
| Fork | 10.67 ms | 0.29 ms |

In the same proxy, a cut that writes 64 dirty chunks (256 MiB) took a median
624 ms with one upload at a time and 434 ms with 8 parallel uploads.

**Workspace lifecycle on two hosts** (Firecracker VMs, SeaweedFS chunk backend):

- 35 of 35 checks passed:
  - create;
  - write;
  - snapshot S1 and S2;
  - fork from S1, which is independent of its parent;
  - a clean move to the second host;
  - a stale-holder replay, which is refused;
  - delete the fork and S1, then GC.
- GC reclaimed 38 of 38 candidate chunks, and every reclaimed chunk returns 404.
- Everything still referenced read back byte-identical, including the moved
  workspace's digest before and after GC.

These results cover the chunk path (`walstore-chunk`), which is a proof of
concept: not yet a default, a release claim or a production path. The
[design reference](design/block-design.md), Part 2, describes how it works.
