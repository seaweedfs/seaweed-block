# Agent workspace disks: measured results

**Use this when:** you need the full workspace measurements behind the README
summary: the workload comparison, `fsync` behavior, snapshot and fork cost, cut
upload time, and the two-host lifecycle run, with how each was measured.

**Source of truth:** lab runs on Firecracker microVMs and a host-side proxy, as
described below.

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
- The cost in these runs was `fsync` latency, about 7 ms against 3 ms local,
  over the NVMe/TCP loopback frontend. These are the original runs, kept as
  history; the next section measures `fsync` after the group-commit fix.
- The same comparison on an Azure `D16s_v5` nested-virtualization host gives
  the same picture.

**`fsync` after the group-commit fix** ([#173](https://github.com/seaweedfs/seaweed-block/pull/173);
one bare-metal lab host; Firecracker guest; fio 4 KiB random write, one request
at a time, `fsync` after every write, 60 s; median of 3 boots per setup):

| Setup | `fsync` p50 | `fsync` p99 | Write IOPS | 2,000 × create + write + `fsync` |
|---|---|---|---|---|
| ublk, `walstore-chunk` | 3.23 ms | 9.11 ms | 283 | 8.48 s |
| ublk, `smartwal` (experimental) | 3.26 ms | 5.21 ms | 299 | 8.29 s |
| NVMe/TCP loopback, `walstore-chunk` | 3.59 ms | 9.63 ms | 258 | 9.27 s |
| NVMe/TCP loopback, `smartwal` (experimental) | 3.59 ms | 7.57 ms | 263 | 8.68 s |
| Local file-backed disk | 2.74 ms | 4.42 ms | 357 | 6.11 s |

- Before the fix, on the same pack and host, guest `fsync` p50 was 5.54 ms
  (ublk) and 5.87 ms (NVMe/TCP) against 2.74 ms local.
- The fix removed an idle wait: a lone `Sync` used to sit out the group-commit
  collection delay with nobody to batch with. On `walstore-chunk` the
  group-commit wait fell to about 8 µs (histogram p50 bucket), and one backend
  `Sync` now takes about 1.2 ms. Each guest `fsync` causes two backend syncs.
- Every setup passed a gate that every guest `fsync` reaches the backend sync
  (at least 2,000 flushes and 2,000 backend syncs per run); an unsafe-cache
  control showed zero.
- The other workloads (`npm ci`, `git clone`, untar, `find` + `stat`) stayed at
  local-disk speed.
- One time window, 3 runs per setup: descriptive, not a performance gate.

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
[agent workspace design](design/agent-workspace-design.md) describes how it works.
