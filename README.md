# Seaweed Block

<p align="center">
  <img src="docs/assets/seaweed-block-agent-workspace.svg" alt="Seaweed Block agent workspace disks: a local-speed disk per agent microVM, incremental snapshots to SeaweedFS, fork and resume on any host" width="100%">
</p>

Seaweed Block is a block storage service written in Go. It has two uses:

1. **Agent workspace disks (new).** Each agent sandbox or microVM gets a
   local-speed block device. Snapshots, forks and host moves are cheap because
   the data is stored as content chunks in [SeaweedFS](https://github.com/seaweedfs/seaweedfs).
2. **Kubernetes block volumes (alpha).** PVC-backed volumes through a CSI driver,
   with RF=3 replication, iSCSI or NVMe/TCP, and read-only operations evidence.
   See [Kubernetes block storage](#kubernetes-block-storage-alpha).

## Agent workspace disks

An agent sandbox needs a disk that works like a laptop disk (`npm ci`,
`git clone`, compilers, SQLite), and it also needs snapshot, fork, rollback and
restore-on-another-host. Seaweed Block gives each workspace:

- **A local-speed disk.** The volume runs next to the VM. The guest sees a
  plain virtio block device through NVMe/TCP loopback or a ublk device. Writes
  and `fsync` complete against a local write-ahead log, and nothing waits on
  the network.
- **Durable, incremental snapshots in SeaweedFS.** A cut uploads only the 4 MiB
  chunks changed since the last cut. Chunks are immutable. The volume's state
  is a small HEAD manifest, a root of about 2 KB that points at segment
  manifests, and every publish is a fenced, conditional update of that root.
- **Cheap snapshot and fork.** A snapshot or fork copies only the HEAD root;
  chunks are shared. Unreferenced chunks are reclaimed by a two-phase, graced
  GC.
- **Move to any host.** Detach on one host. Another host takes over with a
  higher epoch, starts from the HEAD alone and fetches chunks lazily on first
  read. A stale holder cannot publish after takeover, because every HEAD
  update is conditional on `{gen, epoch}`.
- **Single replica by design.** Workspaces run with one local replica, and
  durability comes from SeaweedFS snapshots, so there is no synchronous
  replication on the write path. Losing a host loses at most the writes since
  the last cut. Cut on semantic points: after an agent step, before a risky
  command, on detach.

```text
agent microVM ──virtio-blk──> blockvolume (local WAL + extent)
                                   │  cut: changed chunks only
                                   ▼
                          SeaweedFS (immutable chunks + HEAD manifest)
                                   │  snapshot / fork = copy HEAD root
                                   ▼
                  another host: take over HEAD (epoch+1), lazy-load chunks
```

### Measured

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
- The cost today is `fsync` latency, about 7 ms against 3 ms local. Work in
  progress to close that gap: the ublk frontend, group commit, and a per-stage
  `fsync` latency breakdown (`/status` histograms).
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

### Try the chunk lifecycle

The `sw-block ops chunk` commands drive the workspace lifecycle against a
running blockmaster and blockvolume (`--durable-impl=walstore-chunk`,
`--chunk-filer <filer>`):

| Command | What it does |
|---|---|
| `sw-block ops chunk cut` | Publish the changed chunks and advance HEAD |
| `sw-block ops chunk snapshot` / `snapshots` | Name a snapshot / list snapshots |
| `sw-block ops chunk fork` | Create a new volume from a snapshot (root copy only) |
| `sw-block ops chunk detach` | Final cut and release, ready to resume elsewhere |
| `sw-block ops chunk volume-delete` | Delete a volume; refused while it is attached or has snapshots |
| `sw-block ops chunk gc` | Preview or apply two-phase GC of unreferenced chunks |

The chunk path (`walstore-chunk`) is a proof of concept. It is not yet a
default, a release claim or a production path.

### Correctness work

- **Formal models.** The chunk lifecycle, host takeover, and WAL crash
  recovery protocols are model-checked with TLA+ and TLC. Counterexamples found
  this way became fixes and regression tests, for example the persisted-tail
  WAL reuse fix and last-writer-wins recovery replay.
- **Fail closed.** Recovery reports typed reasons such as `wal_corrupt` and
  `io_error` instead of serving uncertain data.

### Not claimed yet

- Production readiness, SLOs, or a published performance claim.
- `fsync` parity with local disk. That is work in progress, as described above.
- ublk at high density. Unprivileged per-volume servers need a privileged
  control broker, which is not done.
- Crash-restart recovery of a ublk server. A crashed server fails closed.
- Multi-replica workspaces combined with chunk snapshots.
- The `smartwal` storage engine. It is experimental: in-place overwrites
  have no rollback on a torn write, blocks lose their per-block CRC once
  their ring records recycle, the handling of a corrupted committed record
  is still open, and it has no chunk-snapshot support. New volumes use
  `walstore`; `blockvolume --durable-impl` defaults to auto, which creates
  new volumes as `walstore` and opens an existing volume with its on-disk
  format. A `walstore-chunk` volume is refused under auto and needs
  `--durable-impl=walstore-chunk` with its remote configuration.

## Kubernetes block storage (alpha)

The Kubernetes path is focused on one user-visible loop:

```text
install on Kubernetes
-> create a PVC-backed block volume
-> mount it in an app pod
-> write/read data
-> inspect status and support evidence
-> clean up
```

This is an **alpha** product path for supported lab clusters, not production.

<p align="center">
  <img src="docs/assets/seaweed-block-hero.svg" alt="Seaweed Block alpha architecture: Kubernetes PVC to CSI, blockmaster, blockvolume, iSCSI, and WAL-backed recovery" width="100%">
</p>

### Kubernetes feature & status

| Feature | Status | Feature | Status |
|---|---|---|---|
| Kubernetes PVC provisioning | Available | Helm install path | Available |
| First PVC writer/reader verification | Available | Read-only report/dashboard | Available |
| Support-bundle replay | Available | Negative-first status reasons | Available |
| Multi-volume RF=3 lab path | Gated | CSI reattach recovery | Gated |
| iSCSI ALUA/dm-multipath mounted failover | Gated | Restart persistence with hostPath | Gated |
| Actionable read-only CRD status + Events | Available | Bounded SwBlockVolume finalizer lifecycle | Beta candidate |
| Returned-replica ACK eligibility executor | Beta candidate | Returned-replica failback runtime | Source-gated |
| Returned-replica rebuild traffic | Planned | Frontend publication after failback | Planned |
| NVMe/TCP CSI multipath + backend write batching | Source-gated | NVMe/RDMA Kubernetes CSI publish/attach | Source-gated |
| WAL multi-block record opt-in | Source-gated | WAL format default change | Not claimed |
| Backup/snapshot/restore | Planned | NVMe/RDMA failover or performance SLO | Not claimed |
| Production SLO/performance claims | Not claimed | Hosted production UI | Not claimed |

### What You Can Do Today

- Install Seaweed Block through Helm on a supported Kubernetes/k3s lab.
- Create and mount PVC-backed block volumes through normal Kubernetes PVCs.
- Verify writer/reader persistence with ordinary app pods.
- Run multiple RF=3 volumes in the gated lab path.
- Validate gated recovery paths:
  - CSI reattach with pod recreate,
  - iSCSI ALUA + dm-multipath transparent mounted failover on the proven
    Stage-2 path,
  - interleaved multi-volume failover isolation.
- Inspect cluster, volume, replica, primary, frontend, timeline, and reason
  evidence through read-only CLI/report/dashboard surfaces.
- Publish the same read-only status into Kubernetes-native `SwBlockCluster` /
  `SwBlockVolume` `.status` and Events on the gated operator-status path.
- Inspect node readiness, support evidence refs, cleanup-required status, and
  safe read-only/scripted next-step hints through that same status model.
- Use the beta-candidate lifecycle-owner path to protect `SwBlockVolume` CRs
  with a Seaweed Block finalizer and release it only after clean externally
  supplied cleanup evidence.
- Inspect install drift status for current versus desired chart/app/image
  identity. This is visibility only, not upgrade execution.
- In the gated returned-replica path, inspect a bounded authority-executor
  result that records ACK eligibility on `SwBlockReplicaEligibility.status`
  after live evidence proves the old primary remains fenced, the current primary
  is unchanged, and the required durable frontier is covered.
- From source, run the opt-in returned-replica failback runtime gates. This path
  can move authority through blockmaster only when a `SwBlockReplicaFailback`
  target, explicit execution policy, expected-current evidence, and terminal
  evidence are all present. It is not enabled by default and is not yet a
  published release claim.
- From source, run the supported-lab NVMe/TCP gates: ANA/direct-host baseline,
  CSI protocol selection, Kubernetes CSI multipath attach for one NQN/NSID with
  multiple NVMe frontend paths, one-path-loss status honesty, mounted pod
  write/read after one observed path loss, mounted pod write/read after the
  removed path is restored, multi-volume mounted path isolation, bounded
  multi-volume path churn, repeated stage/unstage residue checks, a bounded
  writer/reader soak, a baseline over the configured 100GbE TCP frontend
  address, a source-gated CSI-node reconnect owner contract, and a live
  host-path reconnect gate for scoped `nvme disconnect -d` path loss, plus a
  live desired path-set replacement gate where CSI-node connects the new
  desired NVMe path and prunes the stale old host path without remounting the
  pod, and a durable backend full-block write-batching gate with
  product-owned `/status/durable` counters. The source-gated WAL multi-block
  record opt-in has mounted profile and restart/recovery compatibility evidence,
  plus mounted durable-status `HeadLSN` confirmation, but remains default-off
  and requires a future matching-image release smoke before it becomes a public
  image claim. The opt-in NVMe/RDMA path now carries an explicit transport from
  StorageClass through blockmaster publication and CSI NodeStage, mounts a
  dynamic PVC on a second RoCE host, and verifies writer/reader data through a
  standard Linux `nvme connect -t rdma` controller. It remains source-gated.
  These are supported-lab source-gated claims, not broad NVMe compatibility,
  production HA, or performance/SLO claims.
- Replay support bundles offline.

These are narrow alpha claims tied to documented gates. See
[release notes](docs/releases/README.md) for exact run evidence.

### What You Should Not Expect Yet

- Production readiness or production SLOs.
- A production-grade operator or broad mutating admin workflow. The current
  lifecycle-owner owns only the Seaweed Block `SwBlockVolume` protection
  finalizer.
- Automatic cleanup execution, host repair, or PVC/PV/workload deletion.
  Delete-safety uses externally supplied cleanup evidence.
- Backup, snapshot, or restore.
- Returned-replica rebuild traffic, frontend publication after failback, or
  automatic deployed failback. The failback runtime path is explicit, opt-in,
  and source-gated until a release smoke validates it on published images.
- Transparent Kubernetes node-loss failover without pod recreate.
- Broad NVMe/RoCE compatibility, performance/SLO, or transparent failover
  parity beyond the documented supported-lab gates. The CSI-node reconnect
  owner, host-path reconnect gate, desired path-set replacement gate, and
  stale path pruning gate exist only as source-gated NVMe/TCP evidence. The
  NVMe/RDMA path has standalone and Kubernetes single-path publish/attach
  evidence, but no RDMA multipath, reconnect/failover, performance, or SLO
  claim.
- Broad distro/kernel/initiator compatibility.
- Upgrade or rollback execution. The status layer can report install drift, but
  it does not run Helm or kubectl mutations.

### Five-Minute Quick Start

From the repository root:

```bash
go build -o sw-block ./cmd/sw-block
export PATH="$PWD:$PATH"

sw-block ops generate-helm-values \
  --out values.day1.yaml \
  --image ghcr.io/seaweedfs/seaweed-block:sha-dc2972d0059b \
  --csi-image ghcr.io/seaweedfs/seaweed-block-csi:sha-dc2972d0059b

helm install sw-block charts/seaweed-block \
  --namespace kube-system \
  --create-namespace \
  -f values.day1.yaml \
  --wait \
  --timeout 10m

SW_BLOCK_INSTALL_MODE=helm \
SW_BLOCK_HELM_RELEASE=sw-block \
SW_BLOCK_HELM_NAMESPACE=kube-system \
SW_BLOCK_HELM_VALUES_FILE=values.day1.yaml \
  bash scripts/run-basic-app-example.sh "$PWD"
```

Expected summary in the latest `/tmp/sw-block-basic-app-*/first-volume-summary.txt`:

```text
first_volume_status=ok
writer_verified=true
reader_verified=true
inventory_status=ok
status_report=status/report/index.html
cleanup_status=ok
```

Current validated published quickstart image tag: `sha-dc2972d0059b`.
This tag covers the v0.4 read-only/status foundation path. The v0.5
bounded lifecycle-owner path requires matching `sw-block` and `sw-block-csi`
images published from the Phase 44 release commit; do not use the older
quickstart tag to validate lifecycle-owner behavior.
The v0.6 returned-replica ACK eligibility executor path likewise requires
matching images published from the Phase 54 release commit and must be validated
with the release smoke before it is marked shipped.
The returned-replica failback runtime added after v0.6 is source-gated and
requires a future release smoke before it becomes a public image claim. The
NVMe/TCP supported-lab path is also source-gated until matching `seaweed-block`
and `seaweed-block-csi` images are published and pass the NVMe release smoke.

Mutable `:alpha` is a smoke/demo tag only; it can drift from the source tree.

### Operations

Port-forward blockmaster when reading live cluster state:

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
```

Common read-only commands:

| Command | Use |
|---|---|
| `sw-block ops cluster --master-api 127.0.0.1:9333 -o json` | Cluster snapshot. |
| `sw-block ops volumes --master-api 127.0.0.1:9333` | List volumes, status, primary, node, frontend. |
| `sw-block ops describe volume <id> --namespace default --master 127.0.0.1:9333` | Describe one volume. |
| `sw-block ops timeline volume --from-bundle <dir> <id> -o jsonl` | Read a saved event timeline. |
| `sw-block ops explain volume --from-bundle <dir> <id>` | Explain ready, blocked, stale, or recovering state. |
| `sw-block ops report --master-api 127.0.0.1:9333 --out <dir>` | Generate static report artifacts. |
| `sw-block ops dashboard --master-api 127.0.0.1:9333 --listen 127.0.0.1:9334` | Serve local read-only dashboard. |
| `sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out <dir>` | Collect replica-level inventory. |

Reports and dashboard expose `operator-snapshot.json`, a read-only status
projection. The gated operator-status controller can publish that model into
Kubernetes CRD `.status` and Events, including node readiness, cleanup/delete
safety, and install-drift visibility. It has no mutating admin actions.

Support-bundle replay:

```bash
bash scripts/collect-helm-support-bundle.sh "$PWD"
sw-block ops report --from-bundle <bundle-or-artifact-dir> --out /tmp/sw-block-report
sw-block ops explain volume <volume-id> --from-bundle <bundle-or-artifact-dir>
sw-block ops dashboard --from-bundle <bundle-or-artifact-dir> --listen 127.0.0.1:9334
```

The status surface is negative-first. If evidence is missing or stale, the
system reports `Ready=Unknown` / `EvidenceStale=True` rather than claiming a
false ready state.

### Cleanup

```bash
helm uninstall sw-block --namespace kube-system
bash scripts/uninstall-k8s-alpha.sh "$PWD"
bash scripts/verify-helm-cleanup.sh
```

The cleanup verifier checks Kubernetes resources, iSCSI sessions, iSCSI node
DB records, dm-multipath maps, `dmsetup` devices, product processes, and
hostPath residue.

## Documentation

- [Kubernetes quickstart](docs/quickstart-kubernetes.md) - first install and PVC.
- [User capabilities](docs/user-capabilities.md) - detailed current behavior.
- [Release notes](docs/releases/README.md) - exact validated claims and evidence.
- [NVMe/TCP supported-lab claim](docs/releases/nvme-tcp-supported-lab.md) -
  source-gated NVMe evidence, release-smoke boundary, and non-claims.
- [Roadmap](docs/roadmap.md) - public planning summary.
- [Architecture](docs/architecture.md), [developer architecture](docs/developer-architecture.md),
  and [runtime state machines](docs/runtime-state-machines.md) - engineering
  references, not release-claim sources.

## Development

Run release-relevant scoped tests:

```bash
go test ./cmd/sw-block ./core/ops ./core/csi ./core/launcher ./core/host/master -count=1
```

`go test ./...` is the full repository sweep and may include unrelated
in-progress packages outside the current release gate.

Script activation remains available for development/local-image fallback:

```bash
bash scripts/activate-k8s-alpha.sh "$PWD"
bash scripts/run-basic-app-example.sh "$PWD"
```

## License

Apache-2.0
