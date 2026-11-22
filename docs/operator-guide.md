# Operator guide (v1.0, standalone walstore-chunk volumes)

This page is the entry point for running Seaweed Block v1.0 outside Kubernetes. It says what is supported, where each
task is documented, and which limits apply. Every command on the linked pages is listed at the end with the test
that runs it.

## What v1.0 supports

- **Volumes:** walstore-chunk volumes, one volume per `blockvolume` process, with snapshots and moves through
  SeaweedFS.
- **Frontend:** NVMe/TCP, the standalone default (the example units). iSCSI is supported too, and is the Kubernetes CSI default.
- **Layout:** `blockmaster` and its authority store run **off every volume host**, on a node of their own (for
  example the SeaweedFS master or filer node), with durable storage. A volume host holds no authority state.
  - Why: a forced move after a host loss needs the authority record, the lease horizon and the lost range.
  - If those lived on the lost host, the volume could not be moved anywhere.

## Install

[Standalone deployment](standalone-deployment.md):
- the matching release files;
- the required setup that makes a volume known to `blockmaster`;
- the `blockmaster` and `blockvolume` settings;
- a first read-only check, and a write test for a scratch host.

**Host volume lock directory.** Each blockvolume locks `/run/sw-block/locks/<volume id>.lock`, so a host runs at most
one live blockvolume per volume ([one process per volume](releases/one-process-per-volume.md)).
- A process run as root creates the directory itself.
- The shipped unprivileged unit cannot create it, so install the tmpfiles.d entry
  [`deploy/systemd/sw-block.tmpfiles.conf`](../deploy/systemd/sw-block.tmpfiles.conf):

  ```bash
  sudo install -m 0644 deploy/systemd/sw-block.tmpfiles.conf /etc/tmpfiles.d/sw-block.conf
  sudo systemd-tmpfiles --create /etc/tmpfiles.d/sw-block.conf
  ```

- Without it, blockvolume exits at start with `host volume lock directory unavailable`. Install the entry, or chown
  the directory to the unit's user.

## Size a host

- **Geometry:** a volume's size is `--durable-blocks` x `--durable-blocksize` on `blockvolume`. It must equal
  `size_bytes` in the cluster spec (for example 65536 x 4096 = 268435456).
- **Memory:** plan for about 32-36 MiB per running volume. That was measured with 150-200 volumes on one host.
- **Measured throughput on one host** (16 cores, 23.7 GB RAM, one consumer NVMe drive, fsync on every volume):

  | Volumes | Aggregate IOPS, qd1 4k + fsync | Aggregate IOPS, 4k qd32 |
  |---|---|---|
  | 100 | about 14,000 | 47,000-49,000 |
  | 150 | 5,300-9,100 | 37,000-39,000 |
  | 200 | about 4,300 | about 32,000 |

  - Above about 100 volumes the drive's flush rate, not the software, sets the limit.
  - Treat these numbers as one host class, not a guarantee.
- **SeaweedFS space:** every cut and snapshot stores the chunks that changed since the previous one. Space comes
  back only by GC and then a SeaweedFS vacuum (below).

## Recovery point and sync policy

- **A stopped volume** keeps everything it acknowledged, on its host.
- **A lost host:** the volume comes back from its last cut, the last point published to SeaweedFS.
  - Default: no periodic capture. One scheduled snapshot a day (`--chunk-snapshot-schedule=daily:7`).
  - So with defaults a host loss can lose up to a day of acknowledged writes.
- **For a newer recovery point:** set `--chunk-capture-interval` (at least 5 minutes), or take a snapshot before
  risky work.
- **What does not publish:** a stop, a lease revocation and a lease expiry publish nothing. Only a cut, a snapshot or
  a seal (`ops chunk detach --plain`) does.

## Snapshots, restore and space

[Standalone deployment: snapshots, restore and space](standalone-deployment.md#snapshots-restore-and-space-walstore-chunk):
- scheduled and manual snapshots;
- deleting snapshots, and getting space back:
  - GC deletes the chunks after the grace (`--grace`, 1 hour by default), so a deleted snapshot's space is
    reclaimed about 2 hours after the delete;
  - then the asynchronous SeaweedFS vacuum compacts the volume files;
- `ops chunk verify`;
- restoring a snapshot into a new, independent volume.

## Moving a volume, and returning it

[Volume lifecycle](volume-lifecycle.md):
- the planned hand-over to another host or back to the same host;
- restarts inside and after the lease;
- the forced move when a host is lost, with the lost range;
- which of these procedures run end to end, and which do not.

## When a host is lost

1. Make sure the lost host stays down for this volume. The product does not power off an old holder.
   - If it comes back after the move, its blockvolume gets no assignment, does not serve, and exits with status 75.
   - The example unit starts it again about every 35 s (30 s of waiting, then `RestartSec=5s`), until its start
     limit (10 starts in 600 s), and then shows it as failed (`start-limit-hit`).
   - Stop and disable that unit on the old host. Run `systemctl reset-failed` only after you have dealt with its
     local root ([do not](volume-lifecycle.md#do-not)).
2. Run the forced move to another registered node: `--dry-run` first, then the move. See
   [the holder never comes back](volume-lifecycle.md#after-a-crash).
   - It is admitted after the barrier: at most 65 s after the holder stopped, with default flags.
3. Read the printed lost range: writes acknowledged after the last cut and before the move's time may be lost.
   - Read it again later with `sw-block ops workspace lost-range`.
4. Start `blockvolume` on the new node, then reconnect the guest.
5. Optionally restore an older snapshot into a new volume and verify it with `ops chunk verify --depth data`.

## Limits

- **One filer per chunk store.** It is a single point of availability for cuts, snapshots and moves.
- **No guest freeze.** Snapshots and seals are crash-consistent only.
- **Setup:** a `--topology`-only setup cannot move a volume at all. The cluster spec with a lifecycle store is
  required.
- **One node:** with one node in the cluster spec, a revoke not followed by an ACK cannot be undone.
- **Unauthenticated control endpoint.** `--chunk-control-endpoint` has no authentication; keep it on loopback.
- **Destructive SeaweedFS commands.** Never run SeaweedFS `volume.fsck` with its delete flags on a cluster holding
  chunk volumes ([why](standalone-deployment.md#seaweedfs-maintenance-commands-that-destroy-chunk-volumes)).
- **One live process per volume per host.** The host lock keeps two processes for one volume apart only on one host;
  two hosts are kept apart by the assignment and the lease. A container needs the host's `/run/sw-block/locks`
  ([one process per volume](releases/one-process-per-volume.md)).
- **Manual GC.** Automatic GC is off (`--chunk-maintenance-gc`): run GC passes yourself.

## Commands and their tests

Every command on the pages above, and the test that runs it.
- "binary": the test runs the real binaries (build tags `subprocess chunkstore_integration`).
- "CLI": the test drives the `sw-block` command with its flags.

| Command | Test | Kind |
|---|---|---|
| `blockmaster ... --cluster-spec ... --lifecycle-store ... --workspace-rpc ...` | `TestStandalonePlannedHandoffToTheSameHost` (`cmd/blockvolume`) | binary |
| `blockvolume ... --durable-impl walstore-chunk ... --workspace-lease ...` | same | binary |
| `sw-block ops workspace show`, `revoke --sealed-gen`, `move --clean` | same | binary |
| `sw-block ops chunk detach --plain`, `chunk show`, `chunk cut` | same; `TestProcedureForcedMoveWithTheOldHostReturning` | binary |
| `sw-block ops workspace force-move [--dry-run]`, `lost-range` | `TestProcedureForcedMoveWithTheOldHostReturning`, `TestProcedureRevokeWithoutACKThenForcedMove` | binary |
| `sw-block ops volume-unheld --path` | `cmd/sw-block/volume_unheld_test.go` | CLI |
| `sw-block ops chunk snapshot`, `snapshots`, `snapshot-delete` | `cmd/sw-block/chunk_test.go` | CLI |
| `sw-block ops chunk gc --chunk-filer --master --apply [--grace]` | `cmd/sw-block/chunk_gc_test.go`; `chunk_gc_integration_test.go` (real SeaweedFS) | CLI |
| `sw-block ops chunk verify --all-snapshots --depth data` | `cmd/sw-block/chunk_verify_test.go` | CLI |
| `sw-block ops chunk restore --source --snapshot --target`, `--volume ... [--status]` | `cmd/sw-block/chunk_restore_test.go` | CLI |
| `curl STATUS/status?volume=v1` | the status checks in the binary tests above | binary |
| `nvme connect`, `nvme list`, `nvme disconnect` | not run here: the binary tests use an NVMe/TCP host inside the test | not run |
| `blockvolume` under systemd: exit 75 after an expired start, restart, re-grant | `TestSystemdExpiredStartupExits75ThenRegrantServes` (`cmd/blockvolume`, a real systemd manager, tag `systemd_integration`) | binary |
| The shipped units' start limits (`StartLimitIntervalSec=600`, `StartLimitBurst=10`, `RestartPreventExitStatus=78`) | `TestBlockvolumeUnitsBoundStartupRetries` (`internal/testops`, reads the unit files) | unit files |
| `systemctl enable`, `stop`, `disable`, `reset-failed`, `systemd-tmpfiles --create` as written on these pages | not run: the tests above start transient units, not the installed ones | not run |
