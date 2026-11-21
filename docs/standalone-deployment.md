# Standalone deployment (systemd)

**Use this when:** you run `blockmaster` and `blockvolume` directly on a Linux
host, outside Kubernetes, for example as a local disk for VMs or for the agent
workspace proof of concept.

**Source of truth:** the flag definitions in `cmd/blockmaster/main.go`
(`parseFlags`), `cmd/blockvolume/main.go` (`parseFlags`) and
`cmd/blockvolume/chunk_remote.go` (`validateChunkFlags`). If this page and
`--help` disagree, `--help` wins.

For a first, read-only try without services, see
[first volume (read-only check)](#first-volume-read-only-check). For a write
test, see [write test (scratch host only)](#write-test-scratch-host-only).

## Get the matching files

The unit and settings files must come from the same commit as your
binaries. Always pin a full commit id (or a release tag once one exists),
never a branch name such as `main`. Run every command on this page from the
root of that checkout:

```bash
git clone https://github.com/seaweedfs/seaweed-block && cd seaweed-block
git checkout <commit>   # the commit your binaries come from
```

`<commit>` is the commit in your image tag (`sha-<commit>`) or the one you
built. Without git, download the same paths from
`https://raw.githubusercontent.com/seaweedfs/seaweed-block/<full-commit-id>/`
into a directory with the same layout (`deploy/config/`, `deploy/systemd/`)
and run from there, with the binaries in that directory too. If your image's
commit has no `deploy/` directory (images published before these files were
added), do not mix revisions: build the binaries from your checkout instead
(Install, option 2).

## Files

| File | Install as | Purpose |
|---|---|---|
| [`deploy/config/blockmaster.env`](../deploy/config/blockmaster.env) | `/etc/sw-block/blockmaster.env` | blockmaster settings, commented |
| [`deploy/config/blockvolume.env`](../deploy/config/blockvolume.env) | `/etc/sw-block/blockvolume.env` | blockvolume settings, commented |
| [`deploy/config/blockvolume-ublk.env`](../deploy/config/blockvolume-ublk.env) | `/etc/sw-block/blockvolume-ublk.env` | Overrides for the root ublk variant |
| [`deploy/config/topology.yaml`](../deploy/config/topology.yaml) | `/etc/sw-block/topology.yaml` | One volume, one replica slot |
| [`deploy/systemd/sw-blockmaster.service`](../deploy/systemd/sw-blockmaster.service) | `/etc/systemd/system/` | blockmaster, as `sw-block`; state in `/var/lib/sw-block-master` |
| [`deploy/systemd/sw-blockvolume.service`](../deploy/systemd/sw-blockvolume.service) | `/etc/systemd/system/` | One blockvolume over NVMe/TCP or iSCSI, as `sw-block`; state in `/var/lib/sw-block-volume` |
| [`deploy/systemd/sw-blockvolume-ublk.service`](../deploy/systemd/sw-blockvolume-ublk.service) | `/etc/systemd/system/` | The same volume over ublk, as root; state in `/var/lib/sw-block-volume-ublk` |

Each unit has its own `StateDirectory=`, so the root ublk unit can never take
ownership of blockmaster's or the unprivileged volume's files. Run either
`sw-blockvolume` or `sw-blockvolume-ublk`, not both (they conflict).

> **Not a data migration.** The two volume units use different durable roots
> (`/var/lib/sw-block-volume/volumes` and `/var/lib/sw-block-volume-ublk/volumes`).
> Switching from one unit to the other does not move or reopen the existing
> volume: the other unit creates a new, empty volume. Keep using the unit
> that created a volume. Do not point both units at one directory or chown one
> unit's state for the other.

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin sw-block
sudo install -m 0755 blockmaster blockvolume sw-block /usr/local/bin/
sudo install -d -m 0750 -g sw-block /etc/sw-block
sudo install -m 0640 -g sw-block deploy/config/*.env deploy/config/topology.yaml /etc/sw-block/
sudo install -m 0644 deploy/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sw-blockmaster sw-blockvolume
journalctl -u sw-blockvolume -f    # wait for "nvme-listening"
```

The binaries take command-line flags. The `SWB_*` variables are read only by
the example units, which turn them into flags. blockvolume itself reads only
`SW_BLOCK_ISCSI_CHAP_USERNAME` and `SW_BLOCK_ISCSI_CHAP_SECRET`.

## blockmaster settings

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| `SWB_MASTER_AUTHORITY_STORE` | `--authority-store` | required | Durable authority store: assignments, epochs, leases |
| `SWB_MASTER_LISTEN` | `--listen` | `127.0.0.1:0` | gRPC address; unauthenticated, keep it on loopback or a trusted network |
| `SWB_MASTER_TOPOLOGY` | `--topology` | empty (no assignments minted) | Accepted-topology YAML: volumes and replica slots |
| `SWB_MASTER_EXPECTED_SLOTS` | `--expected-slots-per-volume` | `3` | Replica slots per volume; `1` for a single replica |
| `SWB_MASTER_FRESHNESS_WINDOW` | `--freshness-window` | `30s` | How long a heartbeat stays fresh |
| `SWB_MASTER_PENDING_GRACE` | `--pending-grace` | `1s` | Startup grace before a missing observation is unsupported |
| `SWB_MASTER_WORKSPACE_RPC` | `--workspace-rpc` | `false` | Workspace lease, revocation and move RPCs |
| `SWB_MASTER_WORKSPACE_HEAD_BASE_URL` | `--workspace-head-base-url` | empty | Filer HTTP directory of HEAD files; required with `--workspace-rpc` |
| `SWB_MASTER_WORKSPACE_MAX_CLOCK_SKEW` | `--workspace-max-clock-skew` | `20s` | Clock-skew bound for workspace leases and forced moves |
| `SWB_MASTER_EXTRA_ARGS` | any | empty | Other flags; the Kubernetes `--launcher-*` and `--lifecycle-*` flags are set by the Helm chart |

## blockvolume settings

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| `SWB_MASTER_ADDR` | `--master` | required | blockmaster gRPC address |
| `SWB_SERVER_ID`, `SWB_VOLUME_ID`, `SWB_REPLICA_ID` | `--server-id`, `--volume-id`, `--replica-id` | required | Must match a topology slot |
| `SWB_DATA_ADDR`, `SWB_CTRL_ADDR` | `--data-addr`, `--ctrl-addr` | required | Data-path and control-path addresses |
| `SWB_STATUS_ADDR` | `--status-addr` | empty (off) | Status HTTP endpoint; loopback only, unauthenticated |
| `SWB_HEARTBEAT_INTERVAL` | `--heartbeat-interval` | `2s` | Heartbeat interval |
| `SWB_RECOVER_TIMEOUT` | `--recover-timeout` | `30s` | Time to become promotion-ready before reporting `recovery_timeout` |
| `SWB_DURABLE_ROOT` | `--durable-root` | empty (in memory, not durable) | Storage directory; writable by the service user |
| `SWB_DURABLE_IMPL` | `--durable-impl` | empty (auto) | Auto creates new volumes as `walstore`, opens existing ones with their on-disk format, and refuses a `walstore-chunk` root. Explicit: `walstore`, `walstore-chunk` (workspace, proof of concept), `smartwal` (experimental). The examples set `walstore`. |
| `SWB_DURABLE_BLOCKS` | `--durable-blocks` | `2048` | Blocks on first create |
| `SWB_DURABLE_BLOCKSIZE` | `--durable-blocksize` | `4096` | Block size on first create |
| `SWB_NVME_LISTEN` | `--nvme-listen` | empty (off) | NVMe/TCP target; loopback only, no authentication |
| `SWB_NVME_SUBSYSNQN` | `--nvme-subsysnqn` | empty | Subsystem NQN; required with `--nvme-listen` |
| `SWB_NVME_NS` | `--nvme-ns` | `1` | Namespace id |
| `SWB_NVME_MAX_H2C_DATA_LENGTH` | `--nvme-max-h2c-data-length` | `0` (target default) | MaxH2CDataLength in bytes |
| `SWB_UBLK_DEVICE` | `--ublk-device` | `-1` (off) | Serve as `/dev/ublkbN`; needs root and `ublk_drv` |
| `SWB_UBLK_QUEUES` | `--ublk-queues` | `0` (1 queue) | ublk hardware queues |
| `SWB_UBLK_QUEUE_DEPTH` | `--ublk-queue-depth` | `0` (64) | In-flight requests per queue |
| `SWB_ISCSI_LISTEN` | `--iscsi-listen` | empty (off) | iSCSI target; loopback only |
| `SWB_ISCSI_IQN` | `--iscsi-iqn` | empty | Target IQN; required with `--iscsi-listen` |
| `SWB_ISCSI_PORTAL_ADDR` | `--iscsi-portal-addr` | bound address | Advertised TargetAddress |
| `SWB_ISCSI_LUN` | `--iscsi-lun` | `0` | LUN id |
| `SWB_ISCSI_DATAOUT_TIMEOUT` | `--iscsi-dataout-timeout` | `0` (target default) | R2T/Data-Out wait bound |
| `SW_BLOCK_ISCSI_CHAP_USERNAME`, `SW_BLOCK_ISCSI_CHAP_SECRET` | `--iscsi-chap-username`, `--iscsi-chap-secret` | empty | Target-side CHAP; requires `--iscsi-listen` |
| `SWB_REPLICATION_ACK` | `--replication-ack` | `best-effort` | `best-effort`, `sync-quorum` or `sync-all` |
| `SWB_WAL_RETENTION_LSNS` | `--wal-retention-lsns` | `0` | walstore: WAL kept past the checkpoint for catch-up |
| `SWB_RECOVERY_MODE` | `--recovery-mode` | `legacy` | `legacy` or `dual-lane` rebuild |
| `SWB_DEGRADED_PROBE_INTERVAL` | `--degraded-probe-interval` | `0` (off) | Degraded-peer probe interval |
| `SWB_DEGRADED_PROBE_COOLDOWN_BASE` / `_CAP` | `--degraded-probe-cooldown-base` / `-cap` | `5s` / `60s` | Per-peer probe backoff |
| `SWB_CHUNK_FILER` | `--chunk-filer` | empty | SeaweedFS filer gRPC address; required with `walstore-chunk` |
| `SWB_CHUNK_PATH` | `--chunk-path` | `/swblock/chunks` | Filer directory, one directory per volume |
| `SWB_CHUNK_COLLECTION`, `SWB_CHUNK_REPLICATION` | `--chunk-collection`, `--chunk-replication` | empty (filer default) | Placement of chunk needles |
| `SWB_CHUNK_CAPTURE_INTERVAL` | `--chunk-capture-interval` | `0` (no timer) | Automatic cut interval, e.g. `15m` |
| `SWB_CHUNK_SNAPSHOT_SCHEDULE` | `--chunk-snapshot-schedule` | `daily:7` | Scheduled named snapshots: `daily:N` or `weekly:N` (one per period at a per-volume offset, the newest N kept), or `off` |
| `SWB_CHUNK_UPLOAD_CONCURRENCY` | `--chunk-upload-concurrency` | `8` | Parallel chunk uploads (1..64) |
| `SWB_CHUNK_CONTROL_ENDPOINT` | `--chunk-control-endpoint` | `false` | `/chunk/*` routes on the status listener |
| `SWB_WORKSPACE_LEASE` | `--workspace-lease` | `false` | Require a master lease; fail closed without one |
| `SWB_VOLUME_EXTRA_ARGS` | any | empty | Other flags (Kubernetes gate and source-gated flags) |

The `--chunk-*` and `--workspace-lease` flags are refused unless
`--durable-impl=walstore-chunk`; leave them at their defaults otherwise.

`--chunk-filer` must name the only filer of its filer group. At startup
blockvolume asks that filer for its masters, then every master for the
filers in the group. It starts only when a master lists exactly that filer.
Any master listing another filer, or several, refuses at once. If no master
lists it yet (a fresh filer registers with its master some seconds after it
serves), or none answers, blockvolume asks again every second until 60 s,
then refuses. This is checked at startup only; do not add filers to the
group while volumes run.

## Notes

- The control plane, the status endpoint and the NVMe/TCP target have no
  authentication. Keep them on loopback or a trusted network.
- A volume's geometry is fixed when it is created. Changing
  `SWB_DURABLE_BLOCKS` later has no effect on an existing volume.
- **Put `SWB_DURABLE_ROOT` on low-latency storage (NVMe or equivalent).**
  - **Why:** the durable root holds the volume's WAL and extent files. Attaching or taking over a volume runs synchronous writes on it (the store open and recovery, then the HEAD claim's local seed: an fsync and a checkpoint publish), and their latency adds directly to the attach time.
  - **Measured:** on one host whose root was a SATA disk (`sda`), that fsync took 158-171 ms and the checkpoint publish 0.5-1 s in half of the attaches. On an NVMe root, and on the same SATA disk in its fast attaches, both took tens of microseconds.
  - **Check the device:** `lsblk -d -o NAME,ROTA,TRAN,MODEL`, and the line blockvolume logs at start, `storage: WAL/extent sync=... file=... reason=...`, whose reason names the root's device.
- ublk needs root. Use `sw-blockvolume-ublk.service` instead of editing the
  unprivileged unit, and load `ublk_drv` at boot
  (`echo ublk_drv | sudo tee /etc/modules-load.d/ublk.conf`).

## First volume (read-only check)

Start one local volume, attach it over NVMe/TCP on loopback, and look at the
device and its status. This walkthrough does not format or write the attached
device; it only creates local backing files, a topology file and logs under
`./swb`. For a write test, see
[write test (scratch host only)](#write-test-scratch-host-only).

Needs Linux, bash, `nvme-cli`, `ss` (iproute2), `lsblk`, `curl`, `sudo`, and
the binaries from [Install](../README.md#install) in the current directory.
Paste the blocks into one bash shell. A failed check returns an error instead
of exiting your shell.

```bash
NQN=nqn.2026-05.io.seaweedfs:v1
sudo modprobe nvme_tcp
mkdir -p swb/master swb/vol && cd swb
printf 'volumes:\n  - volume_id: v1\n    slots:\n      - replica_id: r1\n        server_id: s1\n' > topology.yaml

swb_alive() { [ -n "$1" ] && kill -0 "$1" 2>/dev/null; }
swb_owns()  { ss -ltnpH "sport = :$1" | grep -q "pid=$2,"; }   # port $1 held by pid $2
swb_stop() {   # idempotent: undoes only what swb_start did; safe to run again
  [ -n "$SWB_OWN_NQN" ] && grep -qsx "$NQN" /sys/class/nvme-subsystem/nvme-subsys*/subsysnqn &&
    sudo nvme disconnect -n "$NQN" >/dev/null
  local p
  for p in $VPID $MPID; do swb_alive "$p" && kill "$p"; done
  [ -n "$VPID$MPID" ] && wait $VPID $MPID 2>/dev/null
  SWB_OWN_NQN= VPID= MPID=
  return 0
}
swb_start() {
  [ -z "$VPID$MPID" ] || { echo "refusing: run swb_stop first"; return 1; }
  grep -qsx "$NQN" /sys/class/nvme-subsystem/nvme-subsys*/subsysnqn &&
    { echo "refusing: $NQN is already connected"; return 1; }
  SWB_OWN_NQN=1   # from here on, a connection with this NQN is this run's
  ../blockmaster --authority-store ./master --listen 127.0.0.1:9180 \
    --topology ./topology.yaml --expected-slots-per-volume 1 > master.log 2>&1 &
  MPID=$!
  # A 256 MiB volume (65,536 blocks of 4 KiB) on walstore, NVMe/TCP on loopback.
  ../blockvolume --master 127.0.0.1:9180 --server-id s1 --volume-id v1 --replica-id r1 \
    --data-addr 127.0.0.1:9201 --ctrl-addr 127.0.0.1:9202 --status-addr 127.0.0.1:9203 \
    --durable-root ./vol --durable-impl walstore --durable-blocks 65536 \
    --nvme-listen 127.0.0.1:4420 --nvme-subsysnqn "$NQN" > volume.log 2>&1 &
  VPID=$!
  # Ready within 30 s: both processes alive, port 9180 held by our blockmaster
  # and port 4420 by our blockvolume.
  local i
  for i in $(seq 30); do
    swb_alive "$MPID" && swb_alive "$VPID" || break
    swb_owns 9180 "$MPID" && swb_owns 4420 "$VPID" && break
    sleep 1
  done
  swb_alive "$MPID" && swb_alive "$VPID" && swb_owns 9180 "$MPID" && swb_owns 4420 "$VPID" ||
    { echo "startup failed"; tail -n 20 master.log volume.log; swb_stop; return 1; }
  sudo nvme connect -t tcp -a 127.0.0.1 -s 4420 -n "$NQN" ||
    { echo "nvme connect failed"; swb_stop; return 1; }
}
swb_show() {   # read-only: the namespace under our NQN, then the volume status
  local s b found=
  for s in /sys/class/nvme-subsystem/nvme-subsys*; do
    [ "$(cat "$s/subsysnqn" 2>/dev/null)" = "$NQN" ] || continue
    for b in "$s"/nvme*n*; do
      [ -e "/sys/block/${b##*/}" ] || continue
      ls -l "/dev/${b##*/}"; lsblk "/dev/${b##*/}"; found=1
    done
  done
  [ -n "$found" ] || echo "no namespace for $NQN yet"
  curl -s 'http://127.0.0.1:9203/status?volume=v1'; echo
}
```

Start, look, stop:

```bash
swb_start && sleep 2 && swb_show
swb_stop
```

`lsblk` shows a 256M disk. The status JSON shows `VolumeID`, `Epoch`,
`Healthy` and `AuthorityRole` (`primary`).

If you interrupt a step (Ctrl-C) or one fails midway, run `swb_stop` again.
It disconnects the NQN only if this run connected it, and stops only the two
recorded processes that are still alive, so repeating it is safe. If the shell
itself was closed, the recorded PIDs are gone: find the processes with
`pgrep -af 'blockvolume --master 127.0.0.1:9180'` and
`pgrep -af 'blockmaster --authority-store ./master'`, stop them, and run
`sudo nvme disconnect -n nqn.2026-05.io.seaweedfs:v1` if it is still listed.

For a same-host device without NVMe/TCP, use ublk instead (needs root and the
`ublk_drv` kernel module). In `swb_start`: refuse if `/dev/ublkb0` already
exists; run `sudo -v`, then start `blockvolume` with `sudo` and
`--ublk-device 0` in place of the `--nvme-*` flags; treat it as ready when
`sudo kill -0 $VPID` succeeds and `/dev/ublkb0` exists. Show it with `ls -l`
and `lsblk`, and stop it with `sudo kill $VPID`, since it runs as root.

## Write test (scratch host only)

> **Warning:** this formats a block device. Run it only on a scratch host or
> VM that has no data you need and no other Seaweed Block NVMe target
> connected.

Use the self-checking script from a checkout, at the repository root, instead
of typing destructive commands by hand:

```bash
SW_BLOCK_DURABLE_IMPL=walstore bash scripts/run-nvme-os-smoke.sh "$PWD"
```

`scripts/run-nvme-os-smoke.sh` runs under `set -euo pipefail`, so any failed
step stops it. In order, it:

- builds `blockmaster` and `blockvolume` into its work directory
  (`SW_BLOCK_NVME_WORK_DIR`, default `/tmp/sw-block-nvme-os`), unless binaries
  are already in `SW_BLOCK_BIN_DIR`;
- removes its own previous run state under that directory, kills processes
  started from its own bin directory, and disconnects its own NQN
  (`SW_BLOCK_NVME_NQN`, default `nqn.2026-05.io.seaweedfs:os-smoke-v1`);
- starts blockmaster and one blockvolume on free loopback ports with a
  256 MiB volume (65,536 blocks of 4 KiB), and waits (bounded) for each to be
  ready;
- for each iteration (`SW_BLOCK_NVME_ITERATIONS`, default 1): `nvme connect`;
  finds the device of its NQN in `nvme list-subsys -o json` (it falls back to
  a model-name match in `nvme list` only when `python3` or that output is
  missing); runs `mkfs.ext4 -F` on it and mounts it; writes a random 4 KiB
  file, syncs, reads it back and checks its SHA-256; optionally runs `dd` or
  `fio` (`SW_BLOCK_NVME_STRESS`); unmounts and disconnects;
- fails if the subsystem is still present after the last disconnect, then
  prints `PASS` and the artifact directory. An exit trap unmounts,
  disconnects and stops the processes.

The script's default engine is `SW_BLOCK_DURABLE_IMPL=smartwal`; the command
above sets `walstore`, the engine used by the Kubernetes chart and the
workspace path. There is no equivalent script for ublk yet.
