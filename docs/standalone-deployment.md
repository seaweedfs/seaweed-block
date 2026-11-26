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

## Upgrade order

Upgrade `blockmaster` first, then `blockvolume`. Assignments and workspace
lease requests require the assigned server id. A new volume against an older
pre-release master stays not serving until the master is upgraded; an older
volume must also be upgraded to gain the assignment guard. Do not use endpoint
matching as a compatibility fallback. See [holder identity and upgrades](releases/holder-identity.md).
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
| [`deploy/systemd/sw-block.tmpfiles.conf`](../deploy/systemd/sw-block.tmpfiles.conf) | `/etc/tmpfiles.d/sw-block.conf` | `/run/sw-block/locks`, the host volume locks; the units cannot create it and blockvolume refuses to start without it ([one process per volume](releases/one-process-per-volume.md)) |

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
sudo install -m 0644 deploy/systemd/sw-block.tmpfiles.conf /etc/tmpfiles.d/sw-block.conf
sudo systemd-tmpfiles --create /etc/tmpfiles.d/sw-block.conf
sudo systemctl daemon-reload
sudo systemctl enable --now sw-blockmaster sw-blockvolume
journalctl -u sw-blockvolume -f    # wait for "adopted as PRIMARY" (see the required setup below)
```

The binaries take command-line flags. The `SWB_*` variables are read only by
the example units, which turn them into flags. blockvolume itself reads only
`SW_BLOCK_ISCSI_CHAP_USERNAME` and `SW_BLOCK_ISCSI_CHAP_SECRET`.

Both blockvolume units restart failed processes after 5 seconds, except exit 78
(a no-restart outcome; the final log line names the reason). Exit 75 retries
through the ordinary lease and HEAD-claim gates.
`StartLimitIntervalSec=600` and `StartLimitBurst=10` in `[Unit]` allow at most
10 starts in a 10-minute window; another start in that window is refused and
the unit enters `failed` (`start-limit-hit`). This bounds retries on a host
that no longer holds the volume. Each retry cycle is the assignment wait
(default 30 seconds) plus `RestartSec=5s`, so the retries reach the limit in
about 6 minutes, inside 600 seconds. Raising the assignment wait above about
55 seconds prevents this window from filling; raise `StartLimitIntervalSec`
with it to retain the bound. After correcting the assignment or disabling
the obsolete unit, an operator can use `systemctl reset-failed <unit>` and
`systemctl start <unit>` to resume attempts. An ACKed planned stop exits 0
and is not automatically restarted.

## Required setup: make the volume known to blockmaster

blockmaster gives a blockvolume an assignment only for a volume and replica
slot it already knows. It does not learn volumes from heartbeats. Without an
assignment, blockvolume starts, opens its storage and its listeners, and
serves no I/O.

Outside Kubernetes, a volume becomes known through the accepted-topology file.
Do these steps before you start blockvolume:

1. **Write the topology file.** One entry per volume, one slot per replica;
   `volume_id`, `replica_id` and `server_id` are required. This is
   [`deploy/config/topology.yaml`](../deploy/config/topology.yaml):

   ```yaml
   volumes:
     - volume_id: v1
       slots:
         - replica_id: r1
           server_id: s1
   ```

2. **Start blockmaster with the file and the matching slot count.**
   `--expected-slots-per-volume` must equal the number of slots of each
   volume: `1` here (the default is `3`).

   ```bash
   blockmaster --authority-store /var/lib/sw-block-master/authority --listen 127.0.0.1:9180 \
     --topology /etc/sw-block/topology.yaml --expected-slots-per-volume 1
   ```

   In `blockmaster.env` these are `SWB_MASTER_TOPOLOGY` and
   `SWB_MASTER_EXPECTED_SLOTS`. blockmaster reads the file only at start:
   after you change it, run `sudo systemctl restart sw-blockmaster`.

   **A replicated cluster** (volumes with `replication_factor` above 1, in a
   `--cluster-spec`) needs two flags that are off by default:
   - `--lifecycle-product-loop-interval 100ms` (the shipped chart's value):
     a replicated volume gets its primary from this loop. With the default
     `0` the loop is off and no replicated volume ever gets a primary.
   - `--failback-runtime-rpc`: the operator's failover (`sw-block ops
     failover`, below) calls it. Without it there is no manual failover.

   The failover needs an admin client certificate, so this blockmaster runs
   with authentication: the four mutual-TLS flags and `--audit-log`, which is
   required with them (see
   [Authenticate the control API](operator-guide.md#authenticate-the-control-api)).
   The single-replica start above needs none of these.

   ```bash
   blockmaster --authority-store /var/lib/sw-block-master/authority \
     --lifecycle-store /var/lib/sw-block-master/lifecycle \
     --cluster-spec /etc/sw-block/cluster-spec.yaml --expected-slots-per-volume 3 \
     --lifecycle-product-loop-interval 100ms --failback-runtime-rpc \
     --tls-cert /etc/sw-block/tls/master.crt --tls-key /etc/sw-block/tls/master.key \
     --tls-client-ca /etc/sw-block/tls/ca.crt --role-file /etc/sw-block/tls/roles \
     --audit-log /var/lib/sw-block-master/audit.log \
     --listen 192.0.2.10:9180
   ```

   **Replicated volumes in this release:**
   - **One replicated factor per cluster.** `--expected-slots-per-volume` is one
     number for the whole cluster, and blockmaster checks every replicated
     volume against it. One volume with another slot count stops assignment
     for all of them; it shows only as a controller error on every tick.
     - Under `--cluster-spec`, single-replica volumes can share a cluster with
       3-replica volumes (`--expected-slots-per-volume 3`).
     - 2-replica and 3-replica volumes cannot share a cluster.
     - A static `--topology` holds one factor only.
   - **Failover.** blockmaster replaces a primary by itself only when that
     primary answers that it is not ready.
     - A primary whose host is off or unreachable does not answer. It is shown
       as unreachable and is not replaced, at every replica count, with the
       loop on or off.
     - Then the operator powers the old node off or cuts it from its clients,
       and runs `sw-block ops failover` with `--confirm-old-node-off`.
     - The failover needs blockmaster started with `--failback-runtime-rpc`
       and an admin client certificate (`SW_BLOCK_TLS_CERT`,
       `SW_BLOCK_TLS_KEY`, `SW_BLOCK_TLS_CA`).
     - If its fencing round is refused, run the command again.
       `sw-block ops failover -h` describes the round and its exit codes.

3. **Start blockvolume with the identity of one slot.** `--volume-id`,
   `--replica-id` and `--server-id` (`SWB_VOLUME_ID`, `SWB_REPLICA_ID`,
   `SWB_SERVER_ID`) must equal one slot of the file: `v1`, `r1`, `s1` here.

**Check before starting blockvolume.** Without a usable topology, blockmaster
logs this line at start; if `journalctl -u sw-blockmaster` shows it, redo
steps 1 and 2:

```
blockmaster: WARNING: --topology not supplied or empty; controller will not mint assignments until topology is provided
```

A file that cannot be read or parsed stops blockmaster with
`blockmaster: topology: ...`.

**Check after.** blockvolume logs `blockvolume: volume v1 adopted as PRIMARY
at r1@<epoch>`, and its status shows a positive `Epoch`, `"Healthy":true` and
`"AuthorityRole":"primary"`:

```bash
journalctl -u sw-blockvolume | grep 'adopted as PRIMARY'
curl -s 'http://127.0.0.1:9203/status?volume=v1'; echo
```

`nvme-listening` alone does not prove this: blockvolume starts its NVMe/TCP
listener before it has an assignment. Without one, the status shows
`"Epoch":0`, `"Healthy":false` and `"AuthorityRole":"unknown"`.

| What you see | Missing step |
|---|---|
| blockvolume logs `blockvolume: subscribe stream: rpc error: code = Unknown desc = subscribe: volume "v1" not in accepted topology`, again every few seconds | blockmaster has no topology, or `v1` is not in it (steps 1, 2) |
| `walstore-chunk` only: `durable: chunk layer for volume=v1 waits for master authority (up to 30s): projection lacks healthy=false epoch=0`, and 30 s later `durable: chunk authority wait for volume=v1 ended after ...: assignment unavailable, not serving (projection lacks healthy=false epoch=0)` | The same. This wait runs once per start: after the fix, restart blockvolume too |
| No error in either log, and `"Epoch":0` stays | `--expected-slots-per-volume` differs from the slot count (step 2), or `--server-id` or `--replica-id` differ from the slot (step 3) |
| blockmaster logs `authority: rejected ask ...: workspace publication: no lifecycle VolumeRecord for v1; register it (CreateVolume) before publishing` | blockmaster runs with `--lifecycle-store`: a topology slot is then not enough, the volume also needs a volume record (below) |

**Workspace lease volumes (`--workspace-lease`).** Besides the steps above,
blockmaster needs `--workspace-rpc`, `--chunk-filer` (and blockvolume's
`--chunk-path`, when not the default) and `--lifecycle-store`, and the volume needs a single-replica volume record in
that store. This release has no `sw-block` command that creates that record. Check with `sw-block ops workspace show --master 127.0.0.1:9180
--volume v1`; it names the first missing piece: `workspace RPC disabled`,
`lifecycle store unavailable`, or `workspace move requires an existing RF1
volume`. A move target must also be a registered node, or the move is refused
with `move target node "s2" is not registered`.

### Resolve the LOCAL volume's layout id

Before starting a standalone `walstore-chunk` blockvolume for an existing
volume, ask the same master for its layout id:

```bash
sw-block ops volume resolve --master 127.0.0.1:9180 v1
```

The command prints only the full 32-hex-digit id and a newline. Pass that
value as `blockvolume --layout-id <printed-id>` along with `--volume-id v1`
and the other startup flags. For the example systemd units, add
`--layout-id <printed-id>` to `SWB_VOLUME_EXTRA_ARGS` in
`/etc/sw-block/blockvolume.env` before starting the service. Resolve each
volume separately; do not mint an id or copy another volume's value.

This is a read-only query of the master's volume record, using the same
TLS and token environment settings as `ops chunk`. For a replicated installation
using mutual TLS, set all three file paths: `SW_BLOCK_TLS_CERT` (client
certificate), `SW_BLOCK_TLS_KEY` (client private key) and `SW_BLOCK_TLS_CA`
(CA that verifies the master). A missing volume, an
invalid identity or an unreachable master prints no id. Exit 75 means the
master answer is unavailable or unknown: retry the query. Exit 78 means a
definite refusal (including not found or an invalid identity): correct it
before starting blockvolume. Invalid command arguments return 2. The query does not create a
volume or prove frontend readiness.

## blockmaster settings

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| `SWB_MASTER_AUTHORITY_STORE` | `--authority-store` | required | Durable authority store: assignments, epochs, leases |
| `SWB_MASTER_LISTEN` | `--listen` | `127.0.0.1:0` | gRPC address; unauthenticated, keep it on loopback or a trusted network |
| `SWB_MASTER_TOPOLOGY` | `--topology` | empty (no assignments minted: no volume serves) | Accepted-topology YAML: volumes and replica slots; required outside Kubernetes, read only at start |
| `SWB_MASTER_EXPECTED_SLOTS` | `--expected-slots-per-volume` | `3` | Replica slots per volume; `1` for a single replica |
| `SWB_MASTER_FRESHNESS_WINDOW` | `--freshness-window` | `30s` | How long a heartbeat stays fresh |
| `SWB_MASTER_PENDING_GRACE` | `--pending-grace` | `1s` | Startup grace before a missing observation is unsupported |
| `SWB_MASTER_WORKSPACE_RPC` | `--workspace-rpc` | `false` | Workspace lease, revocation and move RPCs; HEAD is read through the layout catalog (`--chunk-filer`, `--chunk-path` in `SWB_MASTER_EXTRA_ARGS`) |
| `SWB_MASTER_WORKSPACE_MAX_CLOCK_SKEW` | `--workspace-max-clock-skew` | `20s` | Clock-skew bound for workspace leases and forced moves; must be >10.1s and <60s so half-skew exceeds the configured fence-exit budget, assuming normal scheduling |
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
| `SWB_DURABLE_IMPL` | `--durable-impl` | empty (auto) | Auto creates new volumes as `walstore`, opens existing ones with their on-disk format, and refuses a `walstore-chunk` root; so does every explicit value other than `walstore-chunk` (a chunk root holds data that may live only in the remote HEAD: open it with `--durable-impl=walstore-chunk` and its remote configuration). Explicit: `walstore`, `walstore-chunk` (workspace, proof of concept), `smartwal` (experimental). The examples set `walstore`. |
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
  - **Why:** the root holds the volume's WAL, and every guest flush is acknowledged only after an fsync there. The root's fsync latency is therefore the floor of the guest's flush latency, at all times.
  - **Attach shows it too:** attaching or taking over a volume runs synchronous writes on the root (the store open and recovery, then the HEAD claim's local seed: an fsync and a checkpoint publish).
    - **Measured on one host,** with its root on another disk (not NVMe), in half of the attaches: that fsync took 158-171 ms and the checkpoint publish 0.5-1.0 s.
    - **On an NVMe root,** and in that disk's other attaches: the fsync took 50-75 microseconds and the publish 2-3 milliseconds.
    - **Cause:** not yet established.
  - **Check the device:** `lsblk -d -o NAME,ROTA,TRAN,MODEL`, and the line blockvolume logs at start, `storage: WAL/extent sync=... file=... reason=...`, whose reason names the root's device.
- **NVMe/TCP needs native NVMe multipath on the host (`nvme_core multipath=Y`, the Ubuntu default).**
  The device lookup below, and the CSI node plugin's, find the namespace as an `nvmeXnY` entry under
  `/sys/class/nvme-subsystem/nvme-subsysN/`; with multipath off that directory holds no namespace entry and
  neither finds the device. Check: `cat /sys/module/nvme_core/parameters/multipath` must print `Y`.
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

## Planned workspace hand-over

Setup, expected output of each step, return to the same host, crash recovery
and symptoms: [volume lifecycle](volume-lifecycle.md).

For an RF1 chunk volume with a workspace lease, inspect the current logical
holder with `sw-block ops workspace show --master MASTER --volume V`. Copy the
replica and epoch exactly. The view prints the master's lease state, recorded
revocation ACK (including its HEAD generation), move records and bindings. It
does not infer a physical holder from the binding list; a binding is not proof
of a current assignment. Reported validity is a snapshot, not permission to
serve I/O.
`expired` means the master reported no positive **host validity** at query
time; it is not a claim that the master's forced-move horizon has passed.
Only the master admits a move.

Perform a clean hand-over in this order:

1. Stop the application/writer and prevent new I/O to the old attachment.
2. Seal the idle volume while its lease still permits publication:

   ```bash
   sw-block ops chunk detach --plain --volume V --status-addr OLD_STATUS --timeout 120s >seal.json
   ```

   Busy admission refuses the operation. Do not proceed on any failure or
   unknown outcome. JSON remains on stdout; a successful command also prints
   `sealed HEAD generation: G` on its own stderr line, taken from the same
   JSON `headGen` field on stdout. Copy the positive `G`.
3. Request revocation using that seal and the identity from `show`:

   ```bash
   sw-block ops workspace revoke --master MASTER --volume V --replica R --epoch E --sealed-gen G
   ```

   Seal **before** revocation: revoking first can prevent the old host from
   publishing the final image and leave only a lossy forced move. The CLI
   requires exactly one of `--sealed-gen G` or `--without-seal`. The former is
   your statement, not a storage check. The latter explicitly abandons the
   unpublished tail and does not promise that a clean move is possible.
4. Run `ops workspace show --master MASTER --volume V` until the **recorded
   ACK** names the same replica `R`, epoch `E` and HEAD generation `G`.
   A successful `revoke` reply alone is not that ACK.
5. Ask for the clean authority move:

   ```bash
   sw-block ops workspace move --clean --master MASTER --volume V --replica R --epoch E \
     --target-host NEW_HOST --target-data DATA_ADDR --target-ctrl CTRL_ADDR --request-id MOVE_ID
   ```

   Use registered target endpoints. The master checks the seal and matching
   ACK. The target may be the source host once sealed. A command refuses rather than bypassing a missing
   seal, ACK or current identity. On an **unknown outcome**, rerun with the
   **SAME `--request-id MOVE_ID`**; never use a new id to guess whether it ran.
6. Start `blockvolume` on the target separately with the granted replica and
   new epoch and the normal workspace-lease options. The move command only
   transfers authority; it does not start the target or reconnect clients.

Workspace commands exit 0 on success, 1 on a definite server refusal, 2 on local
usage refusal, 3 on an unknown outcome, and 4 on an unproven readout. Plain
detach keeps the chunk CLI's existing JSON error and exit policy. Neither CLI
retries or polls automatically.

## Snapshots, restore and space (walstore-chunk)

**Recovery point.**
- **A stopped volume** keeps everything it acknowledged, on its host.
- **If the host is lost,** the volume is restored from its last cut. With the defaults (periodic capture off, one scheduled snapshot a day), that cut is at most one day old.
- **For a newer recovery point,** cut more often (`--chunk-capture-interval`, at least 5 minutes), or take a snapshot right before risky work.

**Consistency.** A snapshot is crash-consistent:
- it holds every write the volume acknowledged up to the cut;
- a guest file system restored from it replays its journal;
- data an application had not fsynced may be missing.

The commands below use these placeholders:
- `FILER` is the SeaweedFS filer gRPC address (`--chunk-filer`).
- `MASTER` is the blockmaster address the blockvolumes use.
- `WEED_MASTER` is the SeaweedFS master HTTP address.
- `STATUS` is a blockvolume's status address (its `--status-addr`, with `--chunk-control-endpoint` set).

**About the control endpoint.** `--chunk-control-endpoint` carries destructive operations (cut, snapshot, snapshot delete, detach, restore) on the status listener.
- **Binding:** that listener binds loopback addresses only, unless `--allow-external-status-bind` is set; that flag is refused together with `--master-tls-cert`.
- **No authentication:** nothing authenticates it in this release. Keep it on loopback, or behind your own access control.

### Scheduled snapshots

- **Schedule:** with the default `--chunk-snapshot-schedule=daily:7`, each attached volume takes one named snapshot (`sched-<UTC time>`) once per 24 h, at a per-volume offset rather than at a wall-clock hour.
- **Retention:** keeps the newest 7 scheduled snapshots. It counts only the scheduled class, so other snapshots are not affected.
- **Deleting by hand:** scheduled snapshots are safe to delete with `snapshot-delete` like any other.
- **Restoring volumes:** a volume still restoring is skipped, and the skip is counted.
- **Status:** `GET STATUS/status/durable?volume=<id>` shows the schedule under `Chunk.ScheduledSnapshot`: `lastOkNs`, `nextDueNs`, `lastFailure`, `skipped` and `snapshot_overdue`.

```bash
sw-block ops chunk snapshots --master MASTER --volume v1 --chunk-filer FILER          # list
sw-block ops chunk snapshot --volume v1 --name before-upgrade --status-addr STATUS   # take one now
```

The list is a JSON array, one entry per published snapshot in name order: `[{"name":"sched-20261120T031204Z","root":"<64 hex>"}]`, with `"dependents":["v2-<id8>"]` on a snapshot that restore targets depend on.

### Deleting snapshots and getting space back

Space comes back in two steps. Neither is immediate.

1. **The holder's cleaning deletes the chunks.**
   - Cleaning runs only on the volume server that holds the volume, in its maintenance pass: right after each snapshot delete through that server, and every `--chunk-maintenance-interval` (hourly by default). It is on by default; `--chunk-maintenance-gc=false` turns it off. There is no operator command.
   - A pass removes the volume's own chunks that no root in HEAD names: not the current image, not a snapshot. A chunk that a restore target still names stays, because the source's snapshot keeps it while the target depends on it.
   - A deleted snapshot's unshared chunks are therefore removed by the first pass after the delete. A pass waits while a snapshot is being taken (it is tried again shortly), a cut cancels a pass in progress, and a server that no longer holds the volume removes nothing. A snapshot deleted with `sw-block ops chunk` (not through the holder) is cleaned at the next periodic pass.

   ```bash
   sw-block ops chunk snapshot-delete --master MASTER --volume v1 --name sched-20261120T031204Z --chunk-filer FILER
   # the holder's next maintenance pass removes the chunks only it named;
   # each pass logs "maintenance volume=... removed=N"
   ```
2. **SeaweedFS vacuum compacts the volume files.** Reclaimed chunks are deleted needles.
   - SeaweedFS counts them at once in `DeletedByteCount`.
   - The disk space returns only when SeaweedFS vacuums those volumes, which runs asynchronously.

   To check the count, and to ask for a vacuum now:

   ```bash
   curl http://WEED_MASTER/vol/status                               # DeletedByteCount per volume
   curl "http://WEED_MASTER/vol/vacuum?garbageThreshold=0.01"      # compact volumes with more than 1% garbage
   # or, in weed shell:  volume.list   and   volume.vacuum -garbageThreshold 0.01
   ```

Check the data afterwards. `--depth data` reads every chunk once.

| Exit | Meaning |
|---|---|
| 0 | clean |
| 3 | the pass found problems |
| 1 | the pass could not complete, for example a filer or master is unreachable; the partial result is printed |
| 2 | the command could not run: a usage error, or the filer is not the only filer of its group (it refuses to start) |

```bash
sw-block ops chunk verify --volume v1 --chunk-filer FILER --all-snapshots --depth data --timeout 30m
```

### SeaweedFS maintenance commands that destroy chunk volumes

A walstore-chunk volume keeps its data in SeaweedFS as needles that **no file's chunk list names**: the volume's
own records in the filer point at them. SeaweedFS tools that decide what is referenced from file chunk lists
therefore see every chunk of every volume and snapshot as unreferenced.

- **Never run `volume.fsck` with `-reallyDeleteFromVolume`** (alone or with `-forcePurging`) on a SeaweedFS
  cluster that stores walstore-chunk volumes. It deletes the needles it considers orphaned, once they are older
  than `-cutoffTimeAgo` (5 hours by default): that is the volumes' data, including every snapshot. There is no undo.
- **Never run `volume.fsck -findMissingChunksInFiler` with `-reallyDeleteFilerEntries` or
  `-reallyDeleteFromVolume`** on such a cluster either.
- `volume.fsck` **without** those flags only reports. Its orphan list will contain the chunk needles; that is
  expected and is not a fault.
- **Give chunk data its own collection** (`--chunk-collection`, `SWB_CHUNK_COLLECTION`) so it is easy to keep
  cluster maintenance away from it. Do not put a TTL on that collection and do not delete it: both remove needles
  without regard to what refers to them.
- `filer.sync`, `filer.backup` and `filer.replicate` copy file chunk lists. They do **not** copy a walstore-chunk
  volume: the copied records would point at needles that do not exist on the target.
- Space is returned only by the two steps above (the holder's cleaning, then a SeaweedFS vacuum). `volume.vacuum`
  is safe: it compacts needles that were already deleted.

### Fork and template retirement

Fork is not supported in this release; the command is removed. Restore from a snapshot instead (below).

Retiring the source of a template is not supported in this release either: a restore target depends on its source snapshot for its whole life (see [Lifetime](#lifetime-of-a-restore-target)), so the source snapshot, and therefore its volume, cannot be deleted while any target exists.

### Restoring a snapshot into a new volume

A restore makes a new volume from a snapshot. The new volume reads every chunk it has not made local from the source's snapshot, so the snapshot stays in use for as long as the new volume exists.

1. **Add the target to blockmaster's cluster spec as placement only.** A restore needs blockmaster's lifecycle store, so blockmaster runs with `--cluster-spec` (see [the volume record](volume-lifecycle.md#setup)). Add the target (for example `v2`) with its id, replication factor and placement, and **no `size_bytes`**:

   ```yaml
   volumes:
     - id: v2                   # a new name: never used, never deleted
       replication_factor: 1
       placements:
         - server_id: s2
           replica_id: r1
   ```

   Restart blockmaster to read it (it reads the cluster spec only at start). Volumes already serving keep serving through a short restart: each holder renews every 10 s or less and may serve until 20 s after its last renewal (the 30 s lease less half the 20 s clock-skew margin, with defaults), so a blockmaster that answers again within 10 s interrupts nothing (`TestPhase0BaselineMasterKilledAndRestartedUnderWrites`: killed and back after 5 s, no guest write fails). A longer restart is fail-closed: a holder whose lease ends stops I/O, exits 75 (`reason=lease_expired`), is started again by the unit and waits, and serves again once blockmaster is back and its barrier has passed, at least 50 s after the restart (`TestPhase0BaselineMasterDown60sOneVolume`, `TestUnitSurvivesTenMinuteMasterOutage`). Add restore targets ahead of time, several at once, to restart less often.

   An entry without `size_bytes` creates no volume record; the restore creates it, with the snapshot's geometry. An entry with `size_bytes` creates an ordinary empty volume, and the restore into it is then refused with `target_exists` (`v2 is bound to another restore request`).
2. **Create the restore:**

   ```bash
   sw-block ops chunk restore --master MASTER --source v1 --snapshot sched-20261120T031204Z --target v2      --chunk-filer FILER --request-id restore-v2
   ```

   This creates `v2` from the snapshot and prints one JSON document. It is idempotent by `--request-id` (a generated id is printed when you omit it); reuse it to retry an uncertain result. `--master` is required: it resolves each volume's layout id.

   The JSON `target` is the store directory, `"target":"v2-<id8>"`, where `<id8>` is the first eight hex digits of v2's layout id. The public volume name remains `v2`: keep using `--volume v2`, `--source v2`, `blockvolume --volume-id v2` and status queries with `volume=v2`. The directory printed in the result is not a replacement name for those arguments. Resolve v2's full layout id through the master for `blockvolume --layout-id` as described [above](#resolve-the-local-volumes-layout-id).
3. **Start v2's blockvolume** with the normal standalone command, its own volume id, endpoints and local root, the source's geometry (`--durable-blocks` and `--durable-blocksize`, for example `65536` and `4096`), and the same chunk settings as the source (`--chunk-filer`, `--chunk-path`, `--chunk-control-endpoint`). v2 serves at once. A start with other geometry flags is refused with `geometry_mismatch` and exits 78, creating no local store; the refusal names the record's values.
4. **Reads fill v2.** A block in a chunk that v2 does not hold yet is read from the source's snapshot; the read waits for that chunk, which is then kept locally. There is no background copy: a chunk that is never read stays in the source's snapshot.
   - Progress is the volume's count of chunks still absent: `Chunk.Stats.AbsentChunks` in `GET http://STATUS_OF_V2/status/durable?volume=v2` (0 means every chunk is local).
   - That progress is written to v2's local store at its next checkpoint. If v2's blockvolume is killed before then, the chunks it made local since are absent again after the restart and are fetched again from the source's snapshot on their next read. No data is lost: the source's snapshot still holds them.
   - To make every chunk local, read the whole device once (for example with `dd` to `/dev/null`).
   - **A restored volume takes no scheduled snapshot until its whole device has been read once**: while any chunk is absent, each scheduled snapshot is skipped and counted as `restoring`.
   - A manual snapshot (`ops chunk snapshot`) is taken in that state too. It names the source's chunks for every chunk v2 has not made local, so, like any snapshot of v2, it cannot be restored (below).

#### Lifetime of a restore target

- **The source snapshot cannot be deleted** while v2 exists: `ops chunk snapshot-delete` is refused with `snapshot_has_dependents`. Delete v2 first (see [Delete a volume](volume-lifecycle.md#delete-a-volume)); the snapshot can be deleted after that.
- **A snapshot of v2 cannot be restored:** it names the source's chunks. The restore exits 1 with `"code":"not_admitted"` and a message ending in `snapshot names another volume's chunks: "<snapshot>"`. The refused request still leaves the target's name bound to it, so a later restore under that name from another snapshot is refused with `target_exists`: restore from the source's snapshot under a new target name.
- **v2's own writes stay in v2:** the source's snapshot never changes.
- **Deleting v2 releases the source snapshot:** `ops chunk volume-delete` of v2 removes it from the snapshot's dependents.
- **`ops chunk release` is a repair step, not part of this procedure.** `sw-block ops chunk release --master MASTER --source v1 --target v2 --chunk-filer FILER` removes a restore target from its source snapshot's dependents. It works only when the target no longer names the source's chunks (it was deleted), or when the target never got its first HEAD (an interrupted restore). In the second case it first tombstones the target's name, so that name can never be created. A live target is refused with `target_live`.
