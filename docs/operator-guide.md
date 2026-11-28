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
  - Do not run two `blockmaster` processes against the same store.
  - Why: a forced move after a host loss needs the authority record, the lease horizon and the lost range.
  - If those lived on the lost host, the volume could not be moved anywhere.
- **SeaweedFS durability (a premise):** sw-block assumes SeaweedFS acknowledges a write only when it is durable;
  configuring SeaweedFS for that is the SeaweedFS deployment's responsibility.

## Clock safety requirement

While volumes are served, blockmaster's host must keep its wall clock **slewed, not stepped**. Configure chrony to
allow `makestep` only at boot (for example, `makestep 1 3` for the initial updates), and do not run `ntpdate` from a
timer. Do not restart an initial-step clock configuration while leases are live. A wall-clock step larger than the
configured clock-skew bound while a lease is live is **outside the v1.0 supported envelope**. Blockmaster requires half of `--workspace-max-clock-skew` to exceed its configured fence-exit budget (watch cadence 50 ms + drain timeout 5 s), assuming normal scheduling, so the skew bound must be above 10.1 s; the default 20 s passes.

**Hosts' clocks stay within `--workspace-max-clock-skew` (default 20 s) of blockmaster's clock.** Beyond it the lease bound does not hold: a volume could keep serving after blockmaster considers its lease over, and a start-up wait can be cut short. Keep every volume host and blockmaster on the same time source.

This is a safety premise, not a tested wall-step guarantee. The host's monotonic lease deadline does not remove this
requirement on blockmaster's clock. See Part T `TERM-LEASE-NOW-001` in the [invariant contract](design/invariants.md).

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

## Authenticate the control API

`blockmaster` authenticates every gRPC call with mutual TLS when it starts with all four of `--tls-cert`,
`--tls-key`, `--tls-client-ca` and `--role-file`. With them `--audit-log <path>` is required: without it blockmaster
refuses to start (`authentication needs --audit-log: every changing call is recorded before it runs, and is refused
when it cannot be`). Without the four it is in development mode: plaintext, loopback only,
and it logs `UNAUTHENTICATED CONTROL API on <addr> (development mode)` once a minute. **Upgrading:** `--insecure-external-bind`
is refused at start; drop it, and give a blockmaster that listens off-host the four mutual-TLS flags.

**Who may call.** The role file has one line per certificate: `sha256:<hex fingerprint> <role> <id>`. The role is
`viewer`, `operator`, `admin` (people and tools; each includes the one before it) or `node` (a volume host; its id is
its `--server-id`). A certificate's CN must equal its line's id. A node may act only for its own server id. Two lines with one node id are two keys of one server (for a rotation: add the new line, move the host to the new certificate, remove the old line), never two hosts: one server id names one host. A forced
move needs `admin`. `kill -HUP` reloads the file. A removed line is refused from the next call, and its open streams
end at the reload. A malformed file keeps the previous table and says so in the log. The audit log appends
one JSON line per person's call, per role-file reload and per refused node call (at most once a minute per
certificate and method). A changing call (create, delete, place, revoke, move, register a binding, failback) is
written as an attempt before it runs: if the line cannot be written within 2 s the call is refused as "audit
unavailable" and nothing happens. The attempt line is written without fsync: it survives a blockmaster crash,
not a host crash or power loss. An unwritable audit log stops operator changes, never the volumes: node calls
never wait on it, and reads and refusals are dropped and counted (`GET /status/audit`) when it falls behind.

**Issue the certificates** (one CA, one key per host or person; a node key gives control of that host's volumes, so
never copy it into a host image):

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
  -subj "/CN=sw-block CA" -keyout ca.key -out ca.crt
# blockmaster: the name volume hosts dial must be in subjectAltName
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=blockmaster" \
  -keyout master.key -out master.csr
openssl x509 -req -in master.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 \
  -extfile <(printf "subjectAltName=DNS:master.example,IP:192.0.2.10") -out master.crt
# a volume host: CN = its --server-id
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=node-a" \
  -keyout node-a.key -out node-a.csr
openssl x509 -req -in node-a.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 -out node-a.crt
# its role-file line
echo "sha256:$(openssl x509 -in node-a.crt -outform DER | sha256sum | cut -d' ' -f1) node node-a" >> roles
```

**Volume hosts** dial with `--master-tls-cert`, `--master-tls-key` and `--master-ca`. A volume refuses to start
when its certificate's CN differs from `--server-id`. **sw-block** reads `SW_BLOCK_TLS_CERT`, `SW_BLOCK_TLS_KEY` and
`SW_BLOCK_TLS_CA`. **blockcsi** takes `--master-tls-cert`, `--master-tls-key` and `--master-ca`. Certificate and key
files are read at every new connection, so a rotated file takes effect without a restart. blockmaster closes each
client connection after 24 hours, so a rotated or removed certificate is seen within a day even on an idle link.
The volume's own HTTP routes (`/status`, chunk routes) are not authenticated and stay on loopback.

**In Kubernetes** each in-cluster client has its own certificate and Secret, with the least role its calls need:
the CSI node and the status operator are `viewer`; the CSI controller (it deletes volumes) and the failback executor
are `admin`. The CSI controller's key is an admin key: guard its Secret like an operator's admin key. The CSI
node's certificate must have the viewer role; check the role file (the chart refuses a CSI node or status Secret
named like an admin client's or blockmaster's, but it cannot see a certificate's role).
`scripts/k8s-alpha-certs.sh` issues all four for a trial.

## Size a host

- **Geometry:** a volume's size is `--durable-blocks` x `--durable-blocksize` on `blockvolume`. It must equal
  `size_bytes` in the cluster spec (for example 65536 x 4096 = 268435456).
  A first start whose `--durable-blocks` or `--durable-blocksize` disagrees with the volume record is refused before
  any local store is created: it exits 78 (the unit does not restart it) with a line `geometry_mismatch: --durable-blocks
  ... disagree with the volume record`; correct the flag and start it again.
  If that first start cannot read the volume record because the filer failed the read, it exits 75 with `the volume
  record of <vol> could not be read ... no local store was created; retrying` and the unit starts it again. During a long
  outage the unit can reach its start limit; once the filer answers, run `systemctl reset-failed <unit>` and start it.
  If the record is there but corrupt, it exits 78 with `the volume record of <vol> is unreadable (corrupt: ...)`: restore
  or recreate the record, then start it.
  A wrong-size local store left by an earlier release's first start is different: its log repeats `chunk attach check:
  chunkstore: geometry_mismatch` and it exits; waiting or restarting with the right flags does not help. In one sitting: stop the unit first; then check that the volume never served:
  `sw-block ops chunk show --master <master> --volume <vol> --chunk-filer <filer> --chunk-path <path>` prints `"Epoch":0` and
  `"Holder":""` in its `head` object; only then remove `<vol>.bin`, `<vol>.bin.chunkfresh`, `<vol>.bin.chunkmap` and
  `<vol>.replica-floor` from `--durable-root`, and start it with the right flags. If the head shows an epoch above 0 or
  a holder, do not remove these files; if the volume served before, its local store has its original size: correct the
  flag and start it again (not run as a test in this release).
- **Memory:** plan for about 32-36 MiB per running volume. That was measured with 150-200 volumes on one host.
- **Measured throughput on one host** (16 cores, 23.7 GB RAM, one consumer NVMe drive, fsync on every volume):

  | Volumes | Aggregate IOPS, qd1 4k + fsync | Aggregate IOPS, 4k qd32 |
  |---|---|---|
  | 100 | about 14,000 | 47,000-49,000 |
  | 150 | 5,300-9,100 | 37,000-39,000 |
  | 200 | about 4,300 | about 32,000 |

  - Above about 100 volumes the drive's flush rate, not the software, sets the limit.
  - Treat these numbers as one host class, not a guarantee.
- **Local disk:** a volume's store grows sparsely up to its full size. Size each durable root's file system for
  every volume on it:
  - the volume size;
  - plus its WAL, 64 MiB;
  - plus room for a cut's copy-on-write side files. A cut writes them next to the store while guest writes overwrite
    blocks it has not uploaded yet.

  blockvolume refuses to create a volume when the durable root has less free space than its size plus the WAL (the
  refusal names both numbers), and warns at start when the free space is below its remaining growth plus the WAL;
  there is no override. The refusal ends the process with status 1 (`blockvolume: durable storage open: ... needs
  N (size S + WAL W)`), so the shipped unit starts it again every 5 s, refusing each time, until its start limit stops
  it; free space on that file system, then start the unit again. A restore target refused this way exists in
  the catalog but cannot start, and it can be neither deleted nor moved until it has started once: a delete wants
  it sealed, revoked and acknowledged, and a forced move is refused because the master knows no holder of it. To
  recover it, free space on that file system, or start it again on the same host with `--durable-root` on a local
  file system with room; then delete it with the usual steps if it is not wanted. A refused target does not block
  its source: the source's snapshot and the source volume can still be deleted, and the target keeps its data.

  Alert on local free space. On a full local disk, a Linux guest's writes fail after about 75 s with an I/O error
  (EREMOTEIO), and the guest file system may then go read-only. Once space is freed, new writes succeed without a
  restart. Data the guest had already flushed is kept.
- **`--host-limit-dir`:** keep it on a local file system with free space. When it cannot be written, background cuts
  are refused (they fail closed); the seal at stop still runs.
- **SeaweedFS space:** every cut and snapshot stores the chunks that changed since the previous one. Space comes
  back only by GC and then a SeaweedFS vacuum (below). An upload interrupted before its filer entry is
  created can leave an unreferenced needle; include this crash-window overhead in space accounting.
  Do not run volume.fsck with real deletion on a block cluster.

Each volume process reports its own heartbeat slot (server, volume and replica).
The master orders each slot against that slot's previous report. In version 1.0,
a backward host-clock step longer than the freshness window (30 seconds by
default) can expire the host's slots; keep host clocks synchronized.

## Recovery point and sync policy

- **A stopped volume** keeps everything it acknowledged, on its host.
- **A lost host:** the volume comes back from its last cut, the last point published to SeaweedFS.
  - Default: no periodic capture. One scheduled snapshot a day (`--chunk-snapshot-schedule=daily:7`).
  - So with defaults a host loss can lose up to a day of acknowledged writes.
- **For a newer recovery point:** set `--chunk-capture-interval` (at least 5 minutes), or take a snapshot before
  risky work.
- **What does not publish:** a stop, a lease revocation and a lease expiry publish nothing. Only a cut, a snapshot or
  a seal (`ops chunk detach --plain`) does.
- **When SeaweedFS is unavailable** (the filer or a volume server is down):
  - Guest I/O continues from the local disk; in the tests no guest command failed and the slowest answered in about 2 s.
  - A cut in progress answers `remote_unavailable` (retryable) after about 30 s; no write the guest flushed is lost.
  - The next cut after the store returns commits normally and publishes everything written meanwhile.
  - A cut holds copy-on-write data on the local disk up to at most half of `--host-limit-cow-bytes` (1 GiB by
    default). A larger dirty set is uploaded before the cut point is fixed, so during a store outage such a cut fails
    without using local space. Guest I/O does not wait for the copy-on-write reserve.
  - Measured for outages of about 60 s, with the filer down and with a volume server down. Longer outages are not
    tested in 1.0.

## Snapshots, restore and space

A chunk volume delete refuses to reserve a volume while its lease may still be live. Once the permanent
reservation succeeds at blockmaster, the volume cannot be returned to service, even if a snapshot dependent
refuses the next step. Resolve snapshot dependents and rerun with the same request id;
the command asks blockmaster for a fresh no-live-lease proof before the HEAD compare-and-swap.
A late upload by a dead holder into a deleted volume can remain; rerun volume-delete to sweep it.

Deleting a restore target: its first cut compares every restored chunk locally (about 1.4 GiB/s measured). The
default `--timeout` of 120 s covers up to about 84 GiB; above that, pass `--timeout 5m` (the server's limit for one
request). At the 256 GiB maximum the first cut takes about 3 min. On an `outcome_unknown` answer, retry with
`--timeout 5m` if you used less; if it still answers `outcome_unknown` at 5m, the target is above this release's
limit (its filled size must be at most about 4 min x your disk's read rate). The volume keeps serving and nothing is
lost.


[Standalone deployment: snapshots, restore and space](standalone-deployment.md#snapshots-restore-and-space-walstore-chunk):
- scheduled and manual snapshots;
- deleting snapshots, and getting space back:
  - GC deletes the chunks after the grace (`--grace`, 1 hour by default), so a deleted snapshot's space is
    reclaimed about 2 hours after the delete unless still referenced by a snapshot or a restore target;
  - then the asynchronous SeaweedFS vacuum compacts the volume files;
- `ops chunk verify`;
- restoring a snapshot into a new volume, which reads the chunks it has not made local from that snapshot (fork is not supported in this release).

A volume holds at most 10 snapshots. The next one is refused with `snapshot_limit` (HTTP 409, not retryable;
`at most 10 per volume; delete a snapshot, or let retention prune`) and nothing changes: no snapshot, no cut, HEAD
as it was (`TestChunkSnapshotPastTheLimitIsRefusedWithHEADUnchanged`). Delete a snapshot, or let retention prune,
then take it again. A scheduled snapshot refused this way still publishes the volume's image by an ordinary cut,
and `GET STATUS/status/durable?volume=<id>` shows `Chunk.ScheduledSnapshot.lastFailure.code: snapshot_limit`, then
`snapshot_overdue` (`TestScheduledSnapshotAtTheLimitStillPublishes`).

A snapshot can be refused with `host_slot_busy` / `no background slot` (retryable) while every host slot is held by
other volumes' cuts; retry with the same request id. With many volumes on a host, a snapshot may wait for a slot:
measured up to 15 s at 32 volumes with the default 3 slots.

A snapshot can answer `head_conflict` (HTTP 409, retryable): another write to the volume's HEAD, such as a snapshot
delete, landed during its commit. Retry it with the same name. In this release, if that retry answers `internal`
`snapshot "NAME" of VOL is not in HEAD after its commit`, retry once more. The volume's data is committed either way;
the snapshot name exists only after a request for it succeeds (check with `sw-block ops chunk snapshots`)
(`TestKnownDefectSnapshotRetryAfterAGenuineConflictCommitsTheDataUnnamed`). A retry that names the snapshot at once is
planned for 1.0.1.

A snapshot's time grows with the data changed since the volume's last cut: 64 GiB changed took 122 s (about 2 s
per GiB). `sw-block ops chunk snapshot` waits 60 s by default, so pass `--timeout` above the expected time; the
server bounds one request at 5 minutes whatever `--timeout` says. A request that runs
out of time answers a retryable code: `remote_unavailable` or `deadline_exceeded` when it ran out before its commit,
`outcome_unknown` when it ran out during the commit. Before the commit, the name is withdrawn and no later cut
publishes it; the volume's data is unchanged and stays queued for the next cut.
After `outcome_unknown`, the commit may have landed with the name.
Rerun the snapshot with the same name and a larger `--timeout`: a name already in HEAD is answered with the
snapshot it names, so a rerun never takes a second one. A rerun keeps what a timed-out request already
uploaded: it finds those chunks and sends only the rest. A longer or derived default is planned for 1.0.1.

With `--host-limit-dir` set (it is off by default), uploads are paced at `--host-limit-upload-bytes-sec` (64 MiB/s by
default) shared by the host's 4 slots, about 16 MiB/s per volume, and the pacing is charged for chunks already
uploaded too. A cut or snapshot with more than about 4.7 GiB changed then cannot finish within the 5 minutes, and a
rerun does not help: the volume is stuck. Do not use `--host-limit-dir` in 1.0.0 for volumes that change more than
about 4 GiB between snapshots. A fix is planned for 1.0.1.

## Moving a volume, and returning it

[Volume lifecycle](volume-lifecycle.md):
- the planned hand-over to another host or back to the same host;
- restarts inside and after the lease;
- the forced move when a host is lost, with the lost range;
- which of these procedures run end to end, and which do not.

## Replicated volumes (preview)

Replicated volumes (`replication_factor` above 1) are a preview in v1.0. In the acknowledging modes (`sync-quorum`,
`sync-all`) they keep acknowledged data. The default `best-effort` acknowledges a write with no replica, and a
`best-effort` volume is not failed over automatically: blockmaster holds it, and only the operator's forced failover
(`sw-block ops failover`) moves it, which may lose the writes acknowledged last.
Under a writing guest a single fault can stop writes until an operator
acts. The limits, as they show on a running system:

- **No snapshots, restore, moves or object-store copy.** v1.0 has two designs: the single-replica volume with the
  object store (`--durable-impl walstore-chunk`: snapshots, restore, moves) and replicated volumes on local stores
  without any of those. `walstore-chunk` refuses replication at start
  (`--durable-impl=walstore-chunk is single-replica: --replication-ack="sync-quorum" needs replica peers (...)`) and
  refuses any assignment with replica slots, because a chunk snapshot takes its position from the local log, which
  the replicas may not hold.

- **A replica that misses writes is not caught up by itself.** While the guest writes, the primary keeps only a few
  write-ahead-log entries beyond its last checkpoint. A replica that falls behind them (it was restarted, or a
  failover fenced it) needs a rebuild, and a rebuild onto the data it already has is refused
  (`re-add with a retained base is not supported in v1`). The replica stays out (`degraded`) until it is wiped and
  added again. `--wal-retention-lsns` does not help in this release: with it set, a lagging replica still always
  needed a rebuild.
  - **Wipe and add again (not a tested procedure in this preview):** first confirm from the primary's status which
    server is the left-out replica; never run this on the primary's host. On the replica's host, stop its `blockvolume`
    unit, remove everything under its `--durable-root`, and start the unit again with the same flags. It then
    starts empty and takes a full rebuild from the primary; until that ends, the primary's status lists it as not
    healthy, and the volume keeps serving while the remaining replicas meet the ack mode.
  - **If the re-add stops midway** (the rebuild is interrupted), the replica refuses every later rebuild: wipe it
    and add it again.
- **A durability shortfall is an immediate I/O error, not a wait.** When the replicas that must acknowledge a write
  cannot (`sync-quorum`: a quorum; `sync-all`: every replica), the write fails at once and the guest gets `EIO`.
  With three replicas and `sync-quorum`, one replica left out as above plus one more fault loses the quorum; a
  single failover can already leave too few replicas, depending on timing.
- **`sync-all` refuses every write while any replica is down.** It needs every replica for each write.
- **Under `best-effort` (the default), a replica can fall behind while it shows `healthy`.** A `best-effort` write
  waits for no replica, so a replica whose connection dropped can keep showing `healthy` in the primary's status
  while it misses writes. Use `sync-quorum` when the replicas must hold the data.
- **No path failover in the guest.** A guest connects to one target, and a target that is no longer the primary
  answers with a status the guest does not retry: its I/O fails with `EIO` instead of moving to another path. There is
  no multipath for replicated volumes in v1.0; after a failover the guest is started on the new primary's host (see
  [host connect settings](#host-connect-settings)).
- **The same holds on Kubernetes** for RF3 volumes. This preview has no remedy for a pod-backed replica that is
  left out: it stays out, and the volume serves while the remaining replicas meet the ack mode.

In the acknowledging modes, acknowledged writes were read back intact in every run that found these limits. What the
release supports without these limits is the single-replica volume.

**Planned for v1.0.1.** Two replicated configurations are planned to become supported:
- **RF3 `sync-quorum`, installed on hosts:** writes continue through one replica's fault, and that replica returns
  by itself.
- **RF2 `sync-quorum`, with its limit:** no acknowledged write is lost and failover is automatic, but writes stop
  while either replica is out.

`sync-all` and Kubernetes RF3 stay a preview. In every replicated configuration, an old primary cut off from
blockmaster by a partition may keep serving reads of older data until its lease ends.


## When a host is lost

The steps below are for a single-replica volume. A `sync-quorum` or `sync-all`
replicated volume under `--cluster-spec` with the lifecycle loop on is failed
over by blockmaster itself when its primary's heartbeat goes stale and enough
replicas answer the fencing round; the operator's `sw-block ops failover` is for
when that round cannot decide, and for a `best-effort` volume, which is never
failed over by itself (see
[replicated volumes in this release](standalone-deployment.md)).

1. Make sure the lost host stays down for this volume. The product does not power off an old holder.
   - If it comes back after the move, blockmaster refuses its lease: it does not serve, logs `exiting status=78
     reason=moved` and exits 78, and the example unit does not start it again
     (`TestProcedureForcedMoveWithTheOldHostReturning`).
   - Stop and disable that unit on the old host. Run `systemctl reset-failed` only after you have dealt with its
     local root ([do not](volume-lifecycle.md#do-not)).
2. Run the forced move to another registered node: `--dry-run` first, then the move. See
   [the holder never comes back](volume-lifecycle.md#after-a-crash).
   - It is admitted after the barrier: at most 65 s after the holder stopped, with default flags.
3. Read the printed lost range: writes acknowledged after the last cut and before the move's time may be lost.
   - Read it again later with `sw-block ops workspace lost-range`.
4. Start `blockvolume` on the new node, then reconnect the guest.
5. Optionally restore an older snapshot into a new volume and verify it with `ops chunk verify --depth data`.

## Host connect settings

The guest's kernel keeps an NVMe/TCP controller whose target went away and keeps reconnecting to the same address.
Three `nvme connect` options decide how long it waits and what the guest sees meanwhile. The values below are
derived from the waits these pages state, and were run with exactly these options (below).

| Option | Kernel default | Set it to | Why |
|---|---|---|---|
| `--ctrl-loss-tmo` | 600 s | 180 | Longer than the longest wait for the same target to serve again: a restarted holder is re-granted at most 65 s after it stopped; a start not granted within 30 s after that barrier ends its wait, and the unit starts it again after `RestartSec=5s` ([after a crash](volume-lifecycle.md#after-a-crash)): 65 + 30 + 5 = about 100 s |
| `--reconnect-delay` | 10 s | 2 | The controller retries every 2 s; connection setup and the network add to it. Linux bounds the retries by count, not by time: `--ctrl-loss-tmo` / `--reconnect-delay` = 90 attempts |
| `--fast_io_fail_tmo` | off | leave off | Any value shorter than the wait above fails I/O to the filesystem before the target serves again |

```bash
sudo nvme connect -t tcp -a 127.0.0.1 -s 4420 -n "$NQN" --ctrl-loss-tmo=180 --reconnect-delay=2
```

What the guest sees:
- **These values, holder process stopped** (its host up, so a connect attempt is refused at once; the case a
  standalone guest meets, since it connects over loopback to the holder on its own host): I/O waits and
  continues when the target serves again. Measured with exactly these options: the holder killed, a write issued,
  the holder started again 5 s later (as the unit would): the write completed after 6.1 s, with no error. Kept away,
  the write failed with `EIO` after 181.5 s (90 attempts, 2 s apart).
- **These values, host powered off or unreachable** (nothing answers, so each attempt waits for the connect timeout,
  about 3 s; the case of a guest on another host than the holder, which in v1 is a Kubernetes pod): the loss is
  noticed by keep-alive (5 s), then the 90 attempts take about 5 s each. Measured: a pending write failed with `EIO`
  after 464 s, about kato + 90 × (2 s + the connect timeout).
- **Kernel defaults** (`ctrl_loss_tmo` 600 s, `reconnect_delay` 10 s: 60 attempts; every connect command shipped
  today, including the CSI node): a target that does not come back holds I/O for about 600 s when its host refuses
  the connection (a 9-minute blocked read was observed in a fault run), and for about 60 × (10 s + 3 s), some 13
  minutes, when its host is unreachable (derived from the attempt count and the per-attempt connect timeout
  measured above; not run).
- **Too short** (`--ctrl-loss-tmo` or `--fast_io_fail_tmo` below the wait): I/O errors reach the filesystem before
  the target serves again; ext4 mounted with `errors=remount-ro` turns read-only and must be remounted.

A blockmaster outage longer than the lease stops the holders: a holder keeps serving until its lease ends (up to
20 s after its last renewal) and serves again at least 50 s after blockmaster is back
([what a blockmaster restart does to serving volumes](standalone-deployment.md#restoring-a-snapshot-into-a-new-volume)).
Guest I/O therefore waits at least the rest of the outage plus those 50 s; these pages give no upper bound. An outage
longer than `--ctrl-loss-tmo` less those 50 s can fail guest I/O. Raise `--ctrl-loss-tmo` if the guest must ride
through longer outages; I/O waits that long.

**After a failover the guest must connect to the new primary.** These options retry the configured address; they
never redirect it. A standalone volume's NVMe/TCP target binds loopback only (`--nvme-listen`; a non-loopback bind
needs `--allow-external-nvme-bind`, unauthenticated, for test gates only), so a guest reaches only the target on its
own host. Each replica's target reports itself optimized only while it is the healthy primary, but no guest is
connected to the other replicas' targets. A replicated `sync-quorum` volume gets its new primary on another host
about 30 to 35 s after its host is lost, with default flags (derived from `--freshness-window` and the two probe
rounds; not yet measured). In a standalone deployment the guest runs on the volume's primary host. If the host is lost, the guest goes down
with it; if only its holder stopped, the guest's I/O waits and then fails as above (or completes if the holder
restarts first). Either way, after a failover start the guest on the new primary's host and connect it there: that host's blockvolume logs `adopted as
PRIMARY` and its `/status?volume=<id>` shows `"AuthorityRole":"primary"`.

**Kubernetes.** The CSI node connects with a plain `nvme connect` and the kernel defaults. A pod whose holder does
not come back holds I/O for about 600 s when the holder's node is up, and some 13 minutes when the node is powered
off or unreachable (derived as above): its reads and writes block (processes in uninterruptible sleep, probes that
touch the volume time out), then fail. Recovery is by recreating the pod, which stages the volume again on the
current primary ([operations](operations-v1.md)). The CSI node passing the settings above is planned for 1.0.1.

## A volume waiting for blockmaster

A `blockvolume` that cannot get its lease from blockmaster waits inside its process: the unit stays **active**
and the volume does not serve. A monitor that only watches for failed units does not see it.
- `/status` shows `NotReadyReason=lease_not_held` and `MasterWait` with `Reason`, `SinceMs` and, when the wait has a
  bound, `UntilMs`; the process logs `blockvolume: waiting for the master: <reason> ...` when the reason changes and
  every 10 minutes.
- Alert when the reason is `unreachable`, `credentials` or `identity refused: ...` for more than 60 s, and on any
  reason for more than 5 minutes.
- `identity refused: unregistered` or `wrong_host`: blockmaster does not know the unit's `--server-id`, or the
  node calls from another address than the one registered for it in the cluster spec. The check compares the
  address exactly: an address change, another outgoing interface, IPv4 against IPv6, or a host name in the cluster
  spec all refuse the right host. Fix the cluster spec or the unit; the next call passes without a restart.

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
- **Cleaning is on by default.** The holder removes chunks no root names after each snapshot delete and every
  `--chunk-maintenance-interval` (hourly by default); `--chunk-maintenance-gc=false` turns it off, and then nothing
  removes them. The disk space returns when SeaweedFS vacuums its volumes.
- **Node reattach events under mutual TLS.** Node reattach events are not recorded in the cluster event log under
  mutual TLS: a CSI node's certificate is `viewer`, so blockmaster refuses the event and the node logs one line per
  reattach.

## Commands and their tests

Every command on the pages above, and the test that runs it.
- "binary": the test runs the real binaries (build tags `subprocess chunkstore_integration`).
- "CLI": the test drives the `sw-block` command with its flags.

| Command | Test | Kind |
|---|---|---|
| `blockmaster ... --cluster-spec ... --lifecycle-store ... --workspace-rpc ...` | `TestStandalonePlannedHandoffToTheSameHost` (`cmd/blockvolume`) | binary |
| `blockmaster ... --cluster-spec ... --expected-slots-per-volume 3 --lifecycle-product-loop-interval ...` (a replicated volume) | `TestDoubleFailoverReadsBackEveryAckedWrite`, `TestFencingRoundHealContinuesThePrimary` (`cmd/blockvolume`, run with `200ms`) | binary |
| `blockvolume ... --durable-impl walstore-chunk ... --workspace-lease ...` | same | binary |
| `sw-block ops workspace show`, `revoke --sealed-gen`, `move --clean` | same | binary |
| `sw-block ops chunk detach --plain`, `chunk show`, `chunk cut` | same; `TestProcedureForcedMoveWithTheOldHostReturning` | binary |
| `sw-block ops workspace force-move [--dry-run]`, `lost-range` | `TestProcedureForcedMoveWithTheOldHostReturning`, `TestProcedureRevokeWithoutACKThenForcedMove` | binary |
| `sw-block ops volume-unheld --path` | `cmd/sw-block/volume_unheld_test.go` | CLI |
| `sw-block ops chunk snapshot`, `snapshots`, `snapshot-delete` | `cmd/sw-block/chunk_test.go` | CLI |
| `sw-block ops chunk verify --depth data [--orphans]` | `cmd/sw-block/chunk_verify_test.go` | CLI |
| `sw-block ops chunk restore --source --snapshot --target`, `--volume ... [--status]` | `cmd/sw-block/chunk_restore_test.go` | CLI |
| `curl STATUS/status?volume=v1` | the status checks in the binary tests above | binary |
| `nvme connect`, `nvme list`, `nvme disconnect` | not run here: the binary tests use an NVMe/TCP host inside the test | not run |
| `blockvolume` under systemd: exit 75 after an expired start, restart, re-grant | `TestSystemdExpiredStartupExits75ThenRegrantServes` (`cmd/blockvolume`, a real systemd manager, tag `systemd_integration`) | binary |
| The shipped units' start limits (`StartLimitIntervalSec=600`, `StartLimitBurst=10`, `RestartPreventExitStatus=78`) | `TestBlockvolumeUnitsBoundStartupRetries` (`internal/testops`, reads the unit files) | unit files |
| `systemctl enable`, `stop`, `disable`, `reset-failed`, `systemd-tmpfiles --create` as written on these pages | not run: the tests above start transient units, not the installed ones | not run |
| Headroom refusal at create, and both ways out (free space; `--durable-root` on a file system with room) | `TestHeadroomRefusesRestoreTargetsThenServesThem`, `TestHeadroomRefusedTargetBlastRadiusAndAnotherRoot`, `TestHeadroomRefusedRestoreTargetKeepsItsSourceSnapshot` (`cmd/blockvolume`, root, loop-mounted file systems) | binary |
| A snapshot refused with `host_slot_busy` / `no background slot` while every host slot is held, retried with the same request id | `TestSnapshotLadderF1VolumesOneHost` (`cmd/blockvolume`, `F1_RUN=1`, `F1_HOST_LIMIT_DIR` set, 3 slots; `F1_SLOT_RETRY_SECONDS=0` is its negative control) | binary |
| Full local disk; full `--host-limit-dir` | `TestProcedureLocalDiskFull` (`cmd/blockvolume`, root, `testdata/diskfull.sh`); what a Linux guest sees: `TestProcedureLocalDiskFullKernelInitiator` (kernel NVMe/TCP host) | binary |
| A first start with the wrong `--durable-blocks`: refused before any local store is created (exit 78) | `TestWrongGeometryFlagIsRefusedBeforeTheStoreIsCreated` (`cmd/blockvolume`) | binary |
| A first start over a corrupt volume record: exit 78 before any local store is created; a start once the record is restored | `TestCorruptRecordIsRefusedBeforeTheStoreIsCreated` (`cmd/blockvolume`) | binary |
| A wrong-size local store left by an earlier release (`geometry_mismatch` at attach): the guard (`sw-block ops chunk show`), removing the local files, a start with the right flags | `TestWrongGeometryCureAsWritten` (`cmd/blockvolume`; the store is planted from a capture of such a run) | binary |
| SeaweedFS (filer or volume server) down during a cut | `TestProcedureStoreDownDuringACut` (`cmd/blockvolume`) | binary |
| `blockmaster` refuses a `--workspace-max-clock-skew` whose half does not exceed its fence-exit budget (at or below 10.1 s) | `TestBlockmasterRefusesASkewBoundAtOrBelowTheFenceExit` (`cmd/blockvolume`, tag `subprocess`) | binary |
| The missing lock directory (`host volume lock directory unavailable`) and its cure (tmpfiles entry or chown) | not run in 1.0 | not run |
| [First volume (read-only check)](standalone-deployment.md#first-volume-read-only-check): `swb_start`, `swb_show`, `swb_stop` | not run in 1.0 (needs `sudo` and the kernel NVMe/TCP host) | not run |
| [Write test](standalone-deployment.md#write-test-scratch-host-only): `scripts/run-nvme-os-smoke.sh` | not run in 1.0 | not run |
| [Upgrade order](standalone-deployment.md#upgrade-order): `blockmaster` first, then `blockvolume` | not run in 1.0 (no mixed-version run) | not run |
| Scheduled snapshots (`--chunk-snapshot-schedule`) and periodic capture (`--chunk-capture-interval`) on a running volume | not run in 1.0 as written: the schedule is tested with a fake clock in `core/host/volume`, the interval only as a flag; the binary tests cut and snapshot on demand | not run |
| SeaweedFS vacuum (`/vol/vacuum`, `volume.vacuum`) | not run in 1.0: a SeaweedFS command; the tests check that GC deletes the chunks, not that SeaweedFS compacts its files | not run |
