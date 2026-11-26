# Volume lifecycle (walstore-chunk workspace volumes)

**Use this when:** you run a single-copy `walstore-chunk` volume with
`--workspace-lease` on the [standalone deployment](standalone-deployment.md),
and you need to stop it, hand it to another host or back to the same host, or
recover it after a crash.

**Source of truth:** the flags in `cmd/blockmaster/main.go` and
`cmd/blockvolume/main.go`, and the commands in `cmd/sw-block/workspace*.go`
and `cmd/sw-block/chunk*.go`. If this page and `--help` disagree, `--help` wins.

Snapshots, retention and restore are in
[Snapshots, restore and space](standalone-deployment.md#snapshots-restore-and-space-walstore-chunk).

## Words used on this page

- **Holder:** the one `blockvolume` process allowed to serve the volume. It is
  named by the logical replica id and an **epoch**, for example `r1@3`. Every
  move gives a higher epoch.
- **Lease:** blockmaster's permission for the holder to serve, 30 seconds
  (fixed), renewed by the holder every few seconds. Without a valid lease the
  holder serves no I/O and takes no cut.
- **Cut:** one upload of the changed data to the object store, ending with a
  new **HEAD** (the volume's root record in the object store, one per volume,
  with a generation number that grows with every change).
- **Seal:** the final cut of a volume that takes no more writes. The sealed
  HEAD has `State: "detached"` and names the holder, epoch, claim id and position.
- **ACK:** the old holder's acknowledgement, recorded by blockmaster, that it
  stopped and read the sealed HEAD at generation G.

## The three records, and what each proves

| Record | How to read it | What it proves |
|---|---|---|
| The lease | `sw-block ops workspace show --master MASTER --volume v1`: `replica:`, `epoch:`, `lease state:`, `holder: r1@E` | Which holder blockmaster lets serve. `granted`, `revoking` or `expired` is the host validity at query time, not permission to move |
| HEAD | `sw-block ops chunk show --master MASTER --volume v1 --chunk-filer FILER`: `head.Gen`, `head.Epoch`, `head.Holder`, `head.ClaimID`, `head.Position`, `head.State`, `head.Current` | The committed image through `Position`. `State` `detached` names the holder's stopped image; there is no separate `detachedBy` field |
| The ACK | the same `show`: `recorded ACK: r1@E HEAD generation G` | The old holder drained and closed after reading the sealed HEAD G. A clean move needs it |

`ops chunk show` prints JSON with `volume` and `head` objects. The HEAD view
uses the field spellings above (capitalized); it is not the on-store HEAD
encoding. A live HEAD has `State: ""`; a stopped HEAD has `State: "detached"`.
`Position` is the committed write position, `ClaimID` identifies the claim,
and `Current` names the committed root (null before an image is committed).
`Gen` is the current record generation, not the holder's identity.

`MASTER` is blockmaster's `--listen` address, `FILER` the SeaweedFS filer gRPC
address (`--chunk-filer`), `STATUS` a blockvolume's `--status-addr` (with
`--chunk-control-endpoint`; it answers on loopback only, so run `ops chunk
detach` on the volume's own host).

## Setup

First do [Required setup: make the volume known to blockmaster](standalone-deployment.md#required-setup-make-the-volume-known-to-blockmaster).
For a workspace volume that can change host, this page adds three things.

**1. Which setup you have.** Run
`sw-block ops workspace show --master MASTER --volume v1`:

| Answer | Setup | What this page offers |
|---|---|---|
| exit 0, `volume: v1 (query snapshot, ...)` | cluster spec with a lifecycle store and workspace RPCs | everything below |
| exit 1, ends with `workspace RPC disabled` or `lifecycle store unavailable` | `--topology` only | **nothing on this page.** With `--workspace-lease` the volume never gets a lease and never serves; without it, it serves on its host but cannot be moved: neither the planned hand-over nor the forced move works |
| exit 1, ends with `workspace move requires an existing RF1 volume` | lifecycle store without a volume record | fix the cluster spec below |

**2. The volume record comes from the cluster spec.** There is no command that
creates it. A volume in `--cluster-spec` with `size_bytes` above 0 is
registered when blockmaster starts; its `placements` say where it starts.
List every host the volume may move to under `nodes`, with addresses on that
host. Example for one host (`/etc/sw-block/cluster-spec.yaml`):

```yaml
nodes:
  - server_id: s1
    data_addr: 127.0.0.1:9201
    ctrl_addr: 127.0.0.1:9202
volumes:
  - id: v1
    size_bytes: 268435456      # 65536 blocks x 4096 bytes, as on blockvolume
    replication_factor: 1
    placements:
      - server_id: s1
        replica_id: r1
```

A restore target is the one exception: list it with `id`,
`replication_factor` and `placements` only, **without `size_bytes`**. Such an
entry places the volume but creates no record; `sw-block ops chunk restore`
creates the record, with the source snapshot's geometry. With `size_bytes` the
entry creates an ordinary empty volume, and the restore into it is refused
with `target_exists`. Use a new name: a name that was ever deleted is never
created again. blockmaster reads the cluster spec only at start, so adding the
target means restarting it: see [what that does to serving volumes](standalone-deployment.md#restoring-a-snapshot-into-a-new-volume).

```yaml
  - id: v2                     # restore target: no size_bytes
    replication_factor: 1
    placements:
      - server_id: s2
        replica_id: r1
```

In a multi-host cluster, register each node by the address it connects to
blockmaster from, never loopback. One server id names one host: a volume host whose
server id is registered on `127.0.0.1` is refused with `server id s1 is registered on
host 127.0.0.1 but connects from <address>: register the node by the address it
connects from (not loopback) in the cluster spec`. The loopback
addresses above fit only when blockmaster and blockvolume run on the same machine.

**3. blockmaster flags.** `--cluster-spec` replaces `--topology` (setting both
stops blockmaster). blockmaster reads HEAD through the layout catalog: `--chunk-filer`
is the filer gRPC address and `--chunk-path` is blockvolume's:

```bash
blockmaster --authority-store /var/lib/sw-block-master/authority \
  --lifecycle-store /var/lib/sw-block-master/lifecycle \
  --cluster-spec /etc/sw-block/cluster-spec.yaml --expected-slots-per-volume 1 \
  --listen 127.0.0.1:9180 --workspace-rpc \
  --chunk-filer FILER_GRPC --chunk-path /swblock/chunks
```

This page's volumes have one replica. A volume with `replication_factor`
above 1 also needs `--lifecycle-product-loop-interval 100ms` (the shipped
chart's value; the default `0` turns the loop off and such a volume never gets
a primary): see [the replicated start](standalone-deployment.md).

With the example units: `SWB_MASTER_TOPOLOGY=` (empty), `SWB_MASTER_EXPECTED_SLOTS=1`,
`SWB_MASTER_WORKSPACE_RPC=true`, and `--lifecycle-store`, `--cluster-spec`,
`--chunk-filer` and `--chunk-path` in `SWB_MASTER_EXTRA_ARGS`. blockmaster
reads the file only at start.

blockvolume runs with its identity from the spec (`--server-id s1 --volume-id v1
--replica-id r1`) and:

```bash
--durable-impl walstore-chunk --durable-root /var/lib/sw-block-volume/volumes \
--durable-blocks 65536 --durable-blocksize 4096 --chunk-filer FILER \
--chunk-path /swblock/chunks --chunk-control-endpoint --workspace-lease
```

**Check:** blockvolume logs `blockvolume: volume v1 adopted as PRIMARY at r1@E`,
and `ops workspace show` prints `lease state: granted` and `holder: r1@E`.

## Rule: one live process per volume per host

`blockvolume` takes an exclusive lock on `/run/sw-block/locks/<volume id>.lock`
before it opens the volume's store and holds it until it exits. A second
`blockvolume` for the same volume on the same host is refused at start, whatever
its `--durable-root`. The kernel releases the lock when the process exits, also
on a crash or a kill; there is nothing to clean up. Setup and limits are in
[One live process per volume per host](releases/one-process-per-volume.md).

The lock covers one host. Two hosts are kept apart by blockmaster's assignment
and the lease, not by this lock.

An existing local root attaches only if its durable position reaches the
`Position` of HEAD. A root that is behind (a copy taken before a later cut)
is refused, never serves, and its process exits 78 (no automatic restart) with
`reason=superseded cure="remove or quarantine the local root to attach fresh
from HEAD"`. A power loss does not cause this: a cut makes its position durable
locally before it publishes it.

Before you move a local root aside, make sure its process is gone:

```bash
systemctl status sw-blockvolume        # inactive (dead)
sw-block ops volume-unheld --path /var/lib/sw-block-volume/volumes/v1.bin
```

`volume-unheld` exits 0 (`unheld: ...`) only when no process on this host holds
the file; exit 1 is `held: a live process on this host holds ...`. It proves
nothing about another host.

### Do not

These are rules for the operator. The product does not detect every breach.

- **One local root per volume per host.** A second `blockvolume` for the volume on the
  same host is refused by the lock; do not work around it (another lock directory, a
  container that does not share the host's `/run/sw-block/locks`).
- **Never restore a local root from a copy** (a file copy, a disk snapshot, a backup).
  A copy older than the last cut is refused. A copy taken after the last cut is NOT
  detected: it attaches and loses the writes acknowledged after the copy was taken. To go
  back, start from the remote image with a fresh root.
- **After a forced move, stop and disable the old host's unit.** Until you do, a
  start of it is refused by blockmaster: it logs `exiting status=78 reason=moved`,
  exits 78 and cannot serve. The shipped unit does not restart a process that exits
  78, so it stays failed; disable it so that a reboot does not start it again.
  This refusal can happen before any assignment is delivered to the old host;
  its `reason=moved` exit is the evidence that the start was refused.
- **Before moving a volume back to a host that held it earlier, move that host's old
  root aside** (after the check above). The old root is stale; the new holder must start
  on a fresh one.
- **Never revoke before the seal is confirmed** (see the hand-over steps): a revoke
  without an ACK cannot be undone on one node.
- **Never restore blockmaster's directory from an older copy** (its `--authority-store`
  and `--lifecycle-store`). They decide who may write each volume. An older copy can hand
  out epochs that were already used; do not do it. A lost store needs a recovery
  procedure, not a restore.
- **Run exactly one blockmaster per cluster.** It alone issues each volume's epochs and
  leases; there is no second or standby blockmaster in this release.
- **A deleted volume's name cannot be created again.** Its record stays, deleted, and a
  create under that name is refused. To start over, create the new volume under another
  name. (A snapshot name is different: once its delete has finished, the name can be
  taken again.)

## Which procedures run end to end

Each procedure below runs as written on this page, with the real binaries, two
`blockvolume` processes with distinct server ids (`s1`, `s2`) on one host, one
`blockmaster` and SeaweedFS (tests in `cmd/blockvolume`, build tags
`subprocess chunkstore_integration`).

| Procedure | Test | Result |
|---|---|---|
| Planned hand-over to another host | `TestProcedurePlannedHandoffToAnotherHost` | runs |
| Planned return to the same host | `TestStandalonePlannedHandoffToTheSameHost` | runs |
| Clean move refused without the ACK | `TestStandaloneCleanMoveWithoutTheACKIsRefused` | runs |
| Same host: restart inside the lease | `TestProcedureSameHostRestartInsideTheLease` | runs |
| After a crash: return after a lapsed lease (started 65 s after the stop) | `TestProcedureReturnAfterALapsedLease` | runs |
| Revoke without the ACK, then forced move to another node | `TestProcedureRevokeWithoutACKThenForcedMove` | runs |
| Forced move, then the old host returns | `TestProcedureForcedMoveWithTheOldHostReturning` | runs: the returning old host does not take the new host's assignment, does not report ready and does not listen for I/O; it logs `exiting status=78 reason=moved` and exits 78, on its old local root and on a fresh one; the new host keeps serving |
| Same host: restart after a crash inside the lease | `TestProcedureSameHostCrashRestartInsideTheLease` | runs |
| After a crash: started 40 s after the stop, the barrier passes inside the start wait | `TestProcedureStartInsideTheBarrierWaitIsReGranted` | runs: does not serve while renewals are refused, serves once re-granted |
| Delete a volume: seal, revoke, ACK, delete the snapshots, volume-delete | `TestProcedureDeleteAVolume` | runs: refused while a snapshot exists, with nothing deleted; completes after the snapshot is deleted; a rerun with the same request id completes again |
| Delete after the process was stopped by a signal (start again, revoke, ACK, delete) | `TestProcedureDeleteAfterASignalStop` | runs: refused for the missing ACK, with nothing deleted; completes after the restart, the revoke and the ACK |
| After a crash: the process is killed while the guest writes, restart inside the lease | `TestProcedureCrashWithIOInFlight` | runs: every write acknowledged by a flush reads back |
| The holder cannot reach blockmaster, then a forced move | `TestProcedureMasterUnreachableFencesBeforeForcedMove` | runs: the forced move is refused while the holder is alive and admitted only after it exited 75 (`reason=lease_expired`) |
| Hand-over between two machines | none | not run (the tests use one machine's loopback) |

## Planned hand-over

Nothing the volume acknowledged is lost. The target is another registered
host, or the same host (next section).

Which steps run end to end is listed in [Which procedures run end to end](#which-procedures-run-end-to-end).

**1. Read the holder.**

```bash
sw-block ops workspace show --master MASTER --volume v1
```

Copy `replica:` (`R`) and `epoch:` (`E`). `lease state:` must be `granted`.

**2. Stop the writer.** Stop the guest or unmount the file system, so that no
I/O is in flight. Leave `blockvolume` running: the seal runs inside it.

**3. Seal (plain detach)**, while the lease is granted:

```bash
sw-block ops chunk detach --plain --volume v1 --status-addr STATUS --timeout 120s
```

Exit 0: stdout is JSON with `volumeID`, `cutLSN`, `headGen`, `headEpoch` and
`lastWrittenLSN`; stderr has `sealed HEAD generation: G`. Copy `G`. From now on
the volume refuses I/O on this host. Do not go on after any other answer (see
the table below). Repeating the command is safe: it answers from the sealed
HEAD.

To check: `sw-block ops chunk show --master MASTER --volume v1 --chunk-filer FILER` shows
`head.State` `detached`, `head.Holder` `s1/R`, `head.Epoch` E, a nonempty
`head.ClaimID`, `head.Gen` G and a non-null `head.Current`. Its `head.Position`
is the sealed position L; the HEAD itself is the stop proof, with no `detachedBy`.

**4. Revoke**, giving the generation you copied:

```bash
sw-block ops workspace revoke --master MASTER --volume v1 --replica R --epoch E --sealed-gen G
```

Exit 0: `revocation requested: volume v1 holder R@E expiry_ns ...` and
`operator-stated sealed HEAD generation: G (not verified); ...`. The CLI does
not check `G`; blockmaster checks it at the move.

**Do not stop `blockvolume` yourself now.** It sees the revocation at its next
renewal, drains, reads the sealed HEAD, sends the ACK and exits with status 0
(so `Restart=on-failure` does not start it again). A signal skips the ACK. It
must ACK before its lease ends (30 seconds).

**5. Wait for the ACK.** Repeat `ops workspace show` until it prints:

```text
recorded ACK: R@E HEAD generation G
```

The revoke reply alone is not the ACK.

**6. Move (clean):**

```bash
sw-block ops workspace move --clean --master MASTER --volume v1 --replica R --epoch E \
  --target-host S2 --target-data DATA2 --target-ctrl CTRL2 --request-id move-v1-1
```

`S2` is a `server_id` under `nodes` in the cluster spec; `DATA2` and `CTRL2`
are addresses on that node's host (any free port). Exit 0:

```text
clean move issued: volume v1 request move-v1-1: R@E -> R@E2 on S2 (sealed HEAD gen G, remote LSN L)
next: start volume v1 on S2 (this command moved authority only)
```

If the outcome is unknown (exit 3), run the same command again with the **same**
`--request-id`; never a new one. Rerunning a move that was already issued is safe:
it prints the issued move again.

**7. Start the new holder within 30 seconds.** The move starts the new epoch's
30-second lease. On `S2`, start `blockvolume` with `--server-id S2`, the same
`--volume-id` and `--replica-id`, `--data-addr DATA2 --ctrl-addr CTRL2`, the
same chunk flags and geometry, and no local copy of `v1` (a host whose old copy
predates another host's HEAD refuses it, see the table). If you start it
later, its first renewal waits until 65 seconds after the move: start it after
that, as in [after a crash](#after-a-crash).

**8. Verify.** The new holder logs `blockvolume: volume v1 adopted as
PRIMARY at R@E2`. Its completed claim is observable in HEAD. `ops workspace show`
prints `holder: R@E2` and a line
`move: request=move-v1-1 kind=clean issued=true ...`. `ops chunk show` shows
`head.State` `""` (live), `head.Epoch` E2, `head.Holder` `S2/R`, a nonempty
`head.ClaimID`, `head.Gen` above the sealed generation G, a non-null `head.Current`
and `head.Position` L. Reconnect the guest to the new host.

## Same host

**Restart without a hand-over.** `systemctl restart sw-blockvolume` keeps the
holder and epoch. If the new process renews within the 30-second lease, it
serves again at once from its local WAL: nothing acknowledged is lost. If not,
see [after a crash](#after-a-crash).

**Planned return to the same host** (for example: stop tonight, start
tomorrow). Do steps 1 to 5 above. The volume then stays sealed and no process
needs to run; the seal and ACK stay valid while HEAD is unchanged. To start
again, do step 6 with your own host as target (`--target-host s1` and the
volume's usual `--target-data`/`--target-ctrl`), then step 7 with the unchanged
`blockvolume` settings and local root. The new process starts at epoch E2,
claims the sealed HEAD and serves its sealed image. Starting `blockvolume`
without the move does not serve: a sealed HEAD stays sealed for its own epoch.

## After a crash

**The holder comes back within the lease.** A restarted process (the example
unit restarts a crashed one after `RestartSec=5s`) renews at once and serves
from its local WAL.

**The lease lapsed, and the holder is still the recorded holder.** A holder whose
lease lapses fences itself, drains and exits with status 75, logging
`workspace lease: fenced volume=v1 replica=r1 epoch=E: lease expired at ...` and
`blockvolume: exiting status=75 reason=lease_expired`. The example unit starts it
again (`Restart=on-failure`, `RestartSec=5s`); so does any supervisor that restarts on
a non-zero status. You do not need to time the restart.

blockmaster grants the lease again only after a barrier: the stored lease horizon
(up to 45 s after the last renewal) and the last grant, each plus
`--workspace-max-clock-skew` (default `20s`). With the defaults that is at most
**65 seconds after the process stopped**; if blockmaster restarted meanwhile, also at
least 50 seconds (30 s plus the skew) after that restart, whichever is later. The
refusal says when: `workspace lease: renewal failed after ...: workspace forced move
lease expiry plus clock-skew margin has not elapsed (re-grant refused until <time>, in
<duration>)`. The process sleeps until just after that time (then polls every 200 ms
for 5 s), does not serve meanwhile, and `/status` shows `MasterWait` with reason
`barrier` and that time. After the grant it reclaims HEAD and serves its local WAL:
nothing acknowledged is lost.

**A start waits for its lease.** While blockmaster gives no reply, or refuses the
node's credentials or identity, the wait has no bound: the process serves nothing, holds no lease,
and logs `blockvolume: waiting for the master: <reason> since <time> (no bound)` when
the reason changes and every 10 minutes (`/status` shows the same as `MasterWait`).
The wait ends 30 seconds after the first answer received once the local store is
open and recovered, or 30 seconds after the master's barrier time when that is later
(fixed, no flag): a slow open does not use up the wait, and a start before the
barrier waits through it and serves once re-granted
(`TestProcedureStartInsideTheBarrierWaitIsReGranted`). Only when blockmaster keeps
answering without a grant for 30 seconds past its barrier does the process log
`durable: chunk authority wait for volume=v1 ended after ...: assignment unavailable,
not serving (...)` and exit with `blockvolume: exiting status=75
reason=assignment_timeout`; the unit starts it again. A master outage therefore costs
at most one restart (the holder's lease fence), not the unit's start limit (10 starts
in 10 minutes). A master that answers and drops in turn postpones the 30 seconds for
as long as it flaps.

No re-grant happens after a revoke, a recorded ACK or a move of the volume.

**The holder never comes back: forced move.** The new holder starts from the
last HEAD. **Writes acknowledged after that HEAD's cut are lost.** The target
must be another registered node; the same host is refused.

```bash
sw-block ops workspace force-move --master MASTER --volume v1 \
  --target-host S2 --target-data DATA2 --target-ctrl CTRL2 \
  --request-id force-v1-1 --confirm-forced v1@E --dry-run
```

`--dry-run` writes nothing; it shows whether the move is admitted and, if not,
`admissible at <time>, master clock`. The same barrier as above applies; there
is no bypass. Run the command again without `--dry-run`. It prints the lost
range, for example `volume v1 restored to the last published image at <time>
(gen N, LSN L): writes with LSN <= L are preserved; writes acknowledged by r1@E
after that and before <time>, master clock, may be lost; ...`, and `next: start
volume v1 on S2`. Read it later with `sw-block ops workspace lost-range
--master MASTER --volume v1`. Then do step 7 on `S2`. Exit codes: 0 done, 1
refused (nothing moved), 2 refused locally, 3 unknown (rerun with the same
`--request-id`), 4 issued but the lost range is not proven.

The product does not power off an old holder that is still alive: it stops
serving when its lease ends. Make sure it stays down.

## Delete a volume

A volume is deleted only after its holder has given it up: sealed, revoked and
acknowledged, exactly as in steps 2 to 5 of the planned hand-over, and after its
snapshots are deleted. Sealing and stopping the process is not enough: until the
ACK is recorded, blockmaster still counts the volume as in use.

**1. Stop the writer and seal** (hand-over steps 2 and 3):

```bash
sw-block ops chunk detach --plain --volume v1 --status-addr STATUS --timeout 120s
```

It answers `{"volumeID":"v1","cutLSN":...,"headGen":G,"headEpoch":E,...}` and prints
`sealed HEAD generation: G`.

**2. Revoke and wait for the ACK** (hand-over steps 4 and 5). Do not stop
`blockvolume` yourself: it sends the ACK and exits with status 0 on its own.

```bash
sw-block ops workspace revoke --master MASTER --volume v1 --replica R --epoch E --sealed-gen G
sw-block ops workspace show   --master MASTER --volume v1
```

`show` must print `lease state: revoking` and `recorded ACK: R@E HEAD generation G`.

**3. Delete every snapshot of the volume:**

```bash
sw-block ops chunk snapshots       --master MASTER --volume v1 --chunk-filer FILER
sw-block ops chunk snapshot-delete --master MASTER --volume v1 --chunk-filer FILER --name s1
```

`snapshots` answers a JSON array with one entry per published snapshot, in name
order: `[{"name":"s1","root":"<64 hex>"}]`. An entry also lists
`"dependents":["v2-<id8>"]` while restore targets depend on that snapshot. An
entry has no state or cut position: a listed snapshot is published. Each delete
answers `{"volume":"v1-<id8>","name":"s1","state":"absent"}`. Here `v1-<id8>` is
the volume's store directory: its name and the first 8 hex digits of its layout id.
A delete is one conditional update of HEAD. If another change of HEAD (the
holder's cleaning, which runs after a delete, or a snapshot) lands between its
read and its write, it exits 1 with `"code":"conflict"` and `"retryable":true`
and changes nothing: run it again.

**4. Delete the volume:**

```bash
sw-block ops chunk volume-delete --volume v1 --chunk-filer FILER --master MASTER --request-id del-v1
```

On completion it answers `{"volume":"v1-<id8>","state":"deleted"}`, where
`<id8>` is the first eight hex digits of the volume's layout id. `volume` in
this result is the store directory; subsequent commands still use the public
name `--volume v1`.
Keep the `--request-id` and use the same one if you have to run the command again.
Afterwards `sw-block ops chunk show --master MASTER --volume v1 --chunk-filer FILER` shows
`head.State` `deleted`. The HEAD view has no `chunkCount` field; use the
volume's object listing or verification report to check its remaining objects.

**If you sealed and then stopped `blockvolume` with a signal,** the ACK was skipped
and step 4 is refused with `live authority requires clean revocation ACK`. Nothing
was deleted. Start `blockvolume` again with the same flags and local root, then do
step 2 and continue. This recovery runs as written (`TestProcedureDeleteAfterASignalStop`).

**A volume that never served** has no holder to seal or ACK: its blockvolume
was stopped at its first lease grant, or a clean move went to a host that
never started. Run step 4 without steps 1-3. While the lease that start was
granted could still be live, step 4 is refused with `workspace delete: live
until <time>: ... lease expiry plus clock-skew margin has not elapsed`, and
nothing is deleted. Nobody renews that lease, so run the same command (the same
`--request-id`) after `<time>`, about 60 seconds later with default flags;
it then completes. In this release the delete waits for that time
(`TestNeverStartedLineIsDeletableWithoutARevoke`).

## Snapshot names and retries

A snapshot request first takes its name, then publishes the snapshot. Three rules
follow from that order:

- **A name taken by an unfinished request is not listed, but it is held.**
  `sw-block ops chunk snapshots` shows only published snapshots. If a snapshot request
  was interrupted after it took its name (the process crashed, or the caller gave up),
  the name does not appear in the list, yet another request for that name is refused
  with `snapshot_exists`, and deleting the volume is refused with `volume v1 has
  snapshots (NAME); delete them first`. Free it with
  `sw-block ops chunk snapshot-delete --master MASTER --volume v1 --chunk-filer FILER --name NAME`,
  or finish it by repeating the request with the same `--request-id`.
- **After `outcome_unknown`, repeat the request with the same `--request-id` within
  5 minutes.** Later, the unfinished attempt may have been swept, and the request is
  then refused with `this request's attempt was aborted; delete the name to reuse
  it`: delete the name and take the snapshot again.
- **A `--request-id` protects against a duplicate only while its snapshot name
  exists.** After `snapshot-delete`, repeating the same request takes a new snapshot.
- **A repeated request never reports success for a snapshot that is being deleted.**
  If the snapshot of a `--request-id` is being deleted when the request is repeated, the
  answer is `snapshot_deleting ... is being deleted; wait for the delete to finish, then
  reuse the name`, not the earlier success.

## Symptoms

| You see | It means | Do |
|---|---|---|
| `sw-block ops chunk detach: plain_detach_busy: durable: plain detach requires idle frontend I/O` | I/O was in flight; nothing changed | Stop the writer, retry step 3 |
| detach answers `detach_aborted` | Nothing was published; the volume serves here again | Retry step 3 |
| detach answers `outcome_unknown` | The seal may have landed; I/O stays stopped | Retry step 3: it settles the outcome |
| volume-delete: `volume_in_use: ... live authority requires clean revocation ACK` | The volume was sealed (or only stopped) but never revoked and acknowledged; nothing was deleted | "Delete a volume", step 2 |
| volume-delete: `volume v1 has snapshots (s1); delete them first` | Snapshots still exist, or a name is held by an unfinished request (such a name is not in the list); nothing was deleted | "Delete a volume", step 3; for a name that is not listed, "Snapshot names and retries" |
| snapshot: `snapshot_exists ... this request's attempt was aborted; delete the name to reuse it` | An earlier attempt of this request did not finish and was swept | `snapshot-delete` the name, then take the snapshot again |
| snapshot: `snapshot_deleting ... is being deleted; wait for the delete to finish, then reuse the name` | The request was repeated while its snapshot is being deleted | Wait for the delete to finish; take the snapshot again if it is still wanted |
| revoke: `workspace revoke: no matching lease` | `--replica`/`--epoch` are not the current holder | Read them again with `show` |
| move: `workspace clean move needs detached HEAD and matching revocation ACK` | No ACK yet, or its generation differs from HEAD | Wait for the ACK line (step 5); compare `G` with `ops chunk show` |
| move: `move target node "S2" is not registered` | `S2` is not under `nodes` | Add it to the cluster spec, restart blockmaster |
| move: `move target endpoints ... are not registered (...)` | The addresses are not on the node's host | Use addresses on that host |
| move: `move target "s1" is the current source host` | A forced move named its own host | Choose another node |
| old holder logs `blockvolume: workspace lease closeout: workspace lease expired before clean ACK` | Revoked, but no ACK | Only a forced move to another node remains (HEAD is sealed: if step 3 succeeded, nothing is lost) |
| renewal fails with `workspace lease: revocation or move pending` | The volume was revoked or moved | The process cannot serve again; continue the hand-over |
| renewal fails with `workspace lease: stale authority` | A move gave the volume a newer epoch | This process is the old holder: stop it, do not restart it |
| renewal fails with `... clock-skew margin has not elapsed (re-grant refused until <time>, ...)` | Waiting for the re-grant barrier | Wait: the process sleeps until that time (at most 65 s after the stop, with defaults) |
| `blockvolume: waiting for the master: unreachable since <time> (no bound)` | blockmaster gives no reply; the process waits without exiting | Bring blockmaster back; the process continues by itself |
| `blockvolume: waiting for the master: credentials since <time> (no bound)` | blockmaster refuses this node's credentials; the process waits without exiting | Fix the node's certificate or role; the next call passes without a restart |
| `blockvolume: waiting for the master: identity refused: unregistered` (or `wrong_host`) `since <time> (no bound)` | blockmaster does not know this `--server-id`, or the node calls from another host than its registered one; the process waits, never serves | Fix the cluster spec (the node's id and the address it connects from) or the unit's `--server-id`; the next call passes without a restart |
| `durable: chunk authority wait for volume=v1 ended after ...: assignment unavailable, not serving` | blockmaster answered for 30 s (past its barrier) and granted no lease; the process then exits 75 (`reason=assignment_timeout`) | Nothing under the example unit: it is started again. A unit for a volume this host no longer serves stops at its start limit: disable it |
| `blockvolume: exiting status=78 reason=moved` | blockmaster refused the lease: the volume moved to another host at a newer epoch. Not restarted | The volume serves elsewhere. If it is later moved back to this host, start the unit again (`systemctl start`); otherwise disable it |
| `blockvolume: exiting status=78 reason=deleted` | blockmaster refused the lease: the volume's delete is committed. Not restarted | Disable the unit and remove its settings |
| `durable: refusing to attach volume=v1 root=...: HEAD holder=... is not this holder ... (remove or quarantine the local root)` | This host's local copy is older than a HEAD another holder published | When the process is gone (see the rule above), move `<durable-root>/v1.bin` and the files `v1.bin.chunk*` beside it into another directory, then start again: it starts fresh from HEAD |
| `force-move: ... admissible at <time>, master clock` | The barrier has not passed | Wait until that time, rerun the same command |

## What this release does not do

- No command creates a volume record or registers a node: both come from
  `--cluster-spec` at blockmaster start.
- A `--topology`-only setup cannot move a workspace volume at all.
- With one node in the cluster spec, a revoke that is not followed by an ACK
  cannot be undone: the volume needs a forced move to a second node.
- A stop, a revocation or a lease expiry publishes nothing: only a cut, a
  snapshot or a seal does. A forced move loses what was acknowledged after the
  last cut; cut more often (`--chunk-capture-interval`) to bound it.
- No guest freeze: snapshots and seals are crash-consistent only.
- No automatic start of the new holder, and no reconnect of the guest.
- Never run SeaweedFS `volume.fsck` with its delete flags on a cluster holding
  walstore-chunk volumes: see
  [SeaweedFS maintenance commands that destroy chunk volumes](standalone-deployment.md#seaweedfs-maintenance-commands-that-destroy-chunk-volumes).
