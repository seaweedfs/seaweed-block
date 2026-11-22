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
  HEAD has `state: "detached"` and names the holder, epoch and generation.
- **ACK:** the old holder's acknowledgement, recorded by blockmaster, that it
  stopped and read the sealed HEAD at generation G.

## The three records, and what each proves

| Record | How to read it | What it proves |
|---|---|---|
| The lease | `sw-block ops workspace show --master MASTER --volume v1`: `replica:`, `epoch:`, `lease state:`, `holder: r1@E` | Which holder blockmaster lets serve. `granted`, `revoking` or `expired` is the host validity at query time, not permission to move |
| HEAD | `sw-block ops chunk show --volume v1 --chunk-filer FILER`: `head.gen`, `head.epoch`, `head.holder`, `head.remoteLSN`, `head.state`, `head.detachedBy` | What any host can start from: every write up to `remoteLSN`. `state` `detached` with `detachedBy` means sealed by that holder |
| The ACK | the same `show`: `recorded ACK: r1@E HEAD generation G` | The old holder drained and closed after reading the sealed HEAD G. A clean move needs it |

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

**3. blockmaster flags.** `--cluster-spec` replaces `--topology` (setting both
stops blockmaster). `--workspace-head-base-url` is the filer HTTP address plus
blockvolume's `--chunk-path`:

```bash
blockmaster --authority-store /var/lib/sw-block-master/authority \
  --lifecycle-store /var/lib/sw-block-master/lifecycle \
  --cluster-spec /etc/sw-block/cluster-spec.yaml --expected-slots-per-volume 1 \
  --listen 127.0.0.1:9180 --workspace-rpc \
  --workspace-head-base-url http://FILER_HTTP/swblock/chunks
```

With the example units: `SWB_MASTER_TOPOLOGY=` (empty), `SWB_MASTER_EXPECTED_SLOTS=1`,
`SWB_MASTER_WORKSPACE_RPC=true`, `SWB_MASTER_WORKSPACE_HEAD_BASE_URL=...`, and
`--lifecycle-store` and `--cluster-spec` in `SWB_MASTER_EXTRA_ARGS`. blockmaster
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

## Rule: one live process per holder

blockmaster cannot tell two processes with the same identity apart. The only
guard is the exclusive lock on the volume file `<durable-root>/v1.bin`, and it
works only when both processes use the same file. Moving the local root aside
while the old process runs bypasses it.

So, before you start a second process for the volume, or move a local root
aside, make sure the old process is gone:

```bash
systemctl status sw-blockvolume        # inactive (dead)
sw-block ops volume-unheld --path /var/lib/sw-block-volume/volumes/v1.bin
```

`volume-unheld` exits 0 (`unheld: ...`) only when no process on this host holds
the file; exit 1 is `held: a live process on this host holds ...`. It proves
nothing about another host.

## Planned hand-over

Nothing the volume acknowledged is lost. The target is another registered
host, or the same host (next section).

Each step below is covered by its own tests. The same-host case of the whole
sequence runs end to end, with these commands as written, in
`TestStandalonePlannedHandoffToTheSameHost` (`cmd/blockvolume`), and the refusal of
a clean move without the ACK in `TestStandaloneCleanMoveWithoutTheACKIsRefused`.
A move to another host is not yet run end to end.

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

To check: `sw-block ops chunk show --volume v1 --chunk-filer FILER` shows
`head.state` `detached` and `head.detachedBy` `{"holder":"s1/R","epoch":E,"gen":G}`.

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

**8. Verify.** The new holder logs `durable: HEAD claim for volume=v1 done in
...: gen=... epoch=E2 holder="S2/R"` and `blockvolume: volume v1 adopted as
PRIMARY at R@E2`. `ops workspace show` prints `holder: R@E2` and a line
`move: request=move-v1-1 kind=clean issued=true ...`. `ops chunk show` shows
`head.state` `active`, `head.epoch` E2 and `head.remoteLSN` L. Reconnect the
guest to the new host.

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

**The lease lapsed, and the holder is still the recorded holder.** A lapsed
holder fences itself, drains and exits with status 0, logging
`workspace lease: fenced volume=v1 replica=r1 epoch=E: lease expired at ...`.
Start it again yourself. blockmaster grants it again only after a barrier: the
stored lease horizon (up to 45 s after the last renewal) and the last grant,
each plus `--workspace-max-clock-skew` (default `20s`). With the defaults that
is at most **65 seconds after the process stopped**; if blockmaster restarted
meanwhile, also at least 50 seconds (30 s plus the skew) after that restart,
whichever is later. Until then the process logs, every second,
`workspace lease: renewal failed after ...: ... workspace forced move lease
expiry plus clock-skew margin has not elapsed`. After the grant it reclaims
HEAD and serves its local WAL: nothing acknowledged is lost.

**Start it no earlier than 65 seconds after it stopped.** blockvolume waits for
its lease only 30 seconds after start (fixed, no flag). Started too early, it logs
`durable: chunk authority wait for volume=v1 ended after ...: assignment
unavailable, not serving (...)`, keeps running and does not serve. Restart it
once more: its lease is then valid and it serves at once.

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

## Symptoms

| You see | It means | Do |
|---|---|---|
| `sw-block ops chunk detach: plain_detach_busy: durable: plain detach requires idle frontend I/O` | I/O was in flight; nothing changed | Stop the writer, retry step 3 |
| detach answers `detach_aborted` | Nothing was published; the volume serves here again | Retry step 3 |
| detach answers `outcome_unknown` | The seal may have landed; I/O stays stopped | Retry step 3: it settles the outcome |
| revoke: `workspace revoke: no matching lease` | `--replica`/`--epoch` are not the current holder | Read them again with `show` |
| move: `workspace clean move needs detached HEAD and matching revocation ACK` | No ACK yet, or its generation differs from HEAD | Wait for the ACK line (step 5); compare `G` with `ops chunk show` |
| move: `move target node "S2" is not registered` | `S2` is not under `nodes` | Add it to the cluster spec, restart blockmaster |
| move: `move target endpoints ... are not registered (...)` | The addresses are not on the node's host | Use addresses on that host |
| move: `move target "s1" is the current source host` | A forced move named its own host | Choose another node |
| old holder logs `blockvolume: workspace lease closeout: workspace lease expired before clean ACK` | Revoked, but no ACK | Only a forced move to another node remains (HEAD is sealed: if step 3 succeeded, nothing is lost) |
| renewal fails with `workspace lease: revocation or move pending` | The volume was revoked or moved | The process cannot serve again; continue the hand-over |
| renewal fails with `workspace lease: stale authority` | A move gave the volume a newer epoch | This process is the old holder: stop it, do not restart it |
| renewal fails with `... clock-skew margin has not elapsed` | Waiting for the re-grant barrier | Wait (at most 65 s after the stop, with defaults) |
| `durable: chunk authority wait for volume=v1 ended after ...: assignment unavailable, not serving` | Started before its lease was granted | Restart it once more after the barrier |
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
