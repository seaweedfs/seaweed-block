# Fencing Gaps TODO (G1-G5)

Status: OPEN. Opened 2026-11-06 at `7d016ed` (main).

Found while writing a design walkthrough of the epoch / fencing protocol.
Each item below was checked against the code at `7d016ed`. G1-G3 are
confirmed by reading the code; no test reproduces them yet. G4 is a design
choice that needs an explicit decision. G5 is plausible and needs a test
before any fix.

Each fix follows the usual order: red test first, then the fix, then a new
row in [`invariant-ledger.md`](./invariant-ledger.md).

## Summary

| ID | Gap | Status | Priority | Proposed fix (short) |
|---|---|---|---|---|
| G1 | Replica lineage floor lives only in memory | Confirmed by code | P1 | Persist an epoch floor; reject mutations until floor is known |
| G2 | New primary does not fence its own replica listener | Fixed (red test + fix; ledger `INV-FENCE-SELF-ON-PROMOTE-001`) | P1 | On becoming primary, raise own floor and refuse inbound ship |
| G3 | Production promotion runs without a required ACK frontier | Confirmed by code | P1 | Probe all survivors (R+W>N), then record the ACK frontier in master |
| G4 | No primary-side lease; a cut-off primary keeps serving reads | Decided 2026-11-07: option A | P2 | Keep as an explicit non-claim for alpha/beta (see G4) |
| G5 | Caught-up check compares LSN without epoch | Plausible, needs test | P2 | (epoch, LSN) comparison plus truncation at the epoch start |

Order: G2, G1, G3 (small, local changes that close the stale-writer
paths), then the G5 test. G4 is decided (option A).

## G1: Replica lineage floor lives only in memory

Where:

- `core/transport/replica.go:59` - `activeLineage` is a plain field.
- `core/transport/replica.go:436-439` - when `activeLineage` is zero, the
  first well-formed lineage is accepted and becomes the floor.
- `cmd/blockvolume/main.go:762` - production builds the listener with
  `NewReplicaListener` (no apply hook) at process start, before any
  assignment is known. Nothing loads or persists the floor.

Failure scenario:

1. New primary P2 (epoch 2) fences replica R; R's floor becomes epoch 2.
2. R restarts. Its floor is zero again.
3. Old primary P1 (epoch 1), cut off from master but able to reach R, ships
   to R. R accepts epoch 1 as its first lineage.
4. P1 can count R's ACK toward its write quorum, and R now holds epoch-1
   bytes that P2 never wrote.

Proposed fix:

1. Add an epoch floor to the replica listener, separate from
   `activeLineage`. Mutating traffic (`MsgShipEntry`,
   `MsgRecoveryLaneStart`, rebuild) with `epoch < floor` is rejected and the
   connection closed.
2. Persist the floor in the durable root (same fsync discipline as the
   authority record). Write it before replying to a fence barrier.
3. At start, load the persisted floor. Until the host has received its
   current assignment from master, reject all mutating traffic. After the
   assignment arrives, set `floor = max(persisted, assignment epoch)`.

Open question: confirm that a replica-role host receives the current epoch
in its assignment. If it does not, step 3 needs master to send it.

Red test: start a listener, accept a ship at epoch 2, stop and restart the
listener on the same store, send a ship at epoch 1. Today it is accepted;
after the fix it must be rejected.

Ledger row on close: `INV-FENCE-FLOOR-DURABLE-001` - a replica never accepts
mutating traffic below the highest epoch it has acknowledged, across
restarts.

## G2: New primary does not fence its own replica listener

Where:

- `core/transport/executor.go:379-402` - `Fence` sends a barrier to peers
  only.
- `cmd/blockvolume/main.go:762` - the replica listener runs for the whole
  process life, whatever the role. It is stopped only on shutdown and error
  paths.
- No storage `ApplyEntry` implementation checks role
  (`core/storage/walstore.go:916`, `core/storage/smartwal/store.go:411`,
  `core/storage/parallelwal/store.go:1203`).

Failure scenario:

1. P1 (epoch 1) ships to R2. R2's listener floor is epoch 1.
2. Master promotes R2 to primary at epoch 2. R2 fences R3, not itself.
3. P1 is cut off from master but can still reach R2. It ships at epoch 1.
   R2's listener sees the same lineage as before and applies it.
4. R2 is now primary and also applying the old primary's writes. P1 counts
   R2's ACK toward its quorum.

Proposed fix:

1. When the host takes the primary role at epoch e, set its own listener
   floor to e (same floor as G1) before the frontend serves IO.
2. While the host is primary, the listener refuses `MsgShipEntry`,
   `MsgRecoveryLaneStart` and rebuild frames. Probes may still be answered.

Red test: two listeners in one test. Ship at epoch 1 to listener B, then
mark B primary at epoch 2 (through the host role path, not a test-only
setter), ship again at epoch 1. Today it is applied; after the fix it must
be rejected.

Ledger row on close: `INV-FENCE-SELF-ON-PROMOTE-001` - a primary at epoch e
applies no inbound replication traffic, and no traffic below e.

## G3: Production promotion runs without a required ACK frontier

Where:

- `cmd/blockmaster/main.go:338-341` - the production evidence provider is
  built without `RequiredLSNByVolume`.
- `core/host/master/promotion_probe.go:74` - `SyncAckLSN` therefore is 0.
- `core/host/master/promotion_probe.go:94-97` - a candidate whose probe
  fails is recorded as not ready and skipped.
- `core/host/master/product_loop.go:451-479` - with `SyncAckLSN == 0`,
  `promotionAllowedReplicas` picks the highest `DurableLSN` among the
  candidates that answered.

Failure scenario (RF=3, sync-quorum W=2):

1. Primary P and replica R2 hold LSN 100 (acknowledged to the client).
   Replica R3 holds LSN 90.
2. P dies. R2's status endpoint is briefly unreachable from master.
3. Master probes: R2 fails, R3 is ready at 90. R3 is promoted.
4. Writes 91-100 were acknowledged but are not on the new primary.

The test suite covers the strict path (`SyncAckLSN: 52` in
`core/host/master/product_loop_test.go`), but production never takes it.

Proposed fix, in two steps:

1. Short term (small): when `SyncAckLSN == 0`, allow promotion only if
   every non-current slot answered the probe. With the primary dead and
   W=2 of N=3, both survivors must be read (R+W>N). If one is unreachable,
   block promotion and emit `promotion_blocked` with a reason. Cost: an
   extra failure (primary plus one survivor) blocks failover instead of
   risking lost writes.
2. Long term: the primary reports its quorum-acknowledged LSN with each
   heartbeat; master persists it per volume and feeds it to
   `RequiredLSNByVolume`. Then one reachable survivor that covers the
   frontier is enough.

Red test: `promotionAllowedReplicas` with `SyncAckLSN: 0`, candidate r2
unreachable (probe error), r3 ready at a lower LSN. Today r3 is allowed;
after step 1 nothing is allowed and the volume is reported blocked.

Ledger row on close: extend `INV-PROMOTE-FRONTIER-001` so its evidence names
the production configuration, not only the test configuration.

## G4: No primary-side lease

Where: no time-based lease or self-demotion in `core/host/volume` or
`cmd/blockvolume`. This is deliberate: "heartbeat implies authority" is on
the anti-pattern list, and correctness does not depend on clocks.

What it means today:

- Writes from a cut-off old primary fail once replicas are fenced (after
  G1 and G2 are fixed), because the quorum ACK cannot form.
- Reads are served locally, so a cut-off old primary can return stale data
  to a client that still talks to it, until that primary learns it was
  replaced.

Decision needed (owner):

- Option A: keep it as an explicit non-claim. Document that stale reads are
  possible from a partitioned old primary, and rely on host multipath to
  move clients to the new primary.
- Option B: add a read lease. The primary serves reads only while it has
  heard from master or a write quorum within T (monotonic clock). A new
  primary waits T plus a drift bound before serving. Correctness then
  depends on a bounded clock rate, not on clock sync.

Decision (owner, 2026-11-07): A for alpha and beta, written into the
non-claims list; B only if a customer needs linearizable reads across
partitions.

## G5: Caught-up check compares LSN without epoch

Where:

- `core/engine/apply.go:486-491` - a replica with `R >= H` is classified as
  caught up; the comparison has no epoch.
- `core/recovery/doc.go:53-66` - already lists the related overlap-history
  case as a candidate invariant.

Failure scenario:

1. Old primary P1 ships LSN 101-105 at epoch 1 to replica R3 only, and the
   writes are never acknowledged (no quorum).
2. R2 is promoted at epoch 2 with head 100 and writes new LSN 101+.
3. R3 reports `R = 105 >= H`, so it is classified as caught up. Its
   101-105 are epoch-1 bytes that the new primary never had.
4. Later live entries at LSN 101-105 on the same LBA may be treated as
   stale by the per-LBA apply gate, so the divergent bytes can stay.

Proposed fix: compare `(epoch, LSN)`. The new primary records the LSN at
which its epoch starts (as Kafka's leader-epoch cache and Raft's log
matching do). A replica whose entries after that LSN carry an older epoch
must truncate to the epoch start, or rebuild, before it counts as caught up.

Red test first: component test with three replicas, an unacknowledged tail
on one replica, failover to another, then compare per-LBA bytes after the
returning replica reports caught up. If the test passes today, close G5 and
record which layer already prevents it.
