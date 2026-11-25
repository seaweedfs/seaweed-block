# RF=1 workspace moves

The opt-in master surface is `WorkspaceService` in `workspace.proto`. Enable it
with `blockmaster --workspace-rpc --workspace-head-base-url <filer directory URL>`
and a lifecycle store. Other volumes continue using the existing assignment
protocol. Workspace operations require an existing RF=1 volume. The target node
must be registered; explicit volumes use their prepared per-volume endpoints,
while volumes with no binding retain node-pair compatibility. The source comes
from its placement or the previous issued move, not a caller's evidence flag.

## Per-volume endpoint preparation and placement

W6 allocates listener ports and calls `RegisterWorkspaceBinding` with volume,
registered server ID, data address and control address before placement or move.
The binding is a durable endpoint reservation, not authority or readiness.
Registration is idempotent for the same pair; changing a live binding or taking
another live volume's endpoint is refused. Prepared pairs also reserve their
ports. Numeric IP/port spellings are normalized for collision checks without
performing DNS lookup. Callers must use consistent DNS names for endpoints.

`PlaceWorkspace {volume_id, server_id, replica_id}` records chosen-node intent
for an existing, unassigned RF=1 volume with an active prepared binding. It does
not create a volume or mint an epoch. Existing different placement intent is
refused rather than silently replacing a queued plan. Observation and the
existing controller/publisher path still establish the assignment. Repeated
identical intent is allowed before assignment; assigned volumes use Move.

Move validates the PREPARED target binding and leaves the source placement
unchanged. Once a volume has any explicit binding, it never uses the legacy
node-level pair, even if all its bindings are retired. CLEAN retires the source
binding only after its durable clean ACK. FORCED retires it only after the new
epoch is issued. Retirement carries the old epoch, making crash recovery and
old-request replay harmless to a later re-prepared binding. Registration may
reuse retired ports; a live target must be prepared again before another move.
Volume deletion removes its binding marker and reservations only after a clean
revocation ACK for current authority. Pending moves and queued unassigned explicit
placement refuse deletion. Canceled queued deletes do not mutate state. Failed
deletion can be retried. Retirement watermarks are retained even before the first
explicit binding, without switching a legacy volume to explicit mode.

Bindings are a file-synced, atomic snapshot with directory fsync. Opening the
store also syncs its parent after directory creation, before admitting writes. Ambiguous
publication errors fence binding reads/mutations until restart. Startup and
registration reconcile retirement from persisted ACK/issued move records, so
a crash between authority persistence and binding retirement is recoverable.
`QueryWorkspace` returns the per-volume bindings including their retired state.

Host workspace mutations serialize with `workspaceMu`. Remote HEAD reads and
promotion probes run outside it; local intent, binding and reservation checks are
repeated under the lock before queueing or committing their results. Ordinary
Publisher mints also acquire that same host lock before checking the live volume,
current binding owner/pair and reservations, and retain it through authority
persistence/commit. The existing already-current product check is repeated for
ordinary Reassign asks here, so a snapshot plus its direct promotion fallback
cannot mint duplicate epochs after both were dequeued. Dequeued legacy asks therefore cannot bypass a later binding
or publish after successful deletion. Hosts without lifecycle stores retain their
static authority path. Guarded Move already holds this boundary and does not
recursively acquire it.
Lock order is host workspace mutex, then Publisher or lifecycle-store locks.
Initial placement persists under a Publisher unassigned guard; its callback
must never call Publisher. Store reads do not hold their mutex while calling
Publisher. Lease renewal remains independent of the host workspace mutex.

The RPCs grant/renew a lease, request revocation, record the old holder's ACK,
request a move, and query persisted state. Leases bind volume, holder and epoch.
The master owns a 30-second expiry. After expiry, that same assignment renews
only through a durable re-grant after the forced-move barrier
(`docs/workspace-lease.md`). Reassignment needs a move; ordinary bind/reassign and retired-slot endpoint
refresh cannot bypass an enrolled workspace's move path.

## Move admission and persistence

Clean: the configured filer HEAD reader must observe B's detached state.
After the holder drains, `AckWorkspaceRevoked` reads that HEAD, checks its epoch
and holder against the authenticated caller, and persists its published position
in the ACK. Move admission matches that detached epoch, holder and position.
Snapshot deletion and restore dependencies may change generation without
changing the stopped image; generation is diagnostic readout only. A higher
epoch claim ends the stable detached predicate. Revocation stops extending the
lease. The ACK is a trusted host drain fact, not a request to close another host.

Forced: the request must explicitly say `kind: "forced", force: true`; the
master's lease must have expired. The journal records the observed HEAD gen and
remote LSN as lost-window evidence. Local fsyncs since that cut are outside the
remote durability point. The orchestrator kills an old VM that remains alive.

The publisher holds its mutex while checking the expected old line and doing:

1. Persist the old authority plus a move reservation containing old/new node and
   holder, kind, reserved new epoch, HEAD gen and remote LSN.
2. Persist the new authority plus that same journal record marked issued.
3. Update live state and notify subscribers.

The file store requires successful directory fsync for workspace records. A
storage error latches the volume uncertain until restart, because a failed return
may follow an applied write. A restart after step 1 resumes the identical request
ID/target at the reserved epoch. A restart after step 2 sees both assignment and
journal. Pending reservations are not issued epochs and are not GC fencing proof.
The new epoch exceeds all prior published/reserved epochs. The remote HEAD still
must be takeover-CASed by the new host before I/O; granting authority alone does
not fence an old filer's CAS. GC may expire an intent only after observing the
remote HEAD's epoch greater than the intent epoch AND age greater than intent TTL;
an issued master epoch alone is not sufficient.

Persisted lease time never expires the authority line itself. In process, forced
admission also checks a monotonic deadline; after restart it waits at least a full
TTL before relying on a persisted expiry. A clock step must not shorten a live
holder's grant. Workspace fields are optional in the checksummed authority record,
so existing records retain their encoding. After workspace enrollment, downgrade
to a binary that does not understand those fields is unsupported.

## Host protocol and integration boundary

`volume.WorkspaceLeaseGuard` is the host-side protocol component. It wraps the
projection used by both frontend I/O and chunk capture. Before a first lease,
after expiry, or after revocation it reports unhealthy. It anchors reply validity
to request start, checks the old deadline before installing a renewal, and never
reopens a fenced guard. Per-request checks do not depend on timely timer dispatch.
Its Stop callback must close/drain the frontend and stop cuts before returning
the last committed HEAD generation. A failed Stop never ACKs revocation.

`blockvolume --workspace-lease` wires it (`cmd/blockvolume/workspace_lease.go`):
the guard gates both frontend I/O and captures, Stop drains the frontends and
cuts, and the ACK carries the stored detached HEAD generation that Detach
published. The orchestrator updates desired placement/launches the
destination; this endpoint does not launch a VM or mutate that desired placement.

Lock order: publisher mutex -> authority store mutex. Remote HEAD reads happen
before acquiring the publisher mutex; subscriber delivery happens after release.
The lease guard serializes renewal/drain under its operation mutex, but releases
its state mutex before base projection, RPC and Stop callbacks. Projection never
takes the operation mutex. No hardware or VM fence is inferred from an ACK test
double.

## Replica identity versus HEAD holder

The RPC identity, lease, move and grant fields are explicitly named replica_id
(expected_replica_id / target_replica_id on a move). They are logical assignment
IDs, never qualified HEAD holders. Their protobuf field numbers are unchanged.
The master resolves the source server from placement or the issued move journal,
then compares HEAD.Holder to exactly server-id + "/" + expected_replica_id.
It never strips a host prefix from a supplied holder. A different server prefix,
an unqualified HEAD holder, or another replica is refused before journaling.
The destination provider must use ChunkHolder = new_host + "/" + new_replica_id;
its projection still uses the logical new_replica_id. Moving the same logical
replica to a different server is valid; it changes the qualified holder and epoch.

## v1 move identity limit

A workspace move keeps its logical replica ID. The target physical host and
qualified HEAD holder change, and the epoch advances; expected_replica_id and
target_replica_id must be equal. ID-changing requests are refused before any
move journal or authority epoch is persisted. Dynamic subscription/peer changes
for a different logical slot are outside v1. Use the existing logical replica
ID for CLEAN and FORCED moves.
