# RF=1 workspace moves

The opt-in master surface is `WorkspaceService` in `workspace.proto`. Enable it
with `blockmaster --workspace-rpc --workspace-head-base-url <filer directory URL>`
and a lifecycle store. Other volumes continue using the existing assignment
protocol. Workspace operations require an existing RF=1 volume. The target node
must be registered with matching data/control endpoints; the source node comes
from its placement or the previous issued move, not a caller's evidence flag.

The RPCs grant/renew a lease, request revocation, record the old holder's ACK,
request a move, and query persisted state. Leases bind volume, holder and epoch.
The master owns a 30-second expiry. After expiry, that same assignment cannot
renew. Reassignment needs a move; ordinary bind/reassign and retired-slot endpoint
refresh cannot bypass an enrolled workspace's move path.

## Move admission and persistence

Clean: the configured filer HEAD reader must observe `state: "detached"` and
`detachedBy: {holder, epoch, gen}` matching the installed HEAD's holder, epoch and
generation. W2 writes both in the same final-cut CAS. State is exactly `active`
or `detached`; missing state means active for older HEADs. No boolean `detached`
is accepted. A new holder's takeover CAS sets active and clears detachedBy; the
move journal retains history. The master's stored revocation ACK must name that
exact HEAD generation. Revocation stops extending the lease. `AckWorkspaceRevoked`
is a trusted host protocol fact, not a request to close another host remotely.

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

This master-side Draft supplies and tests that component and protocol, not its
`cmd/blockvolume` chunk-lifecycle wiring. That integration belongs with the host
restore/provider successor: it must pass the guard to both providers/captures,
wire actual frontend/cut shutdown into Stop, and return the last committed HEAD.
Until that is wired, legacy blockvolume processes must not be used as move-capable
workspace holders. Named Detach production of `state: "detached"` is a separate
chunk-control dependency. The orchestrator updates desired placement/launches the
destination; this endpoint does not launch a VM or mutate that desired placement.

Lock order: publisher mutex -> authority store mutex. Remote HEAD reads happen
before acquiring the publisher mutex; subscriber delivery happens after release.
The lease guard serializes renewal/drain under its operation mutex, but releases
its state mutex before base projection, RPC and Stop callbacks. Projection never
takes the operation mutex. No hardware or VM fence is inferred from an ACK test
double.
