# Workspace host lease

Workspace launches must pass `--workspace-lease` together with
`--durable-root` and `--durable-impl=walstore-chunk`. The workspace launcher
must set this flag. Ordinary chunk volumes retain their existing behavior
when the flag is absent.

With the flag enabled, blockvolume owns renewal through WorkspaceRPC. Its
shared projection refuses frontend I/O and captures before the first grant,
when renewal expires, and after revocation or cancellation. A missing lease
or disabled WorkspaceRPC never selects an unguarded fallback. A fenced
process cannot become healthy again; start a new process for its assignment.

For a CLEAN move, stop guest I/O and call Detach while the lease is valid.
Detach installs the final cut and canonical detached HEAD proof. Then revoke
the assignment. Blockvolume closes frontend admission, drains and closes the
frontend, replication, and provider, reads the stored detached HEAD, and only
then ACKs its exact generation. The authority refuses an expired lease's ACK.
Revocation does not perform a final cut. Lease expiry or cancellation closes
the host without a CLEAN ACK or cut. A new process of the unchanged holder
and epoch may return through a durable re-grant as described below; a move
to a different holder still requires the move protocol.

The shared projection loads the guard atomically. Guard operations serialize
renewal, drain, and ACK with the operation mutex; the state mutex is held only
for lease state, never across a base projection, RPC, or stop callback. The
projection never takes the operation mutex. The blockvolume main goroutine
alone closes physical resources; the lease worker requests closure and waits
for its result before ACK. Host shutdown fences admission synchronously.
The host installation mutex latches this terminal fence even before a guard
exists; a later-installed guard is born fenced. Its lock order is installation
mutex then guard state mutex. Canceled renewal replies cannot publish a grant.

## Clock skew

`blockmaster --workspace-max-clock-skew` defaults to 20s (two nominal 10s
renewal intervals for the 30s lease). Set a positive bound below 60s that
covers the deployment's clock drift. The bound is persisted with each lease;
a restart or smaller setting cannot shorten an outstanding grant's bound.
The master grants a FORCED move only after both the persisted lease horizon
and the granted or conservative restart deadline plus the full bound. `ValidForNs` in lease
replies excludes half the bound. Hosts anchor that shortened duration to
request start, self-fencing early without comparing remote wall-clock times.
Delayed replies cannot extend admission. The bound is an operational clock
assumption; arbitrarily stopped or unbounded clocks are not covered.

## Master-side renewal

Blockmaster answers a renewal of an established lease from memory: it takes
only a small leaf lock, writes nothing and never waits for another authority
write or a lifecycle store write. The grant is the smaller of now plus the TTL
and the durable horizon, the lease expiry stored on disk. A grant that would
leave the host no validity (grant minus half the skew bound not after now) is
refused with an error, so the host keeps its remaining validity instead of
fencing on an invalid reply.

Only the first grant of a lease is written synchronously; it stores a horizon
of now plus 1.5 times the TTL. When a renewal finds the horizon less than
1.25 times the TTL ahead, it starts one background extension for that volume.
The extension is an ordinary authority write under the publisher lock and
stores the renewal time plus 1.5 times the TTL. A failed extension write is
an unknown outcome: the volume becomes uncertain, renewals that would issue a
new or extended grant are refused, and its host fences within its current
validity. A Revoking lease still gets its bounded drain reply, never extended. A restart reloads the stored
horizon, and a FORCED move waits for that horizon plus the skew bound.

All authority writes stay serialized. With N live leases and store write time
P, extensions stay ahead without shortening grants only while N·P is below
TTL/4 minus one host renewal interval r (about (TTL − skew/2)/3), and hosts
stay unfenced while N·P stays below 1.25·TTL − skew/2 − 2r (about 14 s at the
defaults). Beyond that, grants shrink and hosts fence: the design fails
closed. Blockmaster logs one warning per failure streak, with the write's
latency and the remaining horizon, when an extension fails, and one when
grants start to be clamped to the horizon, and a line when either recovers.


## Returning after an expired lease

Restarting the same holder at the same epoch can retain its acknowledged
local WAL after a long outage. Re-grant waits for the existing forced-move
barrier: both the durable horizon and the granted or conservative restart
deadline, each plus the full configured/persisted skew bound, must have
passed. The master then persists one new horizon before returning the grant;
neither horizon nor skew is reduced. Immediate repeated renewals use memory,
so they do not add durable re-grant writes while that grant is live.

The authority record must still name this holder and epoch. A pending move,
revocation/ACK or uncertain store prevents a usable re-grant. Re-grant and
forced issue serialize; whichever commits first makes the other refuse.
If an authority writer holds the lock, this renewal attempt refuses
immediately; a returning process may retry after the writer settles.
An already fenced guard never reopens: start a new process, which claims a
new HEAD generation before serving and retains its local WAL. Its first cut
covers those acknowledged bytes. If the local root is absent, recovery
restores only the sealed/published HEAD image and never claims missing local
bytes. A same-epoch detached HEAD stays detached even after a lease re-grant.

## Planned return on the same host

After plain detach seals HEAD and revocation records its exact generation,
a clean move may target the source host itself. Prepare the binding again
before requesting the move. The move journals a higher epoch even when its
endpoints are unchanged; start the returning blockvolume at that new epoch.
It claims the sealed HEAD before serving and preserves the sealed image and
remote frontier. The matching seal/ACK remains valid overnight while HEAD
is unchanged. A same-epoch reopen remains detached, and a forced move to the
same host is refused. Every ordinary binding, endpoint collision, journal
isolation and uncertain-outcome check still applies.
