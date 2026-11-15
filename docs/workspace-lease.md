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
the host without a CLEAN ACK or cut; recovery uses a FORCED move.

The shared projection loads the guard atomically. Guard operations serialize
renewal, drain, and ACK with the operation mutex; the state mutex is held only
for lease state, never across a base projection, RPC, or stop callback. The
projection never takes the operation mutex. The blockvolume main goroutine
alone closes physical resources; the lease worker requests closure and waits
for its result before ACK. Host shutdown fences admission synchronously.
The host installation mutex latches this terminal fence even before a guard
exists; a later-installed guard is born fenced. Its lock order is installation
mutex then guard state mutex. Canceled renewal replies cannot publish a grant.
