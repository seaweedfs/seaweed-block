# Workspace placement and moves

`sw-block ops workspace bind|place|move|journal --master HOST:PORT` reads one
protobuf JSON request from stdin. Use `--input request.json` to read a file.
The request limit is 64 KiB; unknown fields and malformed JSON are refused.
`--timeout` defaults to 30s and must be positive, at most 5m.

These commands use the master's opt-in WorkspaceService. Create the RF=1
lifecycle volume and register its servers first. The caller allocates distinct data/control
ports for every workspace on a host.

```sh
printf '%s' '{"volumeId":"ws1","serverId":"host-a","dataAddr":"127.0.0.1:19200","ctrlAddr":"127.0.0.1:19100"}' | sw-block ops workspace bind --master 127.0.0.1:9330
printf '%s' '{"volumeId":"ws1","serverId":"host-a","replicaId":"r1"}' | sw-block ops workspace place --master 127.0.0.1:9330
printf '%s' '{"volumeId":"ws1"}' | sw-block ops workspace journal --master 127.0.0.1:9330
```

Bind reserves endpoints durably; place records chosen-node intent. The existing
observation/controller/Publisher path establishes authority. Prepare the target
binding before moving. A CLEAN move requires Detach while the lease is valid,
then Revoke and the host's drain ACK. Supply this JSON to `move`:

```json
{"requestId":"move-ws1-001","volumeId":"ws1","expectedReplicaId":"r1","expectedEpoch":"1","targetReplicaId":"r1","targetServerId":"host-b","targetDataAddr":"127.0.0.1:29200","targetCtrlAddr":"127.0.0.1:29100"}
```

A FORCED move requires `move --force` and the old host's expired lease. JSON
`force:true` alone cannot enable it. The master records the old HEAD generation
and remote LSN as the lost-window evidence; local writes since that cut can be
lost. The orchestrator terminates the old VM. The new host must install its
higher-epoch takeover HEAD before serving data. A move retains the logical
replica ID; only the host, endpoints and epoch change.

Success prints protobuf JSON (64-bit integers are strings) and exits 0. Failure
prints `{code,message,retryable}` JSON on stdout and diagnostics on stderr:
exit 2 for invalid CLI/input, exit 1 for RPC/output failure. Codes are derived
from gRPC status, never from message substring matching: `failed_precondition`,
`not_found`, `already_exists`, `conflict`, `remote_unavailable`, `outcome_unknown`,
`permission_denied`, `unauthenticated`, `resource_exhausted`, `internal`.

The client makes one mutation RPC and never automatically replays it. A deadline
or cancellation after dispatch is `outcome_unknown`. Inspect `journal` before
retrying an ambiguous move, and reuse its exact requestId and target. The journal
returns bindings, current lease/revocation state and issued or pending move records.
