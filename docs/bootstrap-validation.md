# Bootstrap and Validation

This doc describes how to start, inspect, and validate the
runnable sparrow in this repository. It is the Phase 05 operability
surface. It is NOT a production operator CLI — per the V3 operations
design, the production interface is `weed shell` after integration.

## Supported

| Action | How |
|---|---|
| Run the three canonical demos (default) | `go run ./cmd/sparrow` |
| Print scope statement (supported / unsupported) | `go run ./cmd/sparrow --help` |
| Print version | `go run ./cmd/sparrow --version` |
| Repeatable validation | `go run ./cmd/sparrow --runs 10 --json` |
| Machine-readable output for CI | `go run ./cmd/sparrow --json` |
| Optional read-only HTTP inspection | `go run ./cmd/sparrow --http :9090` |

## Not supported in Phase 05

These will either not exist or will return an explicit error:

- persistence or crash recovery — storage is in-memory
- master service or dynamic assignment
- any mutation via the HTTP ops surface — POST/PUT/PATCH/DELETE return 501
- RF3 or multi-replica flows
- frontend protocols (iSCSI, NVMe-oF)
- concurrent writer during replication
- a real operator CLI — `weed shell` is the eventual interface

## Default run

```bash
go run ./cmd/sparrow
```

Exercises the three canonical recovery paths in one process:

1. **Healthy** — replica already caught up (R ≥ H), no recovery needed
2. **Catch-up** — replica within retained WAL window (R ≥ S, R < H)
3. **Rebuild** — replica beyond retained WAL window (R < S)

Exit code 0 when all three reach `healthy`; 1 otherwise.

Default output is human-readable text.

## Machine-readable output

```bash
go run ./cmd/sparrow --json
```

Emits one JSON summary object covering all runs:

```json
{
  "version": "0.5.0-phase05",
  "runs": 1,
  "total": 3,
  "passed": 3,
  "failed": 0,
  "all_passed": true,
  "results": [
    { "run": 1, "name": "healthy",  "pass": true, "mode": "healthy", "R": 10, "S": 1,  "H": 10 },
    { "run": 1, "name": "catch-up", "pass": true, "mode": "healthy", "R": 20, "S": 1,  "H": 20 },
    { "run": 1, "name": "rebuild",  "pass": true, "mode": "healthy", "R": 20, "S": 21, "H": 20 }
  ]
}
```

Failing results carry a `reason` field. Exit code reflects pass/fail.

## Repeatable validation

```bash
go run ./cmd/sparrow --json --runs 10
```

Repeats the full three-demo sequence N times. A single FAIL in any
iteration flips `all_passed` to false and sets a non-zero exit code.

This is the CI-friendly form: one command, deterministic outcome,
machine-readable, exit code tells you if something broke.

## Read-only HTTP inspection

```bash
go run ./cmd/sparrow --http :9090
```

Starts an HTTP server alongside the demo run. The sparrow runs its
three demos first (so there is state to inspect), then keeps the
HTTP server alive until `Ctrl+C`.

| Endpoint | Method | Returns |
|---|---|---|
| `/status` | GET | version, uptime, scope statement, current demo label |
| `/projection` | GET | latest adapter `ReplicaProjection` as JSON |
| `/trace` | GET | latest adapter trace as JSON array |
| any path | POST/PUT/PATCH/DELETE | 501 Not Implemented + "not supported in Phase 05" |
| unknown path | GET | 404 with a `hint` field pointing to the three valid paths |

### Example

```bash
# In one shell:
go run ./cmd/sparrow --http :9090 &

# In another:
curl -s localhost:9090/status | jq
curl -s localhost:9090/projection | jq
curl -s localhost:9090/trace | jq
```

### Invariants enforced at the HTTP layer

- Every mutation verb returns 501 with a structured body.
- No handler calls `engine.Apply` directly; the ops package only reads
  through `adapter.Projection()` and `adapter.Trace()`.
- No handler can choose recovery semantics or redefine `targetLSN`.
- The scope statement in `/status` matches the `--help` output exactly.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | All demos passed across all runs |
| 1 | One or more demos failed (validation use) |
| 2 | Usage or flag error |
| 3 | Runtime setup failure (currently: `--http` bind failed) |

## What the interface is NOT

- Not a volume manager. There is no `create`, `delete`, `list`, `resize`.
- Not a master. Assignment is hardcoded in the sparrow for demo purposes.
- Not a frontend. There is no iSCSI / NVMe-oF here.
- Not a persistence layer. Everything is in memory; killing the process
  loses all state, by design.

Each of these is deliberately out of Phase 05 scope. The eventual
production operator experience is `weed shell block.*` per
`sw-block/design/v3-operations-design.md` §1.3.
