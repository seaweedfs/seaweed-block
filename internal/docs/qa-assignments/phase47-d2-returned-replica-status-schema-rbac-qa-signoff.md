# Phase 47 D2 Returned-Replica Status Schema/RBAC QA Sign-off

Status: DEV VALIDATED. Real-apiserver QA rerun still recommended before any
mutating executor phase.

Validated source: local branch `phase47-returned-replica-executor-gate`.

## Gate

D2 extends the schema-aware status API conformance test so the new Phase 47
returned-replica action payload is validated against the same CRD schema and
RBAC boundary that previously caught live-only failures.

## Evidence

Targeted test:

```text
go test -count=1 ./core/ops
```

The conformance path now writes a `SwBlockVolume.status` payload containing:

- `replicaReintegrations[]` with camelCase frontier/fencing fields,
- `allowedActions[]` entry for `authority.reintegrate_returned_replica`,
- `decision=allowed`,
- `mode=dry_run`,
- `mutationAllowed=false`,
- returned-replica evidence and invariant refs.

## Boundary

The schema/RBAC mock still enforces:

- operator-status writes only `/status`,
- main-object patch is forbidden to operator-status,
- finalizers endpoint is forbidden,
- lifecycle-owner main-object patch remains finalizer-only in its own gate,
- unsupported enum/casing drift returns 422.

## Non-Claims

This is not a real Kubernetes API server. It is a schema-aware local gate that
shifts CRD casing/enum mistakes left. QA should still run a real-apiserver
server-side-dry-run validation before any later executor phase is allowed to
mutate storage or authority state.
