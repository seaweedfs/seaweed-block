# Phase 47 D2 Returned-Replica Status Schema/RBAC QA Sign-off

Status: PASS.

Validated source: `2f3b864` plus the D2 live gate script/scenario additions.

## Gate

D2 extends the schema-aware status API conformance test so the new Phase 47
returned-replica action payload is validated against the same CRD schema and
RBAC boundary that previously caught live-only failures.

## Evidence

Local targeted test:

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

Live TestOps gate:

```text
returned-replica-status-schema-rbac-chain.yaml
run: 20260620-101008-dbca
result: PASS, 12/12
```

Live summary:

```text
phase47_returned_replica_status_schema_rbac_status=ok
valid_returned_replica_status_server_dry_run=true
snake_case_action_rejected=true
unsupported_action_mode_rejected=true
operator_status_main_patch_allowed=no
server_dry_run_status_mutated=false
```

## Boundary

The schema/RBAC mock still enforces:

- operator-status writes only `/status`,
- main-object patch is forbidden to operator-status,
- finalizers endpoint is forbidden,
- lifecycle-owner main-object patch remains finalizer-only in its own gate,
- unsupported enum/casing drift returns 422.

## Cleanup

The temporary namespace and RBAC were removed:

```text
sw-block-phase47-status-gate: absent
sw-block-phase47-operator-status ClusterRole/Binding: absent
```

## Non-Claims

This validates status payload admission and RBAC only. It does not enable or
execute a mutating returned-replica executor.
