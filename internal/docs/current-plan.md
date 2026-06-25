# Current Plan: Phase 63 Rebuild Runtime Target Contract

Status: complete.

## Goal

Phase 60 proved the existing engine/adapter/transport rebuild/catch-up data
path can move bytes and converge durable content. Phase 61 added the authority
executor runtime call-site seam. Phase 62 added an explicit HTTP runtime
transport.

Phase 63 closes the missing addressing contract between Kubernetes status and
that runtime transport. It does not add the blockvolume runtime endpoint yet.
Instead, it makes the next step safe by requiring exact runtime target facts
before any executor can issue rebuild traffic:

```text
SwBlockVolume.status.replicaReintegrations[]
  -> SwBlockReplicaRebuild.spec
  -> authority-executor runtime request
```

The key rule is fail-closed: do not infer runtime endpoint, data address,
session ID, epoch, endpoint version, or frontier hints from partial evidence.

## Delivered

### D1: Runtime Target Schema

`SwBlockVolume.status.replicaReintegrations[]` and
`SwBlockReplicaRebuild.spec` now carry the same runtime target fields:

```text
runtimeEndpoint
targetDataAddr
sessionID
epoch
endpointVersion
fromLsn
frontierHintLsn
basePinLsn
```

The CRD tests assert camelCase schema fields and reject snake_case leaks.

### D2: Target Owner Fail-Closed Creation

`rebuild-target-owner` now creates a `SwBlockReplicaRebuild` target only when
the returned-replica fact carries complete runtime target evidence. If the
facts are missing, it reports:

```text
runtime_target_missing=1
mutation_allowed=false
```

No target is created from guessed data-path or session facts.

### D3: Authority Executor Target Validation

`authority-executor` can use the target's own `spec.runtimeEndpoint` when a
runtime is selected. Before posting to the runtime, it validates the target
contains:

```text
runtimeEndpoint
sessionID
epoch
endpointVersion
frontierHintLsn
```

If any required runtime fact is missing, it writes blocked status:

```text
state=blocked
reasonCode=rebuild_runtime_target_missing
```

and does not POST to the runtime.

### D4: Gate

Gate files:

```text
scripts/run-phase63-rebuild-runtime-target-contract-gate.sh
testops/scenarios/rebuild-runtime-target-contract-chain.yaml
```

The gate proves schema, target-owner, executor, CLI, and Kubernetes writer
behavior using terminal key/value evidence.

## Non-Claims

Phase 63 does not claim:

```text
blockvolume_runtime_endpoint_wired
transport.StartRebuild called by blockvolume
frontend publication
failback
session ID inference
automatic recovery-session minting
```

Those belong to the next storage runtime phase. The blockvolume endpoint must
own or validate the recovery session before it calls `StartRebuild` or
`StartCatchUp`.

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\rebuild-runtime-target-contract-chain.yaml
```

Live:

```text
20260625-011115-b01b rebuild-runtime-target-contract-chain PASS 22/22
```

Terminal evidence:

```text
phase63_rebuild_runtime_target_contract_status=ok
runtime_target_fields_schema_locked=true
runtime_target_camel_case=true
target_owner_requires_runtime_facts=true
target_owner_creates_only_when_runtime_facts_complete=true
target_owner_missing_runtime_no_target=true
authority_executor_missing_runtime_target_blocks=true
authority_executor_runtime_request_carries_target_lineage=true
runtime_target_can_drive_http_runtime=true
session_id_inferred=false
blockvolume_runtime_endpoint_wired=false
start_rebuild_called=false
frontend_publication_allowed=false
failback_allowed=false
```

## Next

Phase 64 can add the blockvolume-side runtime endpoint only if it preserves the
same contract:

```text
exact target facts -> local assignment/session validation -> StartRebuild/StartCatchUp
```

The endpoint must fail closed on stale assignment, wrong replica, missing
session, wrong epoch, or insufficient frontier evidence. NVMe should still wait
until the rebuild runtime endpoint has a real terminal-evidence gate.
