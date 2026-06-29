# Phase 63 Finished Plan: Rebuild Runtime Target Contract

Status: complete.

QA: PASS.

## Goal

Phase 63 closes the addressing gap between returned-replica evidence,
`SwBlockReplicaRebuild` targets, and the Phase 62 authority-executor HTTP
runtime transport.

The main risk was not lack of code; it was unsafe inference. The existing
transport calls need exact runtime facts:

```text
StartRebuild(replicaID, sessionID, epoch, endpointVersion, frontierHint)
StartCatchUp(replicaID, sessionID, epoch, endpointVersion, fromLSN, frontierHint)
```

Phase 63 therefore schema-locks those facts and makes target creation and
execution fail closed when they are missing.

## Delivered

Code:

```text
core/ops/lifecycle_owner_controller.go
core/ops/operator_status_controller.go
core/ops/rebuild_target_owner_controller.go
core/ops/authority_executor_controller.go
cmd/sw-block/main.go
```

CRDs:

```text
charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml
charts/seaweed-block/crds/swblockreplicarebuilds.block.seaweedfs.com.yaml
```

Gate:

```text
scripts/run-phase63-rebuild-runtime-target-contract-gate.sh
testops/scenarios/rebuild-runtime-target-contract-chain.yaml
```

Docs:

```text
internal/docs/current-plan.md
internal/docs/qa-assignments/phase63-rebuild-runtime-target-contract-qa-signoff.md
internal/docs/product-roadmap.md
```

## Behavior

Runtime target fields now flow through:

```text
SwBlockVolume.status.replicaReintegrations[]
  -> rebuild-target-owner
  -> SwBlockReplicaRebuild.spec
  -> authority-executor runtime request
```

Target-owner creates a rebuild target only when runtime target facts are
complete. Missing facts produce:

```text
runtime_target_missing=1
mutation_allowed=false
```

Authority-executor blocks runtime execution if a selected target lacks required
runtime facts:

```text
SwBlockReplicaRebuild.status.state=blocked
reasonCode=rebuild_runtime_target_missing
```

## Non-Claims

Phase 63 does not claim:

```text
blockvolume_runtime_endpoint_wired
transport.StartRebuild called by a blockvolume endpoint
frontend publication
failback
session ID inference
automatic recovery-session minting
```

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
target_owner_requires_runtime_facts=true
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

Phase 64 should wire the blockvolume-side runtime endpoint. The endpoint must
validate local assignment/session/epoch/endpoint-version facts and then call the
transport only from that validated local context. It must not accept arbitrary
frontend/failback mutation or guessed session state.
