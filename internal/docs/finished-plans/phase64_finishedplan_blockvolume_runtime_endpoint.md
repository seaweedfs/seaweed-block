# Phase 64 Finished Plan: Blockvolume Runtime Rebuild Endpoint

Status: complete.

QA: PASS.

## Goal

Phase 64 connects the Phase 63 rebuild runtime target contract to a real
blockvolume-side endpoint. The endpoint starts in-process replication recovery
only after validating local primary readiness and exact lineage/session facts.

The goal is a bounded runtime-start capability, not a full returned-replica
rebuild lifecycle.

## Delivered

Code:

```text
cmd/blockvolume/main.go
core/host/volume/status_server.go
core/replication/volume.go
core/replication/peer.go
core/ops/authority_executor_controller.go
```

Gate:

```text
scripts/run-phase64-blockvolume-runtime-endpoint-gate.sh
testops/scenarios/blockvolume-runtime-endpoint-chain.yaml
```

Docs:

```text
internal/docs/current-plan.md
internal/docs/qa-assignments/phase64-blockvolume-runtime-endpoint-qa-signoff.md
internal/docs/product-roadmap.md
```

## Behavior

`cmd/blockvolume` now supports:

```text
--runtime-rebuild-endpoint
```

When enabled, the status server exposes:

```text
POST /runtime/rebuild
```

The request must carry exact runtime target facts:

```text
volumeID
replicaID
targetDataAddr
sessionID
epoch
endpointVersion
fromLsn
frontierHintLsn
basePinLsn
```

The endpoint refuses non-primary local state and incomplete lineage. The
replication layer then validates replica identity, data address, epoch, and
endpoint version before calling the peer executor.

The endpoint response reports:

```text
runtimeState=started
durableFrontierKnown=false
```

The authority executor treats `runtimeState=started` as a running state and
does not mark the rebuild target blocked or caught-up.

## Non-Claims

Phase 64 does not claim:

```text
terminal durable frontier
caught_up after runtime start
frontend publication
failback
ACK eligibility mutation
NVMe behavior
```

## Verification

Local:

```text
go test ./core/ops ./core/host/volume ./core/replication ./cmd/blockvolume
C:\work\swblock.exe validate testops\scenarios\blockvolume-runtime-endpoint-chain.yaml
```

Live:

```text
20260625-012440-775a blockvolume-runtime-endpoint-chain PASS 18/18
```

Terminal evidence:

```text
phase64_blockvolume_runtime_endpoint_status=ok
runtime_state_started_supported=true
authority_executor_started_result_not_blocked=true
blockvolume_runtime_endpoint_opt_in=true
blockvolume_runtime_endpoint_posts_started=true
blockvolume_runtime_endpoint_requires_primary=true
blockvolume_runtime_endpoint_requires_lineage=true
replication_runtime_rejects_lineage_drift=true
runtime_endpoint_terminal_frontier_claimed=false
frontend_publication_allowed=false
failback_allowed=false
```

## Next

Phase 65 should add terminal runtime evidence: a way for the running rebuild
session to report durable frontier/caught-up completion back to the authority
target without relying on start-time assumptions. Frontend publication,
failback, and NVMe should remain out of scope until that terminal evidence is
gated.
