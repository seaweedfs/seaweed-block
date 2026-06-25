# Phase 65 Finished Plan: Runtime Terminal Evidence

Status: complete.

QA: PASS.

## Goal

Phase 65 closes the terminal-evidence gap left by Phase 64. A blockvolume
runtime start is no longer the only observable fact; the runtime session can
now report caught-up completion with the achieved durable frontier.

## Delivered

Code:

```text
core/transport/executor.go
core/transport/rebuild_sender.go
core/replication/volume.go
core/replication/peer.go
core/host/volume/status_server.go
core/ops/authority_executor_controller_test.go
```

Gate:

```text
scripts/run-phase65-runtime-terminal-evidence-gate.sh
testops/scenarios/runtime-terminal-evidence-chain.yaml
```

Docs:

```text
internal/docs/current-plan.md
internal/docs/qa-assignments/phase65-runtime-terminal-evidence-qa-signoff.md
internal/docs/product-roadmap.md
```

## Behavior

`BlockExecutor` records terminal recovery-session results:

```text
caught_up: sessionID, replicaID, achievedLSN
failed: sessionID, replicaID, failureKind, failReason
```

`ReplicationVolume.RuntimeRecoveryStatus` validates the same lineage as
`StartRuntimeRecovery` and exposes that runtime fact upward.

`POST /runtime/rebuild` now behaves as an idempotent start-or-status endpoint:

```text
unknown session -> start -> runtimeState=started
running session -> runtimeState=started
caught_up session -> runtimeState=caught_up, durableFrontierKnown=true
failed session -> HTTP 409
```

The authority executor can use the terminal response to write:

```text
SwBlockReplicaRebuild.status.state=caught_up
reasonCode=rebuild_runtime_caught_up
durableFrontierCaughtUp=true
```

## Non-Claims

Phase 65 does not claim:

```text
ACK eligibility mutation
frontend publication
failback
automatic publish target change
NVMe/ANA behavior
```

## Verification

Local:

```text
go test ./core/transport ./core/replication ./core/host/volume ./core/ops ./cmd/blockvolume
C:\work\swblock.exe validate testops\scenarios\runtime-terminal-evidence-chain.yaml
```

Live:

```text
20260625-013718-69c8 runtime-terminal-evidence-chain PASS 14/14
```

Terminal evidence:

```text
phase65_runtime_terminal_evidence_status=ok
runtime_terminal_status_recorded=true
runtime_terminal_frontier_reported=true
runtime_endpoint_terminal_caught_up=true
runtime_endpoint_terminal_does_not_restart=true
authority_executor_running_to_caught_up=true
runtime_start_without_terminal_still_running=true
frontend_publication_allowed=false
failback_allowed=false
ack_eligibility_mutation_allowed=false
```

## Next

Phase 66 should use caught-up status as a precondition for the next bounded
operation decision. That should still be modeled as fact -> judgment -> action
-> evidence, with explicit admission/RBAC before any ACK eligibility,
frontend-publication, or failback mutation is enabled.
