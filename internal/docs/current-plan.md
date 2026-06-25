# Current Plan: Phase 65 Runtime Terminal Evidence

Status: complete.

## Goal

Phase 64 added the opt-in blockvolume runtime endpoint, but it deliberately
returned only:

```text
runtimeState=started
durableFrontierKnown=false
```

Phase 65 closes the next required gap: terminal runtime evidence. A runtime
session that was started by the blockvolume endpoint can now later report its
terminal durable frontier back through the same runtime HTTP contract.

The rule is:

```text
started -> running -> terminal session close -> caught_up
```

No frontend publication, failback, ACK eligibility mutation, or NVMe claim is
added.

## Delivered

### D1: Transport Session Terminal Status

`BlockExecutor` now records terminal session results from the existing
`finishSession` / recovery close path:

```text
state=caught_up
achievedLSN=<durable frontier>
```

Failures are recorded as:

```text
state=failed
failureKind=<typed transport/storage failure>
failReason=<diagnostic text>
```

This is runtime-owned evidence, not a status-surface guess.

### D2: Replication Runtime Status Query

`ReplicationVolume.RuntimeRecoveryStatus` validates the same replica identity,
target address, epoch, and endpoint-version facts used by
`StartRuntimeRecovery`, then reads the executor's session status.

### D3: HTTP Runtime Terminal Response

`POST /runtime/rebuild` is now idempotent for the same session:

```text
unknown -> start runtime recovery -> runtimeState=started
running -> runtimeState=started
caught_up -> runtimeState=caught_up, durableFrontierKnown=true
failed -> HTTP 409
```

Terminal caught-up responses do not restart recovery traffic.

### D4: Authority Executor Transition

The authority executor already understood terminal runtime results. Phase 65
adds the regression that two reconciles over the same target transition:

```text
runtimeState=started -> SwBlockReplicaRebuild.status.state=running
runtimeState=caught_up + durableFrontierLsn -> state=caught_up
```

### D5: Gate

Gate files:

```text
scripts/run-phase65-runtime-terminal-evidence-gate.sh
testops/scenarios/runtime-terminal-evidence-chain.yaml
```

## Non-Claims

Phase 65 does not claim:

```text
frontend publication
failback
ACK eligibility mutation
automatic publish target change
NVMe/ANA behavior
```

The rebuild target can become `caught_up`, but publication/failback must remain
a later gated decision.

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
transport_records_caught_up_session=true
replication_reports_terminal_frontier=true
runtime_endpoint_returns_caught_up_without_restart=true
runtime_endpoint_still_starts_unknown_session=true
authority_executor_started_then_caught_up=true
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

Phase 66 should decide the next bounded operation step. The clean next step is
not NVMe yet; it is to consume `caught_up` as a precondition for a still-bounded
publication decision, while keeping failback/frontend mutation disabled until a
separate admission/RBAC/evidence gate exists.
