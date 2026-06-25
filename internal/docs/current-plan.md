# Current Plan: Phase 61 Authority Executor Runtime Call-site

Status: complete.

Branch target: `phase54-returned-replica-reintegration-executor`

## Goal

Phase 60 proved the existing rebuild/catch-up data path below Kubernetes:

```text
engine / adapter -> transport/recovery -> byte-equal convergence
```

Phase 61 connects the authority executor to a runtime call-site seam so rebuild
execution is no longer only a planned status write:

```text
SwBlockVolume.status.executorContracts[]
  -> authority-executor
  -> AuthorityRebuildRuntime.ExecuteRebuild(...)
  -> SwBlockReplicaRebuild.status running/caught_up/blocked
```

## Scope

In scope:

- Add a narrow `AuthorityRebuildRuntime` interface to the authority executor.
- Preserve Phase 59 behavior when no runtime is provided: write `planned`.
- When a runtime is provided, write `running`, invoke the runtime, then write
  `caught_up` if terminal durable-frontier evidence covers the required LSN.
- Write `blocked` if the runtime fails or returns insufficient terminal
  evidence.
- Keep non-claims explicit: no frontend publication, no failback, no ACK
  eligibility mutation, no blockvolume RPC yet.
- Add a TestRunner gate for the call-site and status mapping.

Out of scope:

- No blockvolume RPC/HTTP/gRPC runtime transport.
- No Kubernetes pod-to-pod rebuild command.
- No RF=3 orchestration.
- No frontend publication.
- No failback.
- No ACK eligibility mutation.

## Deliverables

### D1: Runtime Call-site Interface

Status: complete.

Added:

```text
AuthorityRebuildRuntime
AuthorityRebuildRuntimeRequest
AuthorityRebuildRuntimeResult
```

The authority executor uses this interface only for
`allowed-mutation-class=rebuild_traffic`.

### D2: Status Mapping

Status: complete.

Runtime path status flow:

```text
planned path: no runtime -> state=planned
runtime path: runtime invoked -> state=running -> state=caught_up
failure path: runtime error or insufficient frontier -> state=blocked
```

### D3: Regression Tests

Status: PASS.

Added tests proving:

- planned status is preserved when no runtime exists;
- runtime request carries volume/replica/frontier/fencing evidence;
- successful runtime writes `running` then `caught_up`;
- failed runtime writes `blocked`.

### D4: TestRunner Gate

Status: QA PASS.

Added:

```text
scripts/run-phase61-authority-executor-runtime-callsite-gate.sh
testops/scenarios/authority-executor-runtime-callsite-chain.yaml
```

Live run:

```text
20260623-212206-8afb authority-executor-runtime-callsite-chain PASS 28/28
```

Terminal evidence:

```text
phase61_authority_executor_runtime_callsite_status=ok
runtime_callsite_invoked=true
rebuild_status_running_written=true
rebuild_status_caught_up_written=true
rebuild_status_blocked_on_runtime_failure=true
rebuild_traffic_started_when_runtime_invoked=true
durable_frontier_caught_up_after_runtime=true
planned_status_preserved_without_runtime=true
blockvolume_rpc_wired=false
frontend_publication_allowed=false
failback_allowed=false
ack_eligibility_mutation_allowed=false
```

### D5: Close Docs

Status: complete.

Sign-off:

```text
internal/docs/qa-assignments/phase61-authority-executor-runtime-callsite-qa-signoff.md
```

Finished plan:

```text
internal/docs/finished-plans/phase61_finishedplan_authority_executor_runtime_callsite.md
```

## Phase 62: Authority Executor HTTP Runtime Transport

Status: complete.

QA: PASS.

## Why

Phase 61 proved the authority executor can call an in-process
`AuthorityRebuildRuntime`. Phase 62 adds a concrete HTTP transport selected by
`--rebuild-runtime-url`, while staying honest that blockvolume does not yet have
a mutating rebuild control endpoint.

The current blockvolume status HTTP surface is read-only, and the current CRD
evidence does not carry enough session/addressing facts to safely call
`transport.StartRebuild` directly. Phase 62 therefore wires the executor to an
explicit runtime URL and keeps the blockvolume endpoint as Phase 63 work.

## D1: HTTP Runtime Client

Status: implemented; local tests PASS.

Added `HTTPAuthorityRebuildRuntime`, which:

- POSTs `AuthorityRebuildRuntimeRequest` as JSON;
- decodes terminal `AuthorityRebuildRuntimeResult`;
- treats non-2xx responses as execution failures, letting the reconciler write
  blocked status.

## D2: CLI Wiring

Status: implemented; local tests PASS.

Added:

```text
sw-block ops authority-executor --rebuild-runtime-url <url>
```

The flag is accepted only with:

```text
--allowed-mutation-class rebuild_traffic
```

No runtime URL preserves the planned-only fallback from Phase 61.

## D3: Gate

Status: QA PASS.

Added:

```text
scripts/run-phase62-authority-executor-http-runtime-gate.sh
testops/scenarios/authority-executor-http-runtime-chain.yaml
```

Required terminal evidence:

```text
phase62_authority_executor_http_runtime_status=ok
http_runtime_posts_request=true
http_runtime_decodes_terminal_frontier=true
cli_rebuild_runtime_url_enabled=true
rebuild_status_running_written=true
rebuild_status_caught_up_written=true
blockvolume_runtime_endpoint_wired=false
frontend_publication_allowed=false
failback_allowed=false
```

Live run:

```text
20260624-170419-1409 authority-executor-http-runtime-chain PASS 26/26
```

## Exit

Phase 62 closed when local tests and the TestRunner gate proved the explicit
HTTP runtime transport can drive `running -> caught_up` / `blocked` status,
without claiming a blockvolume endpoint or frontend/failback behavior.

## Next: Phase 63

Add the blockvolume-side runtime endpoint/addressing contract. The first
acceptable slice is not "call StartRebuild somehow"; it must define the runtime
target evidence needed to safely identify the primary process, target replica,
session, epoch, endpoint version, and transport lane.
