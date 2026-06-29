# Phase 62 Finished Plan: Authority Executor HTTP Runtime

Status: complete.

QA: PASS.

## Goal

Phase 62 converts the Phase 61 in-process rebuild runtime seam into a concrete
HTTP transport path. It keeps the runtime opt-in and explicit, because the
current blockvolume control surface is read-only and the CRD does not yet carry
all addressing/session facts needed to safely call the live transport.

## Delivered

Code:

```text
core/ops/authority_rebuild_runtime_http.go
core/ops/authority_rebuild_runtime_http_test.go
core/ops/authority_executor_controller.go
cmd/sw-block/main.go
cmd/sw-block/main_test.go
```

Gate:

```text
scripts/run-phase62-authority-executor-http-runtime-gate.sh
testops/scenarios/authority-executor-http-runtime-chain.yaml
```

Docs:

```text
internal/docs/current-plan.md
internal/docs/qa-assignments/phase62-authority-executor-http-runtime-qa-signoff.md
internal/docs/product-roadmap.md
```

## Behavior

Without `--rebuild-runtime-url`, authority-executor keeps the Phase 61 planned
fallback:

```text
SwBlockReplicaRebuild.status.state=planned
rebuildTrafficStarted=false
```

With `--rebuild-runtime-url` and `--allowed-mutation-class rebuild_traffic`:

```text
state=running
POST runtime request
runtime terminal frontier accepted -> state=caught_up
runtime error or insufficient frontier -> state=blocked
```

The HTTP request carries:

```text
volumeName, volumeID, pvcName, replicaID
durableFrontierKnown, durableFrontierLsn
requiredFrontierKnown, requiredFrontierLsn
frontendFenced, frontendPrimaryReady
noFrontendPublication, noCrossVolumeMutation
evidenceRefs
```

## Non-Claims

Phase 62 does not claim:

```text
blockvolume_runtime_endpoint_wired
frontend_publication
failback
ack_eligibility_mutation
automatic runtime URL discovery
```

Those require a blockvolume-side endpoint and runtime addressing facts.

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block
C:\work\swblock.exe validate testops\scenarios\authority-executor-http-runtime-chain.yaml
```

Live:

```text
20260624-170419-1409 authority-executor-http-runtime-chain PASS 26/26
```

Terminal evidence:

```text
phase62_authority_executor_http_runtime_status=ok
http_runtime_posts_request=true
http_runtime_decodes_terminal_frontier=true
cli_rebuild_runtime_url_enabled=true
rebuild_status_running_written=true
rebuild_status_caught_up_written=true
rebuild_status_blocked_on_runtime_failure=true
blockvolume_runtime_endpoint_wired=false
frontend_publication_allowed=false
failback_allowed=false
```

## Next

Phase 63 should add the blockvolume-side runtime endpoint/addressing contract.
That work must not infer data-path addresses or session facts that are not in
the CRD/runtime target evidence.
