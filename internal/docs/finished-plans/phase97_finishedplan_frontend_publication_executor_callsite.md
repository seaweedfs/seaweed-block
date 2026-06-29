# Phase 97 Finished Plan: Frontend Publication Executor Call-site

Status: complete.

## Problem

Phase 96 created a disabled `SwBlockFrontendPublication` target from terminal
failback evidence. The next gap was the executor side: the CLI and chart did
not expose an explicit execution policy for frontend publication, and the
runtime request did not carry failback-source facts.

## What Changed

Phase 97 wires the explicit execution path:

- CLI flags:
  - `--enable-execution`
  - `--execution-policy`
  - `--frontend-publication-runtime-url`
- Helm values:
  - `frontendPublicationExecutor.execution.enabled`
  - `frontendPublicationExecutor.execution.policy`
  - `frontendPublicationExecutor.execution.runtimeUrl`
- runtime request fields:
  - `sourceFailbackName`
  - `failbackCompleted`
  - `authorityEpochAdvanced`
  - `singlePrimaryAfterFailback`
  - `publishTargetSwappedAfterFailback`

The executor can invoke `FrontendPublicationRuntime` for an enabled
failback-source target and write terminal published status only when runtime
terminal evidence is valid.

## Boundary

The phase does not claim:

- Kubernetes-deployed frontend publication;
- workload-visible data-path switch;
- application I/O after failback;
- failback re-entry;
- storage mutation.

The runtime result must say:

```text
frontendPublished=true
failbackStarted=false
noStorageMutation=true
noCrossVolumeIdentityChange=true
```

## Verification

Scenario:

```text
testops/scenarios/frontend-publication-executor-callsite-chain.yaml
```

Gate:

```text
scripts/run-phase97-frontend-publication-executor-callsite-gate.sh
```

Validated:

```text
swblock run testops/scenarios/frontend-publication-executor-callsite-chain.yaml
run=20260626-160330-ecc9
result=PASS 16/16
```

Key evidence:

```text
phase97_frontend_publication_executor_callsite_status=ok
failback_target_runtime_invoked=true
frontend_publication_attempts=1
frontend_published=true
failback_attempts=0
failback_started=false
publication_status_reason=frontend_published
publication_mutation_allowed=false
frontend_publication_executor_default_off=true
frontend_publication_execution_requires_policy=true
frontend_publication_runtime_url_requires_enable=true
storage_mutation_allowed=false
```

## Next

Phase 98 should be the workload-visible frontend publication gate: deploy the
suite, publish the post-failback path through product-owned runtime, and verify
reader/writer I/O against that path without cross-volume publication.
