# Current Plan: Phase 97 Frontend Publication Executor Call-site

Status: complete.

## Goal

Phase 97 connects the frontend-publication executor's explicit execution flags
to the runtime call-site for a post-failback target:

```text
SwBlockFrontendPublication target
sourceFailbackName=<SwBlockReplicaFailback>
failbackCompleted=true
frontendPublicationDecision=enabled
frontendPublicationMutationAllowed=true
runtimeEndpoint=<http runtime>
        |
        v
frontend publication executor
        |
        v
FrontendPublicationRuntime request
        |
        v
status.state=published
reasonCode=frontend_published
```

This is still **not** a workload-visible I/O claim. The phase proves the
executor/runtime handoff and terminal evidence contract only.

## Deliverables

### D1: Failback-source Runtime Request

`FrontendPublicationRuntimeRequest` now carries the post-failback source facts:

```text
sourceFailbackName
failbackCompleted
authorityEpochAdvanced
singlePrimaryAfterFailback
publishTargetSwappedAfterFailback
```

The executor accepts an enabled failback-source target only when these facts are
present and `noCrossVolumeIdentityChange=true`.

### D2: Explicit CLI Policy

`sw-block ops frontend-publication-executor` now supports:

```text
--enable-execution
--execution-policy
--frontend-publication-runtime-url <url>
```

Guards:

- `--frontend-publication-runtime-url` without `--enable-execution` is rejected;
- `--enable-execution` without `--execution-policy` is rejected by the
  reconciler;
- runtime terminal evidence must report `frontendPublished=true`,
  `failbackStarted=false`, `noStorageMutation=true`, and
  `noCrossVolumeIdentityChange=true`.

### D3: Helm Default-off Wiring

`frontendPublicationExecutor.execution` was added to chart values:

```yaml
execution:
  enabled: false
  policy: false
  runtimeUrl: ""
```

Default render still omits the executor. Explicit render can add:

```text
--enable-execution
--execution-policy
--frontend-publication-runtime-url=<url>
```

### D4: Runner Gate

Added:

```text
scripts/run-phase97-frontend-publication-executor-callsite-gate.sh
testops/scenarios/frontend-publication-executor-callsite-chain.yaml
```

The gate asserts:

- failback-source target invokes runtime only with explicit policy;
- policy-disabled execution is rejected;
- runtime URL without `--enable-execution` is rejected;
- runtime failure and invalid terminal evidence do not falsely publish;
- Helm default remains off;
- enabled Helm render includes execution flags;
- `failback_attempts=0`;
- `failback_started=false`;
- `storage_mutation_allowed=false`.

## Verification

Local checks:

```text
go test ./core/ops ./cmd/sw-block ./core/host/master -count=1
helm lint charts/seaweed-block
swblock validate testops/scenarios/frontend-publication-executor-callsite-chain.yaml
git diff --check
```

Runner check:

```text
swblock run testops/scenarios/frontend-publication-executor-callsite-chain.yaml
run=20260626-160330-ecc9
result=PASS 16/16
```

Terminal evidence:

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

The remaining boundary is workload-visible frontend path switching:

```text
run the deployed failback + frontend-publication suite
publish the post-failback frontend path through product-owned runtime
verify reader/writer against the new path
prove no cross-volume publication
cleanup leaves zero residue
```

That should remain separate because it is the first user-visible data-path
claim after failback.
