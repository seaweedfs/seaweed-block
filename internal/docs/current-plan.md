# Current Plan: Phase 88 Failback Deployed Suite Packaging

Status: complete.

## Goal

Phase 88 closes the next safe step after the source-gated failback runtime:
prove the deployable Kubernetes component suite can be rendered together with
explicit opt-in values, bounded RBAC, and schema coverage.

This phase does **not** claim automatic live failback. It packages the pieces
needed for a later live release smoke:

```text
blockmaster FailbackService RPC
failback target owner
failback executor
executor gRPC runtime address
explicit execution policy
```

## Deliverables

### D1: Helm Values Schema

Added `failbackTargetOwner` to `charts/seaweed-block/values.schema.json` so the
full failback component suite is schema-visible, not just template-visible.

### D2: Deployed Suite Gate

Added:

```text
scripts/run-phase88-failback-deployed-suite-gate.sh
testops/scenarios/failback-deployed-suite-chain.yaml
```

The gate proves:

```text
defaults omit failback runtime RPC, target owner, executor, and execution flags
explicit values render all three deployable pieces
target owner can create only SwBlockReplicaFailback targets
executor can write only SwBlockReplicaFailback.status
execution still requires explicit policy and gRPC address
frontend publication after failback is still not rendered
values schema covers target owner and executor knobs
```

## Verification

```text
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase88-failback-deployed-suite-gate.sh .
C:\work\swblock.exe validate testops/scenarios/failback-deployed-suite-chain.yaml
```

Expected terminal evidence:

```text
phase88_failback_deployed_suite_status=ok
default_omits_failback_target_owner=true
default_omits_failback_executor=true
enabled_renders_failback_target_owner=true
enabled_renders_failback_executor=true
values_schema_covers_failback_suite=true
target_owner_rbac_create_targets_only=true
executor_rbac_status_only=true
automatic_failback_claimed=false
frontend_publication_after_failback_claimed=false
```

## Next

The next step is the first real Kubernetes failback release smoke:

```text
fresh local images
install with failback suite enabled
create a real volume and returned-replica target
executor calls blockmaster gRPC
authority moves only with terminal evidence
no frontend publication claim until the publication phase lands
```

If that smoke is too lab-expensive, defer the live claim and start the next
operation-layer close gate before NVMe.
