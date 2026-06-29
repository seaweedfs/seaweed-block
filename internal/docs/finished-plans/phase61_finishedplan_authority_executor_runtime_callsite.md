# Phase 61 Finished Plan: Authority Executor Runtime Call-site

Status: complete.

QA: PASS.

## Goal

Phase 61 connects returned-replica rebuild planning to a bounded executor
runtime seam. Phase 59 could only write `planned`; Phase 60 proved the data path
below Kubernetes. Phase 61 adds the missing call-site shape between those two
layers.

## Delivered

Code:

```text
core/ops/authority_executor_controller.go
core/ops/authority_executor_controller_test.go
```

Gate:

```text
scripts/run-phase61-authority-executor-runtime-callsite-gate.sh
testops/scenarios/authority-executor-runtime-callsite-chain.yaml
```

Docs:

```text
internal/docs/current-plan.md
internal/docs/qa-assignments/phase61-authority-executor-runtime-callsite-qa-signoff.md
docs/roadmap.md
```

## Behavior

The authority executor now supports an optional `AuthorityRebuildRuntime`:

```text
nil runtime:
  SwBlockReplicaRebuild.status.state=planned

runtime success:
  state=running
  -> ExecuteRebuild(...)
  -> state=caught_up

runtime failure or insufficient terminal evidence:
  state=blocked
```

The runtime request carries the volume identity, replica id, durable/required
frontier, fencing facts, and evidence references. The status mapper keeps the
non-claims explicit:

```text
no_frontend_publication
no_failback
no_primary_authority_change
no_cross_volume_mutation
```

## Verification

Local:

```text
go test ./core/ops -run "TestAuthorityExecutorReconciler(WritesRebuildPlannedStatus|ExecutesRebuildRuntimeAndWritesCaughtUpStatus|WritesBlockedStatusWhenRebuildRuntimeFails)" -count=1
C:\work\swblock.exe validate testops\scenarios\authority-executor-runtime-callsite-chain.yaml
```

Live:

```text
20260623-212206-8afb authority-executor-runtime-callsite-chain PASS 28/28
```

Sign-off:

```text
internal/docs/qa-assignments/phase61-authority-executor-runtime-callsite-qa-signoff.md
```

## Boundary

This phase does not connect a concrete blockvolume RPC/HTTP/gRPC transport. It
proves the executor call-site and terminal-status mapping. Phase 62 should
provide the concrete runtime implementation and run it against the Phase 60
data path.
