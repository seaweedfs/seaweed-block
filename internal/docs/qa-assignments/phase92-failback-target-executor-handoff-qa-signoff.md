# Phase 92 QA Sign-off: Failback Target -> Executor Handoff

Status: pending QA.

## Scope

Validate the local/fake-runtime handoff from failback target owner to failback
executor.

## Gate

Run:

```text
bash scripts/run-phase92-failback-target-executor-handoff-gate.sh .
swblock validate testops/scenarios/failback-target-executor-handoff-chain.yaml
```

## Required Evidence

The summary must contain:

```text
phase92_failback_target_executor_handoff_status=ok
go_test_core_ops_failback_handoff=pass
target_owner_created_enabled_target=true
executor_consumed_target=true
runtime_request_expected_current_replica=r2
runtime_request_expected_current_epoch=7
executor_terminal_state=failed_back
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
```

## Pass Criteria

- Target owner creates exactly one enabled target from current authority facts.
- Executor consumes the target and calls the supplied runtime.
- Runtime request includes expected-current replica and epoch.
- Executor writes `failed_back` only after terminal evidence.
- No frontend publication or storage mutation is claimed.

## Non-Claims

This is not a live deployed failback smoke. It does not prove a published image
or a Kubernetes blockmaster gRPC service path.
