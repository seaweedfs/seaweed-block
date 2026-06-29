# Phase 94 QA Sign-off: Failback Deployed gRPC Smoke

Status: pending QA.

## Scope

Validate that the full opt-in failback suite renders coherently and that the
executor can call a real blockmaster `FailbackService` through gRPC in local
test.

## Gate

Run:

```text
bash scripts/run-phase94-failback-deployed-grpc-smoke-gate.sh .
swblock validate testops/scenarios/failback-deployed-grpc-smoke-chain.yaml
```

## Required Evidence

The summary must contain:

```text
phase94_failback_deployed_grpc_smoke_status=ok
helm_lint=pass
default_omits_failback_runtime_rpc=true
default_omits_failback_target_owner=true
default_omits_failback_executor=true
default_omits_activate_targets=true
default_omits_enable_execution=true
enabled_renders_failback_runtime_rpc=true
enabled_renders_failback_target_owner=true
enabled_renders_failback_executor=true
enabled_target_owner_activates_targets=true
enabled_target_owner_policy=true
enabled_target_owner_runtime_endpoint=true
enabled_executor_execution=true
enabled_executor_policy=true
enabled_executor_grpc_runtime=true
executor_grpc_uses_real_master_service=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
terminal_evidence_required=true
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
live_kubernetes_install_claimed=false
```

## Pass Criteria

- Default chart remains non-mutating and omits failback components.
- Fully enabled chart renders blockmaster RPC, target-owner activation, and
  executor gRPC runtime flags.
- Local real-master gRPC tests pass.
- Terminal evidence is required for `failed_back`.
- No live Kubernetes install or frontend publication is claimed.
