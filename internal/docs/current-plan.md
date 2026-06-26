# Current Plan: Phase 94 Failback Deployed gRPC Smoke

Status: complete.

## Goal

Phase 94 connects the two halves of the recent failback work:

```text
Helm deployable suite renders with all opt-in failback components
executor can call a real blockmaster FailbackService through gRPC
```

This is not a live Kubernetes install / real PVC failback claim. It is the
deployed-suite coherence gate plus the existing real-master gRPC smoke.

## Deliverables

### D1: Deployed Suite Render Gate

The gate renders Helm defaults and the fully enabled suite.

Defaults must omit:

```text
--failback-runtime-rpc
failback target owner
failback executor
--activate-targets
--enable-execution
```

Explicit values must render:

```text
blockmaster --failback-runtime-rpc
failback target owner --activate-targets --activation-policy --runtime-endpoint=...
failback executor --enable-execution --execution-policy --failback-runtime-grpc-addr=...
```

### D2: Real Master gRPC Smoke

The gate runs:

```text
TestFailbackServiceDefaultDisabled
TestFailbackServiceEnabledUsesHostRuntime
TestFailbackExecutorGRPCRuntimeUsesRealMasterService
```

This proves the executor can call a real blockmaster `FailbackService`, the
master publisher advances authority, and terminal evidence drives
`failed_back`.

### D3: Gate

Added:

```text
scripts/run-phase94-failback-deployed-grpc-smoke-gate.sh
testops/scenarios/failback-deployed-grpc-smoke-chain.yaml
```

## Verification

```text
bash scripts/run-phase94-failback-deployed-grpc-smoke-gate.sh .
swblock validate testops/scenarios/failback-deployed-grpc-smoke-chain.yaml
```

Expected terminal evidence:

```text
phase94_failback_deployed_grpc_smoke_status=ok
enabled_renders_failback_runtime_rpc=true
enabled_target_owner_activates_targets=true
enabled_executor_grpc_runtime=true
executor_grpc_uses_real_master_service=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
live_kubernetes_install_claimed=false
```

## Next

Phase 95 can pay the real lab cost:

```text
fresh local images
install full failback suite in k3s
create SwBlockVolume + returned-replica evidence
target owner creates enabled target
executor calls live blockmaster service
verify authority status and cleanup
```

Frontend publication must remain a separate later phase.
