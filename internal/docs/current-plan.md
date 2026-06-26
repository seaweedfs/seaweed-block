# Current Plan: Phase 84 Failback Integrated gRPC Smoke

Status: complete.

## Goal

Phase 84 proves the executor-to-blockmaster failback runtime path as one local
product loop:

```text
FailbackExecutorReconciler
  -> GRPCFailbackRuntime
  -> blockmaster FailbackService
  -> master-owned FailbackAuthorityRuntime
  -> live Publisher.apply(IntentReassign)
```

This closes the fake-service gap from Phase 82. The test uses the real
blockmaster service and real Publisher, not a fake gRPC service.

## Deliverables

### D1: Real-Service Integration Test

Added:

```text
TestFailbackExecutorGRPCRuntimeUsesRealMasterService
```

The test:

```text
starts a master Host with FailbackRuntimeRPC enabled
seeds verified existing placement so r2 is current
runs FailbackExecutorReconciler with NewGRPCFailbackRuntime(h.Addr())
writes failed_back status from terminal evidence
asserts master Publisher advances r2@N -> r1@N+1
asserts publish target swaps to r1 endpoint
asserts no frontend publication and no storage mutation
```

### D2: Default-Off Carry-Forward

The gate also keeps the service boundary tests in scope:

```text
TestFailbackServiceDefaultDisabled
TestFailbackServiceEnabledUsesHostRuntime
```

This proves the RPC remains disabled by default and only delegates to the
host-owned runtime when explicitly enabled.

### D3: Gate

Added:

```text
scripts/run-phase84-failback-integrated-grpc-smoke.sh
testops/scenarios/failback-integrated-grpc-smoke-chain.yaml
```

## Non-Claims

Phase 84 does not implement:

```text
deployed Kubernetes failback controller loop
automatic failback target selection
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

## Verification

```text
go test ./core/host/master -run "Test(FailbackServiceDefaultDisabled|FailbackServiceEnabledUsesHostRuntime|FailbackExecutorGRPCRuntimeUsesRealMasterService)" -count=1 -v
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase84-failback-integrated-grpc-smoke.sh .
C:\work\swblock.exe validate testops\scenarios\failback-integrated-grpc-smoke-chain.yaml
```

Terminal evidence:

```text
phase84_failback_integrated_grpc_status=ok
core_host_master_failback_grpc_tests=pass
service_default_disabled_test=true
service_enabled_uses_host_runtime=true
executor_grpc_uses_real_master_service=true
executor_status_failed_back=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
terminal_evidence_required=true
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Next

The next phase should move from local integration to deployed-loop safety:
either a Kubernetes smoke with explicitly enabled failback components, or a
controller policy gate that proves no automatic failback happens without a
ready `SwBlockReplicaFailback` target and explicit execution policy.
