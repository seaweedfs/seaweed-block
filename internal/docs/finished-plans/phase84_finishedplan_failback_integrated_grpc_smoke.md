# Phase 84 Finished Plan: Failback Integrated gRPC Smoke

Status: complete.

Date: 2026-06-26.

## What Changed

Phase 84 adds an integrated local product smoke for the failback execution
path. The new test starts a real blockmaster Host with the failback RPC enabled,
seeds a current r2 authority line, then runs the failback executor reconciler
through a gRPC runtime pointed at that blockmaster.

The path under test is:

```text
executor reconciler -> gRPC runtime -> FailbackService -> master Publisher
```

This is stronger than the Phase 82 fake-service test because the final authority
mutation is performed by the product-owned master Publisher.

## Evidence

The gate proves:

```text
service disabled by default
enabled service delegates to host runtime
executor gRPC runtime uses the real master service
executor writes failed_back status
master Publisher epoch advances
publish target swaps to returned replica
frontend publication remains false
storage mutation remains false
```

## Verification

```text
go test ./core/host/master -run "Test(FailbackServiceDefaultDisabled|FailbackServiceEnabledUsesHostRuntime|FailbackExecutorGRPCRuntimeUsesRealMasterService)" -count=1 -v
scripts/run-phase84-failback-integrated-grpc-smoke.sh .
swblock validate testops/scenarios/failback-integrated-grpc-smoke-chain.yaml
```

Result:

```text
phase84_failback_integrated_grpc_status=ok
executor_grpc_uses_real_master_service=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
```

## Non-Claims

Phase 84 does not enable automatic deployed failback, frontend publication,
storage catch-up traffic, workload mutation, or NVMe ANA behavior.
