# Phase 81 Finished Plan: Failback Service RPC

Status: complete.

## Problem

Phase 80 exposed a master-owned runtime factory, but the failback executor is a
separate process. It cannot call an in-memory `Host` method. The next boundary
is a blockmaster RPC that can reach the live Publisher while remaining disabled
by default.

## Implementation

Added protobuf service:

```text
FailbackService.ExecuteFailback(FailbackRequest) -> FailbackResponse
```

Regenerated:

```text
core/rpc/control/control.pb.go
core/rpc/control/control_grpc.pb.go
```

Added blockmaster config/flag:

```text
Config.FailbackRuntimeRPC
--failback-runtime-rpc
```

Default is false. The chart does not enable it.

## Behavior

Default:

```text
ExecuteFailback -> FailedPrecondition("failback runtime RPC is disabled")
authority unchanged
```

Enabled:

```text
ExecuteFailback
  -> h.FailbackAuthorityRuntime().ExecuteFailback
  -> Publisher.apply(IntentReassign)
  -> FailbackResponse terminal evidence
```

Request evidence:

```text
expected_current_replica_id
expected_current_epoch
ack_eligible
frontend_fenced_before_failback
durable_frontier_covered
no_cross_volume_identity_change
```

The request carries no minted epoch or endpoint version.

## Gate

Added:

```text
scripts/run-phase81-failback-service-rpc-gate.sh
testops/scenarios/failback-service-rpc-chain.yaml
```

The gate checks:

```text
FailbackService registered
RPC disabled by default
flag default false
flag can opt in
enabled RPC advances Publisher through master runtime
frontend publication remains false
storage mutation remains false
```

## Verification

```text
go test ./core/host/master -run "TestFailbackService|TestHostFailbackAuthorityRuntimeUsesLivePublisher" -count=1 -v
go test ./cmd/blockmaster -run "TestParseFlags_FailbackRuntimeRPCDisabledByDefault|TestBlockmasterBareTopologyRegistersVolumeControlServices" -count=1 -v
go test ./core/authority ./core/ops ./core/host/master ./cmd/blockmaster ./cmd/sw-block
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase81-failback-service-rpc-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-service-rpc-chain.yaml
git diff --check
```

Terminal evidence:

```text
phase81_failback_service_rpc_status=ok
core_master_failback_service_tests=pass
cmd_blockmaster_failback_service_tests=pass
failback_service_default_disabled=true
enabled_failback_service_advances_publisher=true
failback_runtime_rpc_flag_default_false=true
failback_runtime_rpc_flag_opt_in=true
failback_service_registered=true
authority_epoch_advanced=true
single_primary_after_failback=true
publish_target_swapped_after_failback=true
public_rpc_enabled_by_default=false
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Non-Claims

Phase 81 does not implement:

```text
chart-enabled failback RPC
automatic failback from the deployed controller loop
failback executor HTTP/gRPC client to blockmaster
blockvolume frontend switching
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

## Next

Add the failback executor client transport to call this RPC only when explicit
execution policy is enabled.
