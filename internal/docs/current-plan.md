# Current Plan: Phase 81 Failback Service RPC

Status: complete.

## Goal

Phase 80 exposed a master-owned failback runtime factory, but a separate
executor process still needs a transport boundary to reach the master-owned
Publisher. Phase 81 adds that boundary as a blockmaster FailbackService RPC,
disabled by default.

The RPC is registered but not usable unless explicitly enabled:

```text
--failback-runtime-rpc
```

Default installs do not pass that flag.

## Deliverables

### D1: Wire Contract

Added protobuf service:

```text
service FailbackService {
  rpc ExecuteFailback(FailbackRequest) returns (FailbackResponse);
}
```

`FailbackRequest` carries:

```text
volume_id
replica_id
target_data_addr
target_ctrl_addr
expected_current_replica_id
expected_current_epoch
ack_eligible
frontend_fenced_before_failback
durable_frontier_covered
no_cross_volume_identity_change
evidence_refs
```

The request still carries no epoch to mint and no endpoint version. Those are
authored only inside the master Publisher.

### D2: Disabled-by-Default Gate

Added blockmaster config/flag:

```text
Config.FailbackRuntimeRPC
--failback-runtime-rpc
```

Default:

```text
FailbackRuntimeRPC=false
```

When disabled, `ExecuteFailback` returns `FailedPrecondition` and does not
mutate authority.

### D3: Enabled Path

When explicitly enabled, the handler delegates to:

```text
h.FailbackAuthorityRuntime().ExecuteFailback(...)
```

The enabled test proves:

```text
product-loop current authority -> ExecuteFailback -> Publisher line advanced
authorityEpochAdvanced=true
singlePrimaryAfterFailback=true
publishTargetSwappedAfterFailback=true
noStorageMutation=true
```

### D4: Gate

Added:

```text
scripts/run-phase81-failback-service-rpc-gate.sh
testops/scenarios/failback-service-rpc-chain.yaml
```

The gate proves:

```text
FailbackService is registered
RPC is disabled by default
flag default is false
flag can opt in
enabled RPC advances Publisher through master runtime
frontend publication remains false
storage mutation remains false
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

## Next

The next phase should wire the failback executor to call this RPC when and only
when execution policy is explicitly enabled, keeping default installs
status-only.
