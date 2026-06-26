# Current Plan: Phase 80 Master Failback Runtime Factory

Status: complete.

## Goal

Phase 79 wired the failback executor to an in-process authority runtime adapter,
but the adapter still needed a safe construction point from the component that
owns the live Publisher. Phase 80 exposes that construction point from
`master.Host`.

This remains deliberately pre-RPC and disabled by default:

```text
no public failback RPC
no automatic deployed failback loop
no frontend publication
no storage mutation
```

## Deliverables

### D1: Master Factory

Added:

```text
(*master.Host).FailbackAuthorityRuntime() authority.FailbackAuthorityRuntime
```

The factory returns a runtime backed by:

```text
h.Publisher()
```

Constructing the runtime does not execute failback. Callers still need the
Phase 79 explicit policy gate before invoking it.

### D2: Host-Level Test

Added:

```text
TestHostFailbackAuthorityRuntimeUsesLivePublisher
```

The test seeds authority through the normal product-loop path:

```text
placement + observation -> RunLifecycleProductTick -> r2 current authority
```

Then it invokes the host failback runtime:

```text
expected current: r2@current_epoch
target returned replica: r1
```

The host's live Publisher advances to:

```text
r1@(current_epoch+1)
```

### D3: Gate

Added:

```text
scripts/run-phase80-master-failback-runtime-factory-gate.sh
testops/scenarios/master-failback-runtime-factory-chain.yaml
```

The gate proves:

```text
host failback runtime uses live Publisher
publisher authority line advances
authority epoch advances
single primary after failback
publish target swaps after failback
no public failback RPC was added
automatic failback remains disabled
frontend publication remains false
storage mutation remains false
```

## Non-Claims

Phase 80 does not implement:

```text
public blockmaster failback RPC
automatic failback from the deployed controller loop
blockvolume frontend switching
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

## Verification

```text
go test ./core/host/master -run "TestHostFailbackAuthorityRuntimeUsesLivePublisher" -count=1 -v
go test ./core/authority ./core/ops ./core/host/master ./cmd/blockmaster ./cmd/sw-block
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase80-master-failback-runtime-factory-gate.sh .
C:\work\swblock.exe validate testops\scenarios\master-failback-runtime-factory-chain.yaml
git diff --check
```

Terminal evidence:

```text
phase80_master_failback_runtime_factory_status=ok
core_master_failback_runtime_tests=pass
host_failback_runtime_uses_live_publisher=true
publisher_authority_line_advanced=true
authority_epoch_advanced=true
single_primary_after_failback=true
publish_target_swapped_after_failback=true
no_storage_mutation=true
no_cross_volume_identity_change=true
automatic_failback_enabled=false
public_failback_rpc_added=false
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Next

The next phase should add the first disabled-by-default product wiring from
executor composition to this master factory, or decide that a public RPC is the
right boundary and add that as an explicitly gated API.
