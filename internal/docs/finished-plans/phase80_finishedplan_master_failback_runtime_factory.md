# Phase 80 Finished Plan: Master Failback Runtime Factory

Status: complete.

## Problem

Phase 79 proved that the failback executor can call an in-process authority
runtime adapter. The remaining wiring question was where that adapter should get
the live `Publisher`. The correct owner is blockmaster, not the CLI and not a
separate operator pod.

## Implementation

Added:

```text
(*master.Host).FailbackAuthorityRuntime() authority.FailbackAuthorityRuntime
```

The method returns the Phase 78 authority runtime backed by the host's live
Publisher:

```text
authority.FailbackAuthorityRuntime{Publisher: h.Publisher()}
```

This keeps dependency direction clean:

```text
master -> authority
ops -> authority adapter
```

`core/host/master` does not import `core/ops`.

## Behavior

The host-level test seeds a current authority line through the normal product
loop:

```text
placement + observation -> RunLifecycleProductTick -> current r2
```

Then it invokes the host failback runtime with:

```text
expectedCurrentReplicaID=r2
expectedCurrentEpoch=<current>
target replica=r1
```

The host's Publisher advances to:

```text
r1@(current+1)
```

The runtime result proves:

```text
authorityEpochAdvanced=true
singlePrimaryAfterFailback=true
publishTargetSwappedAfterFailback=true
noStorageMutation=true
noCrossVolumeIdentityChange=true
```

## Gate

Added:

```text
scripts/run-phase80-master-failback-runtime-factory-gate.sh
testops/scenarios/master-failback-runtime-factory-chain.yaml
```

The gate checks:

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

## Next

Connect the failback executor composition to this host factory behind an
explicit disabled-by-default product wiring gate, or introduce a public
blockmaster failback RPC with equivalent admission and evidence gates.
