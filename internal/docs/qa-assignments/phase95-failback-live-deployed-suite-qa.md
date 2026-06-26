# Phase 95 QA: Failback Live Deployed Suite Smoke

Verdict: **PASS**.

Live runner:

```text
swblock run testops/scenarios/failback-live-deployed-suite-chain.yaml
run=20260626-152618-5993
result=PASS 22/22
```

Terminal evidence:

```text
phase95_failback_live_deployed_suite_status=ok
live_kubernetes_install_claimed=true
local_images_built_and_imported=true
remote_import_nodes=192.168.1.181
helm_lint=pass
helm_install=pass
deployed_suite_pods_ready=true
blockmaster_grpc_ready=true
first_volume_writer_reader=pass
swblockvolume_failback_contract_patched=true
failback_target_created=true
failback_executor_completed=true
executor_status_failed_back=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
failback_status_mutation_allowed=false
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
executor_patch_failback_status_allowed=yes
executor_patch_swblockvolumes_denied=no
executor_patch_pvc_denied=no
executor_create_pods_denied=no
cleanup_status=ok
```

`target.final.json` confirms:

```text
status.state=failed_back
status.reasonCode=failback_completed
status.failbackStarted=true
status.authorityEpochAdvanced=true
status.singlePrimaryAfterFailback=true
status.publishTargetSwappedAfterFailback=true
status.noCrossVolumeIdentityChange=true
status.failbackMutationAllowed=false
```

Lab cleanup after the run was clean: no sw-block pods/deployments, no example
PVC/PV, and no `io.seaweedfs` iSCSI node records.

## Gate Notes

The first iterations exposed three gate/harness issues before the final PASS:

- The first-volume summary field is `pvc=...`, not `pvc_name=...`.
- The gate needed to restore `set -e` after its cleanup helper; otherwise a
  failed rollout could continue into later assertions.
- Because csi-node is a DaemonSet, local test images must be imported to m01 as
  well as m02, or Helm can time out on image pull.

These were fixed in the gate. The product path that matters for this phase
passed once the harness represented the intended deployment topology.

## Objective

Validate that the opt-in returned-replica failback deployed suite can run in a
real k3s cluster and complete one authority failback through the live
blockmaster gRPC service.

This gate must not treat the result as frontend publication or workload
data-path failback.

## Source

Branch:

```text
phase54-returned-replica-reintegration-executor
```

Primary gate:

```text
scripts/run-phase95-failback-live-deployed-suite-gate.sh
testops/scenarios/failback-live-deployed-suite-chain.yaml
```

## Required Run

Run on the k3s lab node with the runner:

```text
swblock run testops/scenarios/failback-live-deployed-suite-chain.yaml
```

The gate builds and imports fresh local images, installs Helm, runs the first
PVC writer/reader path, injects a returned-replica failback contract into
`SwBlockVolume.status`, and waits for the deployed failback target-owner and
executor to complete.

## PASS Criteria

The summary file must include:

```text
phase95_failback_live_deployed_suite_status=ok
live_kubernetes_install_claimed=true
helm_install=pass
deployed_suite_pods_ready=true
first_volume_writer_reader=pass
swblockvolume_failback_contract_patched=true
failback_target_created=true
failback_executor_completed=true
executor_status_failed_back=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
failback_status_mutation_allowed=false
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
executor_patch_failback_status_allowed=yes
executor_patch_swblockvolumes_denied=no
executor_patch_pvc_denied=no
executor_create_pods_denied=no
cleanup_status=ok
```

Also inspect `target.final.json` and confirm:

- `status.state=failed_back`
- `status.reasonCode=failback_completed`
- `status.failbackStarted=true`
- `status.authorityEpochAdvanced=true`
- `status.singlePrimaryAfterFailback=true`
- `status.publishTargetSwappedAfterFailback=true`
- `status.noCrossVolumeIdentityChange=true`
- `status.failbackMutationAllowed=false`

## FAIL Criteria

Fail the gate if:

- Helm install requires unpublished or stale-image-only behavior;
- any failback component CrashLoops;
- first-volume writer/reader fails;
- target-owner does not create `SwBlockReplicaFailback`;
- executor cannot call blockmaster gRPC;
- executor writes anything other than `failed_back/failback_completed` after a
  valid terminal result;
- RBAC allows executor to patch `SwBlockVolume`, PVCs, Pods, or StorageClasses;
- the report claims frontend publication or storage mutation;
- cleanup verifier reports residue.

## Scope Boundary

This gate proves:

```text
deployed target-owner/executor can execute a bounded authority failback through
live blockmaster gRPC and write terminal target status
```

This gate does not prove:

```text
frontend publication after failback
workload path switch
CSI republish
data-path IO after failback
```

Those must remain separate phases.
