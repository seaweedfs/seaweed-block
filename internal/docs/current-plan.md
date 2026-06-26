# Current Plan: Phase 95 Failback Live Deployed Suite Smoke

Status: complete.

## Goal

Phase 95 pays the real Kubernetes cost that Phase 94 intentionally deferred:

```text
fresh local images
install the opt-in failback deployed suite in k3s
create live first-volume authority state
inject returned-replica failback terminal evidence into SwBlockVolume.status
target owner creates an enabled SwBlockReplicaFailback target
executor calls the live blockmaster FailbackService over gRPC
executor writes failed_back terminal status
cleanup leaves zero residue
```

This is still **not** a frontend-publication or data-path failback claim. The
gate proves the deployed authority failback control path can run in Kubernetes.
Publishing the new frontend path to workload/CSI remains a later phase.

## Deliverables

### D1: Live Gate Script

Added:

```text
scripts/run-phase95-failback-live-deployed-suite-gate.sh
```

The script:

- builds and imports fresh `sw-block:phase95` / `sw-block-csi:phase95` images;
- generates Day-1 Helm values for a single-node lab target;
- enables only the failback suite:
  - `blockmaster.failbackRuntimeRPC=true`;
  - `failbackTargetOwner.create=true`;
  - `failbackTargetOwner.activation.enabled/policy=true`;
  - `failbackExecutor.create=true`;
  - `failbackExecutor.execution.enabled/policy=true`;
  - `failbackExecutor.execution.failbackRuntimeGrpcAddr=blockmaster.<ns>.svc:9333`;
- installs the chart and waits for blockmaster, failback target owner, and
  failback executor Deployments;
- runs the documented first-volume writer/reader path without immediate cleanup;
- extracts the live volume ID, primary replica, and authority epoch;
- patches a minimal `SwBlockVolume.status` failback contract with terminal
  returned-replica evidence;
- waits for the target owner to create `SwBlockReplicaFailback`;
- waits for the executor to write `status.state=failed_back`;
- checks terminal evidence fields and RBAC;
- uninstalls and verifies cleanup.

### D2: Runner Scenario

Added:

```text
testops/scenarios/failback-live-deployed-suite-chain.yaml
```

The scenario runs the live gate on `m02` and asserts terminal summary keys.

## Verification

Local/static checks:

```text
swblock validate testops/scenarios/failback-live-deployed-suite-chain.yaml
bash -n scripts/run-phase95-failback-live-deployed-suite-gate.sh
helm lint charts/seaweed-block
```

Live runner check:

```text
swblock run testops/scenarios/failback-live-deployed-suite-chain.yaml
```

Expected terminal evidence:

```text
phase95_failback_live_deployed_suite_status=ok
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
cleanup_status=ok
```

Validated live:

```text
swblock run testops/scenarios/failback-live-deployed-suite-chain.yaml
run=20260626-152618-5993
result=PASS 22/22
```

## Next

The next boundary is the real frontend handoff after failback:

```text
failed_back target evidence
frontend publication target creation
frontend publication executor remains default-off
explicit-policy publication writes only the bounded frontend target
workload-visible path switch gets its own gate
```

That should remain separate because it changes the user-facing data path, while
Phase 95 only closes the deployed authority call path.
