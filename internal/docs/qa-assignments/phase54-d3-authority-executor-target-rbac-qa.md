# Phase 54 D3 QA: Authority Executor Target RBAC

Status: assigned, pending live QA.

## Purpose

Validate the first ACK eligibility target boundary before the authority
executor is allowed to call the writer.

The selected target is:

```text
SwBlockReplicaEligibility.status
```

The gate must prove:

- default authority-executor RBAC remains Phase 53 read-only,
- execution-enabled RBAC can patch only
  `swblockreplicaeligibilities/status`,
- the executor still cannot patch `SwBlockVolume` main/status/finalizers,
- the executor still cannot create Events,
- the executor still cannot mutate pods, PVCs, storageclasses, or delete the
  target object.

## Run

```bash
swblock run testops/scenarios/authority-executor-target-rbac-chain.yaml
```

The scenario invokes:

```bash
scripts/run-phase54-authority-executor-target-rbac-gate.sh
```

## Required PASS Lines

```text
phase54_authority_executor_target_rbac_status=ok
default_patch_swblockreplicaeligibilities_status_denied=no
exec_patch_swblockreplicaeligibilities_status_allowed=yes
exec_update_swblockreplicaeligibilities_status_allowed=yes
exec_patch_swblockreplicaeligibilities_main_denied=no
exec_patch_swblockvolumes_status_denied=no
exec_patch_swblockvolumes_finalizers_denied=no
exec_create_events_denied=no
exec_create_pods_denied=no
exec_patch_pvc_denied=no
exec_update_storageclass_denied=no
exec_delete_swblockreplicaeligibilities_denied=no
```

## Blocking Findings

Any of these blocks D3:

- default authority-executor can patch the target status,
- execution-enabled authority-executor cannot patch target status,
- execution-enabled authority-executor can patch target main object,
- execution-enabled authority-executor can patch SwBlockVolume status,
- execution-enabled authority-executor can patch SwBlockVolume finalizers,
- execution-enabled authority-executor can create Events or mutate workload /
  storage resources.

## Non-Claims

D3 does not prove ACK eligibility execution.

The executor still fails closed because the call-site is not wired yet. D3 only
proves that the future writer target has a narrow Kubernetes permission
boundary.
