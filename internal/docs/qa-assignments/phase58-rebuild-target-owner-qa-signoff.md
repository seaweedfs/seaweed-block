# Phase 58 Rebuild Target Owner QA Sign-off

Status: PASS.

Validated on: 2026-06-23.

Source: local Phase 58 tree on
`phase54-returned-replica-reintegration-executor`.

Runner evidence:

```text
20260623-164948-4735 rebuild-target-owner-rbac-chain PASS 18/18
```

## Scope

Phase 58 adds the bounded owner that creates `SwBlockReplicaRebuild` target
CRs for ready `authority.rebuild_returned_replica` executor contracts.

This phase does not start rebuild traffic, write rebuild status, publish a
frontend, fail back a replica, or change primary authority.

## Local Checks

```text
go test ./core/ops ./cmd/sw-block ./scripts
helm lint charts/seaweed-block
swblock validate testops/scenarios/rebuild-target-owner-rbac-chain.yaml
```

All passed.

## Live Gate

Scenario:

```text
testops/scenarios/rebuild-target-owner-rbac-chain.yaml
```

Result:

```text
=== rebuild-target-owner-rbac-chain === PASS (10.647s)
18 actions: 18 passed, 0 failed
```

Terminal evidence:

```text
phase58_rebuild_target_owner_rbac_status=ok
default_get_swblockvolumes_allowed=yes
default_create_swblockreplicarebuilds_denied=no
default_patch_swblockreplicarebuilds_status_denied=no
owner_get_swblockvolumes_allowed=yes
owner_get_swblockreplicarebuilds_allowed=yes
owner_create_swblockreplicarebuilds_allowed=yes
owner_patch_swblockreplicarebuilds_main_denied=no
owner_update_swblockreplicarebuilds_denied=no
owner_delete_swblockreplicarebuilds_denied=no
owner_patch_swblockreplicarebuilds_status_denied=no
owner_patch_swblockvolumes_denied=no
owner_patch_swblockvolumes_status_denied=no
owner_patch_swblockvolumes_finalizers_denied=no
owner_create_events_denied=no
owner_create_pods_denied=no
owner_patch_pvc_denied=no
owner_update_storageclass_denied=no
default_create_rebuild_target_runtime_denied=true
owner_create_rebuild_target_runtime_allowed=true
owner_patch_rebuild_target_runtime_denied=true
runtime_rebuild_target_volume_name=phase58-volume
runtime_rebuild_target_volume_id=pvc-phase58
runtime_rebuild_target_pvc_name=phase58-pvc
runtime_rebuild_target_replica_id=r2
runtime_rebuild_target_status_state=
```

## Verdict

PASS.

The rebuild target owner can create only `SwBlockReplicaRebuild` main objects
and cannot mutate target status, volume status/finalizers, events, pods, PVCs,
or storage classes. The created target carries only spec identity; status
remains empty until the authority executor writes planned progress in a
separate mutation class.

Lab residue check after the run showed no Phase 58 namespace, RBAC, or
temporary CRDs remaining.
