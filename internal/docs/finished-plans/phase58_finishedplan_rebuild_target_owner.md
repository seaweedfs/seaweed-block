# Phase 58 Finished Plan: Rebuild Target Owner

Status: complete.

Completed on: 2026-06-23.

Branch: `phase54-returned-replica-reintegration-executor`.

## What Changed

Phase 58 added the missing target-owner layer for returned-replica rebuild
planning:

```text
SwBlockVolume.status.executorContracts[]
        |
        v
rebuild-target-owner creates SwBlockReplicaRebuild.spec
        |
        v
authority-executor writes SwBlockReplicaRebuild.status
```

The owner creates a `SwBlockReplicaRebuild` target only when the existing
`SwBlockVolume.status.executorContracts[]` entry is a ready disabled
`authority.rebuild_returned_replica` contract with
`allowedMutationClass=rebuild_traffic`.

## Product Boundary

This phase intentionally claims only target creation.

Non-claims:

- no rebuild data movement;
- no WAL/block copy;
- no rebuild status write by the target owner;
- no frontend publication;
- no failback;
- no primary authority change;
- no cross-volume mutation.

## Deliverables

- `core/ops/rebuild_target_owner_controller.go`
  - plans and creates rebuild target CRs;
  - skips existing targets;
  - rejects invalid contracts.
- `sw-block ops rebuild-target-owner`
  - supports `--dry-run`, `--namespace`, and `--interval`;
  - emits terminal evidence for planned/existing/created/invalid counts.
- `KubernetesStatusClient.CreateSwBlockReplicaRebuild`
  - POSTs the target main object;
  - sends only `apiVersion`, `kind`, `metadata`, and `spec`;
  - never sends `status`.
- Helm packaging:
  - optional `rebuildTargetOwner.create`;
  - default disabled and dry-run;
  - narrow ServiceAccount/RBAC.
- Runner gate:
  - `scripts/run-phase58-rebuild-target-owner-rbac-gate.sh`;
  - `testops/scenarios/rebuild-target-owner-rbac-chain.yaml`.

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block ./scripts
helm lint charts/seaweed-block
swblock validate testops/scenarios/rebuild-target-owner-rbac-chain.yaml
```

Live:

```text
20260623-164948-4735 rebuild-target-owner-rbac-chain PASS 18/18
```

Key terminal evidence:

```text
owner_create_swblockreplicarebuilds_allowed=yes
owner_create_rebuild_target_runtime_allowed=true
default_create_rebuild_target_runtime_denied=true
owner_patch_rebuild_target_runtime_denied=true
owner_patch_swblockreplicarebuilds_status_denied=no
owner_create_events_denied=no
runtime_rebuild_target_status_state=
```

## Result

Phase 58 closes the rebuild target lifecycle gap before real rebuild traffic.
The control-plane chain now has separate bounded identities:

- target owner: creates rebuild target CRs;
- authority executor: writes rebuild target status;
- no component in this phase starts data movement or changes authority.

Next logical step: decide whether to add a read-only rebuild progress surface
for user/operator visibility or move toward the first real rebuild data-path
primitive behind an explicit gate.
