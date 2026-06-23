# Phase 57 Finished Plan: Rebuild Progress Target

Status: complete.

Branch: `phase54-returned-replica-reintegration-executor`

## Goal

Move beyond Phase 56's disabled rebuild contract by adding the narrow status
target that a future rebuild executor can use:

```text
SwBlockReplicaRebuild.status
```

The milestone is intentionally a control-plane boundary, not a data-plane
rebuild implementation.

## What Changed

Phase 57 added a new namespaced CRD:

```text
SwBlockReplicaRebuild
```

Its spec identifies the volume and returned replica:

```text
volumeName
volumeID
pvcName
replicaID
sourceReplicaID
```

Its status carries rebuild/catch-up progress evidence:

```text
state
reasonCode
frontendFencedBeforeRebuild
primaryUnchanged
durableFrontierLsn
requiredFrontierLsn
durableFrontierCaughtUp
rebuildTrafficStarted
noFrontendPublication
noCrossVolumeIdentityChange
conditions
evidenceRefs
nonClaims
```

The Kubernetes status writer now supports:

```text
swblockreplicarebuilds/status
```

The authority executor now accepts:

```text
--allowed-mutation-class rebuild_traffic
```

When execution is explicitly requested and policy-enabled, and a matching
`SwBlockReplicaRebuild` target exists, the executor writes a planned status:

```text
state=planned
reasonCode=rebuild_progress_planned
rebuildTrafficStarted=false
noFrontendPublication=true
noCrossVolumeIdentityChange=true
```

## Safety Boundary

ACK eligibility and rebuild progress are separate target CRDs and separate
mutation classes:

```text
ack_eligibility  -> SwBlockReplicaEligibility.status
rebuild_traffic  -> SwBlockReplicaRebuild.status
```

Phase 57 does not allow the authority executor to write:

- `SwBlockVolume.status`;
- finalizers;
- Events;
- pods/PVCs/storage classes;
- frontend publication;
- failback;
- primary authority changes.

The rebuild status payload explicitly says no rebuild traffic has started.

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block ./scripts
helm lint charts/seaweed-block
swblock validate testops/scenarios/authority-executor-rebuild-target-rbac-chain.yaml
```

QA/live:

```text
authority-executor-rebuild-target-rbac-chain
20260623-153515-68cf
26/26 PASS
```

Terminal evidence:

```text
phase57_authority_executor_rebuild_target_rbac_status=ok
default_patch_swblockreplicarebuilds_status_denied=no
exec_patch_swblockreplicarebuilds_status_allowed=yes
exec_patch_swblockreplicarebuilds_main_denied=no
exec_patch_swblockvolumes_status_denied=no
exec_patch_swblockreplicaeligibilities_status_denied=no
exec_create_events_denied=no
default_rebuild_status_patch_runtime_denied=true
runtime_rebuild_status_state=planned
runtime_rebuild_status_reason=rebuild_progress_planned
runtime_rebuild_traffic_started=false
runtime_no_frontend_publication=true
runtime_no_cross_volume_identity_change=true
```

Sign-off:

```text
internal/docs/qa-assignments/phase57-rebuild-progress-target-qa-signoff.md
```

## Next

The next rebuild milestone should decide how a lifecycle/control-plane owner
creates `SwBlockReplicaRebuild` targets and how real data-plane catch-up
progress becomes terminal evidence. Do not add frontend publication or failback
until the rebuild target can prove `durableFrontierCaughtUp=true` from live
data-plane evidence.
