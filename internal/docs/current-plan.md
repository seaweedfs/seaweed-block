# Current Plan: Phase 58 Rebuild Target Owner

Status: complete.

Branch target: `phase54-returned-replica-reintegration-executor`

## Goal

Phase 57 added `SwBlockReplicaRebuild.status` as the narrow status target for
future returned-replica rebuild progress. That still required a target CR to
exist before the executor could write planned rebuild status.

Phase 58 adds the missing owner for that target:

```text
SwBlockVolume.status.executorContracts[]
  actionType=authority.rebuild_returned_replica
  allowedMutationClass=rebuild_traffic
  preflight=ready
        |
        v
SwBlockReplicaRebuild.spec
  volumeName / volumeID / pvcName / replicaID
```

The target owner may create the `SwBlockReplicaRebuild` main object. It does
not write status, start rebuild traffic, publish a frontend, fail back a
replica, or change primary authority.

## Scope

In scope:

- Add a `rebuild-target-owner` reconciler.
- Add `sw-block ops rebuild-target-owner`.
- Add Kubernetes create support for `SwBlockReplicaRebuild` main objects.
- Package an optional Helm Deployment and narrow RBAC identity.
- Gate live RBAC:
  - can read `SwBlockVolume`;
  - can read/create `SwBlockReplicaRebuild`;
  - cannot patch/update/delete rebuild targets;
  - cannot write rebuild target status;
  - cannot write `SwBlockVolume` status/finalizers;
  - cannot mutate events, pods, PVCs, or storage classes.

Out of scope:

- No rebuild data movement.
- No WAL/block copy.
- No rebuild status write by the target owner.
- No frontend publication.
- No failback.
- No primary authority change.
- No cross-volume mutation.

## Deliverables

### D1: Rebuild Target Owner Controller

Status: implemented locally.

The controller lists `SwBlockVolume` objects and existing
`SwBlockReplicaRebuild` targets. It creates one target only when a volume has a
ready disabled rebuild contract:

```text
actionType=authority.rebuild_returned_replica
decision=disabled
reason=executor_disabled
preflightDecision=ready
preflightReason=satisfied
allowedMutationClass contains rebuild_traffic
replicaID != ""
```

Existing targets are detected by volume identity plus replica ID and are not
duplicated.

### D2: CLI and Kubernetes Create Client

Status: implemented locally.

Added:

```text
sw-block ops rebuild-target-owner [--dry-run] [--namespace <ns>] [--interval 30s]
```

The command reports terminal evidence:

```text
rebuild_target_owner=target_mutation|dry_run
targets_planned=<n>
targets_existing=<n>
targets_created=<n>
invalid_contracts=<n>
storage_mutation_allowed=false
frontend_publication_allowed=false
failback_allowed=false
```

The Kubernetes client creates only the main `SwBlockReplicaRebuild` object with
`apiVersion`, `kind`, `metadata`, and `spec`; it does not include `status`.

### D3: Helm Packaging and RBAC

Status: implemented locally.

Added optional chart values:

```yaml
rebuildTargetOwner:
  create: false
  dryRun: true
  interval: 30s
```

The packaged identity is intentionally separate from authority-executor:

- `rebuild-target-owner`: creates target CRs only;
- `authority-executor`: writes target `/status` only.

### D4: Runner RBAC Gate

Status: QA PASS.

Added:

- `scripts/run-phase58-rebuild-target-owner-rbac-gate.sh`
- `testops/scenarios/rebuild-target-owner-rbac-chain.yaml`

The live gate proves both `kubectl auth can-i` and runtime behavior:

- default identity cannot create rebuild targets;
- owner identity can create one CRD-valid target;
- owner identity cannot patch the created target;
- created target has spec identity and empty status;
- owner identity cannot mutate status, Events, workloads, PVCs, or storage
  classes.

## Verification

Local checks:

```text
go test ./core/ops ./cmd/sw-block ./scripts
helm lint charts/seaweed-block
swblock validate testops/scenarios/rebuild-target-owner-rbac-chain.yaml
```

Live gate:

```text
swblock run testops/scenarios/rebuild-target-owner-rbac-chain.yaml
```

Expected terminal evidence:

```text
phase58_rebuild_target_owner_rbac_status=ok
owner_create_swblockreplicarebuilds_allowed=yes
owner_create_rebuild_target_runtime_allowed=true
default_create_rebuild_target_runtime_denied=true
owner_patch_rebuild_target_runtime_denied=true
owner_patch_swblockreplicarebuilds_status_denied=no
owner_create_events_denied=no
runtime_rebuild_target_status_state=
```

## Exit

Phase 58 closes when local tests pass and the runner proves the target owner can
create only rebuild target CRs, while all status, storage, frontend, workload,
and authority mutations remain denied.

Result:

```text
20260623-164948-4735 rebuild-target-owner-rbac-chain PASS 18/18
```

Sign-off:

```text
internal/docs/qa-assignments/phase58-rebuild-target-owner-qa-signoff.md
```
