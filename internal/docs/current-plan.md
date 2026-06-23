# Current Plan: Phase 57 Rebuild Progress Target

Status: complete.

Branch target: `phase54-returned-replica-reintegration-executor`

## Goal

Phase 56 made returned-replica rebuild/catch-up visible as a disabled executor
contract:

```text
authority.rebuild_returned_replica
allowed_mutation_class=rebuild_traffic
execution_enabled=false
```

Phase 57 adds the next bounded control-plane target for that future executor:

```text
SwBlockReplicaRebuild.status
```

The authority executor may write a narrow planned-progress status to that target
when explicitly enabled for `rebuild_traffic`, while still not starting data
movement, not publishing any frontend, not changing primary authority, and not
touching `SwBlockVolume.status`.

## Scope

In scope:

- Add `SwBlockReplicaRebuild` CRD as a narrow rebuild/catch-up status target.
- Add Kubernetes status-writer support for
  `swblockreplicarebuilds/status`.
- Extend authority-executor execution mode with
  `--allowed-mutation-class rebuild_traffic`.
- Write only a planned rebuild status:
  - `state=planned`;
  - `reasonCode=rebuild_progress_planned`;
  - `rebuildTrafficStarted=false`;
  - `noFrontendPublication=true`;
  - `noCrossVolumeIdentityChange=true`.
- Keep ACK eligibility and rebuild progress as separate target CRDs and
  separate mutation classes.
- Gate RBAC so the executor can only read rebuild targets and write their
  `/status` subresource.

Out of scope:

- No rebuild data movement.
- No WAL/block copy.
- No frontend publication.
- No failback.
- No primary authority change.
- No `SwBlockVolume.status` writes by authority-executor.
- No automatic creation of rebuild targets by this phase.

## Deliverables

### D1: SwBlockReplicaRebuild CRD

Status: implemented locally.

Add a namespaced `SwBlockReplicaRebuild` CRD with:

- spec identity:
  - `volumeName`;
  - `volumeID`;
  - `pvcName`;
  - `replicaID`;
  - `sourceReplicaID`;
- status evidence:
  - observed time/generation;
  - executor;
  - state/reason;
  - durable and required frontier;
  - frontend-fenced-before-rebuild;
  - primary-unchanged;
  - rebuild-traffic-started;
  - no-frontend-publication;
  - no-cross-volume-identity-change;
  - conditions/evidence/non-claims.

### D2: Executor Planned-Status Write

Status: implemented locally.

When all of these are true:

- execution is explicitly requested;
- execution policy is explicitly enabled;
- allowed mutation class is `rebuild_traffic`;
- the volume has a disabled `authority.rebuild_returned_replica` contract;
- rebuild preflight is ready;
- the target `SwBlockReplicaRebuild` exists and matches volume + replica;

the executor writes:

```text
SwBlockReplicaRebuild.status.state=planned
SwBlockReplicaRebuild.status.reasonCode=rebuild_progress_planned
SwBlockReplicaRebuild.status.rebuildTrafficStarted=false
SwBlockReplicaRebuild.status.noFrontendPublication=true
SwBlockReplicaRebuild.status.noCrossVolumeIdentityChange=true
```

It does not write ACK eligibility, `SwBlockVolume.status`, Events, finalizers,
frontend state, or storage objects.

### D3: RBAC Target Gate

Status: QA PASS.

Added:

- `scripts/run-phase57-authority-executor-rebuild-target-rbac-gate.sh`
- `testops/scenarios/authority-executor-rebuild-target-rbac-chain.yaml`

The gate proves:

- default authority-executor identity cannot patch
  `swblockreplicarebuilds/status`;
- execution identity can get/list/watch `swblockreplicarebuilds`;
- execution identity can update/patch `swblockreplicarebuilds/status`;
- execution identity can actually patch a CRD-valid planned status payload;
- execution identity cannot patch main rebuild objects;
- execution identity cannot patch `SwBlockVolume.status`;
- execution identity cannot patch eligibility target status;
- execution identity cannot mutate events, pods, PVCs, or storage classes.

## Verification

Current local checks:

```text
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
swblock validate testops/scenarios/authority-executor-rebuild-target-rbac-chain.yaml
```

QA gate:

```text
swblock run testops/scenarios/authority-executor-rebuild-target-rbac-chain.yaml
```

Expected terminal evidence:

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
```

## Exit

Phase 57 closes when the CRD/status writer/executor planned-status path is
locally verified and the runner proves the rebuild target RBAC boundary live.
The phase does not claim rebuild data movement.

Result:

```text
20260623-153515-68cf authority-executor-rebuild-target-rbac-chain PASS 26/26
```

Sign-off:

```text
internal/docs/qa-assignments/phase57-rebuild-progress-target-qa-signoff.md
```
