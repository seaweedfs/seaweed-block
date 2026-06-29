# Phase 57 Rebuild Progress Target QA Sign-off

Verdict: PASS.

Validated tree: Phase 57 worktree before commit.

Scenario:

```text
testops/scenarios/authority-executor-rebuild-target-rbac-chain.yaml
```

QA run:

```text
20260623-153515-68cf
```

Result:

```text
26/26 PASS
```

## Terminal Evidence

From
`/mnt/smb/work/share/g15d-k8s/20260623-153515-68cf-phase57-authority-executor-rebuild-target-rbac/phase57-authority-executor-rebuild-target-rbac-summary.txt`:

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

## Verified Contract

- `SwBlockReplicaRebuild` exists as a narrow rebuild/catch-up progress target.
- The authority-executor execution identity can read rebuild targets and patch
  only `swblockreplicarebuilds/status`.
- The default authority-executor identity cannot patch rebuild target status.
- The execution identity cannot patch rebuild target main objects.
- The execution identity cannot patch `SwBlockVolume.status`.
- The execution identity cannot patch `SwBlockReplicaEligibility.status`.
- The execution identity cannot create Events, pods, PVCs, or storage classes.
- A real Kubernetes status patch lands the planned payload:
  - `state=planned`;
  - `reasonCode=rebuild_progress_planned`;
  - `rebuildTrafficStarted=false`;
  - `noFrontendPublication=true`;
  - `noCrossVolumeIdentityChange=true`.

## Non-Claims

Phase 57 does not implement or claim:

- rebuild data movement;
- catch-up block copy;
- frontend publication;
- failback;
- primary authority change;
- automatic creation of rebuild target CRs.

## Residue

Clean. Post-run checks on m02 showed no phase namespace, no phase RBAC, and no
SwBlock CRDs left by the gate. The script deletes CRDs only when they were not
present before the run.

## Local Verification

```text
go test ./core/ops ./cmd/sw-block ./scripts
helm lint charts/seaweed-block
swblock validate testops/scenarios/authority-executor-rebuild-target-rbac-chain.yaml
```

`go test ./...` still fails in `core/frontend/iscsi`
(`TestT2Process_ISCSI_ReopenAfterMove_ServesNewLineage` timeout). That package
is outside the Phase 57 touched control-plane scope and was not fixed here.
