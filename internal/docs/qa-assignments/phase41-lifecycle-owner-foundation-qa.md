# QA Assignment - Phase 41 Lifecycle Owner Foundation

## Goal

Validate that Phase 41 improves the lifecycle-owner control boundary without
quietly turning `operator-status` into a mutating controller.

The expected product result is:

```text
operator-status remains status/events-only;
finalizer add/remove is deferred;
delete-safety decisions are allowed/rejected/unknown with stable evidence;
dry-run lifecycle-owner actions are visible but never executed.
```

## Source Under Test

Branch:

```text
phase41-lifecycle-owner-foundation
```

Relevant commits:

```text
d0ba1fb phase41: define lifecycle owner contract
a362c28 phase41: add lifecycle owner api boundary gate
cabe790 phase41: tighten delete safety preconditions
ab004fc phase41: gate stale cleanup evidence
1f4f769 phase41: choose finalizer strategy and dry-run action
c80fdfc phase41: prove lifecycle action isolation
```

## G1 - Contract Review

Review:

```text
internal/docs/ref/lifecycle-owner-control-contract.md
internal/docs/ref/lifecycle-owner-finalizer-strategy.md
internal/docs/current-plan.md
docs/roadmap.md
```

Pass criteria:

```text
operator-status role = observer/status writer only
lifecycle-owner role is separate from operator-status
executor role is separate from both
finalizer mutation is explicitly deferred in Phase 41
non-claim says delete-safety is status guidance, not deletion protection
```

Fail if:

```text
docs imply operator-status owns finalizers
docs imply automatic cleanup, deletion protection, rebuild, backup, or NVMe work
```

## G2 - API Boundary Gate

Run either:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/run-phase41-lifecycle-owner-api-boundary.ps1 `
  -ProductRoot C:\work\seaweed_block `
  -ArtifactDir C:\work\seaweed_block\results\phase41-api-boundary-qa
```

or the TestOps scenario:

```bash
swblock run testops/scenarios/lifecycle-owner-api-boundary-chain.yaml
```

Pass criteria in
`phase41-lifecycle-owner-api-boundary-summary.txt`:

```text
phase41_lifecycle_owner_api_boundary_status=ok
operator_status_main_patch_allowed=false
lifecycle_owner_finalizer_patch_allowed=true
lifecycle_owner_spec_patch_allowed=false
lifecycle_owner_unrelated_metadata_patch_allowed=false
finalizers_endpoint_allowed=false
```

Note:

This is schema-aware/equivalent-RBAC, not yet a full live-apiserver envtest.
QA should call that out as an open carry-forward, not as a product failure.

## G3 - Delete-Safety Decision Semantics

Run:

```bash
go test ./core/ops -count=1 -run 'TestEvaluateSwBlockVolumeDeleteSafety|TestObservationBundle_DeleteSafety'
```

Pass criteria:

```text
clean cleanup evidence -> state=releasable decision=allowed release_allowed=true
residue evidence -> state=blocked decision=rejected release_allowed=false
missing cleanup evidence -> state=requested decision=unknown release_allowed=false
stale cleanup evidence -> state=requested decision=unknown reason=cleanup_evidence_stale
data-plane Ready is not falsified by missing/stale lifecycle evidence
```

Fail if:

```text
missing/stale evidence becomes allowed
missing evidence is reported as confirmed residue
Ready=True is used to imply finalizer release is safe
```

## G4 - Cleanup Freshness Source

Run a cleanup verifier in any clean lab or inspect the generated summary from an
existing cleanup gate:

```bash
bash scripts/verify-helm-cleanup.sh "$PWD"
```

Pass criteria:

```text
cleanup-summary.txt includes cleanup_observed_at=<RFC3339 UTC timestamp>
cleanup_status remains ok/failed as before
existing residue counters remain present
```

Fail if:

```text
cleanup_observed_at is missing from new verifier output
the timestamp is local time or not RFC3339 parseable
the verifier hides residue or changes its exit behavior
```

## G5 - Dry-Run Lifecycle-Owner Action Surface

Run:

```bash
go test ./core/ops -count=1 -run 'TestOperatorStatusReconcilerProjectsDeleteSafetyWithoutFinalizerMutation|TestOperatorStatusReconcilerDeleteSafetyDoesNotContaminateOtherVolumes'
```

Pass criteria:

```text
SwBlockVolume.status.deleteSafety is populated
allowedActions includes safe_k8s.release_swblockvolume_finalizer
mode=dry_run
ownerExecutor=lifecycle_owner
mutationAllowed=false
decision matches deleteSafety decision
no finalizer_added or finalizer_released Events are emitted
```

Fail if:

```text
operator-status emits finalizer mutation Events
allowedActions claims mutationAllowed=true
release action is missing from CRD status for delete-safety volumes
```

## G6 - Multi-Volume Isolation

Use the same D5 test or a live/from-bundle equivalent with at least four
volumes:

```text
A: blocked residue -> rejected
B: normal ready volume -> no deleteSafety
C: clean delete evidence -> allowed
D: stale cleanup evidence -> unknown
```

Pass criteria:

```text
A does not contaminate B/C/D
C does not release A/B/D
D stays unknown only for D
each volume has its own deleteSafety and lifecycle-owner dry-run action
no finalizer mutation is attempted
```

## G7 - Boundary / Non-Mutation

If running a live operator-status install, verify:

```bash
kubectl auth can-i patch swblockvolumes --as <operator-status-sa> -n kube-system
kubectl auth can-i patch swblockvolumes --subresource=status --as <operator-status-sa> -n kube-system
kubectl auth can-i create events --as <operator-status-sa> -n kube-system
kubectl auth can-i patch pods --as <operator-status-sa> -n kube-system
kubectl auth can-i patch persistentvolumeclaims --as <operator-status-sa> -n kube-system
```

Pass criteria:

```text
patch swblockvolumes main: no
patch swblockvolumes/status: yes
create events: yes
pods/PVC/PV/StorageClass mutation: no
```

## Final Verdict

PASS only if G1-G6 pass and any live boundary check in G7 preserves
status/events-only.

Do not mark Phase 41 fully closed unless the report explicitly records the open
carry-forward:

```text
full live-apiserver/envtest lifecycle-owner RBAC/admission gate is still needed
before any finalizer mutation can ship
```
