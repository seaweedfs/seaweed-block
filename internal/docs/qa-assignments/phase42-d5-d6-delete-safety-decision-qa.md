# QA Assignment - Phase 42 D5/D6 Delete-Safety Decision Gate

## Goal

Validate that Phase 42 does not bypass the Phase 41 delete-safety model after
the lifecycle-owner admission boundary is proven.

Expected result:

```text
clean cleanup evidence permits finalizer-release intent;
residue rejects it;
missing or stale cleanup evidence returns unknown;
decisions are per-volume isolated;
the action remains dry-run/status-only and no cleanup or finalizer mutation is
executed in Phase 42.
```

## Source Under Test

Branch:

```text
phase41-lifecycle-owner-foundation
```

Relevant files:

```text
scripts/run-phase42-delete-safety-decision-gate.sh
scripts/run-phase42-delete-safety-decision-gate.ps1
testops/scenarios/lifecycle-owner-delete-safety-decision-chain.yaml
core/ops/delete_safety_contract.go
core/ops/operator_status_controller_test.go
core/ops/observation_bundle_test.go
```

## G1 - Run The Gate

Run either:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/run-phase42-delete-safety-decision-gate.ps1 `
  -ProductRoot C:\work\seaweed_block `
  -ArtifactDir C:\work\seaweed_block\results\phase42-delete-safety-decision-qa
```

or:

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/phase42-delete-safety-decision-qa \
  bash scripts/run-phase42-delete-safety-decision-gate.sh "$PWD"
```

or the TestOps scenario:

```bash
swblock run testops/scenarios/lifecycle-owner-delete-safety-decision-chain.yaml
```

Pass criteria in `phase42-delete-safety-decision-gate-summary.txt`:

```text
phase42_delete_safety_decision_status=ok
go_test_core_ops=ok
cleanup_execution_attempted=false
clean_delete_safety_decision=allowed
blocked_delete_safety_decision=rejected
missing_delete_safety_decision=unknown
stale_delete_safety_decision=unknown
```

## G2 - Lifecycle-Owner Action Remains Non-Mutating

Pass criteria:

```text
lifecycle_owner_action_type=safe_k8s.release_swblockvolume_finalizer
lifecycle_owner_action_mode=dry_run
lifecycle_owner_action_mutation_allowed=false
finalizer_patch_count=0
no_finalizer_mutation_events=true
```

Fail if the gate reports a finalizer patch, a finalizer mutation Event, or a
mutating action.

## G3 - Multi-Volume Isolation

The tested state includes:

```text
A: blocked residue -> rejected
B: healthy ready volume -> no deleteSafety
C: clean cleanup evidence -> allowed
D: stale cleanup evidence -> unknown
```

Pass criteria:

```text
multi_volume_delete_safety_isolation=true
stale_delete_safety_cleared_when_absent=true
```

Fail if one volume's delete-safety state contaminates another volume, or stale
deleteSafety remains when current evidence disappears.

## Verdict

PASS only if G1-G3 pass and the Phase 42 live admission gate has already passed
D1-D4.

This gate does not replace the live admission gate. It proves the decision model
that a future Phase 43 lifecycle owner must consult before using the admitted
finalizer mutation.
