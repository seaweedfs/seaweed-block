# QA Sign-off — Phase 41 Lifecycle Owner Foundation

Verdict: **PASS (non-mutating slice).** Phase 41 establishes the lifecycle-owner
control boundary, converts delete-safety into executable preconditions, and ships
a dry-run lifecycle-owner action surface — **without** turning `operator-status`
into a mutating controller and **without** any finalizer add/remove. G1–G7 pass.

**Required carry-forward (recorded per the assignment):**

```text
A full live-apiserver/envtest lifecycle-owner RBAC/admission gate is still
required before any finalizer mutation (main-object patch swblockvolumes) can
ship. Phase 41's API boundary gate (G2) is schema-aware/equivalent-RBAC, not a
live apiserver/admission proof.
```

Date: 2026-06-14
Source: branch `phase41-lifecycle-owner-foundation` @ `43c7a87 phase41: prepare
qa close handoff` (commits `d0ba1fb`..`b414a9f` + close handoff)
Runner/env: local `go test` + PowerShell boundary gate on the branch tree; live
`kubectl auth can-i` + `verify-helm-cleanup.sh` on the m02 k3s lab (clean).

## Lab Node Health

m01 `Ready`, m02 `Ready`, **tp01 `NotReady`/unreachable** (unchanged). Phase 41
is non-mutating status/dry-run work; no multi-node RF=3 gate is required. Restore
tp01 before any future live finalizer-mutation gate.

## G1 — Contract Review — PASS

Reviewed `lifecycle-owner-control-contract.md`,
`lifecycle-owner-finalizer-strategy.md`, `current-plan.md`, `docs/roadmap.md`.

- Three roles are explicit and separately permissioned: **observer/status
  writer** (`operator-status`; CRD `/status` + Events only), **lifecycle owner**
  (separate; owns lifecycle metadata, may patch finalizers *only if a future
  phase proves the boundary*), **executor** (separate; not introduced here).
- Finalizer mutation is explicitly **deferred** (strategy doc "Decision": "Phase
  41 does **not** ship `SwBlockVolume` finalizer add/remove").
- Delete-safety is framed as **status guidance, not deletion protection**
  ("Delete-safety is status-only guidance, not Kubernetes deletion protection").
- Non-claims list excludes mutating operator, finalizer add/remove, automatic
  cleanup, rebuild, backup, NVMe — consistent across roadmap and current-plan.

No doc implies operator-status owns finalizers or that cleanup/deletion-protection/
rebuild/backup/NVMe is delivered. Pass criteria met; no fail conditions present.

## G2 — API Boundary Gate — PASS (schema-aware; carry-forward applies)

`scripts/run-phase41-lifecycle-owner-api-boundary.ps1`
(`results/phase41-api-boundary-qa/...-summary.txt`):

```text
phase41_lifecycle_owner_api_boundary_status=ok
go_test_core_ops=ok
operator_status_main_patch_allowed=false
lifecycle_owner_finalizer_patch_allowed=true
lifecycle_owner_spec_patch_allowed=false
lifecycle_owner_unrelated_metadata_patch_allowed=false
finalizers_endpoint_allowed=false
```

As the assignment notes, this is schema-aware/equivalent-RBAC (CRD schema + token
RBAC equivalence), **not** a live-apiserver/admission envtest. Called out as the
open carry-forward, not a product failure.

## G3 — Delete-Safety Decision Semantics — PASS

`go test ./core/ops -run
'TestEvaluateSwBlockVolumeDeleteSafety|TestObservationBundle_DeleteSafety'` → `ok`.
All decision behaviors green (both the evaluator and the observation-bundle
projection):

```text
clean evidence    -> state=releasable decision=allowed   release_allowed=true
residue           -> state=blocked     decision=rejected  release_allowed=false
missing evidence  -> state=requested   decision=unknown   release_allowed=false
stale evidence    -> state=requested   decision=unknown   reason=cleanup_evidence_stale
not requested     -> no premature decision
```

Missing/stale never becomes `allowed`; missing is not reported as confirmed
residue; data-plane `Ready` is not used to imply finalizer release is safe.

## G4 — Cleanup Freshness Source — PASS

`scripts/verify-helm-cleanup.sh` on the clean m02 lab:

```text
cleanup_status=ok
k8s_residue_count=0  iscsi_residue_count=0  process_residue_count=0
multipath_residue_count=0  hostpath_residue_count=0
cleanup_observed_at=2026-06-14T17:56:28Z
```

`cleanup_observed_at` is present and RFC3339 UTC (`date -u +%Y-%m-%dT%H:%M:%SZ`,
script line 237); `cleanup_status` and all residue counters are preserved; exit
behavior unchanged (`exit 1` on failure, line 242). The verifier does not hide
residue.

## G5 — Dry-Run Lifecycle-Owner Action Surface — PASS

`go test ./core/ops -run
'TestOperatorStatusReconcilerProjectsDeleteSafetyWithoutFinalizerMutation|TestOperatorStatusReconcilerDeleteSafetyDoesNotContaminateOtherVolumes'`
→ `ok`. The reconciler projects, for each delete-safety volume, an `allowedActions`
entry `safe_k8s.release_swblockvolume_finalizer` with `Mode=dry_run`,
`OwnerExecutor=lifecycle_owner`, `MutationAllowed=false`, and `Decision` equal to
the volume's `deleteSafety` decision. `FinalizerPatchCount==0` and no
`finalizer_added`/`finalizer_released` Events are emitted.

## G6 — Multi-Volume Isolation — PASS

The isolation test drives four volumes in one reconcile and asserts per-volume
isolation:

```text
A delete-a (blocked, iscsi_node_records_present) -> deleteSafety blocked / decision rejected
B healthy-b (ready, first_volume_verified)       -> NO deleteSafety (uncontaminated)
C clean-c   (clean evidence)                     -> deleteSafety releasable / decision allowed
D stale-d   (stale evidence)                     -> deleteSafety requested / decision unknown / cleanup_evidence_stale
```

A does not contaminate B/C/D; C does not release A/B/D; D stays unknown only for
D; each volume carries its own `deleteSafety` + dry-run lifecycle-owner action;
`FinalizerPatchCount==0` (no finalizer mutation attempted).

## G7 — Live Boundary / Non-Mutation — PASS

Phase 41 makes **no change** to `operator-status` RBAC (git-confirmed: no Phase 41
commit touches `templates/operator-status-rbac.yaml`; the only chart change is +2
lines of SwBlockVolume CRD status schema). Fresh live `kubectl auth can-i` against
the applied Phase 41 operator-status RBAC on the m02 apiserver:

```text
patch swblockvolumes.block.seaweedfs.com                 => no
patch swblockvolumes.block.seaweedfs.com --subresource=status => yes
create events                                            => yes
patch pods                                               => no
patch persistentvolumeclaims                             => no
update storageclasses.storage.k8s.io                     => no
```

`operator-status` remains status/events-only on the live apiserver. (RBAC was
applied for the check and removed; lab left clean.)

## Blocking Findings

None. Phase 41 is a non-mutating boundary + dry-run/status slice; it neither
broadens `operator-status` nor executes any lifecycle mutation.

## Non-Blocking Findings / Carry-Forwards

1. **Live-apiserver/envtest lifecycle-owner RBAC/admission gate is still required
   before any finalizer mutation ships** (the explicit carry-forward above). G2 is
   schema-aware/equivalent-RBAC; it does not exercise a real apiserver admission
   path. This is the natural Phase 42 entry point.
2. **tp01 `NotReady`** — lab infra; restore before any live finalizer-mutation or
   RF=3 gate.

## Recommendation

**Phase 41 can close** as the lifecycle-owner boundary + delete-safety
preconditions + dry-run lifecycle-owner action surface, with `operator-status`
provably still status/events-only. The next phase must build the real
live-apiserver/admission proof for a constrained main-object finalizer patch
before any actual finalizer add/remove is enabled. NVMe / rebuild / backup should
remain deferred until that control-plane proof exists.
