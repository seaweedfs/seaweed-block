# QA Sign-off — Phase 42 D1 Lifecycle Owner Admission Gate

Verdict: **PASS.** First filed FAIL on `bc5ffc0` (VAP CEL errored on absent
optional fields); fixed in `116d381` (G1–G4 green); breadth-expanded to D1–D4 in
`d3a1e0e` and re-validated green on a real VAP-capable cluster. See the
Re-validation sections below; the original FAIL analysis is kept at the bottom.

## Re-validation — 2026-06-15, commit `d3a1e0e phase42: expand lifecycle owner admission checks` (D1–D4 breadth)

The dev widened the same live VAP gate to cover D2–D4: finalizer add/remove
idempotency; annotation / ownerReferences / deletionTimestamp / mixed-metadata
rejects; `/status` subresource denied and main-object status proven unmutated;
object integrity preserved after rejected patches; and create/update/patch/delete
denied for all forbidden resources.

Re-ran on m02 (k3s v1.34.4): `GATE_EXIT=0`. Every criterion in the expanded
assignment holds:

```text
status=ok   harness=live VAP   admission_policy_propagated=true   object_integrity_preserved=true
operator_status_main_patch_allowed=false
finalizer_add_allowed=true   finalizer_add_idempotent=true
finalizer_remove_allowed=true   finalizer_remove_idempotent=true
spec/label/annotation/ownerreferences/deletiontimestamp/foreign_finalizer/
  mixed/mixed_metadata patch_allowed = false
main_status_mutated=false   status_subresource_patch_allowed=false   finalizers_endpoint_allowed=false
forbidden resources {pods,deployments,pvc,pv,storageclasses,secrets,nodes,
  csidrivers,csinodes} × {create,update,patch,delete} = false  (36/36)
G3 final object: spec.pvcName=phase42-a, labels.keep=true, annotations.keep=true, no foreign finalizer
G4 cleanup: ns NotFound, VAP/VAPBinding/ClusterRole/Binding all 0
```

Note: `lifecycle_owner_main_status_patch_request_denied=false` is expected and
safe — a CRD with a status subresource silently strips `.status` from a
main-object patch rather than rejecting the request, and the gate independently
proves `main_status_mutated=false` (the status is not changed). The lifecycle-owner
main-object patch is now comprehensively confined to the
`block.seaweedfs.com/swblockvolume-protection` finalizer against a real Kubernetes
admission server. **Phase 42 D1–D4 pass**; lab left clean.

## Re-validation — 2026-06-15, commit `116d381 phase42: fix lifecycle owner admission gate`

The fix: (a) the VAP CEL now `has()`-guards the optional fields (status, labels,
annotations, ownerReferences, finalizers) so absent-key eval errors no longer
deny legitimate patches; (b) a probe object drives an admission-propagation wait
before the real assertions, with `admission_policy_propagated=true` added to the
pass criteria.

Re-ran `bash scripts/run-phase42-lifecycle-owner-admission-gate.sh` on m02
(k3s v1.34.4, VAP-capable), artifacts `/tmp/p42d1c`. `GATE_EXIT=0`, full summary:

```text
phase42_lifecycle_owner_admission_status=ok
harness=live_kubernetes_validating_admission_policy
admission_policy_propagated=true
operator_status_main_patch_allowed=false
lifecycle_owner_finalizer_add_allowed=true
lifecycle_owner_finalizer_remove_allowed=true
lifecycle_owner_spec_patch_allowed=false
lifecycle_owner_label_patch_allowed=false
lifecycle_owner_foreign_finalizer_allowed=false
lifecycle_owner_mixed_patch_allowed=false
finalizers_endpoint_allowed=false
lifecycle_owner_{pods,deployments,persistentvolumeclaims,persistentvolumes,
  storageclasses,secrets,nodes,csidrivers,csinodes}_patch_allowed=false
```

- **G1** PASS — `status=ok`, `admission_policy_propagated=true`, legitimate
  finalizer add/remove allowed, all forbidden main-object patches (spec, label,
  foreign finalizer, mixed, `/finalizers` subresource) denied; operator-status
  main patch denied (RBAC).
- **G2** PASS — all nine forbidden resource mutations (pods, deployments, pvc,
  pv, storageclasses, secrets, nodes, csidrivers, csinodes) `=false`.
- **G3** PASS — final object integrity: `spec.pvcName=phase42-a`,
  `metadata.labels.keep=true`, `metadata.annotations.keep=true`, no foreign
  finalizer remains (the allowed finalizer add/remove changed nothing else).
- **G4** PASS — `sw-block-phase42-gate` namespace NotFound; no leftover
  `sw-block-phase42-*` VAP / VAPBinding / ClusterRole / ClusterRoleBinding.

The lifecycle-owner main-object patch is now provably confined to the
`block.seaweedfs.com/swblockvolume-protection` finalizer against a real
Kubernetes admission server. **Phase 42 D1 passes**; lab left clean.

---

## Original finding (commit `bc5ffc0`, superseded by the Re-validation above)

Verdict at filing: **FAIL (blocking product defect in the VAP policy).** On a real
VAP-capable cluster the admission gate denies the **legitimate** lifecycle-owner
finalizer add — the policy CEL errors on absent optional fields. This is a
live-only defect: it is invisible to the Phase 41 schema-aware mock and to the
dev's VAP-less rancher-desktop, and is exactly the class Phase 42 exists to catch.

Date: 2026-06-14
Source: branch `phase41-lifecycle-owner-foundation` @ `bc5ffc0 phase42: add live
lifecycle owner admission gate`
Environment: m02 k3s **v1.34.4+k3s1** — `admissionregistration.k8s.io/v1`
`ValidatingAdmissionPolicy` + `ValidatingAdmissionPolicyBinding` served and
enforced (this is **not** the env-blocked case; the gate's environment
requirement is met).

## How It Was Run

`bash scripts/run-phase42-lifecycle-owner-admission-gate.sh /tmp/seaweed_block`
on m02 (`KUBECONFIG=/etc/rancher/k3s/k3s.yaml`), artifacts in `/tmp/p42d1`. A
second diagnostic run injected a 25 s VAP-propagation wait after the policy apply
to separate the propagation race from the policy logic.

## Result

`GATE_EXIT=1`; summary stops at `status=running` (incomplete):

```text
phase42_lifecycle_owner_admission_status=running
harness=live_kubernetes_validating_admission_policy
operator_status_main_patch_allowed=false
lifecycle_owner_finalizer_add_allowed=true     <- first run only (VAP not yet active)
lifecycle_owner_finalizer_remove_allowed=true  <- first run only (VAP not yet active)
```

- **Run 1 (no wait):** the VAP had not propagated, so the lifecycle-owner
  finalizer add/remove succeeded and the **spec-patch deny-check** (the first
  negative case) was *not* denied — the patch went through. Gate aborted at
  `lifecycle-owner-spec-patch`.
- **Run 2 (25 s wait, VAP active):** the gate aborted *earlier*, at the
  **legitimate** `lifecycle-owner-add-finalizer` — the active VAP denied it:

```text
ValidatingAdmissionPolicy 'sw-block-phase42-finalizer-only' ... denied request:
expression '... object.status == oldObject.status ...' resulted in error:
no such key: status
```

## Root Cause

The VAP CEL (script lines 165–178) compares optional object fields directly:

```text
object.spec == oldObject.spec &&
object.status == oldObject.status &&                 <-- errors when .status absent
object.metadata.labels == oldObject.metadata.labels &&
object.metadata.annotations == oldObject.metadata.annotations &&
object.metadata.ownerReferences == oldObject.metadata.ownerReferences &&   <-- also absent on the test object
...
```

The test object `phase42-a` is created with only `metadata` (labels/annotations)
and `spec` — **no `.status`, no `.ownerReferences`**. In Kubernetes VAP CEL,
reading an absent key (`object.status`) raises `no such key: status`. With
`validationActions: [Deny]` and the default `failurePolicy: Fail`, a CEL
**evaluation error is treated as DENY**. So the active policy denies the very
finalizer add it is meant to allow. (Once `status` is guarded, the next absent
field — `ownerReferences` — would error the same way.)

So there are two defects, in priority order:

1. **(Blocking, product) VAP CEL does not handle absent optional fields.**
   `object.status`, `object.metadata.ownerReferences` (and any optional
   metadata) must be `has()`-guarded, e.g. per field:
   `(has(object.X) == has(oldObject.X)) && (!has(object.X) || object.X == oldObject.X)`
   (or CEL optional access `object.?X.orValue(null) == oldObject.?X.orValue(null)`).
   Until this is fixed, the gate cannot pass: a real apiserver denies the
   approved finalizer mutation.
2. **(Gate robustness) No wait for VAP propagation before the deny-checks.**
   The script runs the patch checks immediately after `kubectl apply` of the
   policy/binding (line 194 → 215). VAP enforcement is not synchronous, so the
   negative checks are non-deterministic and, in run 1, raced an inactive policy
   (which masked defect #1 by letting the spec patch through). Add a readiness
   wait — poll until a known-bad patch is actually denied, or `kubectl wait` /
   bounded sleep — before any check.

## Gate Coverage Reached

```text
G1 run the gate                      FAIL  (policy denies legitimate finalizer add; spec deny-check raced in run 1)
G2 forbidden resource mutations      NOT REACHED (gate aborts at G1)
G3 object integrity                  NOT REACHED
G4 cleanup                           PASS  (ns/VAP/VAPBinding/ClusterRole/Binding all removed on failure exit;
                                            verified 0 leftovers after both runs)
```

`operator_status_main_patch_allowed=false` is correct (RBAC-enforced; the
operator-status SA has no main-object patch). The harness is genuinely live VAP —
it *did* enforce in run 2; it enforced the wrong outcome.

## Blocking Findings

1. VAP CEL errors on absent optional fields (`no such key: status`) →
   `failurePolicy:Fail` → the approved lifecycle-owner finalizer add/remove is
   denied on a real API server. Fix the CEL to `has()`-guard optional-field
   immutability comparisons, then re-run.

## Non-Blocking Findings

1. Gate has no VAP-propagation wait; the negative checks are order/timing
   dependent. Add a readiness gate so a fixed policy is deterministically
   enforced before the checks. (Surface this even after #1 is fixed.)
2. tp01 `NotReady` — unrelated to this single-node admission gate.

## Recommendation

**Hold Phase 42 D1.** The live gate did its job — it surfaced a real CEL defect
that no mock could. Dev should: (a) `has()`-guard the optional-field comparisons
in the VAP (status, ownerReferences, and any optional metadata), and (b) add a
VAP-propagation readiness wait before the deny-checks. Then re-run on m02
(v1.34.4, VAP-capable) for the full G1–G4 sweep. I can re-run as soon as the fix
lands.
