# QA Sign-off — Phase 42 D1 Lifecycle Owner Admission Gate

Verdict: **FAIL (blocking product defect in the VAP policy).** On a real
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
