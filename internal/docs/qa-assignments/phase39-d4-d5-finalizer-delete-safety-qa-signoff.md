# QA Sign-off - Phase 39 D4/D5 Finalizer Delete Safety

> Superseded by the lifecycle-owner pivot after `d695c0a`. The findings below
> remain valid evidence that operator-status must not own CRD finalizer mutation
> under the old RBAC model. The revised Phase 39 gate is now delete-safety
> status/events only; finalizer add/remove is deferred to a future lifecycle
> owner.

Verdict: **STILL BLOCKED (after `b371e2e`).** The URL bug is fixed — the operator
now PATCHes the main `SwBlockVolume` object (no `/finalizers` 404) — but that
exposed the deeper, definitive problem I flagged as the RBAC follow-on: the patch
now returns **HTTP 403**. Modifying a CRD's `.metadata.finalizers` requires the
**main** `patch swblockvolumes` permission, and the `swblockvolumes/finalizers`
subresource grant does **not** substitute for it (for CRDs there is no
`/finalizers` endpoint to target, so the authorizer always checks the main verb
first). Confirmed for **both** add-on-fresh and remove-on-terminating. The
finalizers-subresource-only RBAC can never modify a CRD finalizer — so the
bounded-mutation design as built cannot work, and D4/D5 still cannot be
exercised. The status-only `status.deleteSafety` projection continues to work.

Date: 2026-06-09 (404 blocked) → 2026-06-11 (re-validated `b371e2e`: 404 fixed, 403 exposed)

Source commits: `f59784a` (404 blocked) → `b371e2e phase39: fix finalizer patch
endpoint` (URL fixed, now 403; branch `phase33-testops-failure-hardening`)

> See the **Re-Validation (`b371e2e`)** section at the end for the 403 evidence
> and the design options. The original 404 write-up is preserved below.

---

## ORIGINAL FINDING (404, `f59784a`) — preserved

## Lab Node Health

- m01 `Ready`, m02 `Ready`, **tp01 `NotReady`/unreachable** ("No route to host" on
  SSH/image-import; same as Phase 38). D4/D5 are single-`SwBlockVolume` finalizer
  gates and do not need tp01; the install used operator-status pinned to m02 with
  the CSI image imported to m01 only. Flag for the lab admin to restore tp01
  before D6 multi-volume isolation.

## RBAC Boundary — PASS (with a follow-on concern)

```text
patch swblockvolumes --subresource=finalizers: yes
patch swblockvolumes --subresource=status:     yes
create events:                                  yes
patch pods:                                     no
patch persistentvolumeclaims (default):         no
update storageclasses.storage.k8s.io:           no
delete persistentvolumeclaims (default):        no
patch swblockvolumes (spec/main):               no
```

The bounded mutation is scoped to the `finalizers` + `status` subresources +
event create; no pod/PVC/storageclass/spec mutation. **Follow-on concern:** the
SA has `update swblockvolumes/finalizers` (subresource) but **not** `patch
swblockvolumes` (main). Once the URL bug is fixed (patch the main object), the
finalizer-only patch must be authorized by the `finalizers` subresource — verify
this works for both *adding* a finalizer to a not-yet-deleted object and
*removing* it from a deleting object, or the corrected patch may 403.

## D4 — Blocked Delete Holds Finalizer — BLOCKED

Created `SwBlockVolume/del-vol` and fed a delete-safety bundle
(`swblockvolume-delete-summary.txt` + `cleanup-summary.txt`). On reconcile, the
operator wrote `status.deleteSafety.state=not_requested decision=rejected`
(status path works) but **failed to add the finalizer**:

```text
operator_status=write_status ... finalizer_patches=0
sw-block ops operator-status: patch swblockvolumes/del-vol finalizers failed:
  http 404 swblockvolumes.block.seaweedfs.com "del-vol" not found  (reason: NotFound)
```

The same `del-vol` accepts a `/status` patch and accepts a finalizer set via the
**main-object** patch (admin):

```text
kubectl -n kube-system patch swblockvolume del-vol --type=merge \
  -p '{"metadata":{"finalizers":["block.seaweedfs.com/swblockvolume-protection"]}}'
-> del-vol finalizers: ["block.seaweedfs.com/swblockvolume-protection"]
```

So the object exists; only the operator's `/finalizers` URL is wrong. Because the
finalizer is never added, none of the D4 criteria can be reached (no hold, no
`deleteSafety.state=blocked` under a real delete, no idempotency check). BLOCKED.

## D5 — Clean Delete Releases Finalizer — BLOCKED

Same root cause — without a finalizer the operator can add/remove, there is no
release to observe, no `finalizer_released` Event, and a delete of the object
completes immediately (nothing holds it). BLOCKED.

## The Bug (blocking)

`core/ops/kubernetes_status_writer.go`:

```go
func (c *KubernetesStatusClient) finalizersURL(...) string {
    return c.resourceURL(namespace, resource, name) + "/finalizers"   // <- /finalizers
}
func (c *KubernetesStatusClient) patchVolumeFinalizers(...) error {
    ... http.MethodPatch, c.finalizersURL(...),
        body = {"metadata":{"finalizers":[...]}},
        Content-Type: application/merge-patch+json ...
}
```

CRDs expose only the `/status` (and optionally `/scale`) subresources. There is
no `/finalizers` endpoint, so `PATCH .../swblockvolumes/del-vol/finalizers`
resolves to nothing and the API server returns `404 NotFound`. Finalizers on a
CRD are modified by PATCHing the **main object** (`.../swblockvolumes/del-vol`,
no suffix) with `{"metadata":{"finalizers":[...]}}` — exactly the body already
built; only the URL is wrong.

### Fix

1. Drop the `/finalizers` suffix — PATCH the main object URL with the
   metadata.finalizers merge patch. (The `finalizers` RBAC subresource still
   gates the finalizer-only change; confirm it authorizes both add and remove —
   see the RBAC follow-on above.)
2. Add a **live / envtest** regression for the finalizer add+remove against a
   real API server (the existing tests use a mock that accepts any path, which is
   why this passed `go test`). This is the third phase in a row (D3 casing, D2
   node-condition enum, now this) where a writer payload/endpoint that satisfies
   a mock fails the real CRD API — an envtest harness for the
   KubernetesStatusClient would catch all of them.

## Final Cleanup Audit — PASS

`cleanup_status=ok`, `k8s_residue_count=0`, `iscsi_residue_count=0`,
`failure_count=0`; helm 0, pods 0. (The manually-added test finalizer was removed
and `del-vol` deleted.)

## Blocking Findings

1. **Finalizer mutation 404 (URL).** The operator PATCHes a non-existent
   `<crd>/<name>/finalizers` subresource; every finalizer add/remove returns 404.
   The bounded-mutation feature is non-functional against the live API. Fix: PATCH
   the main object URL.

## Non-Blocking Findings

1. **RBAC follow-on.** After the URL fix, verify the `finalizers` subresource
   permission (the SA lacks main `patch swblockvolumes`) authorizes the
   finalizer-only patch for add-on-fresh and remove-on-deleting.
2. **Lab infra: tp01 `NotReady`/unreachable.** Not a Phase 39 defect; restore
   before D6 multi-volume isolation.

## Recommendation for D6 (Multi-Volume Isolation)

Do **not** advance to D6 yet. D6 multiplies the finalizer lifecycle across many
volumes; it cannot pass while the single-volume finalizer patch 404s. Fix the
`/finalizers` URL, add the envtest regression, re-run D4/D5 live (finalizer
added → blocked-hold under a real `kubectl delete` → clean-release with one
`finalizer_released` Event and object deletion completing), and restore tp01 —
then D6.

---

## RE-VALIDATION (`b371e2e`) — STILL BLOCKED (403)

`b371e2e` correctly fixes the URL: `patchVolumeFinalizers` now PATCHes
`c.resourceURL(ns, swblockvolumes, name)` (the main object) with
`{"metadata":{"finalizers":[...]}}`. The 404 is gone. But the operator now hits
**403 Forbidden** — the deeper issue noted in the original RBAC follow-on.

### Add to a fresh object — 403

```text
SwBlockVolume/del-vol created (no deletionTimestamp); reconcile ->
status.deleteSafety written (status path OK), then:
patch swblockvolumes/del-vol finalizers failed: http 403
  User "system:serviceaccount:kube-system:sw-block-seaweed-block-operator-status"
  cannot patch resource "swblockvolumes" in API group "block.seaweedfs.com" in namespace "kube-system"
del-vol finalizers: (empty)  -> finalizer never added
```

### Remove from a terminating object — also 403

```text
admin adds the finalizer; kubectl delete del-vol --wait=false (deletionTimestamp set);
reconcile with delete_requested=true + clean cleanup ->
patch swblockvolumes/del-vol finalizers failed: http 403 (same "cannot patch swblockvolumes")
del-vol still exists, finalizers=["block.seaweedfs.com/swblockvolume-protection"]  -> stuck terminating
```

So **neither add nor remove works** with the current RBAC.

### Why finalizers-subresource-only RBAC cannot work for a CRD

A CRD exposes no `/finalizers` endpoint (that is why the old URL 404'd), so
`.metadata.finalizers` can only be changed by PATCHing the **main object**. The
authorizer evaluates that request as `patch swblockvolumes` (the main verb +
resource) and denies it — the operator was granted only
`swblockvolumes/finalizers`. The `<resource>/finalizers` RBAC subresource is an
*additional* gate applied by the OwnerReferencesPermissionEnforcement admission
plugin *after* authorization; it is **not a substitute** for the main `patch`
permission, and (unlike built-ins such as namespaces) there is no finalizers URL
to make the authorizer evaluate the subresource instead. Net: the
finalizers-subresource-only grant can never modify a CRD finalizer.

### Design options (this needs a decision, not just a code tweak)

1. **Grant the operator `patch`/`update` on `swblockvolumes` (main) and enforce
   finalizer-only changes with a ValidatingAdmissionPolicy/webhook** that rejects
   any operator-status write touching `.spec` or fields other than
   `.metadata.finalizers`. Keeps the "no spec mutation" boundary via admission
   rather than RBAC. This is the standard way to bound a finalizer controller.
2. **Move finalizer add/remove to the component that already owns the
   `SwBlockVolume` lifecycle** (the CSI provisioner/controller that creates the
   CR), gated by the `status.deleteSafety` the operator publishes. The
   operator-status SA then stays status-only (no main patch), which preserves the
   read-only posture established in Phases 35-38.
3. **Accept main `patch` on the SA with a code-enforced boundary** (the
   controller only ever writes finalizers). Weakest — the RBAC no longer proves
   the boundary; not recommended given the whole phase chain's emphasis on
   RBAC-provable read-only.

Option 1 or 2 is the real fix. Whichever is chosen, add a **live/envtest**
regression that performs the add+remove against a real API server with the
operator's actual RBAC (the current tests pass because the mock neither enforces
the CRD subresource surface nor the authorizer — the same gap that hid the 404
and now the 403).

### Status of the gates

- RBAC boundary intent: still correctly scoped (no pod/PVC/storageclass/spec
  mutation), but **insufficient** for the finalizer feature to function.
- D4 blocked-delete hold: **still BLOCKED** (finalizer never added → nothing to
  hold).
- D5 clean-delete release: **still BLOCKED** (operator cannot remove the
  finalizer → object stuck terminating).
- Final cleanup: clean (`cleanup_status=ok`, residue 0); the stuck `del-vol` was
  cleared by an admin finalizer patch.
- Lab: tp01 still `NotReady`/unreachable.

### Bottom line (updated)

`b371e2e` fixes the URL but the bounded-finalizer-mutation design is **not viable
as built** — the operator cannot modify a CRD finalizer with only the
`swblockvolumes/finalizers` subresource grant (403 on both add and remove). This
is a design decision (admission-bounded main patch, or move finalizer ownership),
not a one-line fix. Do not close D4/D5 or advance to D6 until the operator can
actually add the finalizer to a fresh `SwBlockVolume` and remove it from a
terminating one, proven live with its real RBAC.

---

## STATUS-ONLY RE-VALIDATION (`4a51bae`, floor `1630de2`) — D5 PASS, D4 BLOCKED (new 422)

Product pivoted to **status/events-only** (my Option 2): the operator no longer
patches finalizers; finalizer add/remove is deferred to a future lifecycle-owner.
Verified in code: no `EnsureVolumeFinalizer`/`ReleaseVolumeFinalizer` calls in the
controller, and the RBAC `swblockvolumes/finalizers` grant is **removed**.

### RBAC boundary — PASS (status-only)

```text
patch swblockvolumes --subresource=status: yes   create events: yes
patch swblockvolumes (main): no                  patch swblockvolumes --subresource=finalizers: no
patch pods: no   patch pvc (default): no   update storageclasses: no
```

Status + events only; finalizers grant gone; no spec/pod/PVC/storageclass power.
The 403 problem is correctly avoided by **not attempting** the mutation.

### D5 — Clean Delete-Safety Status — PASS

Bundle (`delete_requested=true` + `cleanup_status=ok`, all residue 0):

```text
operator_status=write_status ... volumes=1 events=3 finalizer_patches=0 mutation_allowed=false
SwBlockVolume.status.deleteSafety: state=releasable decision=allowed finalizerReleaseAllowed=true
metadata.finalizers: (empty)   no finalizer patch, no finalizer-released Event
idempotent: re-run keeps state=releasable
```

All D5 criteria met: `releasable`/`allowed`, `finalizerReleaseAllowed=true` as a
**decision fact only**, `finalizer_patches=0`, idempotent, final verifier
`cleanup_status=ok`. PASS.

### D4 — Blocked Delete-Safety Status — BLOCKED (new live-vs-mock 422)

Bundle (`delete_requested=true` + `cleanup_status=failed iscsi_residue_count=1`).
The blocked state's safe action is `observe.verify_cleanup` (`mode=scripted`), and
writing it into `SwBlockVolume.status.allowedActions[]` is rejected:

```text
patch swblockvolumes/del-vol status failed: http 422
  status.allowedActions[1].mode: Unsupported value: "scripted":
  supported values: "read_only", "dry_run"
```

So the entire blocked status patch fails — `deleteSafety`, the `CleanupRequired`
condition, and the verify_cleanup action never land. Root cause: the
`SwBlockVolume` CRD `status.allowedActions[].mode` enum is `{read_only, dry_run}`
(`crds/swblockvolumes...yaml:165-166`) and is **missing `scripted`**, even though
the `SwBlockCluster` CRD `safeNextSteps[].mode` enum **does** include `scripted`
(`crds/swblockclusters...yaml:265`). The blocked delete-safety projection puts the
`scripted` verify_cleanup action onto the volume's `allowedActions`, which the
volume enum rejects.

This is the same class as the D3 casing 422 and the Phase 37 node-condition 422:
a payload the unit-test mock accepts but the live CRD schema rejects.

**Fix:** add `scripted` to the `SwBlockVolume.status.allowedActions[].mode` enum
(one line, mirroring the cluster `safeNextSteps` enum). Then re-run D4 live; the
blocked status should land with `deleteSafety.state=blocked decision=rejected`,
`CleanupRequired=True`, and the `observe.verify_cleanup mutationAllowed=false`
action.

### Status-only gate status

| Gate | Result |
|---|---|
| RBAC boundary (status-only, no finalizers) | PASS |
| D4 blocked delete-safety status | **BLOCKED** — `allowedActions[].mode=scripted` 422 |
| D5 clean delete-safety status | PASS |
| Final cleanup verifier | PASS (`cleanup_status=ok`, residue 0) |
| Lab: tp01 | still `NotReady`/unreachable |

### Bottom line (status-only)

The status-only pivot is the right call and is mostly working: the RBAC is
provably read-only (no finalizers), D5 projects `releasable/allowed` cleanly with
zero finalizer patches, and idempotency holds. **D4 is blocked by a one-line CRD
enum gap** — `scripted` is missing from the SwBlockVolume `allowedActions[].mode`
enum, so the blocked-delete status (which surfaces the scripted verify_cleanup
action) 422s and never lands. Add `scripted` to that enum, re-run D4, **then** D6
multi-volume status isolation. Recommend the live/envtest regression for the
status writer once more — it would have caught this 422 (and the prior 404/403)
before handoff.

---

## D4 RE-RUN (`f167f9a`) — PASS

`f167f9a phase39: allow scripted volume actions` adds `scripted` to the
`SwBlockVolume.status.allowedActions[].mode` enum (now `read_only, dry_run,
scripted`, mirroring the cluster `safeNextSteps` enum). Re-ran D4 live (re-imported
the existing image — binary unchanged — and re-installed with the updated CRD):

```text
operator_status=write_status ... volumes=1 events=3 finalizer_patches=0 mutation_allowed=false  EXIT=0
SwBlockVolume.status.deleteSafety: state=blocked decision=rejected reason=iscsi_node_records_present
condition CleanupRequired: True / iscsi_node_records_present
allowedActions[observe.verify_cleanup]: mode=scripted mutationAllowed=false
metadata.finalizers: (empty)        Ready=True conditions: 0
idempotent: 3 reconciles -> state stays blocked, 2 distinct Events (bounded, not per-reconcile growth)
no finalizer-added/released Events
final cleanup verifier: cleanup_status=ok, residue 0
```

All D4 status-only criteria are met: `blocked`/`rejected` with the verifier
reason, `CleanupRequired=True`, the `observe.verify_cleanup` action surfaces with
`mode=scripted mutationAllowed=false` (no longer 422), no `Ready=True`,
`finalizer_patches=0`, no finalizer Events, bounded Events on repeat, and no
storage/workload/host mutation. PASS.

### Final Phase 39 D4/D5 status (status-only path)

| Gate | Result |
|---|---|
| RBAC boundary (status-only, no finalizers) | PASS |
| D4 blocked delete-safety status | **PASS** (`f167f9a`) |
| D5 clean delete-safety status | PASS |
| Final cleanup verifier | PASS |
| Lab: tp01 | still `NotReady`/unreachable |

**D4 and D5 both pass on the status-only path.** The operator surfaces
delete-safety (`blocked`/`releasable`, decision, `CleanupRequired`, the scripted
verify_cleanup action, `finalizerReleaseAllowed` as a fact) and emits Events,
with **zero finalizer/spec/storage/workload mutation** and an RBAC boundary that
proves it. **Phase 39 D4/D5 can close; proceed to D6 multi-volume status
isolation** (restore tp01 first if D6 exercises multiple nodes). Still recommend
adding the live/envtest status-writer regression — it would have caught the
404/403/422 chain before each handoff.
