# QA Sign-off - Phase 39 D4/D5 Finalizer Delete Safety

Verdict: **BLOCKED.** The first bounded mutating path — adding/removing
`block.seaweedfs.com/swblockvolume-protection` on `SwBlockVolume.metadata.finalizers`
— does not work against the live Kubernetes API. The operator PATCHes a
`<resource>/<name>/finalizers` URL, but CRDs have no `/finalizers` subresource
(only `/status` and `/scale`), so every finalizer patch returns **HTTP 404**. The
finalizer is never added, so neither D4 (blocked-delete hold) nor D5
(clean-delete release) can be exercised. The status-only projection
(`status.deleteSafety`) works; only the finalizer mutation is broken. Same class
as the Phase 35 D3 and Phase 37 live-vs-mock defects: it passes `go test` (mock
HTTP server) and `helm template` but fails against the real k3s API.

Date: 2026-06-09

Source commit: `f59784a` (floor met: `7143c8f`, `fd3977b`, `3340038`, `07c50d3`,
`a90dda3`; branch `phase33-testops-failure-hardening`)

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
