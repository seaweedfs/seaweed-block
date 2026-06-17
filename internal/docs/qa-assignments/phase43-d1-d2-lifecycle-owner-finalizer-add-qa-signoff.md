# QA Sign-off — Phase 43 D1/D2 Lifecycle Owner Finalizer Add

Verdict: **PASS.** The first product path that performs a real Kubernetes
mutation works and is correctly bounded: the lifecycle-owner adds **only** the
`block.seaweedfs.com/swblockvolume-protection` finalizer to existing
`SwBlockVolume` objects, idempotently, while operator-status stays
status/events-only and the lifecycle-owner can mutate nothing else. Validated on
a real VAP-capable cluster.

Date: 2026-06-15
Source: branch `phase41-lifecycle-owner-foundation` @ `1244285 phase43: add
lifecycle owner finalizer path`
Image: fresh local build from `1244285` (`sw-block:local` — the `ops
lifecycle-owner` subcommand is new in this commit; published images predate it),
imported to m01+m02 k3s.
Environment: m02 k3s **v1.34.4+k3s1** (ValidatingAdmissionPolicy enforced).
Install: `helm install ... -f day1.yaml --set operatorStatus.create=true
--set operatorStatus.dryRun=false --set lifecycleOwner.create=true
--set lifecycleOwner.dryRun=false --set lifecycleOwner.interval=15s` (all pods —
lifecycle-owner, operator-status, blockmaster — `1/1 Running`).

## G1 — Local Contract — PASS

- `go test ./core/ops ./cmd/sw-block` → `ok` (both).
- Default `helm template` does **not** render the lifecycle-owner (count 0).
- `--set lifecycleOwner.create=true --set lifecycleOwner.dryRun=false` renders the
  lifecycle-owner **Deployment** (`sw-block-lifecycle-owner`), **ServiceAccount**,
  **ClusterRole/ClusterRoleBinding** (`sw-block-seaweed-block-lifecycle-owner`),
  **ValidatingAdmissionPolicy + Binding**
  (`...-lifecycle-owner-finalizer-boundary`).

## G2 — Identity / RBAC Boundary — PASS

Live `kubectl auth can-i`:

```text
operator-status SA  patch swblockvolumes (main)         => no
lifecycle-owner SA  patch swblockvolumes (main)         => yes
lifecycle-owner SA  create events                        => yes
lifecycle-owner SA  patch swblockvolumes/status          => no
lifecycle-owner SA  {create,update,patch,delete} ×
  {pods, persistentvolumeclaims, persistentvolumes, secrets, storageclasses,
   deployments, nodes, csidrivers, csinodes}             => no  (36/36)
```

operator-status keeps no main-object patch power; the lifecycle-owner has exactly
main-object patch (for finalizers) + Events and nothing else.

## G3 — Admission Boundary — PASS

The product VAP (`...-lifecycle-owner-finalizer-boundary`, `failurePolicy: Fail`,
`UPDATE` only, `has()`-guarded) enforces, against the lifecycle-owner identity:

```text
add protection finalizer                          => allowed
add protection finalizer again (idempotent)       => allowed
patch spec                                         => denied
patch labels                                       => denied
patch annotations                                  => denied
patch ownerReferences                              => denied
add a foreign finalizer                            => denied
mixed (finalizer + spec)                           => denied
add protection alongside a pre-existing foreign    => allowed  (foreign preserved)
remove a pre-existing foreign finalizer            => denied
  final finalizers = [example.com/foreign, block.seaweedfs.com/swblockvolume-protection]
```

The CEL confines the lifecycle-owner to adding/removing only the protection
finalizer (non-protection finalizers must be byte-equal before/after), and all
non-finalizer fields are immutable. status is doubly protected (RBAC denies the
`/status` subresource; a main-object status patch is stripped by the apiserver).

## G4 — Product Finalizer Add — PASS

Created an owned `SwBlockVolume` stub `g4-a` with no protection finalizer and let
the running lifecycle-owner reconcile:

```text
g4-a finalizers: []  ->  [block.seaweedfs.com/swblockvolume-protection]   (exactly one)
spec.pvcName=g4-a  labels.keep=true  annotations.keep=true                (unchanged)
controller log iteration 1: volumes=1 finalizer_patches=1 finalizer_added=1 events=1
controller log iteration 2+: finalizer_patches=0 finalizer_added=0 events=0   (idempotent)
Event: Normal finalizer_added swblockvolume/g4-a "Seaweed Block protection finalizer added"
```

Object gains exactly one protection finalizer; spec/labels/annotations (and
status/ownerReferences) unchanged; repeated reconcile does not duplicate the
finalizer or churn Events; a single bounded `finalizer_added` Event is emitted.
(`mutation_allowed=true` in the lifecycle-owner log is correct — this is the
designated mutating component, bounded by RBAC + admission; operator-status
remains `mutation_allowed=false`.)

## G5 — Cleanup — PASS

```text
helm uninstall: release uninstalled
verify-helm-cleanup.sh: cleanup_status=ok   cleanup_observed_at=2026-06-16T01:50:17Z
swblockvolumes=0  lifecycle-owner VAP/binding=0/0  RBAC=0/0  pods=0  helm releases=0
```

No stuck `SwBlockVolume`, VAP, binding, RBAC, pods, PVCs, PVs, iSCSI, multipath,
or dmsetup residue.

## Blocking Findings

None. No blocking condition is present: operator-status gains no main-object patch;
the lifecycle-owner can mutate only `SwBlockVolume.metadata.finalizers` (protection
only) + Events; admission is present, propagated, and rejects every non-finalizer
mutation; the finalizer add changes no other field; repeated reconcile is bounded.

## Non-Blocking Findings

1. **A protection finalizer blocks its own object's deletion** — this is by design
   (Phase 43 is add-only; finalizer *release* is a future phase). To clean up the
   test object at G5 I removed the finalizer as cluster-admin, which the VAP
   correctly permits for non-lifecycle-owner identities. Users deleting a
   `SwBlockVolume` before the release path ships will see it held in `Terminating`
   until an admin clears the finalizer — expected for this slice; ensure D-series
   release work (or docs) covers it.
2. The `ops lifecycle-owner` subcommand is new in `1244285`, so a fresh image
   build was required (published `sha-*` images predate it). Publish a candidate
   image before any release that enables `lifecycleOwner.create=true`.
3. tp01 `NotReady` — unrelated to this single-node gate.

## Recommendation

**Phase 43 D1/D2 pass.** The bounded protection-finalizer add is real-API-proven:
add-only, idempotent, admission-confined, with operator-status untouched and zero
residue. The lifecycle-owner is correctly disabled by default. Next D-series work
(finalizer release gated on delete-safety) can build on this; finalizer *removal*
on actual deletion is the remaining half before the delete lifecycle is complete.
