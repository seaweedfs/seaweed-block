# Current Plan: Pending Next Product Usability Slice

Status: pending selection. The previous plan closed as
`finished-plans/phase9_finishedplan_light_use_operations_mvp.md`.

## Recommended Next Slice

Light-Use Install And Lifecycle MVP.

## Why This Is Next

The closed operations plan made one read-only loop usable:

```text
observe one volume -> read summary -> attach bundle -> see TestOps control data
```

The next largest blocker to a functional light-use block product is lifecycle
ownership. A user can reproduce our gated scenarios, but too much of
install/create/delete/retry/cleanup still feels like script/TestOps discipline
instead of product behavior.

The previous plan closed the operations MVP only. The next plan should decide
whether we can honestly claim a first usable block workflow, not just a good
debug bundle after another workflow has already produced a volume.

## Draft User Experience

After the next slice, a light-use user should be able to:

1. Install or launch the product in the supported lab/K8s shape with one
   documented command path.
2. Create a StorageClass/PVC and run an app workload without reading internal
   scripts.
3. Delete the workload/PVC and see product-owned cleanup behavior.
4. Use the operations status bundle from the finished plan when something goes
   wrong.
5. Let TestOps prove the same flow through a repeatable scenario gate.

The target close-loop should be concrete:

```text
install/launch -> create PVC -> attach app pod -> write/read -> delete ->
verify cleanup -> collect status bundle on failure
```

## Draft Top Blocking Questions

- Which install path do we claim first: alpha K8s scripts, a product CLI wrapper,
  or a minimal operator/controller shape?
- Does the existing `docs/quickstart-kubernetes.md` path still pass from a fresh
  checkout on the supported lab shape?
- What exact user claim does that path support: pod replacement only, node loss,
  mounted failover, or something narrower?
- Which lifecycle cleanup must be product-owned before we can call the product
  light-use functional?
- Which cleanup checks remain TestOps-only non-claims?
- What is the smallest scenario that proves the user experience without turning
  every validation into a 20-minute integration suite?

## Not Started

No implementation is started under this plan yet. Confirm or adjust the slice
before writing code.
