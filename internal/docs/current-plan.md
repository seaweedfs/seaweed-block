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

## Draft Top Blocking Questions

- Which install path do we claim first: alpha K8s scripts, a product CLI wrapper,
  or a minimal operator/controller shape?
- Which lifecycle cleanup must be product-owned before we can call the product
  light-use functional?
- Which cleanup checks remain TestOps-only non-claims?
- What is the smallest scenario that proves the user experience without turning
  every validation into a 20-minute integration suite?

## Not Started

No implementation is started under this plan yet. Confirm or adjust the slice
before writing code.
