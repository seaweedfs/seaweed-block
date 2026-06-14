# Operation Layer v0.5 Release Train

Status: planning contract.

## Purpose

Phase 41-44 are one product train: finish the Kubernetes operation layer before
starting new storage features such as productized returned-replica rebuild,
NVMe ANA parity, or backup/restore.

This train is not an open-ended ops cleanup loop. It has one exit condition:

```text
Seaweed Block can explain and safely gate Kubernetes lifecycle actions through
facts, judgment, action ownership, CRD status, Events, reports, dashboard, QA
evidence, and cleanup verification.
```

## Phase Boundaries

| Phase | Goal | Mutation allowed |
|---|---|---|
| 41 | Lifecycle owner foundation | none |
| 42 | Real API/admission proof for lifecycle-owner patches | test-only |
| 43 | First bounded finalizer mutation | SwBlockVolume finalizer only |
| 44 | Delete lifecycle close gate and release | finalizer only |

## Phase 41: Lifecycle Owner Foundation

Phase 41 defines the control model:

- observer/status writer,
- lifecycle owner,
- executor.

It keeps `operator-status` status/events-only and exposes delete-safety as
status plus dry-run lifecycle-owner action decisions.

Non-claim:

```text
No SwBlockVolume finalizer add/remove.
Delete-safety is guidance, not Kubernetes deletion protection.
```

## Phase 42: Real API / Admission Gate

Phase 42 proves the future lifecycle owner can receive main-object
`patch swblockvolumes` without opening a broad mutation surface.

The hard claim is:

```text
Only finalizer-shaped main-object patches are admitted for the lifecycle-owner
identity. Spec, labels, annotations, ownerReferences, status-through-main,
storage, workload, and host mutations are rejected.
```

Phase 42 may use envtest, a live throwaway cluster, or an equivalent real
apiserver/admission harness. Mock-only tests are not sufficient.

Phase 42 does not ship finalizer mutation in the product controller.

## Phase 43: First Bounded Lifecycle Mutation

Phase 43 can enable the first real mutation if Phase 42 passes:

```text
add/remove block.seaweedfs.com/swblockvolume-protection finalizer
```

Allowed:

- add finalizer on owned `SwBlockVolume`,
- hold deletion when delete-safety is blocked or unknown,
- remove finalizer when delete-safety is clean and fresh,
- emit Events and status for hold/release decisions.

Forbidden:

- cleanup execution,
- data deletion,
- PVC/PV mutation,
- workload mutation,
- storageclass mutation,
- rebuild/failback/promotion/backup/restore.

## Phase 44: Delete Lifecycle Close Gate And Release

Phase 44 validates the release path:

```text
install -> PVC -> status -> delete requested -> blocked/releasable finalizer
behavior -> cleanup evidence -> support bundle -> uninstall zero residue
```

Release claim should stay narrow:

- Kubernetes-native status/events operator foundation.
- Lifecycle-owner finalizer boundary proven.
- Delete-safety and finalizer lifecycle visible and gated.
- No automatic cleanup.
- No productized returned-replica rebuild/failback.
- No backup/restore.
- No NVMe ANA parity.

## After v0.5

After Phase 44, the next feature train can productize one larger data-plane
capability. The recommended first candidate is returned-replica
rebuild/reintegration/failback because the low-level rebuild and returned
replica safety pieces already exist.

That later train must still use the same five-layer standard:

```text
live facts -> judgment -> action owner -> user-visible status/action -> QA gate
```
