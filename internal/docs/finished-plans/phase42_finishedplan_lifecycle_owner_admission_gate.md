# Phase 42 Finished Plan: Lifecycle Owner API / Admission Gate

Status: closed on 2026-06-15.

Branch: `phase41-lifecycle-owner-foundation`

## Outcome

Phase 42 closes the Phase 41 carry-forward: the lifecycle-owner boundary is now
proven against a real Kubernetes API/admission surface, not only a mock.

The product now has evidence for both halves required before shipping a real
finalizer mutation:

```text
1. lifecycle-owner main-object patch is admitted only for the Seaweed Block
   SwBlockVolume protection finalizer;
2. delete-safety decisions remain clean/blocked/missing/stale, dry-run,
   status-visible, and per-volume isolated.
```

## Delivered

- Live `ValidatingAdmissionPolicy` gate:
  - applies the real `SwBlockVolume` CRD,
  - creates separate `operator-status` and `lifecycle-owner` identities,
  - waits for VAP propagation before assertions,
  - permits only finalizer-shaped lifecycle-owner patches,
  - rejects spec, labels, annotations, ownerReferences, deletionTimestamp,
    foreign finalizers, mixed patches, fake `/finalizers`, `/status`, and
    workload/storage resource mutations.
- Optional-field-safe CEL:
  - guards absent `.status`, labels, annotations, ownerReferences, and finalizers
    with `has()`,
  - fixes the real m02 failure where absent `.status` caused an approved
    finalizer add to be denied.
- Expanded forbidden-resource matrix:
  - create/update/patch/delete denied for pods, deployments, PVCs, PVs,
    storageclasses, secrets, nodes, CSIDrivers, and CSINodes.
- Delete-safety decision gate:
  - clean cleanup evidence -> `allowed`,
  - residue -> `rejected`,
  - missing/stale evidence -> `unknown`,
  - lifecycle-owner action remains `mode=dry_run`,
    `mutation_allowed=false`,
  - no finalizer patches or finalizer mutation Events in Phase 42,
  - multi-volume delete-safety isolation holds.

## QA Evidence

- D1 initial live gate failure:
  `internal/docs/qa-assignments/phase42-d1-lifecycle-owner-admission-gate-qa-signoff.md`
  - `bc5ffc0` failed on m02 because CEL accessed absent optional fields.
- D1 fix and D1-D4 breadth:
  - `116d381` fixed optional-field guards and propagation wait.
  - `d3a1e0e` expanded D2-D4 breadth.
  - QA PASS on m02 (`k3s v1.34.4`) with real VAP:
    finalizer add/remove allowed and idempotent; all forbidden main-object and
    resource mutations denied; object integrity preserved; cleanup left no
    admission/RBAC residue.
- D5/D6 decision model:
  `internal/docs/qa-assignments/phase42-d5-d6-delete-safety-decision-qa-signoff.md`
  - QA PASS on m02 with Go 1.25.0,
  - `phase42_delete_safety_decision_status=ok`,
  - no cleanup execution,
  - no finalizer patch,
  - no finalizer mutation Events,
  - multi-volume isolation true.

## Non-Claims

Phase 42 does not ship product finalizer add/remove, deletion protection,
automatic cleanup, PVC/PV mutation, workload mutation, host cleanup, repair,
rebuild, failback, backup/restore, NVMe ANA parity, or production SLOs.

The only allowed mutation in Phase 42 is test-only against throwaway
`SwBlockVolume` objects inside the admission gate.

## Required Carry-Forward (Phase 43 Entry Point)

Phase 43 may now implement the first product mutation:

```text
add/remove block.seaweedfs.com/swblockvolume-protection on owned
SwBlockVolume objects, using the Phase 42 admission boundary and the
delete-safety decision model.
```

The scope must remain narrow:

```text
finalizer only;
operator-status remains status/events-only;
no cleanup execution;
no PVC/PV/workload/storage mutation;
no rebuild/failback/backup/NVMe.
```

## Next

Phase 43: first bounded finalizer mutation in the product path. The initial
slice should add the lifecycle-owner identity and idempotently add the protection
finalizer to owned `SwBlockVolume` objects while preserving the Phase 42
forbidden-patch matrix.
