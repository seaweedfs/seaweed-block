# Phase 36 D4 QA Assignment - Cleanup Visibility, Not Automatic Cleanup

Status: ready for QA.

Source scope:

- `SwBlockCluster.status.cleanup`
- `SwBlockCluster.status.conditions[]` with `CleanupRequired`
- `SwBlockCluster.status.safeNextSteps[]`
- `operator-snapshot.json.cluster.cleanup`
- `operator-snapshot.json.cluster.conditions[]`
- report `summary.txt`
- report `index.html`

## Goal

Verify that cleanup verifier evidence becomes user-visible status and safe
next-step guidance without the operator deleting anything.

This gate is intentionally not an automatic cleanup gate.

## Required Checks

### G1: Clean State Projects CleanupRequired=False

Start from a clean lab or run the standard cleanup verifier after uninstall.

Expected:

```text
SwBlockCluster.status.cleanup.status=ok
SwBlockCluster.status.conditions has CleanupRequired=False
CleanupRequired reason=cleanup_verified
operator-snapshot.json cluster.cleanup.status=ok
summary.txt includes cleanup_status=ok
index.html includes Lifecycle Cleanup
```

There must be no `observe.verify_cleanup` safe next step in the clean state.

### G2: Residue Projects CleanupRequired=True

Create a controlled residue that the verifier already detects, such as a stale
Seaweed Block iSCSI node DB record or a known safe test residue. Do not use a
residue that can disrupt unrelated lab work.

Run the existing cleanup verifier and feed its summary into the same report /
operator-status path.

Expected:

```text
SwBlockCluster.status.cleanup.status=failed
SwBlockCluster.status.cleanup.<category>ResidueCount > 0 for the residue type
SwBlockCluster.status.conditions has CleanupRequired=True
CleanupRequired reason matches the verifier reason when present
SwBlockCluster.status.safeNextSteps has type=observe.verify_cleanup
safeNextSteps[].mode=scripted
safeNextSteps[].mutationAllowed=false
safeNextSteps[].command mentions verify-helm-cleanup.sh
```

### G3: Surface Agreement

For both clean and residue evidence:

```text
CRD cleanup counters agree with operator-snapshot.json
operator-snapshot.json agrees with summary.txt
index.html shows the same residue counts
safe_next_step lines agree with CRD safeNextSteps[]
```

### G4: Boundary

Verify the operator-status controller does not perform cleanup:

```text
operator-status may patch CRD status and create Events only
operator-status does not run verify-helm-cleanup.sh
operator-status does not run uninstall-k8s-alpha.sh
operator-status cannot delete pods, PVCs, PVs, deployments, storageclasses,
iSCSI sessions, multipath maps, hostPath data, or CRD spec
```

## Pass Criteria

```text
G1 PASS
G2 PASS
G3 PASS
G4 PASS
final lab cleanup verifier returns cleanup_status=ok
```

## Non-Claims

- No automatic cleanup.
- No finalizer or delete safety.
- No host mutation by operator-status.
- No iSCSI/multipath cleanup by operator-status.
- No support-bundle upload.
