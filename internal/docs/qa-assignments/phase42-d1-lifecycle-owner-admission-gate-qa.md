# QA Assignment - Phase 42 D1 Lifecycle Owner Admission Gate

## Goal

Validate the first real Kubernetes API/admission gate for the future
lifecycle-owner. This gate proves the intended boundary before any product
controller is allowed to add or remove `SwBlockVolume` finalizers.

Expected result:

```text
operator-status remains status/events-only;
lifecycle-owner can add/remove only the Seaweed Block finalizer;
spec, unrelated metadata, foreign finalizers, fake /finalizers, and workload or
storage resource mutations are rejected by real Kubernetes API/admission.
```

## Source Under Test

Branch:

```text
phase41-lifecycle-owner-foundation
```

Relevant files:

```text
scripts/run-phase42-lifecycle-owner-admission-gate.sh
scripts/run-phase42-lifecycle-owner-admission-gate.ps1
testops/scenarios/lifecycle-owner-admission-gate-chain.yaml
internal/docs/current-plan.md
internal/docs/ref/phase42-lifecycle-owner-api-admission-gate.md
```

## Environment Requirement

This is a live API/admission gate. It requires a Kubernetes API server with
`admissionregistration.k8s.io/v1` `ValidatingAdmissionPolicy` support.

If the target cluster does not support `ValidatingAdmissionPolicy`, record:

```text
phase42_lifecycle_owner_admission_status=blocked
blocked_reason=validating_admission_policy_unavailable
```

That is an environment blocker, not a product PASS.

## G1 - Run The Gate

Run either:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File scripts/run-phase42-lifecycle-owner-admission-gate.ps1 `
  -ProductRoot C:\work\seaweed_block `
  -ArtifactDir C:\work\seaweed_block\results\phase42-lifecycle-owner-admission-qa
```

or:

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/phase42-lifecycle-owner-admission-qa \
  bash scripts/run-phase42-lifecycle-owner-admission-gate.sh "$PWD"
```

or the TestOps scenario:

```bash
swblock run testops/scenarios/lifecycle-owner-admission-gate-chain.yaml
```

Pass criteria in
`phase42-lifecycle-owner-admission-gate-summary.txt`:

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
```

## G2 - Forbidden Resource Mutations

The summary must include `false` for all lifecycle-owner patch checks:

```text
lifecycle_owner_pods_patch_allowed=false
lifecycle_owner_deployments_patch_allowed=false
lifecycle_owner_persistentvolumeclaims_patch_allowed=false
lifecycle_owner_persistentvolumes_patch_allowed=false
lifecycle_owner_storageclasses_patch_allowed=false
lifecycle_owner_secrets_patch_allowed=false
lifecycle_owner_nodes_patch_allowed=false
lifecycle_owner_csidrivers_patch_allowed=false
lifecycle_owner_csinodes_patch_allowed=false
```

Fail if any forbidden resource mutation is allowed.

## G3 - Object Integrity

Inspect `final-object.stdout.txt`.

Pass criteria:

```text
spec.pvcName remains phase42-a
metadata.labels.keep remains true
metadata.annotations.keep remains true
no foreign finalizer remains
```

Fail if an allowed finalizer patch changes spec, labels, annotations, or any
unrelated metadata.

## G4 - Cleanup

The script must delete its namespace, RBAC, and admission objects on exit.

Pass criteria:

```text
kubectl get ns sw-block-phase42-gate
```

returns NotFound after the run, and no `sw-block-phase42-*` ClusterRole,
ClusterRoleBinding, ValidatingAdmissionPolicy, or
ValidatingAdmissionPolicyBinding remains.

## Verdict

PASS only if G1-G4 pass on a real Kubernetes API/admission surface.

Do not mark Phase 42 D1 passed using the old Phase 41 schema-aware fake server.
That fake server was sufficient for Phase 41's non-mutating slice, but Phase 42
is specifically about the real admission boundary.

The gate must wait until a known-bad lifecycle-owner patch is denied before it
runs the positive and negative assertions. A success before admission policy
propagation is not a product PASS.
