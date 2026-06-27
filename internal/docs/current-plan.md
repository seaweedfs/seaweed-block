# Current Plan: Operation Milestone Release Readiness

Status: active; blocked until matching published images exist.

## Goal

The operation layer has reached a useful close point:

```text
Helm install
  -> first PVC writer/reader
  -> SwBlockVolume CR/status/finalizer ownership
  -> delete-safety hold/release model
  -> returned-replica failback
  -> frontend publication
  -> post-publication workload writer/reader
  -> zero-residue cleanup
```

Before starting Phase 100 (Kubernetes CSI NVMe multipath attach), run one
release-readiness pass that proves the operation milestone can be published as
a beta-quality release. This is a release gate, not a new feature phase.

## Release Claim

Allowed claim:

```text
Operation Layer beta: Seaweed Block can install through Helm, create a first
PVC, publish CRD status/events, protect SwBlockVolume lifecycle with a bounded
finalizer owner, and run the opt-in returned-replica failback/frontend
publication close gate with workload I/O evidence.
```

Non-claims:

- no production HA/SLO;
- no default automatic failback;
- no automatic cleanup execution;
- no backup/snapshot/restore;
- no Kubernetes CSI NVMe multipath parity;
- no broad distro/kernel compatibility or performance claim.

## New Gate

Added:

```text
scripts/run-operation-milestone-release-readiness.ps1
```

The gate requires explicit matching images:

```text
SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>
SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>
```

It performs:

- published image manifest checks;
- release-scope Go tests;
- Helm lint;
- Helm render with operator-status and lifecycle-owner enabled;
- syntax validation for the published-image Day-1 smoke;
- syntax validation for the Phase 98 operation close smoke;
- `git diff --check`.

It intentionally reports:

```text
operation_milestone_release_readiness_status=blocked_missing_release_images
```

when the images are absent. That is the correct state until CI/GHCR publishes
both images from the same commit.

## Required QA Once Images Exist

1. Run `scripts/run-operation-milestone-release-readiness.ps1` with both image
   env vars set.
2. Run the Day-1 published-image scenario:

   ```text
   swblock run --env sw_block_image=<release-image> --env sw_block_csi_image=<release-csi-image> testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml
   ```

3. Run the operation close regression:

   ```text
   swblock run testops/scenarios/failback-frontend-workload-close-chain.yaml
   ```

   This currently remains source-gated/local-image unless the Phase95 build path
   is adapted to skip local build/import for already-published images.

4. Verify final cleanup:

   ```text
   cleanup_status=ok
   k8s_residue_count=0
   iscsi_residue_count=0
   multipath_residue_count=0
   process_residue_count=0
   hostpath_residue_count=0
   failure_count=0
   ```

## Next After Release Gate

If the published-image release smoke passes, tag the operation milestone beta
and then start Phase 100:

```text
Kubernetes CSI NVMe multipath attach
  -> grouped NQN/NSID publish context
  -> NodeStage connects all NVMe paths
  -> app pod sees one mounted namespace
  -> cleanup proves no stale NVMe subsystem residue
```
