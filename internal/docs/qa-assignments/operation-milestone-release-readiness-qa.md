# QA Assignment: Operation Milestone Release Readiness

Status: waiting for published images.

## Purpose

Validate that the operation-layer milestone can be released as a beta-quality
artifact before Phase 100 starts the Kubernetes CSI NVMe multipath work.

This gate must run against the two published images from the same commit:

```text
SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>
SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>
```

Do not use old images such as `sha-dc2972d0059b`, and do not mark release PASS
from `sw-block:local`.

## G1: Readiness Gate

Run:

```powershell
$env:SW_BLOCK_RELEASE_IMAGE="ghcr.io/seaweedfs/seaweed-block:sha-<commit>"
$env:SW_BLOCK_CSI_RELEASE_IMAGE="ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>"
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-operation-milestone-release-readiness.ps1
```

Required:

```text
operation_milestone_release_readiness_status=ok
docker_manifest_release_image=ok
docker_manifest_release_csi_image=ok
go_test_release_scope=ok
helm_lint=ok
helm_template_operation_components=ok
validate_day1_published_image_scenario=ok
validate_phase98_operation_close_scenario=ok
git_diff_check=ok
```

If image env vars are missing, the expected result is:

```text
operation_milestone_release_readiness_status=blocked_missing_release_images
```

That is a release-engineering blocker, not a product failure.

## G2: Published-Image Day-1 Smoke

Run:

```text
swblock run \
  --env sw_block_image=<release-image> \
  --env sw_block_csi_image=<release-csi-image> \
  testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml
```

Required:

```text
helm_install_stack PASS
first_volume_status=ok
writer_verified=true
reader_verified=true
cleanup_status=ok
```

This proves the shipped images can install, create the first PVC, and verify
writer/reader data.

## G3: Operation Close Regression

Run:

```text
swblock run testops/scenarios/failback-frontend-workload-close-chain.yaml
```

Required:

```text
phase98_failback_frontend_workload_close_status=ok
executor_status_failed_back=true
frontend_published=true
frontend_publication_failback_started=false
post_failback_publication_writer_verified=true
post_failback_publication_reader_verified=true
cleanup_status=ok
```

Note: this gate currently uses the Phase95 local build/import path. It remains a
required regression for the operation claim, but it is not yet a pure
published-image gate unless that script is adapted to skip local builds.

## G4: Release Verdict

PASS only if:

- G1 passes against published images.
- G2 passes against published images.
- G3 passes as the operation close regression.
- Final cleanup is zero-residue.
- README/release notes do not claim NVMe multipath, production HA, automatic
  cleanup, backup/restore, or automatic failback.

FAIL if:

- published image install fails;
- writer/reader fails;
- CRD/status/finalizer ownership is broken;
- operation close regression fails;
- cleanup residue remains.

PARTIAL only for clearly external blockers, such as images not published or lab
unavailable.
