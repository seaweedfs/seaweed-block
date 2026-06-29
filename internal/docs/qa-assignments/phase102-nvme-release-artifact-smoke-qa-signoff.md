# Phase 102 NVMe Release Artifact Smoke QA Sign-off

Status: BLOCKED on release artifacts.

Validated branch: `phase102-nvme-release-artifact-smoke`.

## Scope

Phase 102 is not new NVMe product behavior. It validates that matching published
images contain the Phase 100/101 NVMe behavior:

```text
published seaweed-block + seaweed-block-csi images
  -> Kubernetes CSI NVMe multipath attach
  -> Phase 101 standalone path-failure status gate
  -> Phase 101 repeated stage/unstage residue gate
  -> Phase 101 bounded soak gate
```

## Local Gate

Passed:

```text
bash -n scripts/run-phase102-nvme-release-artifact-smoke.sh
C:\work\swblock.exe validate testops/scenarios/nvme-release-artifact-smoke-chain.yaml
go test ./scripts ./internal/testops ./core/ops ./core/host/master ./cmd/sw-block ./core/frontend/nvme -count=1
```

## Artifact Availability

By default the gate expects images tagged from the current source checkout:

```text
ghcr.io/seaweedfs/seaweed-block:sha-<current HEAD short SHA>
ghcr.io/seaweedfs/seaweed-block-csi:sha-<current HEAD short SHA>
```

The image tags can also be pinned explicitly with:

```text
SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>
SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>
```

The blocked-path check confirms the gate stops on missing manifests before any
product work:

```text
phase102_nvme_release_artifact_status=blocked_missing_release_images
release_image=ghcr.io/seaweedfs/seaweed-block:sha-<checked commit>
release_csi_image=ghcr.io/seaweedfs/seaweed-block-csi:sha-<checked commit>
release_image_manifest=missing
missing_image=ghcr.io/seaweedfs/seaweed-block:sha-<checked commit>
```

This is an artifact-readiness block, not a product failure.

## D4 Pending

When matching images are published, run:

```text
C:\work\swblock.exe run \
  -env release_image=ghcr.io/seaweedfs/seaweed-block:sha-<commit> \
  -env release_csi_image=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit> \
  testops/scenarios/nvme-release-artifact-smoke-chain.yaml
```

Required PASS keys:

```text
phase102_nvme_release_artifact_status=ok
phase100_nvme_csi_multipath_live_status=ok
phase101_nvme_path_failure_status=ok
phase101_nvme_stage_unstage_status=ok
phase101_nvme_soak_status=ok
phase101_soak_false_ready_count=0
phase101_soak_identity_drift_count=0
```

## Non-Claims

Until D4 passes on published images, docs must not claim that a public release
artifact includes Phase 101 NVMe hardening. Even after D4 passes, Phase 102 does
not claim RoCE, production HA, broad host compatibility, or performance/SLO.
