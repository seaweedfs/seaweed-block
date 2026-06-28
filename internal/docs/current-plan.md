# Current Plan: Phase 102 NVMe Release Artifact Smoke

Status: active plan; gate scaffold implemented, waiting for matching published
images to run the full release-artifact validation.

## Why This Is Next

Phase 101 closed the source/lab NVMe hardening slice:

```text
Phase 100 Kubernetes CSI NVMe multipath attach
  -> Phase 101 status/path-loss/stage-unstage/bounded-soak hardening
```

Those gates prove the code and lab behavior. They do not prove that published
`seaweed-block` and `seaweed-block-csi` images contain the same behavior. The
project has repeatedly found chart/image skew and stale-image failures, so the
next step is a release-artifact smoke before any public NVMe claim is widened.

## Product Goal

Turn the source-gated NVMe claim into a release-artifact-gated claim:

```text
matching published images
  -> Kubernetes CSI NVMe multipath attach still works
  -> image-extracted blockmaster/blockvolume/sw-block binaries pass Phase 101
     standalone hardening gates
  -> release wording remains narrow and honest
```

## Image Selection

By default, the gate expects both images from the current source commit:

```text
ghcr.io/seaweedfs/seaweed-block:sha-<HEAD>
ghcr.io/seaweedfs/seaweed-block-csi:sha-<HEAD>
```

The tags can be overridden explicitly:

```text
SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>
SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>
```

If either manifest is missing, the gate must report
`phase102_nvme_release_artifact_status=blocked_missing_release_images` and must
not be treated as product failure.

## D1: Gate Scaffold

Status: implemented.

Files:

```text
scripts/run-phase102-nvme-release-artifact-smoke.sh
testops/scenarios/nvme-release-artifact-smoke-chain.yaml
testops/suites/nvme-release-artifact-smoke.yaml
```

The script:

- pulls both published images;
- extracts `blockmaster`, `blockvolume`, and `sw-block` from the published
  `seaweed-block` image;
- writes a release `alpha-images.env` using the published image pair;
- runs the Phase 100 Kubernetes NVMe CSI multipath live gate against the
  published images;
- runs Phase 101 path-failure, stage/unstage, and bounded-soak gates using the
  extracted release binaries.

## D2: Local Contract Validation

Required checks:

```text
bash -n scripts/run-phase102-nvme-release-artifact-smoke.sh
C:\work\swblock.exe validate testops/scenarios/nvme-release-artifact-smoke-chain.yaml
go test ./scripts ./internal/testops ./core/ops ./core/host/master ./cmd/sw-block ./core/frontend/nvme -count=1
```

Success means the scaffold is syntactically valid and does not regress the
Phase 101 code/test packages.

## D3: Missing-Image Blocked Check

Run the script before the matching images are published and assert:

```text
phase102_nvme_release_artifact_status=blocked_missing_release_images
missing_image=ghcr.io/seaweedfs/seaweed-block:sha-<HEAD>
```

This proves the gate fails closed when release artifacts are not available.

## D4: Published-Image Smoke

Run once matching images exist:

```text
swblock run \
  -env release_image=ghcr.io/seaweedfs/seaweed-block:sha-<commit> \
  -env release_csi_image=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit> \
  testops/scenarios/nvme-release-artifact-smoke-chain.yaml
```

Required PASS evidence:

```text
phase102_nvme_release_artifact_status=ok
phase100_nvme_csi_multipath_live_status=ok
phase101_nvme_path_failure_status=ok
phase101_nvme_stage_unstage_status=ok
phase101_nvme_soak_status=ok
phase101_soak_false_ready_count=0
phase101_soak_identity_drift_count=0
```

## D5: Release Wording

Only after D4 PASS may docs say the published release artifacts include:

- supported-lab NVMe/TCP CSI multipath attach;
- NVMe path status/identity projection;
- one-path-loss negative-first status;
- repeated stage/unstage zero-residue gate;
- bounded writer/reader soak.

Docs must still say:

- no RoCE or NVMe/RDMA claim;
- no performance/latency/throughput/SLO claim;
- no broad kernel/distro compatibility claim;
- no production HA or transparent Kubernetes node-loss failover claim.

## Non-Claims

Phase 102 is not a new NVMe feature. It is release-artifact validation for
Phases 100 and 101.
