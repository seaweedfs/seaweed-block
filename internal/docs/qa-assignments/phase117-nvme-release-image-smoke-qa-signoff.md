# Phase 117 NVMe/TCP Published-Image Release Smoke QA

Status: waiting for published image pair.

## Scope

This gate validates the narrow public-image claim for the NVMe/TCP supported-lab
path. It must run against matching immutable published images:

```text
SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>
SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>
```

The gate is intentionally smaller than the source-gated Phase 100-115 matrix. It
proves the representative release artifact path only:

- generated Helm values select `protocol=nvme`;
- chart render enables external NVMe/TCP and stage-2 multipath;
- one RF=2 NVMe/TCP PVC mounts through CSI;
- writer and reader I/O pass;
- `SwBlockVolume.status` is `ready/first_volume_verified`;
- `SwBlockVolume.status.nvme.pathCount=2`;
- cleanup verifier returns zero residue.

## Commands

```bash
SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit> \
SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit> \
bash scripts/run-phase117-nvme-release-image-smoke-gate.sh "$PWD"
```

or through TestOps:

```powershell
C:\work\swblock.exe run `
  -env release_image=ghcr.io/seaweedfs/seaweed-block:sha-<commit> `
  -env release_csi_image=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit> `
  -env release_commit=<commit> `
  testops/scenarios/nvme-tcp-release-image-smoke-chain.yaml
```

## Required Terminal Evidence

```text
phase117_nvme_release_image_smoke_status=ok
image_pair_commit_match=true
helm_values_protocol=nvme
stage2_multipath_enabled=true
writer_verified=true
reader_verified=true
volume_status=ready
volume_reason=first_volume_verified
nvme_path_count=2
cleanup_status=ok
failure_count=0
```

If the images are not yet published, the expected status is
`phase117_nvme_release_image_smoke_status=blocked_missing_release_images`. That
is an artifact blocker, not a product failure.

## Non-Claims

This smoke does not claim RoCE/NVMe-RDMA, performance/SLO, broad host
compatibility, production HA, node-loss survival, backup/restore, or unbounded
path churn. Those remain outside the published-image claim.
