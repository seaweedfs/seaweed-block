# Current Plan: Phase 117 NVMe/TCP Published-Image Release Smoke

Status: gate implemented; waiting for matching published images.

Phase 116 is closed:

```text
Result: docs PASS
Docs:
  README.md
  docs/user-capabilities.md
  docs/releases/README.md
  docs/releases/nvme-tcp-supported-lab.md
```

## Why This Is Next

Phases 100-115 are source-gated and live-lab validated. Phase 116 packaged the
claim boundary for users. The remaining release gap is artifact verification:
the documented NVMe/TCP path must be proven on matching published
`seaweed-block` and `seaweed-block-csi` images before it becomes a
published-image claim.

## Product Goal

Validate that the published image pair contains the same NVMe/TCP behavior that
passed in source-gated lab runs:

- generated Helm values select `protocol=nvme`;
- chart render enables external NVMe/TCP and stage-2 multipath;
- one RF=2 NVMe/TCP PVC mounts into an app pod;
- writer/reader I/O passes;
- `SwBlockVolume.status.nvme.pathCount=2`;
- status is `Ready=True/first_volume_verified`;
- cleanup verifier returns zero residue.

## Inputs Required

Exact matching image tags or digests:

```text
ghcr.io/seaweedfs/seaweed-block:<candidate>
ghcr.io/seaweedfs/seaweed-block-csi:<candidate>
```

Both images must be built from the same source commit. If either image is
missing, mark this phase artifact-blocked, not product-failed.

## Gate

Use a small release-smoke gate rather than rerunning every Phase 100-115 gate:

```text
scripts/run-phase117-nvme-release-image-smoke-gate.sh
testops/scenarios/nvme-tcp-release-image-smoke-chain.yaml
```

If `SW_BLOCK_RELEASE_IMAGE` / `SW_BLOCK_CSI_RELEASE_IMAGE` are not supplied, or
if either manifest is missing, the gate writes
`phase117_nvme_release_image_smoke_status=blocked_missing_release_images`. That
is an artifact-readiness blocker, not a product failure.

Minimum terminal evidence:

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

## Non-Claims

Phase 117 does not add RoCE/NVMe-RDMA, performance/SLO, broad host
compatibility, production HA, node-loss survival, backup/restore, or unbounded
path churn claims. It only verifies that the published image pair can run the
representative supported-lab NVMe/TCP path.
