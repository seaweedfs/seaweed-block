# Current Plan: Phase 118 NVMe/RDMA Transport Seam

Status: D1 implemented locally; QA gate pending.

Phase 117 remains ready but artifact-blocked until matching published images
exist:

```text
SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>
SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>
```

## Why This Is Next

Phases 100-115 proved the NVMe/TCP supported-lab path. Phase 116 packaged the
claim, and Phase 117 prepared the published-image smoke. The next product
expansion is NVMe/RDMA/RoCE, but the current target is hardwired to TCP and
Phase 104 intentionally rejects `--nvme-transport=rdma`.

The first useful implementation step is not a fake RoCE claim. It is a narrow
target transport seam that:

- preserves the TCP data path unchanged;
- gives the target an explicit transport selector;
- returns a typed unsupported error for RDMA;
- keeps the public blockvolume CLI refusal until a real RDMA listener exists.

## Product Goal

Move from "RDMA is only a command-line refusal" to "the NVMe target has a real
transport boundary where an RDMA listener can be inserted and tested."

This lets the next phase focus on the hard part: mapping an RDMA-capable
listener into the existing NVMe session model without bypassing authority,
readiness, ANA, cleanup, or status.

## Deliverables

Implemented:

```text
core/frontend/nvme/transport.go
core/frontend/nvme/transport_test.go
scripts/run-phase118-nvme-rdma-transport-seam-gate.sh
testops/scenarios/nvme-rdma-transport-seam-chain.yaml
internal/docs/qa-assignments/phase118-nvme-rdma-transport-seam-qa-signoff.md
```

Changed:

```text
core/frontend/nvme/target.go
cmd/blockvolume/main.go
```

## Gate Evidence

Required terminal evidence:

```text
phase118_nvme_rdma_transport_seam_status=ok
go_test_nvme_blockvolume=ok
target_transport_seam_present=true
rdma_target_error_typed=true
blockvolume_rdma_public_refusal=true
rdma_listener_implemented=false
roce_claim_allowed=false
```

## Next After D1

Phase 119 should choose one concrete RDMA implementation path:

- implement a minimal RDMA listener that can satisfy the target listener seam;
- or prove, with code and lab evidence, which dependency blocks that listener
  (`libibverbs`, `rdma-core`, kernel ULP, SPDK/NVMe target reuse, or cgo
  packaging).

Do not add Kubernetes/Helm RoCE flags until a local target listener can accept a
real RDMA connection or emits a precise implementation blocker.

## Non-Claims

Phase 118 does not claim NVMe/RDMA attach, RoCE I/O, performance/SLO, broad
host compatibility, production HA, node-loss survival, backup/restore, or
published-image support.
