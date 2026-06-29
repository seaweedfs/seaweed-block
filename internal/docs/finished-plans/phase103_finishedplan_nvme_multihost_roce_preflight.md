# Finished Plan: Phase 103 NVMe Multi-Host / RoCE Preflight

Status: complete. Local checks and m02 TestOps gate passed on 2026-06-29.

## Problem

Phase 100 and Phase 101 proved the supported-lab NVMe/TCP CSI multipath path.
They did not justify RoCE, NVMe/RDMA, performance, or broad multi-host claims.

The next risk was claim drift: seeing an RDMA-capable host and treating that as
evidence that RoCE I/O works. Phase 103 adds a preflight gate that separates
host candidacy from product capability.

## What Changed

Added:

```text
scripts/run-phase103-nvme-multihost-roce-preflight-gate.sh
testops/scenarios/nvme-multihost-roce-preflight-chain.yaml
internal/docs/qa-assignments/phase103-nvme-multihost-roce-preflight-qa-signoff.md
```

Updated:

```text
internal/docs/current-plan.md
internal/docs/product-roadmap.md
docs/roadmap.md
```

The gate reads host facts only:

- `nvme` CLI presence;
- `nvme list-subsys` readability;
- `nvme-fabrics`, `nvme-tcp`, and `nvme-rdma` loaded/available state;
- RDMA device count from `/sys/class/infiniband`;
- NVMe/TCP preflight readiness;
- RoCE preflight candidacy.

It explicitly emits:

```text
roce_claim_allowed=false
roce_live_io_claim=false
performance_claim_allowed=false
```

## Verification

Local:

```text
bash -n scripts/run-phase103-nvme-multihost-roce-preflight-gate.sh
C:\work\swblock.exe validate testops\scenarios\nvme-multihost-roce-preflight-chain.yaml
go test ./scripts ./internal/testops ./core/frontend/nvme ./core/ops ./cmd/sw-block -count=1
git diff --check
```

Live:

```text
C:\work\swblock.exe run testops\scenarios\nvme-multihost-roce-preflight-chain.yaml
run=20260629-001336-db89
result=PASS 14/14
```

Key live facts:

```text
nvme_tcp_preflight_ready=true
rdma_device_count=1
module_nvme_rdma_available=true
roce_preflight_status=candidate_requires_live_roce_gate
roce_preflight_candidate=true
roce_claim_allowed=false
roce_live_io_claim=false
performance_claim_allowed=false
```

## Non-Claims

Phase 103 does not claim:

- RoCE or NVMe/RDMA I/O works;
- multi-host NVMe failover works;
- throughput, latency, or performance SLO;
- broad distro/kernel compatibility;
- release-image validation.

## Next

Choose one separate next phase:

- RoCE live I/O feasibility on this RDMA-capable host;
- multi-host NVMe/TCP non-loopback topology;
- NVMe performance characterization.
