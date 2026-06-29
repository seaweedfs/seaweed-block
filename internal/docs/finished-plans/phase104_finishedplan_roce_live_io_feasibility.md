# Finished Plan: Phase 104 RoCE Live-I/O Feasibility Boundary

Status: complete. Local checks and m02 TestOps gate passed on 2026-06-29.

## Problem

Phase 103 showed that m02 is a RoCE candidate host: RDMA device present and
`nvme-rdma` available. That still does not mean Seaweed Block can serve
NVMe/RDMA. The target implementation is NVMe/TCP only.

Without an explicit refusal, release notes or chart examples could accidentally
turn "host candidate" into "product supports RoCE."

## What Changed

Added:

```text
--nvme-transport
```

to `cmd/blockvolume`:

- default `tcp`;
- only `tcp` is accepted;
- `rdma` fails clearly with:

```text
--nvme-transport="rdma" unsupported; only "tcp" is implemented
```

Added:

```text
scripts/run-phase104-roce-live-io-feasibility-gate.sh
testops/scenarios/roce-live-io-feasibility-chain.yaml
internal/docs/qa-assignments/phase104-roce-live-io-feasibility-qa-signoff.md
```

Updated:

```text
internal/docs/current-plan.md
internal/docs/product-roadmap.md
```

## Verification

Local:

```text
bash -n scripts/run-phase104-roce-live-io-feasibility-gate.sh
C:\work\swblock.exe validate testops\scenarios\roce-live-io-feasibility-chain.yaml
go test ./cmd/blockvolume ./scripts ./internal/testops ./core/frontend/nvme -count=1
git diff --check
```

Live:

```text
C:\work\swblock.exe run testops\scenarios\roce-live-io-feasibility-chain.yaml
run=20260629-002142-9c9e
result=PASS 10/10
```

Key evidence:

```text
target_nvme_transport_supported=tcp
target_nvme_rdma_supported=false
rdma_transport_rejection_test_seen=true
phase104_roce_live_io_result=blocked_target_transport_unsupported
roce_claim_allowed=false
roce_live_io_claim=false
```

## Non-Claims

Phase 104 does not claim:

- RoCE or NVMe/RDMA I/O works;
- Linux host can connect to Seaweed Block with `nvme connect -t rdma`;
- performance or SLO;
- multi-host failover.

## Next

The next phase should choose one path:

- implement a real NVMe/RDMA target/listener;
- continue with multi-host NVMe/TCP non-loopback topology;
- characterize NVMe/TCP performance.
