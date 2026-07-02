# Phase 118 NVMe/RDMA Transport Seam QA

Status: pending QA.

## Scope

Phase 118 starts the NVMe/RDMA/RoCE implementation track without making a false
RoCE claim. The deliverable is the first code seam inside the NVMe target:

- `core/frontend/nvme.Target` now has a transport selector;
- empty transport remains TCP and preserves existing behavior;
- RDMA returns a typed unsupported error at the target layer;
- `cmd/blockvolume --nvme-transport=rdma` still refuses publicly, so users
  cannot accidentally claim RoCE support before a real listener exists.

This phase is not a live RoCE I/O proof.

## Gate

```bash
bash scripts/run-phase118-nvme-rdma-transport-seam-gate.sh "$PWD"
```

or:

```powershell
C:\work\swblock.exe run testops/scenarios/nvme-rdma-transport-seam-chain.yaml
```

## Required Evidence

```text
phase118_nvme_rdma_transport_seam_status=ok
go_test_nvme_blockvolume=ok
target_transport_seam_present=true
rdma_target_error_typed=true
blockvolume_rdma_public_refusal=true
rdma_listener_implemented=false
roce_claim_allowed=false
```

## Non-Claims

Phase 118 does not claim NVMe/RDMA attach, RoCE I/O, performance, production
HA, or published-image support. The next implementation step must either add a
real RDMA listener or produce a concrete blocker from the RDMA library/kernel
integration work.
