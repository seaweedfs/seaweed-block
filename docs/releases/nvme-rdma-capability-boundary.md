# NVMe/RDMA Capability Boundary

Status: **non-claim**. Seaweed Block's implemented product path is still
NVMe/TCP for the supported lab. NVMe/RDMA and RoCE remain future work until a
real product transport and live I/O gates pass.

## Current Implemented Path

The current Block NVMe path is:

```text
Kubernetes PVC
-> CSI publish context
-> blockvolume NVMe/TCP frontend
-> Linux nvme connect -t tcp
-> mounted app pod write/read
```

This is documented in the NVMe/TCP supported-lab evidence. It does not imply
RDMA.

## Host Capability Inputs

Host facts are necessary but not sufficient for a product claim:

- `nvme` CLI is present and can read subsystem state.
- `nvme-fabrics` and `nvme-tcp` are loaded or available for the current TCP
  product path.
- `nvme-rdma` loaded or available makes a host a candidate, not a claim.
- `/sys/class/infiniband` device count and device names identify possible RDMA
  hardware.
- RoCE IP/GID/device configuration must be captured as evidence before any
  RDMA live I/O gate.

These facts can only say "this host might be able to run RDMA." They do not say
Seaweed Block can serve an NVMe/RDMA volume.

## Volume-Server Capability Inputs

A future Block NVMe/RDMA claim needs product-owned volume-server facts, not only
host facts:

- supported frontend transports: `tcp` versus `rdma`;
- RDMA listener state: listening/not listening, bind IP, port, device, and GID;
- advertised NQN/NSID for the RDMA listener;
- status endpoint evidence that the volume server selected RDMA intentionally;
- clear fallback/refusal when RDMA is requested but unsupported.

Today the product boundary is still typed refusal: TCP is implemented and RDMA
is not.

## Product Gap

The missing product work is a real NVMe-oF/RDMA target path:

- no RDMA listener implementation in `blockvolume`;
- no product-owned RDMA queue pair / memory registration path for NVMe-oF;
- no successful `nvme connect -t rdma` against a Seaweed Block target;
- no CSI publish context for RDMA frontend addresses;
- no Kubernetes NodeStage/NodeUnstage RDMA attach cleanup gate;
- no status/report/dashboard/explain surface for RDMA target health;
- no same-shape TCP versus RDMA performance evidence.

External RDMA work under `C:\work\rdma\seaweed-mono-rdma-refresh` is useful
evidence for VFS/object/RustVolume acceleration and library feasibility, but it
is not a Seaweed Block NVMe-oF/RDMA target and must not be described as one.

## Required Product Gates Before Claim

The minimum gates before any NVMe/RDMA claim are:

1. Standalone live I/O gate:
   `blockvolume --nvme-transport rdma` starts a real RDMA listener,
   Linux `nvme connect -t rdma` succeeds, write/read verifies, and cleanup
   leaves no NVMe/RDMA residue.
2. Kubernetes publish/attach gate:
   dynamic PVC publishes RDMA frontend context, CSI NodeStage connects through
   `nvme connect -t rdma`, app pod writes/reads, delete/uninstall cleanup is
   clean.
3. Status surface gate:
   CRD/report/dashboard/explain show transport=`rdma`, endpoint/device evidence,
   and failure reasons without false `Ready=True`.
4. Fallback/refusal gate:
   unsupported or misconfigured RDMA must fail closed with a stable reason and
   leave TCP behavior unchanged.
5. Performance gate:
   only after correctness gates pass, compare same-shape TCP and RDMA runs with
   product-owned counters. Until then, no acceleration or SLO claim.

## Phase 157 Decision

Phase 157 keeps the current public boundary:

```text
NVMe/TCP: source-gated supported-lab product path.
NVMe/RDMA/RoCE: host capability and external-library evidence only; product non-claim.
```

The next useful product step is a read-only capability probe that exposes the
volume server's current frontend transport support and RDMA refusal/fallback
facts before any RDMA data path is implemented.
