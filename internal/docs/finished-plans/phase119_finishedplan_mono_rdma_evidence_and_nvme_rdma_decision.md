# Phase 119 Finished Plan: Mono RDMA Evidence And NVMe/RDMA Decision

Status: closed as an evidence and decision phase.

Phase 119 reviewed `C:\work\rdma\seaweed-mono-rdma-refresh` as read-only
evidence for Seaweed Block's future RDMA direction.

## Result

The mono RDMA work is useful, but it does not directly implement block
NVMe/RDMA:

- `enterprise/rust/sw-rdma` proves real Linux RC verbs, memory registration,
  RDMA READ/WRITE, and typed unsupported behavior outside supported builds.
- `sw-rdma-vfs` and related gates prove practical VFS/object acceleration.
- NIXL-shaped object work is CPU descriptor / plugin compatibility evidence,
  not GPU/cuObject or block-device evidence.
- None of those components is a Linux `nvme connect -t rdma` compatible
  NVMe-oF/RDMA target listener.

## Product Decision

Seaweed Block keeps the Phase 118 boundary:

```text
NVMe/TCP: implemented supported-lab path
NVMe/RDMA/RoCE: typed unsupported / public non-claim
```

Before implementing a real NVMe-oF/RDMA target, the next phase should collect
a repeatable NVMe/TCP performance baseline for the supported Kubernetes PVC
path. That became Phase 120.

## Reference

Detailed evidence assessment:

```text
internal/docs/ref/phase119-mono-rdma-acceleration-assessment.md
```

## Non-Claims

This phase did not claim NVMe/RDMA attach, RoCE I/O, SPDK parity,
GPU/cuObject, NIXL production support, performance/SLO, broad distro support,
or published-image support.
