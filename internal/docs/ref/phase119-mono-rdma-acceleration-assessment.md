# Phase 119 Mono RDMA Acceleration Assessment

Date: 2026-07-02.

Scope: read the current mono RDMA/VFS/RustVolume/NIXL work and decide what it
means for Seaweed Block's NVMe/RDMA direction.

Source reviewed:

```text
C:\work\rdma\seaweed-mono-rdma-refresh
branch: rdma/object-protocol-migration
HEAD observed during review: a4e3ff82d docs(rdma): refresh nixl object smoke blocker
```

The source repo has dirty changes. This assessment treats it as read-only
evidence and does not modify it.

## Executive Decision

Mono RDMA is valuable, but it is not a drop-in Seaweed Block NVMe/RDMA target.

Use it as:

- an RDMA verbs/registered-memory reference;
- a VFS/RustVolume and S3/object performance evidence source;
- a transfer-planning and `pipes x slots` runtime reference;
- a future GPU/NIXL/object-storage bridge.

Do not use it as:

- proof that `nvme connect -t rdma` works against Seaweed Block;
- a replacement for an NVMe-oF/RDMA capsule/session implementation;
- a performance claim for block PVCs;
- a GPU/cuObject claim.

## Component Inventory

### `sw-rdma`

Path:

```text
enterprise/rust/sw-rdma
```

Purpose:

- Linux-only `real-rdma` feature over `rdma_cm` / libibverbs.
- One-sided RDMA READ and WRITE.
- Local MR registration and reuse.
- Completion polling and typed error surface.
- Non-Linux or non-feature builds return `RdmaError::Unsupported`.

Why it matters for block:

- Good reference for safe RDMA resource ownership, local MR reuse, timeout
  behavior, completion handling, and explicit unsupported errors.
- Could help implement a future block RDMA data mover if Seaweed Block owns its
  own protocol.

Why it is not enough:

- It exchanges `{addr,rkey}` through an out-of-band control plane.
- It does not implement NVMe-oF/RDMA capsules, commands, queue pair semantics,
  or Linux `nvme-rdma` initiator compatibility.

### `sw-rdma-loader`

Path:

```text
enterprise/rust/sw-rdma-loader
```

Purpose:

- Shared volume transport for VFS/object paths.
- Maintains persistent per-pipe workers.
- Uses reusable registered local slots, work-request IDs, send-CQ dispatch,
  and bounded `pipes x slots` admission.
- Exposes runtime stats such as endpoint, pipe count, active/high-water,
  completed, failed, and cancelled.

Why it matters for block:

- Strong design reference for bounded concurrency and runtime observability.
- Useful if a future block data path needs a bulk copy engine outside NVMe.

Why it is not enough:

- It is built around Seaweed volume chunk transfer, not NVMe namespace command
  execution.

### `sw-rdma-vfs`

Path:

```text
enterprise/rust/sw-rdma-vfs
```

Purpose:

- VFS/FUSE/kernel-module adapter over the Rust volume RDMA path.
- Lab gate:

  ```powershell
  .\enterprise\rust\sw-rdma-vfs\tests\lab\rdma-write-gate\run.ps1
  ```

Evidence:

- 128 MiB read matrix smoke recorded HTTP at about `86.96 MiB/s`.
- Native RC read recorded about `646.46 MiB/s` with one pipe and
  `627.45 MiB/s` with four pipes in that run.
- Later 1 GiB sequential read notes show about `1.0-1.1 GiB/s` through the VFS
  mount after copy-path fixes.
- Raw RoCE sanity check in the notes reached about `96 Gb/s`, so the remaining
  bottleneck is VFS/userspace/kernel copy path, not the NIC.

Interpretation:

- RDMA helps VFS materially.
- POSIX/VFS is still copy-heavy; it is a compatibility path, not the maximum
  bandwidth path.

Block relevance:

- Useful comparison for the "kernel mount path has copy costs" problem.
- Does not prove block NVMe/RDMA, because NVMe-oF uses a different host
  initiator and protocol.

### `sw-rdma-object`

Path:

```text
enterprise/rust/sw-rdma-object
```

Purpose:

- S3/object RDMA token and `x-rdma-info` path.
- Registered CPU client descriptor.
- Go S3 control plane to Rust RDMA object service.
- Optional `volume-loader-store` route through the shared loader.

Lab gate:

```powershell
.\enterprise\rust\sw-rdma-object\tests\lab\s3-object-loader-store\run.ps1
```

Useful variants:

```powershell
-ClientRdmaEndpoint
-RegisteredClientGetPerf
-NixlCpuSmoke
-NixlPluginSmoke
-ProxyBodyGet
```

Evidence:

- NIXL-shaped CPU smoke passed: PUT, full GET, range GET, multipart token PUT,
  SHA checks, and negative controls.
- Registered-client GET perf recorded about `298.5 MiB/s` for `20MiB/c32` and
  `1799.6 MiB/s` for `128MiB/c32`.
- Proxy-body GET is correctness-migrated but not final-performance: recorded
  around `128 MiB/s` for `20MiB/c32` and `734 MiB/s` for `128MiB/c32` in one
  later no-hidden-fallback run.
- Old SRA rerun still reached about `3.3 GiB/s` normal-body and about
  `5.5 GiB/s` sink/status path, so mono object performance is not yet parity.

Block relevance:

- Good evidence that registered-client/status-response style APIs can be much
  faster than normal body delivery.
- Relevant to future object/GPU paths.
- Not directly relevant to Linux block PVC attach unless the product exposes a
  separate object/loader API.

### NIXL / cuObject Track

Docs:

```text
docs/RDMA-NIXL-INTEGRATION-PLAN.md
docs/RDMA-NIXL-OBJ-COMPATIBILITY.md
docs/RDMA-QA-RESULT-NIXL-SHAPED-CPU-SMOKE.md
docs/RDMA-QA-RESULT-NIXL-NORMAL-OBJ-SMOKE.md
```

Current state:

- Normal NIXL OBJ client can use Seaweed S3 as a standard object provider.
- Seaweed accepts a `type=seaweed` JSON `x-rdma-info` envelope for CPU
  registered buffers.
- Local NIXL `type=seaweed` plugin smoke passes with external helper-created
  descriptors.
- No in-process NIXL descriptor provider yet.
- No GPU/cuObject opaque descriptor support yet.

Block relevance:

- Strong future track for object/storage acceleration, model files, datasets,
  checkpoints, and warm/cold cache snapshots.
- Not a hot KVCache service.
- Not an NVMe block target.

## What A Real Block NVMe/RDMA Path Needs

Seaweed Block currently exposes NVMe/TCP through the NVMe frontend. Phase 118
added an explicit transport seam, but the public CLI still refuses RDMA.

A real NVMe/RDMA implementation must answer:

1. Can the target speak NVMe-oF/RDMA capsules to the Linux `nvme-rdma`
   initiator?
2. Who owns QP/session lifecycle and teardown?
3. How are NVMe controller, namespace, ANA, and multipath facts projected into
   existing `ManagedVolume` / CRD status?
4. How does path loss surface without false `Ready=True`?
5. How does cleanup remove host NVMe/RDMA state without residue?
6. Can this be packaged without shipping a large privileged native dependency
   that users cannot install?

The mono RDMA primitives answer none of those directly. They can inform the
RDMA resource model, but the protocol layer is still open.

## Candidate Next Paths

### Path A: Build Or Bind NVMe-oF/RDMA Target Protocol

This is the direct block path.

Required work:

- choose implementation route: native Go/cgo, Rust sidecar, SPDK binding, or
  existing Linux target integration;
- make `nvme connect -t rdma` pass against a local target;
- prove namespace read/write correctness;
- integrate with Phase 100-115 NVMe multipath status and cleanup model;
- only then expose Helm/CSI RoCE values.

Risk:

- High. NVMe-oF/RDMA is protocol work, not just RDMA memory copy.

### Path B: Keep RDMA In Object/VFS, Keep Block On NVMe/TCP

This is the conservative product path.

Required work:

- document that RDMA acceleration is currently SeaweedFS VFS/object/NIXL lane;
- continue block NVMe/TCP hardening and performance baselines;
- revisit NVMe/RDMA when a target protocol dependency is selected.

Risk:

- Lower product risk, but delays RoCE block-device support.

### Path C: Run Block NVMe/TCP Performance Baseline First

This gives a storage-performance baseline before investing in NVMe/RDMA.

Required work:

- run supported-lab NVMe/TCP sequential/random read/write matrix;
- compare against iSCSI and the mono VFS/object numbers without claiming the
  paths are equivalent;
- identify whether block's bottleneck is frontend protocol, WAL/backend,
  filesystem, K8s attach, or target implementation.

Risk:

- Medium. It may show block needs backend/IO-path work before RDMA transport
  matters.

## Recommendation

Take Path C first unless there is a hard requirement to implement RoCE now.

Reason:

- Phase 118 already prevents a false RoCE claim.
- Mono shows RDMA can accelerate VFS/object, but it also shows copy-path and
  algorithm shape dominate performance.
- Seaweed Block should measure NVMe/TCP before assuming RDMA transport is the
  biggest bottleneck.
- If NVMe/TCP is already limited by backend/WAL/userspace copies, adding
  NVMe/RDMA will not fix the right layer.

If Path C shows network transport is the bottleneck, start Path A with a real
NVMe-oF/RDMA implementation spike. If it does not, keep NVMe/RDMA behind the
typed unsupported seam and spend the next storage phase on the measured
bottleneck.

## Suggested Phase 120

Name:

```text
Phase 120: NVMe/TCP Performance Baseline Before RoCE
```

Exit criteria:

- one supported-lab PVC over NVMe/TCP;
- sequential read/write and random-ish small I/O matrix;
- iSCSI comparison where feasible;
- cleanup zero-residue;
- status/report/dashboard surfaces include protocol and target facts;
- explicit decision: frontend transport bottleneck, backend bottleneck, or
  inconclusive.

Non-claims:

- no RoCE;
- no NVMe/RDMA attach;
- no production SLO;
- no broad kernel/distro compatibility.
