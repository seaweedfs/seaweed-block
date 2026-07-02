# Current Plan: Phase 119 Mono RDMA Evidence And NVMe/RDMA Decision

Status: planning/evidence import. No block data-plane code changes yet.

Phase 118 added the narrow NVMe target transport seam:

```text
TCP target path: implemented and default
RDMA target path: typed unsupported at target layer
blockvolume --nvme-transport=rdma: still public refusal
```

Phase 119 answers the next question before adding code: can the RDMA work in
`C:\work\rdma\seaweed-mono-rdma-refresh` be reused for Seaweed Block's
NVMe/RDMA path, and what does its performance evidence actually prove?

## Source Under Review

Read-only source:

```text
C:\work\rdma\seaweed-mono-rdma-refresh
branch: rdma/object-protocol-migration
```

Important: that repo currently has unrelated dirty worktree changes. Treat it
as a reference source only unless explicitly working in that repo.

Relevant mono components:

- `enterprise/rust/sw-rdma`: Linux `real-rdma` one-sided RC verbs over
  `rdma_cm`/libibverbs; memory registration, RDMA READ/WRITE, local MR reuse,
  completions, and typed unsupported behavior outside Linux/feature builds.
- `enterprise/rust/sw-rdma-loader`: volume transport and `pipes x slots`
  runtime pool used by VFS/object paths.
- `enterprise/rust/sw-rdma-vfs`: VFS adapter and M01/M02 read/write gates.
- `enterprise/rust/sw-rdma-object`: S3/object RDMA token and `x-rdma-info`
  registered-client path.
- NIXL research path: `type=seaweed` object/storage adapter work, currently
  CPU descriptor focused; no GPU/cuObject production claim.

## What The Mono Evidence Proves

The mono work proves real acceleration in the VFS/object lanes:

- VFS 128 MiB read matrix: HTTP about `87 MiB/s`; native RC about
  `627-646 MiB/s` in the recorded smoke.
- Later VFS 1 GiB sequential read after copy-path fixes: practical release
  ceiling about `1.0-1.1 GiB/s` through the mount.
- S3 registered-client empty-body GET: recorded about `298.5 MiB/s` for
  `20MiB/c32` and `1799.6 MiB/s` for `128MiB/c32`.
- NIXL-shaped CPU smoke and local NIXL `type=seaweed` external-descriptor
  plugin smoke pass, but they are object/storage compatibility proofs, not
  GPU/cuObject or block-device proofs.

The mono work also records an important negative result:

- Old SRA object path is still materially faster: normal-body old SRA around
  `3.3 GiB/s`, sink/status path around `5.5 GiB/s`.
- Current mono object path is correctness-migrated but not performance-parity
  with the old SRA hot path.

## Why This Does Not Directly Implement Block NVMe/RDMA

Seaweed Block's NVMe frontend is an NVMe-oF target surface. A Linux host
initiator expects:

```text
nvme connect -t rdma ...
  -> NVMe-oF/RDMA capsules and queue pairs
  -> controller/session semantics
  -> namespace I/O commands
```

The mono `sw-rdma` path provides:

```text
RDMA READ/WRITE primitives
  -> registered memory descriptors
  -> VFS/object transfer maps
  -> out-of-band control plane for addr/rkey
```

Those primitives are useful engineering substrate and performance evidence, but
they are not an NVMe-oF/RDMA listener. Reusing them for block requires a real
NVMe/RDMA protocol layer or a dependency that already provides one. A plain
`net.Listener`-style adapter is not enough.

## Deliverables

1. Record a mono RDMA evidence assessment for block engineers:

   ```text
   internal/docs/ref/phase119-mono-rdma-acceleration-assessment.md
   ```

2. Keep the Phase 118 public boundary unchanged:

   ```text
   --nvme-transport=rdma remains unsupported
   no RoCE attach claim
   no NVMe/RDMA performance claim
   ```

3. Define the next implementation decision as one of:

   - **Path A:** build or bind a real NVMe-oF/RDMA target protocol layer;
   - **Path B:** prove a concrete blocker and keep RDMA scoped to
     VFS/object/NIXL, while block continues on NVMe/TCP;
   - **Path C:** defer NVMe/RDMA and first run a block NVMe/TCP performance
     baseline so the product has a measured storage-performance reference.

## Gate Commands To Reuse From Mono

If the RDMA lab is available, the relevant mono gates are:

```powershell
# VFS/RustVolume read/write and matrix smoke
cd C:\work\rdma\seaweed-mono-rdma-refresh
.\enterprise\rust\sw-rdma-vfs\tests\lab\rdma-write-gate\run.ps1 `
  -RunV1Acceptance -RunReadMatrixSmoke

# S3 object registered CPU descriptor / NIXL-shaped CPU path
.\enterprise\rust\sw-rdma-object\tests\lab\s3-object-loader-store\run.ps1 `
  -ClientRdmaEndpoint -RegisteredClientGetPerf

# NIXL-shaped CPU and local type=seaweed plugin smoke
.\enterprise\rust\sw-rdma-object\tests\lab\s3-object-loader-store\run.ps1 `
  -NixlCpuSmoke -NixlPluginSmoke
```

These gates answer whether the mono VFS/object RDMA lanes are healthy. They do
not answer whether Seaweed Block has a working NVMe/RDMA initiator attach.

## Exit Criteria

Phase 119 can close when:

- the assessment doc lists the reusable mono components, measured evidence, and
  non-claims;
- roadmap wording separates VFS/object/NIXL acceleration from block
  NVMe/RDMA;
- the next block phase has a concrete implementation choice instead of a vague
  "add RoCE" task.

## Non-Claims

Phase 119 does not claim NVMe/RDMA attach, RoCE I/O, SPDK parity, GPU/cuObject,
NIXL production support, performance/SLO, broad distro compatibility, or
published-image support.
