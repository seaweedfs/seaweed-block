# NVMe-oF / ANA Parity Plan

Status: active reference.

Goal: make NVMe-oF a second credible block frontend beside iSCSI. V2 visible
behavior is the minimum height. V3 keeps its own authority, recovery, and
frontend boundaries.

## Product Position

- iSCSI remains the alpha Kubernetes default until NVMe has the same OS,
  multipath, failover, and CSI evidence.
- NVMe-oF is the performance-oriented frontend:
  - lower protocol overhead than iSCSI for some workloads,
  - standard Linux `nvme` tooling,
  - better fit for multi-queue and high-throughput network paths,
  - future RoCE path once TCP correctness is pinned.
- Do not use NVMe to bypass storage truth:
  - protocol code must not decide authority,
  - ANA state must be derived from frontend facts produced by the V3 authority
    and replica lifecycle,
  - stale primary writes must fail closed.

## V2 Reference Points

- V2 advertises NVMe/TCP in-capsule write support:
  - Identify Controller sets `IOCCSZ = (64 + maxDataLen) / 16`,
  - `ICDOFF = 0`,
  - small writes can carry data directly in the command capsule,
  - this avoids an R2T round trip for common 4 KiB writes.
- V2 has ANA support:
  - Identify Controller advertises ANA,
  - Identify Namespace carries an ANA group,
  - Get Log Page ANA returns group state,
  - tests cover optimized / standby / inaccessible behavior.
- V2 has write pressure behavior:
  - `writeWithRetry` retries transient WAL-full errors,
  - WAL pressure can throttle writers before hard failure.
- V2 has benchmark-oriented scenarios:
  - NVMe max-concurrent-write sweeps,
  - NVMe IO queue count sweeps,
  - 10.0.0.x / high-speed network paths,
  - fio-based comparison scenarios.

## Current V3 State

- Present:
  - NVMe/TCP target code under `core/frontend/nvme`,
  - IC handshake and Fabric Connect,
  - Admin and IO queue separation,
  - Identify Controller / Namespace / Namespace Descriptor List,
  - deterministic NGUID / EUI-64,
  - inline payload and R2T / H2C data path,
  - pending capsule buffering during R2T data collection,
  - one outstanding R2T per session,
  - READ / WRITE / FLUSH dispatch to `frontend.Backend`,
  - stale lineage maps to NVMe path-related status,
  - KATO / KeepAlive support,
  - unit and component tests for queue routing, large writes, pipelining, and
    cleanup.
- Deliberately not advertised yet:
  - ANA Identify fields are pinned to zero by tests,
  - ANA Change Notices are not advertised,
  - Dataset Management / Write Zeroes are not advertised.
- Product gaps:
  - no current NVMe alpha install path,
  - no current K8s dynamic PVC path using NVMe,
  - no current Linux `nvme connect -> mkfs -> mount -> fio -> disconnect`
    release gate,
  - no current NVMe multipath mounted failover gate,
  - no RoCE or 25 GbE claim.

## Design Rules

- Standard first:
  - prefer standard NVMe/TCP in-capsule data and R2T/H2C behavior,
  - do not add a custom admin/vendor data path until the standard path is
    measured and found insufficient.
- Advertised means implemented:
  - do not flip ANA Identify bits until Get Log Page ANA and Linux host
    behavior both pass,
  - do not advertise Dataset Management or Write Zeroes before command support
    exists.
- Performance claims require network labels:
  - loopback numbers are correctness evidence only,
  - 1 GbE, 10.0.0.x, 25 GbE, TCP, and RoCE must be reported separately.
- Keep iSCSI and NVMe common where possible:
  - same `frontend.Backend`,
  - same authority facts,
  - same stale-write fence,
  - same CSI volume lifecycle model,
  - protocol-specific transport only at the edge.

## Milestones

### NVMe-P0 Audit And Red Plan

- Goal:
  - pin the V2-to-V3 gap before code changes.
- Tasks:
  - map V2 NVMe files and scenarios to V3 files and tests,
  - confirm what the old "control API carries data" path actually meant:
    standard in-capsule data vs custom admin/vendor shortcut,
  - document which pieces are protocol behavior, which are benchmark knobs,
    and which are V2-specific architecture,
  - create red tests for any missing protocol behavior before implementation.
- Close bar:
  - plan table lists every V2 visible NVMe feature,
  - each item is marked present, missing, intentionally deferred, or rejected
    with product reason.

### NVMe-P1 OS Kernel Baseline

- Goal:
  - prove one real Linux host can use V3 NVMe-oF as a block device.
- Tasks:
  - script: `nvme connect`,
  - discover device,
  - `mkfs.ext4`,
  - mount,
  - write/read checksum,
  - fio 60s,
  - unmount,
  - `nvme disconnect`,
  - no stale sessions or target processes,
  - dynamic ports only.
- #QA:
  - run on M02 first,
  - capture `nvme list`, `nvme id-ctrl`, `nvme id-ns`, fio summary, target
    logs, and cleanup state.
- Close bar:
  - 256 MiB target passes mkfs + fio,
  - no protocol session errors during fio,
  - cleanup leaves no NVMe connection or process residue.

### NVMe-P2 In-Capsule / R2T Performance Path

- Goal:
  - verify the fast small-write path before claiming NVMe performance upside.
- Tasks:
  - assert Identify `IOCCSZ` and `ICDOFF` match the implementation,
  - measure whether Linux sends 4 KiB writes inline or via R2T,
  - add counters/logs for inline writes vs R2T writes,
  - run fio profiles:
    - 4 KiB randwrite, iodepth 1 and 32,
    - 128 KiB randrw,
    - sequential write/read,
  - compare iSCSI vs NVMe on the same backend and same host.
- #design:
  - if V2 used a non-standard admin/vendor command as a data shortcut, decide
    separately whether V3 should port it. Default answer: no for public
    product path; maybe yes for internal benchmark only.
- Close bar:
  - data path classification is visible in artifacts,
  - no performance number is published without network/backend labels.

### NVMe-P3 ANA Identity And Log Page

- Goal:
  - make ANA a real host-visible feature, not only path-related error codes.
- Tasks:
  - add ANA provider based on V3 frontend facts,
  - Identify Controller:
    - set CMIC / ANACAP only after log support lands,
    - set ANAGRPMAX and NANAGRPID,
  - Identify Namespace:
    - set ANAGRPID,
  - Get Log Page ANA:
    - return group count,
    - return group state,
    - return namespace list,
    - increment change count when path state changes,
  - keep stale primary errors mapped to path-related status.
- #QA:
  - run `nvme id-ctrl`, `nvme id-ns`, `nvme get-log -i 0x0c` or equivalent
    host tooling,
  - verify optimized / standby / inaccessible states.
- Close bar:
  - Linux host observes ANA consistently,
  - no Identify field advertises a missing log behavior.

### NVMe-P4 Multipath And Mounted Failover

- Goal:
  - reach iSCSI P6-level behavior for NVMe multipath.
- Tasks:
  - two NVMe/TCP paths to one namespace,
  - common namespace identity,
  - distinct path state,
  - Linux NVMe multipath groups one logical device,
  - mounted ext4 workload survives active path kill,
  - old primary refuses stale writes.
- #QA:
  - mounted workload failover on M02 first,
  - later rerun across m01/M02 if lab wiring is stable.
- Close bar:
  - pre-failover checksum survives,
  - post-failover write succeeds,
  - old primary logs stale-lineage rejection,
  - cleanup leaves no NVMe connections or processes.

### NVMe-P5 CSI Integration

- Goal:
  - allow Kubernetes users to opt into NVMe without changing the app.
- Tasks:
  - StorageClass protocol parameter:
    - default remains iSCSI,
    - `protocol: nvme` selects NVMe path,
  - CSI ControllerPublish / NodeStage carries NVMe address and NQN,
  - CSI node runs `nvme connect` idempotently,
  - NodeUnstage disconnects only the matching NQN/device,
  - transport file records enough state for restart cleanup,
  - no residue after PVC delete.
- #QA:
  - app writer/reader PVC demo with NVMe protocol,
  - K8s fio 60s,
  - attach/detach loop.
- Close bar:
  - same user story as alpha iSCSI demo, protocol switched to NVMe by
    StorageClass only.

### NVMe-P6 RoCE / Network Performance Matrix

- Goal:
  - turn performance from intuition into repeatable data.
- Tasks:
  - run 1 GbE TCP baseline,
  - run 10.0.0.x / 25 GbE TCP path,
  - run RoCE path if supported by lab and host drivers,
  - compare iSCSI vs NVMe,
  - compare walstore vs smartwal,
  - record CPU, latency, bandwidth, IOPS, queue depth, and block size.
- #QA:
  - only run after P1-P5 correctness gates are green.
- Close bar:
  - repeatable matrix with artifact bundle,
  - no "smartwal is faster" claim until the matrix proves it under labelled
    conditions.

## Open Questions

- Did V2 ever use a custom NVMe admin/vendor command for data, or was the
  remembered fast path standard in-capsule data?
- Should V3 expose NVMe alpha before CSI integration, or keep it internal until
  K8s has a user-facing switch?
- Do we want ANA before K8s NVMe, or K8s single-path NVMe before ANA?
- Which lab path is authoritative for performance: loopback, 1 GbE LAN, 10.0.0.x,
  25 GbE, or RoCE?

## Immediate Next Step

- Start NVMe-P0.
- No product code change until the audit table is done.
- First code milestone after P0 should be NVMe-P1 OS kernel baseline, because a
  real Linux host catches wire behavior that in-process tests miss.
