# NVMe V2 Coverage Gap Audit

Status: initial P0 audit.

Purpose: compare the mature V2 NVMe frontend behavior with the current V3
implementation. This is a product-facing coverage audit, not a port request.

Rule: copy expectations first, not code. If V3 rejects a V2 behavior, the audit
must say why.

## 2026-05-09 Refresh: Current Classification

The audit below was written before the NVMe protocol-readiness phase completed.
Current state:

- NVMe OS smoke, inline/R2T classification, ANA Identify/Get Log Page, native
  multipath discovery, mounted failover, and CSI dynamic PVC protocol selection
  are release-gated.
- The Linux kernel accepted the ANA-aware, multi-controller, shared-namespace
  shape after the controller ID, CMIC, NMIC, standby metadata, and script parser
  fixes.
- The remaining NVMe work is not basic protocol enablement. It is beta
  hardening:
  - broader kernel/distro compatibility,
  - labelled performance/queue profiles,
  - backend pressure behavior,
  - advanced optional commands such as DSM/Write Zeroes,
  - keeping CSI lifecycle component tests protocol-neutral.

Updated classifications:

| Area | Current classification | Next action |
|---|---|---|
| IC / Fabric Connect / queue separation | Present and host-gated. | Keep component coverage. |
| ANA Identify + ANA log | Release-gated. | Keep field-level P3/P4 assertions. |
| Multipath mounted failover | Release-gated. | Keep P4 chain periodic. |
| CSI NVMe path | Release-gated. | Keep P5 chain and component propagation tests. |
| In-capsule / R2T | Inline path host-gated; R2T not triggered by current Linux profile. | Keep explicit "not triggered" classification; use alternate initiator or target mode before claiming R2T evidence. |
| WAL pressure / write retry | Active backend concern; target-side retry intentionally rejected. | Define storage-engine pressure contract. |
| Performance sweeps | Not beta-complete. | Add later after correctness and lifecycle hardening. |

## Historical Summary

- V3 already has a substantial NVMe/TCP protocol core:
  - IC handshake,
  - Fabric Connect,
  - admin / IO queue separation,
  - Identify,
  - inline write payload,
  - R2T / H2C data collection,
  - interleaved Capsule Command buffering,
  - READ / WRITE / FLUSH,
  - KATO / KeepAlive,
  - error mapping for stale-lineage and closed backend.
- The largest product gaps are not the basic wire parser. They are:
  - real Linux OS gate,
  - ANA advertisement + Get Log Page ANA,
  - NVMe multipath mounted failover,
  - CSI NodeStage / NodeUnstage NVMe path,
  - labelled performance matrix.
- The remembered V2 high-performance path appears to be standard NVMe/TCP
  in-capsule data, not a custom vendor/admin command:
  - V2 Identify advertises `IOCCSZ`,
  - V2 sets `ICDOFF=0`,
  - small write data can ride in the Capsule Command payload,
  - no custom vendor/admin data command was found in the V2 NVMe package.

## Coverage Table

| Area | V2 behavior | V3 state | Classification | Next action |
|---|---|---|---|---|
| IC handshake | NVMe/TCP ICReq / ICResp with MaxH2CDataLength. | Present in `core/frontend/nvme/session.go`. | Present. | P1 OS gate should verify with Linux `nvme connect`. |
| Fabric Connect | Admin queue Connect allocates CNTLID; IO queue Connect validates CNTLID / NQN. | Present with CNTLID and identity tests. | Present. | P1 OS gate. |
| Queue separation | Admin and IO queues are separate TCP sessions. | Present; tests cover wrong queue opcodes and parallel queues. | Present. | Add host evidence. |
| Number of Queues | V2 grants requested queues up to max. | Present in `admin_features.go`, capped. | Present. | Host fio with `nr_io_queues` profiles later. |
| Identify Controller | V2 advertises broader capabilities including ANA and DSM/WriteZeroes. | V3 intentionally advertises only implemented features; ANA is advertised when an `ANAProvider` is wired, while DSM/WriteZeroes remain zero. | ANA present behind provider; optional commands deferred. | Keep ANA field-level tests; defer DSM/WriteZeroes to later storage features. |
| Identify Namespace | V2 includes namespace identity and ANA group. | V3 includes deterministic NGUID/EUI-64 and advertises ANAGRPID when an `ANAProvider` is wired. | ANA namespace identity present behind provider. | Keep Identify/log group consistency tests. |
| Namespace Descriptor List | V2 has NGUID/EUI style identity. | V3 has deterministic NGUID/EUI and tests. | Present. | P1 host `nvme id-ns` capture. |
| In-capsule write data | V2 advertises `IOCCSZ` and `ICDOFF=0`; small writes can carry payload inline. | V3 advertises `IOCCSZ`, `ICDOFF=0`, and parser accepts inline payload. | Present but not host-measured. | P2 add inline-vs-R2T counters and fio evidence. |
| R2T / H2C write data | V2 supports R2T and chunked H2C. | V3 supports one outstanding R2T per session and buffers interleaved capsules. | Present in component tests. | P1/P2 host large write. |
| Large READ C2H splitting | V2 supports chunked C2H responses. | V3 has C2HData response path and large read tests. | Probably present. | P1/P2 host fio read evidence. |
| FLUSH | V2 supports Flush to durable backend. | V3 dispatches Flush to `Backend.Sync`; test exists. | Present. | P1 fio/fsync evidence. |
| KeepAlive / KATO | V2 accepts KATO / KeepAlive. | V3 stores KATO and responds KeepAlive; no fatal timer. | Present. | P1 host connect stability. |
| Async Event Request | V2 parks AER. | V3 parks one AER, rejects a second slot, Phase 127 completes the pending AER on ANA change-count advancement when an ANA provider is wired, and Phase 128 proves a real Linux host observes `NVME_AEN=0x0c0302`. | ANA Change source/component and live host evidence present. | Keep periodic host gate; next gap is Kubernetes reconnect/restage ownership. |
| Get Log Page ANA | V2 returns ANA log page. | V3 serves ANA Get Log Page when an `ANAProvider` is wired and rejects it when absent. | Present behind provider. | Keep P3/P4 component and host assertions. |
| ANA Identify advertisement | V2 advertises ANA. | V3 advertises ANA Identify fields only when the ANA provider is present, and keeps fields zero without one. | Present behind provider. | Keep no-provider and with-provider tests. |
| ANA state provider | V2 maps role to ANA optimized / standby / inaccessible. | V3 blockvolume has a projection-backed ANA provider that maps primary/standby/recovering/fault states to optimized, non-optimized, ANA-change, and inaccessible states. | Present as projection seam. | Next gap is carrying multi-path Kubernetes facts through CSI, not inventing another state model. |
| Multipath failover | V2 has NVMe failover scenarios. | V3 has direct-host P4 TestOps coverage for ANA-aware multipath and mounted failover outside Kubernetes. | Release-gated outside K8s. | Add Kubernetes CSI NVMe multipath attach as the next product gate. |
| Target-side write retry | V2 retries transient WAL-full writes in target. | V3 test `t2_v2port_nvme_no_retry_test.go` explicitly rejects target-side retry. | Intentionally not ported. | Keep rejected unless product decision changes. |
| WAL pressure throttle | V2 throttles on WAL pressure near frontend. | V3 storage layer has WAL admission/throttle; NVMe target-side throttle is not carried. | Architecture difference. | Performance/soak should observe backend pressure, not hide it in protocol. |
| Dataset Management / Trim | V2 advertises DSM/Trim. | V3 does not advertise DSM. | Deferred. | Later storage feature, not frontend-complete blocker. |
| Write Zeroes | V2 advertises Write Zeroes. | V3 does not advertise Write Zeroes. | Deferred. | Later storage feature. |
| OS nvme-cli smoke | V2 has `nvme_connect` TestOps actions and scenarios. | V3 has `scripts/iterate-m01-nvme.sh`, but no release-grade P1 script. | Partial. | P1 create repeatable OS smoke. |
| NVMe soak | V2 has `cp103-soak-nvme-1h.yaml`. | V3 no equivalent product gate. | Missing. | After P1. |
| IO queue performance sweeps | V2 has IOQ and max-concurrent-write sweeps. | V3 has component queue tests but no product matrix. | Missing as product evidence. | P2/P6. |
| NVMe CSI publish target | V2 TestOps can target NVMe. | V3 `ControlStatusLookup` maps NVMe frontend facts and CSI can select `protocol=nvme`. | Present for single-path NVMe. | Extend publish context to grouped multi-path NVMe facts. |
| NVMe CSI NodeStage | V2 not directly comparable. | V3 NodeStage/NodeUnstage use the NVMe utility path for `protocol=nvme` and have component coverage. | Present for single-path NVMe. | Add multi-path native NVMe connect/stage coverage. |
| NVMe auth | V2 public evidence unclear. | V3 has no NVMe authentication story. | Deferred. | Not in current frontend-complete scope. |
| RoCE / high-speed network | V2 scenarios reference 10.0.0.x and NVMe/RoCE-oriented performance paths. | V3 has no labelled RoCE matrix. | Missing evidence. | P6 after correctness. |

## Current Code Map

- V3 protocol core:
  - `core/frontend/nvme/session.go`
  - `core/frontend/nvme/identify.go`
  - `core/frontend/nvme/admin_features.go`
  - `core/frontend/nvme/fabric.go`
  - `core/frontend/nvme/io.go`
  - `core/frontend/nvme/target.go`
- V3 command wiring:
  - `cmd/blockvolume/main.go` exposes `--nvme-listen`, `--nvme-subsysnqn`,
    and `--nvme-ns`.
- V3 CSI partial wiring:
  - `core/csi/backend.go` has `ProtocolNVMe`,
  - `core/csi/master_backend.go` maps NVMe status frontends,
  - `core/csi/node.go` does not stage NVMe devices yet.
- V2 references:
  - `weed/storage/blockvol/nvme/identify.go`,
  - `weed/storage/blockvol/nvme/admin.go`,
  - `weed/storage/blockvol/nvme/write_retry.go`,
  - `weed/storage/blockvol/testrunner/actions/nvme.go`,
  - `weed/storage/blockvol/testrunner/scenarios/internal/cp103-nvme-*.yaml`,
  - `weed/storage/blockvol/testrunner/scenarios/internal/ha-nvme-failover.yaml`.

## P0 Decisions

- In-capsule data:
  - treat as standard NVMe/TCP behavior,
  - keep it in public product path,
  - measure it before claiming performance.
- Custom admin/vendor data path:
  - no evidence found in V2 NVMe package during this audit,
  - do not invent one for V3 without a separate design.
- Target-side retry:
  - keep rejected for V3,
  - host/kernel retry and storage-layer backpressure are the intended owners,
  - protocol target must surface real backend errors.
- ANA:
  - must be implemented as a complete Identify + Get Log Page + host-observed
    state set,
  - do not partially advertise.
- CSI:
  - do not claim NVMe K8s support until NodeStage/NodeUnstage use `nvme`
    tooling and pass the same app demo as iSCSI.

## Next Red Tests / Harnesses

- NVMe-P1 OS smoke:
  - build blockmaster and blockvolume fresh,
  - start one NVMe target with dynamic control and transport ports,
  - `nvme connect`,
  - find device by NQN,
  - `mkfs.ext4`,
  - mount,
  - checksum write/read,
  - fio 60s,
  - unmount and `nvme disconnect`,
  - assert no `nvme list-subsys` entry for the NQN and no target process.
- NVMe-P2 data-path classification:
  - add debug counters or artifact grep for inline write vs R2T write,
  - run 4 KiB write fio and confirm which path Linux uses,
  - run large write fio and confirm R2T/H2C remains stable.
- NVMe-P3/P4 ANA regression:
  - keep component tests proving ANA Identify and Get Log Page move together,
  - keep direct-host P4 runner coverage for ANA-aware multipath and mounted
    failover,
  - do not claim Kubernetes NVMe multipath from P4 alone.
- NVMe-P5/P6 Kubernetes NVMe multipath:
  - master status exposes more than one NVMe frontend path for one volume,
  - CSI publish context preserves one NQN/NSID plus all addresses,
  - NodeStage connects all paths under native NVMe multipath,
  - app writer/reader verifies one mounted namespace,
  - cleanup asserts no stale `nvme list-subsys` residue.

## Open Risks

- Linux `nvme` host behavior may differ from the Go client tests, especially
  around IO queue count, in-capsule writes, and reconnect.
- ANA correctness requires host tooling and kernel interpretation, not only
  unit tests.
- Performance work can easily pollute correctness work. Keep P1-P4 correctness
  gates separate from P6 benchmark claims.
