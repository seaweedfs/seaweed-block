# Current Plan: Frontend Completeness

Status: active.

Rule: V2 frontend coverage is the minimum height for V3 unless we explicitly
drop a feature with a product reason. V3 should keep its own architecture, but
the user-visible frontend capability should reach and then exceed V2.

References:

- `ref/iscsi-v2-coverage-gap-audit.md`
- `ref/iscsi-os-initiator-compat-plan.md`
- `ref/v2-frontend-protocol-gap-audit.md`
- `ref/iscsi-csi-alua-review-guide.md`
- `ref/iscsi-p6-alua-mpio-design.md`
- `ref/iscsi-alua-technical-note.md`
- `ref/nvme-ana-parity-plan.md`
- `ref/nvme-v2-coverage-gap-audit.md`
- `ref/nvme-ana-technical-note.md`
- `ref/nvme-p4-multipath-failover-design.md`

## Product Goal

- Make iSCSI a credible Kubernetes block frontend, not only a smoke-test path.
- Support real OS initiators, filesystem workloads, stress, auth, lifecycle,
  and eventually multipath/failover.
- Keep protocol code separate from authority and replica readiness decisions.
- Use V2 tests as the coverage inventory, not as code to blindly copy.

## Completed Baseline

- iSCSI-P1 OS initiator correctness:
  - status: done in PR #24.
  - includes Data-Out collector, pending command handling during Data-Out,
    Data-Out timeout, and Linux OS initiator evidence.
- Large READ Data-In splitting:
  - status: done in PR #25.
  - includes residual direction handling.
- iSCSI-P2 first stability pack:
  - status: done in PR #26.
  - includes RX/TX stability tests, large write memory tests, OS smoke script,
    loop mode, stress mode, TestOps registry entry, and sustained sync smoke.
- iSCSI-P2 supplemental session guards:
  - status: done in PR #41.
  - includes rapid login/logout goroutine budget, concurrent target close
    idempotency, target same-address restart, and NOP-Out queued during
    Data-Out.
- iSCSI-P3 attach/detach loop tooling:
  - status: done in PR #41.
  - includes `scripts/run-k8s-attach-detach-loop.sh` and TestOps registry
    scenario `iscsi-p3-attach-detach-loop`.
- iSCSI-P2/P3 lab validation:
  - status: QA green on `iscsi/frontend-completeness@e7c95ee`.
  - OS repeat, OS fio, K8s fio, and K8s attach/detach all passed on M02.
  - evidence: `internal/docs/qa-assignments/iscsi-p2-p3-lab-validation.md`.
- iSCSI smoke harness cleanup:
  - status: done in PR #27.
- Larger alpha PVC smoke:
  - status: done in PR #28.
- Operational iSCSI target knobs:
  - status: done in PR #36.
  - includes portal address / externally advertised target configuration.
- PVC metadata and owner-reference plumbing:
  - status: done in PR #39.
  - relevant to CSI/K8s cleanup, not core iSCSI protocol.
- Owner-reference alpha default:
  - status: done in PR #40.
  - relevant to K8s cleanup and alpha install flow.
- iSCSI-P4 CHAP / Access Control:
  - status: done in PR #41.
  - includes target-side CHAP, CSI node CHAP, Kubernetes Secret wiring, and
    QA evidence.

## Recently Closed Milestone: iSCSI-P2 Stability

- Goal:
  - prevent session breakage, leaks, and unbounded memory behavior under real
    OS-style traffic.
  - turn the recent large I/O debugging class into repeatable local and QA
    tests.

- Tasks:
  - add RX/TX stability test pack:
    - status: done in PR #26.
    - supplemental guards: done in PR #41.
    - rapid login/logout without goroutine leak,
    - many concurrent sessions,
    - target close while I/O is active,
    - double close without panic,
    - NOP-Out during Data-Out,
    - error response StatSN behavior.
  - add large write memory-pressure tests:
    - status: done in PR #26.
    - repeated 4 MiB writes,
    - slow backend,
    - heap/goroutine growth guard.
  - extend real OS harness:
    - status: done in PR #26.
    - `SW_BLOCK_ISCSI_ITERATIONS=N`,
    - `SW_BLOCK_ISCSI_STRESS=dd|fio`,
    - artifact path printed,
    - no active sessions after every loop.
  - #QA run K8s validation after local tests:
    - status: QA green on `iscsi/frontend-completeness@e7c95ee`.
    - larger PVC smoke: done in PR #28.
    - 60s fio: QA PASS on M02.
    - larger PVC,
    - 60s fio,
    - daemon logs and iSCSI state captured,
    - no K8s residue.
  - compare against V2 remaining coverage:
    - status: active.
    - review whether PR #26 fully covers V2 `qa_rxtx_test.go` and
      `large_write_mem_test.go` intent.
    - add missing tests only if the mapping is incomplete.

- Close bar:
  - `go test ./core/frontend/iscsi -count=1` green,
  - OS initiator repeat harness green on M02,
  - K8s larger PVC or fio smoke green,
  - no session leaks,
  - no unbounded buffer or memory behavior.

- #QA assignment:
  - `internal/docs/qa-assignments/iscsi-p2-p3-lab-validation.md`.
  - status: QA green.

## Milestone: iSCSI-P3 Product-Backed Stability

- Goal:
  - prove the protocol path works with real `blockvolume`, WAL, CSI, and K8s
    behavior, not only protocol fakes.

- Tasks:
  - status: done in PR #41.
  - sustained write/read through mounted filesystem,
  - `SYNCHRONIZE_CACHE` pressure,
  - multiple sessions sharing a volume if supported,
  - reconnect after logout/login,
  - #QA repeated attach/detach loop,
    - script prepared: `scripts/run-k8s-attach-detach-loop.sh`,
    - TestOps scenario prepared: `iscsi-p3-attach-detach-loop`,
  - app writer pod replaced by reader pod on the same PVC,
  - blockvolume restart and reattach once durable state is ready.

- Close bar:
  - checksum passes through pod path,
  - no session errors,
  - cleanup leaves no iSCSI or K8s residue,
  - product logs are enough to diagnose failures.

- QA/tooling:
  - repeated attach/detach script exists:
    `scripts/run-k8s-attach-detach-loop.sh`.
  - TestOps registry entry exists:
    `internal/testops/registry/iscsi-p3-attach-detach-loop.json`.
  - default loop count comes from `SW_BLOCK_ATTACH_DETACH_ITERATIONS`.
  - #QA status: PASS on M02, 3 iterations.

## Recently Closed Milestone: iSCSI-P4 CHAP / Access Control

- Goal:
  - reach V2-level iSCSI auth behavior before any security-facing claim.

- Tasks:
  - status: done in PR #41.
  - target-side CHAP login negotiation:
    - status: done in PR #41.
    - direct LoginOp is rejected when CHAP is required,
    - `AuthMethod=None` is rejected when CHAP is required,
    - target emits CHAP MD5 challenge,
    - correct username/response advances to LoginOp,
    - wrong response fails closed,
    - missing `CHAP_R` fails closed.
  - `cmd/blockvolume` opt-in flags:
    - status: done in PR #41.
    - `--iscsi-chap-username`,
    - `--iscsi-chap-secret`,
    - flags require `--iscsi-listen`,
    - username and secret must be set together.
  - OS initiator CHAP smoke script:
    - status: QA green.
    - configure `iscsiadm` node auth before login,
    - prove correct secret succeeds and wrong secret fails without residue.
  - Kubernetes / CSI Secret integration:
    - status: QA green on M02 at `9a1fe07`.
    - CSI node consumes CHAP credentials from `NodeStageVolumeRequest.Secrets`,
    - controller publish path must not copy CHAP secrets into `publish_context`,
    - node configures `iscsiadm` CHAP settings after discovery and before login.
    - launcher can render target-side CHAP env vars from a Kubernetes Secret,
    - alpha runner can create the Secret and inject StorageClass
      node-stage secret refs.
  - replayed challenge rejected if supported by the protocol path,

- Close bar:
  - unauthenticated access fails when CHAP is required,
  - authenticated access succeeds,
  - failed auth leaves no partial session/device state.

- QA/tooling:
  - #QA assignment:
    `internal/docs/qa-assignments/iscsi-p4-chap-lab-validation.md`.
  - #QA assignment:
    `internal/docs/qa-assignments/iscsi-p4-k8s-chap-validation.md`.
  - #QA status:
    - K8s CHAP dynamic PVC PASS on M02.
    - default non-CHAP regression PASS on M02.
  - V2 CHAP tests are the reference coverage inventory.

## Recently Closed Milestone: iSCSI-P5 CSI Node Lifecycle

- Goal:
  - make kubelet retry/restart behavior safe enough for real clusters.

- Tasks:
  - status: QA green on `iscsi/csi-node-lifecycle@4ee35c0`.
  - local CSI node lifecycle guards:
    - status: done in PR #41.
  - NodeStage idempotency,
    - mounted staging path must belong to the same volume,
    - mounted staging path for another volume fails closed.
  - NodeUnstage idempotency,
    - unmounted staging path still logs out and removes local state.
  - login failure cleanup,
    - login failure does not record staged state.
  - mkfs failure cleanup,
    - already covered: successful login is logged out when mount fails.
  - stale session detection,
    - existing iSCSI login without staged volume identity fails closed,
    - plugin restart may reuse an existing login only when `.volume` matches.
  - plugin restart fallback,
    - existing transport-file fallback covers unstage after restart.
    - NodeStage restart identity is covered by `.volume`.
  - repeated stage/unstage,
    - local 3-cycle stage/unstage test leaves no staged state,
      `.volume`, or `.transport`.
  - wrong volume at staging path fails closed.
  - #QA CSI node restart while PVC remains,
    - script prepared: `scripts/run-k8s-csi-node-restart.sh`,
    - TestOps scenario prepared: `iscsi-p5-csi-node-restart`,
    - assignment: `internal/docs/qa-assignments/iscsi-p5-csi-node-lifecycle-validation.md`,
    - status: QA PASS on M02.

- Close bar:
  - kubelet retries do not wedge the node plugin,
  - failed attach leaves no leaked device or session,
  - repeated create/delete works without manual host cleanup.

- QA/tooling:
  - #QA status: PASS on M02.
  - manual kubelet poking is allowed only for first reproduction.

## Recently Closed Milestone: iSCSI-P6 ALUA / MPIO / Mounted Failover

- Goal:
  - make mounted-volume failover a real frontend behavior instead of only a
    reconnect story.

- Tasks:
  - status: completed in PR #42 scope; QA green on
    `iscsi/csi-node-lifecycle@d1025f1`.
  - #design(iscsi-p6-alua-mpio-design) ALUA/MPIO policy and protocol shape:
    - owner: dev.
    - output: `internal/docs/ref/iscsi-p6-alua-mpio-design.md`.
    - must be reviewed before changing protocol behavior.
  - #design(iscsi-p6-qa-assignment) real initiator validation shape:
    - owner: dev.
    - output: `internal/docs/qa-assignments/iscsi-p6-alua-mpio-lab-validation.md`.
    - must define the lab command, non-claims, and pass/fail criteria.
  - V2 coverage alignment:
    - status: design inventory added.
    - do not copy V2 role/state ownership,
    - do match V2 externally visible ALUA/MPIO protocol coverage unless V3
      explicitly rejects a behavior.
  - ALUA state model:
    - status: local protocol slice done on `iscsi/csi-node-lifecycle`.
    - active optimized,
    - active non-optimized,
    - standby,
    - unavailable,
    - transitioning.
  - standby command policy:
    - status: local protocol slice done; tightened by PR #42 review fix.
    - metadata/path probing allowed,
    - data READ, WRITE, and SYNCHRONIZE_CACHE fail closed on non-active
      paths.
  - standard INQUIRY TPGS discipline:
    - status: local protocol slice done.
    - TPGS stays off until REPORT TARGET PORT GROUPS and ALUA VPD identity
      are implemented,
    - when enabled, advertise implicit ALUA only unless explicit transitions
      are implemented.
  - REPORT TARGET PORT GROUPS:
    - status: local protocol slice done.
    - no-provider rejection,
    - short allocation truncation,
    - five-state reporting.
  - VPD 0x83 target-port identity:
    - status: local protocol slice done.
    - NAA stable per volume,
    - target port group and relative target port distinguish paths,
    - short allocation length and no-ALUA branches tested.
  - VPD 0x00 remains advertised-pages-equal-implemented-pages:
    - status: local protocol slice done.
  - state change while I/O is in flight:
    - status: local protocol state-change test done.
  - concurrent REPORT TARGET PORT GROUPS and standby write reject tests:
    - status: done in PR #43.
  - frontend state provider wiring:
    - status: local P6-C slice done on `iscsi/csi-node-lifecycle`.
    - connect ALUA provider to current V3 frontend facts without importing
      authority or placement.
    - mapping:
      - frontend Healthy => active optimized,
      - locally healthy but superseded/non-writable => standby,
      - recovering => transitioning,
      - idle supporting path => standby for metadata/path probing,
      - degraded/identity mismatch => unavailable.
    - path identity:
      - NAA is stable per volume,
      - target port group and relative target port are stable per
        volume/replica path.
  - multipath initiator test:
    - status: QA green on `iscsi/csi-node-lifecycle@88e9301`.
    - script: `scripts/run-iscsi-alua-os-smoke.sh`.
    - script: `scripts/run-iscsi-alua-multipath-smoke.sh`.
    - assignment: `internal/docs/qa-assignments/iscsi-p6-alua-mpio-lab-validation.md`.
    - current claim: one active path reports ALUA through real Linux `sg_inq`
      and `sg_rtpg`, then completes mkfs/mount/checksum/logout.
    - current two-path claim: two iSCSI portals for one volume can be logged
      in by Linux, report active/standby ALUA state, reject standby WRITE,
      and appear as one logical device in `multipath -ll`.
    - standby/probe session prerequisite: local P6-D slice implemented.
      Non-active ALUA paths may use a borrowed metadata backend after
      `Provider.Open` returns not-ready, so Linux can probe INQUIRY/VPD/RTPG
      without allowing data I/O.
    - #QA Test 1B PASS on M02:
      - artifact:
        `/mnt/smb/work/share/g15d-k8s/20260506T093732Z-iscsi-p6-alua-mpath-fix`.
      - evidence: two iSCSI paths, common NAA, distinct TPG/RTP, r1
        active/optimized, r2 standby, standby WRITE rejected, `multipath -ll`
        grouped both paths under `mpatha`.
    - non-claim: mounted workload failover still needs P6-E.
  - primary failover while mounted:
    - status: QA green on `iscsi/csi-node-lifecycle@d1025f1`.
    - script: `scripts/run-iscsi-alua-mounted-failover-smoke.sh`.
    - assignment: `internal/docs/qa-assignments/iscsi-p6-alua-mpio-lab-validation.md`.
    - verified claim: mounted Linux multipath device can read a
      pre-failover checksum and write a post-failover checksum after r1 is
      killed and r2 reaches `Healthy=true` at a newer epoch.
    - #QA Test 2 PASS on M02:
      - artifact:
        `/mnt/smb/work/share/g15d-k8s/20260506T094503Z-iscsi-p6-mounted-failover`.
      - evidence: `/dev/mapper/mpatha` mounted, pre-failover checksum read
        after failover, post-failover checksum written and verified, r2
        promoted to `Epoch=2`, old r1 gate-rejected stale writes/syncs, no
        active sessions or multipath residue after cleanup.
  - old primary cannot serve stale successful I/O:
    - status: QA green for killed-old-primary path on `d1025f1`.
    - old-primary-return proof remains future soak/fault coverage, not required
      for P6 alpha close.

- Close bar:
  - real initiator sees correct ALUA/MPIO behavior,
  - mounted workload survives or fails/reconnects according to documented
    policy,
  - byte-equal data proof after failover,
  - no stale-primary success.

- QA/tooling:
  - #QA active-path ALUA OS script is ready.
  - #QA two-path multipath script is green on M02.
  - #QA mounted multipath failover script is green on M02.
  - do not rely on in-process protocol tests only.

## Recently Closed Milestone: iSCSI-P7 Performance And Backend Matrix

- Goal:
  - make performance experiments comparable without turning early numbers into
    product claims.

- Tasks:
  - status: QA green on `iscsi/p7-performance-matrix-clean@6826139`.
  - walstore baseline fio,
  - smartwal fio behind explicit flag,
  - Linux iSCSI loopback matrix,
  - record fio summary and cleanup state.
  - deferred:
    - RoCE / 10.0.0.x lab path if available,
    - 1GbE vs 25GbE comparison,
    - pgbench scenario,
    - CPU, memory, latency, and bandwidth sweep.
  - script: `scripts/run-iscsi-backend-fio-matrix.sh`.
  - assignment:
    `internal/docs/qa-assignments/iscsi-p7-backend-fio-matrix-validation.md`.
  - #QA PASS on M02:
    - artifact:
      `/mnt/smb/work/share/g15d-k8s/20260506T215457Z-iscsi-p7-backend-fio`.
    - fio profile: 4 KiB randrw, psync, iodepth=1, size=128m,
      runtime=60s.
    - walstore: PASS, about 124 read IOPS / 124 write IOPS.
    - smartwal: PASS, about 124 read IOPS / 125 write IOPS.
    - cleanup: no active iSCSI sessions and no blockmaster/blockvolume
      processes.
    - non-claim: single-host loopback only, not a product performance claim.

- Close bar:
  - same test runner scenario can compare backends,
  - no silent backend switch,
  - results are labelled experimental until SLOs exist.

- QA/tooling:
  - #QA run the backend matrix on M02 first.
  - #QA if RoCE is available, rerun with explicit 10.0.0.x portal path and
    record the network path in the report.
  - avoid manual benchmark notes without a repeatable scenario.

## Recently Closed Milestone: iSCSI-P8 Compatibility And Soak

- Goal:
  - turn the current alpha iSCSI feature set into repeatable compatibility and
    soak evidence without making performance claims.
  - keep frontend protocol behavior stable while K8s and backend tests get
    longer and more varied.

- Tasks:
  - local ALUA concurrency guard:
    - status: done on `iscsi/frontend-hardening`.
    - cover concurrent REPORT TARGET PORT GROUPS while data READ/WRITE on a
      standby path is rejected.
    - closes the P6 pending item for concurrent RTPG plus standby reject.
  - OS initiator soak:
    - #QA green on `iscsi/frontend-hardening@38ff850`.
    - repeat `run-iscsi-os-smoke.sh` with fio for a longer runtime.
    - record session errors, fio summary, goroutine/process cleanup, and
      final `iscsiadm -m session`.
    - artifact:
      `/mnt/smb/work/share/g15d-k8s/20260506T223240Z-iscsi-p8-soak-38ff850`.
    - evidence:
      - 2 iterations,
      - 120s fio per iteration,
      - `iscsiadm mkfs mount write/read logout` PASS,
      - no active sessions after final cleanup.
  - K8s CSI soak:
    - #QA green on `iscsi/frontend-hardening@38ff850`.
    - repeat attach/detach and fio paths with explicit iteration/runtime
      values.
    - record whether launcher owner-reference cleanup remains clean.
    - evidence:
      - `[alpha-fio] PASS`,
      - `[attach-loop] PASS: 3 attach/detach app PVC cycles completed`,
      - no sw-block PVC or deployment residue.
  - compatibility matrix:
    - #QA green for M02.
    - document exact host distro, kernel, open-iscsi version, fio version,
      and sg3-utils/multipath versions when used.
    - add more hosts only when the first soak is repeatable.

- Close bar:
  - local protocol concurrency tests green,
  - one OS fio soak green,
  - one K8s attach/detach or fio soak green,
  - all runs leave no active sessions, mounts, multipath maps, or K8s residue.

- QA/tooling:
  - prefer wrappers with env knobs over manual command sequences.
  - label all runtime/throughput numbers as soak evidence, not benchmark
    claims.
  - #QA assignment:
    `internal/docs/qa-assignments/iscsi-p8-compat-soak-validation.md`.
  - #QA status:
    - PASS on M02 at `38ff850`.
    - final line:
      `[iscsi-soak] PASS: compatibility soak completed`.
    - non-claim:
      compatibility probe only, not a long-running soak or performance claim.

## Current Active Milestone: TestOps For Frontend Lab Gates

- Goal:
  - reduce false failures from manual multi-step lab runs by moving P8-style
    gates into one TestOps entry point.
  - keep existing shell scripts as the execution backend first; do not rewrite
    all harness logic before the gate is stable.

- Tasks:
  - #QA package P8 compatibility soak as a TestOps scenario:
    - status: done; wrapper proven green in standalone runner and carried back
      into the product planning flow.
    - must record commit SHA, command, artifact root, step result, final line,
      and cleanup status in a single result file.
    - initial scenario may call `scripts/run-iscsi-compat-soak.sh`.
  - pin alpha image build/import before K8s frontend gates:
    - status: QA green at `c3a6e28`; workload composition hook active in this
      branch.
    - script: `scripts/build-alpha-images.sh`.
    - TestOps scenario: `alpha-images-pin-build`.
    - contract:
      - build `sw-block:local` and `sw-block-csi:local`,
      - optionally import both images into k3s containerd with
        `SW_BLOCK_IMPORT_K3S=1`,
      - record Docker image IDs,
      - record `blockmaster`, `blockvolume`, and `blockcsi` `--version`
        output,
      - downstream K8s harnesses may consume the build output with
        `SW_BLOCK_ALPHA_IMAGES_ENV=/path/to/pin-build/alpha-images.env`,
      - fail before protocol smoke tests if build/import/version capture fails.
    - composed TestOps workload scenarios:
      - `nvme-p5-csi-dynamic`,
      - `nvme-p5-default-iscsi-regression`.
    - reason:
      - NVMe-P5 showed that stale k3s images can mimic product protocol bugs.
        The build/import step must be one reviewed gate, not manual lab memory.
  - define the minimal result contract:
    - scenario name,
    - repository SHA,
    - host,
    - step table,
    - artifact paths,
    - cleanup checks.
  - keep product development unblocked:
    - TestOps work should not change iSCSI protocol code.
    - product branches can continue using scripts until TestOps reaches parity.

- Close bar:
  - one command runs the P8 full lab gate,
  - old-commit/stale-binary ambiguity is impossible or explicitly reported,
  - result file is enough for review without reading raw terminal output.

- QA/tooling:
  - #QA assignment should be written before implementation.
  - prefer small TestOps wrapper over a wholesale scenario DSL rewrite.

## Current Dev Milestone: NVMe-oF / ANA Parity Planning

- Goal:
  - bring NVMe-oF up to the same product discipline as the now-green iSCSI
    frontend.
  - treat V2 NVMe behavior as the feature floor, not code to copy blindly.
  - understand the old high-performance path before touching protocol code.

- Reference:
  - `internal/docs/ref/nvme-ana-parity-plan.md`.
  - `internal/docs/ref/nvme-v2-coverage-gap-audit.md`.

- Tasks:
  - NVMe-P0 audit:
    - status: initial audit done; keep updating as code lands.
    - compare V2 NVMe implementation and scenarios against current V3.
    - classify every visible feature as present, missing, intentionally
      deferred, or rejected with product reason.
    - specifically answer whether the remembered "control API carries data"
      path was standard NVMe/TCP in-capsule data or a custom V2 shortcut.
    - initial answer: standard in-capsule data; no custom V2 vendor/admin data
      command found in the NVMe package.
  - NVMe-P1 OS kernel baseline:
    - status: QA green on M02.
    - script: `scripts/run-nvme-os-smoke.sh`.
    - #QA assignment:
      `internal/docs/qa-assignments/nvme-p1-os-smoke-validation.md`.
    - #QA evidence:
      - basic OS path PASS on M02,
      - 60s fio PASS on M02,
      - loopback 4 KiB fio used inline writes only,
      - no test NQN or process residue after cleanup.
    - build a repeatable `nvme connect -> mkfs -> mount -> fio/checksum ->
      disconnect` script.
    - dynamic ports only.
    - no stale sessions or target processes.
  - NVMe-P2 in-capsule / R2T performance path:
    - status: QA green on M02 with host-specific classification.
    - target now reports transport counters in `blockvolume.log` on close:
      inline writes, R2T writes, H2C/C2H PDU counts, and read/write/flush
      command counts.
    - P1 observation: Linux `fio --bs=4k` used inline writes exclusively on
      M02 (`r2t_writes=0`).
    - P2 observation: Linux kernel 6.17.0-19 on M02 did not trigger R2T even
      for `fio --bs=128k` or `dd bs=1M`; all writes were fragmented into
      inline/in-capsule transfers.
    - #QA assignment:
      `internal/docs/qa-assignments/nvme-p2-inline-r2t-validation.md`.
    - prove whether Linux uses inline data for small writes.
    - run 128 KiB+ profiles to force or classify the R2T path.
    - add visible counters or artifacts for inline vs R2T writes.
    - follow-up:
      - R2T requires a different initiator/profile or target-side test knob;
        current M02 kernel behavior is not a product failure.
    - compare iSCSI and NVMe only under labelled network/backend conditions.
  - NVMe-P3 ANA identity and log page:
    - status: QA green on M02 at `d330e89`.
    - provider: `core/frontend/nvme.ANAProvider`.
    - product wiring: `cmd/blockvolume` derives ANA state from the same
      frontend projection used by iSCSI ALUA.
    - log page: admin Get Log Page `0x0c` returns one ANA group when a
      provider is configured.
    - mapping:
      - frontend Healthy => optimized,
      - superseded healthy / idle supporting path => non-optimized,
      - recovering => ANA change,
      - degraded / identity mismatch => inaccessible.
    - guard: Identify Controller / Namespace ANA fields remain zero without a
      provider.
    - P3-C behavior: with a provider, Identify Controller advertises ANA,
      Identify Namespace carries the provider's ANA group, and Get Log Page ANA
      reports the same group/state.
    - OAES ANA Change Notice remains off; no async event producer exists yet.
    - #QA assignment:
      `internal/docs/qa-assignments/nvme-p3-ana-log-validation.md`.
    - #QA evidence:
      - ANA log `group_id=1`, `state=0x01`, `nsid=1`,
      - Identify Controller `cmic=0x8`, `anagrpmax=1`, `nanagrpid=1`,
      - Identify Namespace `anagrpid=1`,
      - no `nvme_parse_ana_log` kernel warning,
      - mkfs/mount/checksum PASS after ANA advertisement is enabled.
  - NVMe-P4 multipath and mounted failover:
    - status: fully QA green on M02 at `e1e0e0c`.
    - #design:
      `internal/docs/ref/nvme-p4-multipath-failover-design.md`.
    - #QA assignment:
      `internal/docs/qa-assignments/nvme-p4-multipath-lab-validation.md`.
    - reach the iSCSI P6 bar for NVMe multipath.
    - Test 1/2 discovery and native grouping:
      - status: QA green on M02 at `a5ef1a5`.
      - script: `scripts/run-nvme-multipath-smoke.sh`.
      - evidence:
        - run ID `20260507T161800Z-test`,
        - two NVMe/TCP paths registered immediately,
        - native Linux multipath exposed one namespace device,
        - ANA log `group_id=1`, `state=0x01 optimized`, `nsid=1`,
        - identity `nguid=24634c35194743419febbb18e06446be`,
          `eui64=24634c3519474341`, `anagrpid=1`,
        - final line:
          `[nvme-mpath] PASS: two NVMe/TCP paths expose one ANA-aware namespace`.
      - decision: single ANA group is sufficient for the current two-path
        native multipath identity model.
    - mounted failover:
      - status: QA green on M02 at `e1e0e0c`.
      - script: `scripts/run-nvme-mounted-failover-smoke.sh`.
      - local guard:
        - metadata-only standby NVMe path continues to reject I/O before
          promotion,
        - after ANA state becomes optimized, the same session can pass I/O to
          the backend.
      - #QA evidence:
        - run ID `20260507T170000Z-nvme-p4-mounted-failover`,
        - two TCP paths registered and Linux native multipath merged them to
          `/dev/nvme1n1`,
        - mounted ext4 workload survived active r1 kill,
        - r2 promoted to `Epoch=2`, `AuthorityRole=primary`,
          `FrontendPrimaryReady=true`,
        - `pre.bin` checksum remained OK after failover,
        - `post.bin` write/read/verify succeeded after failover,
        - cleanup left no test NQN and no blockmaster/blockvolume process.
  - NVMe-P5 CSI integration:
    - status: QA green on `frontend/nvme-ana-parity-plan@8e0a28f`.
    - latest QA:
      - red at `622fae7`: StorageClass rendered `protocol: nvme`, but
        lifecycle intent had no protocol and generated blockvolume still used
        iSCSI args.
      - red at `a1d5201`: live StorageClass carried both protocol keys, but
        persisted lifecycle JSON still had no `protocol`; current source has
        protocol on CSI/RPC/lifecycle, so the harness now gates component
        `--version` output to catch stale K8s images before protocol checks.
      - green at `69a1d20`: Test 1 NVMe dynamic PVC passed after rebuilt and
        k3s-imported images; lifecycle persisted `protocol: "nvme"` and
        launcher emitted NVMe args only.
      - green at `8e0a28f`: Test 2 default iSCSI regression passed; all three
        version gates matched HEAD, lifecycle persisted `protocol: "iscsi"`,
        generated manifest used iSCSI args only, and cleanup left no iSCSI or
        NVMe residue.
      - current fix: prefer product-scoped
        `sw-block.seaweedfs.com/protocol`, keep `protocol` as compatibility,
        delete stale cluster-scoped StorageClass before apply, capture
        `storageclass.live.yaml`, and record `--version` for blockmaster,
        blockcsi, and generated blockvolume before judging launcher output.
    - allow StorageClass protocol selection without changing the app.
    - default StorageClass path stays iSCSI.
    - `parameters.sw-block.seaweedfs.com/protocol: nvme` selects NVMe target
      facts end-to-end:
      - CSI CreateVolume records protocol in lifecycle intent,
      - master lifecycle RPC carries protocol,
      - launcher renders `blockvolume` with `--nvme-listen`,
        `--nvme-subsysnqn`, and `--nvme-ns`,
      - CSI ControllerPublish returns `protocol=nvme`, `nvmeAddr`, and `nqn`,
      - CSI NodeStage uses `nvme connect`, formats/mounts the NVMe namespace,
        and disconnects by NQN on NodeUnstage.
    - K8s harness:
      - script: `scripts/run-k8s-alpha-nvme.sh`,
      - underlying env knob: `SW_BLOCK_FRONTEND_PROTOCOL=nvme`,
      - dynamic PVC/app manifest remains unchanged except injected
        StorageClass parameter.
    - #QA assignment:
      `internal/docs/qa-assignments/nvme-p5-csi-protocol-selection-validation.md`.
    - platform lesson:
      - stale k3s images cost multiple lab rounds; TestOps pin-build/import
        should make build, k3s image import, digest capture, and component
        version verification one required step for future release gates.
  - NVMe-P6 RoCE / network performance matrix:
    - status: planned.
    - only after correctness gates are green.

- Close bar for the planning slice:
  - audit table exists,
  - next code task has a red test or lab reproduction,
  - no ANA or performance claim is enabled before matching host evidence.

- QA/tooling:
  - #QA starts with:
    `internal/docs/qa-assignments/nvme-p1-os-smoke-validation.md`.
  - TestOps wrapper work can proceed independently on P8 soak while dev works
    on NVMe-P0/P1.

## Cross-Cutting Technical Rules

- Protocol code must not decide authority.
- Protocol code must not decide replica readiness.
- CSI must consume frontend facts, not infer storage truth.
- Placement intent is not authority.
- Authority movement is not data continuity.
- Best-effort ACK is not full durability.
- If V2 behavior is copied, copy the test expectation first and adapt the code
  to V3 boundaries.

## QA / TestOps Rules

- Prefer scripts or TestOps scenarios over manual command sequences.
- Manual testing is allowed for first reproduction, but convert the result into
  a repeatable script or scenario.
- QA report must include:
  - branch and commit,
  - command,
  - lab host,
  - result,
  - artifact path,
  - cleanup state,
  - exact log line for any failure.

## PR Cadence

- Use milestone PRs, not one PR per tiny fix.
- Target one or two PRs per day at most.
- For the current active milestone, keep iSCSI-P2 local tests and required
  protocol fixes in one coherent PR if possible.
- Split OS/K8s harness work only if it becomes too large to review cleanly.

## Finish Action

- When a milestone closes, move the finished plan and related references into a
  dedicated directory, for example `internal/docs/finished/iscsi-p2/`.
- Keep `current-plan.md` focused on the next active milestone.
