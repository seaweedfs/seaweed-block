# Current Plan: iSCSI Frontend Completeness

Status: active.

Rule: V2 frontend coverage is the minimum height for V3 unless we explicitly
drop a feature with a product reason. V3 should keep its own architecture, but
the user-visible iSCSI capability should reach and then exceed V2.

References:

- `ref/iscsi-v2-coverage-gap-audit.md`
- `ref/iscsi-os-initiator-compat-plan.md`
- `ref/v2-frontend-protocol-gap-audit.md`
- `ref/iscsi-p6-alua-mpio-design.md`
- `ref/iscsi-alua-technical-note.md`

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

## Current Active Milestone: iSCSI-P6 ALUA / MPIO / Mounted Failover

- Goal:
  - make mounted-volume failover a real frontend behavior instead of only a
    reconnect story.

- Tasks:
  - status: active after P5 QA green.
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
    - status: local protocol slice done.
    - metadata/path probing allowed,
    - READ allowed for initiator path probing,
    - WRITE and SYNCHRONIZE_CACHE fail closed.
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
    - status: pending.
  - frontend state provider wiring:
    - status: local P6-C slice done on `iscsi/csi-node-lifecycle`.
    - connect ALUA provider to current V3 frontend facts without importing
      authority or placement.
    - mapping:
      - frontend Healthy => active optimized,
      - locally healthy but superseded/non-writable => standby,
      - recovering => transitioning,
      - degraded/idle/identity mismatch => unavailable.
    - path identity:
      - NAA is stable per volume,
      - target port group and relative target port are stable per
        volume/replica path.
  - multipath initiator test:
    - status: P6-D active-path OS script prepared.
    - script: `scripts/run-iscsi-alua-os-smoke.sh`.
    - assignment: `internal/docs/qa-assignments/iscsi-p6-alua-mpio-lab-validation.md`.
    - current claim: one active path reports ALUA through real Linux `sg_inq`
      and `sg_rtpg`, then completes mkfs/mount/checksum/logout.
    - non-claim: two-path Linux `multipathd` grouping remains blocked until
      standby path representation is solved.
  - primary failover while mounted:
    - status: pending P6-E.
  - old primary cannot serve stale successful I/O:
    - status: pending P6-D/P6-E.

- Close bar:
  - real initiator sees correct ALUA/MPIO behavior,
  - mounted workload survives or fails/reconnects according to documented
    policy,
  - byte-equal data proof after failover,
  - no stale-primary success.

- QA/tooling:
  - #QA active-path ALUA OS script is ready.
  - #QA needs mounted workload failover script after design is accepted.
  - do not rely on in-process protocol tests only.

## Milestone: iSCSI-P7 Performance And Backend Matrix

- Goal:
  - make performance experiments comparable without turning early numbers into
    product claims.

- Tasks:
  - status: planned.
  - walstore baseline fio,
  - smartwal fio behind explicit flag,
  - RoCE / 10.0.0.x lab path if available,
  - 1GbE vs 25GbE comparison,
  - pgbench scenario,
  - record CPU, memory, latency, bandwidth, and cleanup state.

- Close bar:
  - same test runner scenario can compare backends,
  - no silent backend switch,
  - results are labelled experimental until SLOs exist.

- QA/tooling:
  - #QA needs TestOps scenario that records network, backend, block size, fio job,
    and cleanup state.
  - avoid manual benchmark notes without a repeatable scenario.

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
