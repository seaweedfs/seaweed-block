# Current Plan: iSCSI Frontend Completeness

Status: active.

Rule: V2 frontend coverage is the minimum height for V3 unless we explicitly
drop a feature with a product reason. V3 should keep its own architecture, but
the user-visible iSCSI capability should reach and then exceed V2.

References:

- `ref/iscsi-v2-coverage-gap-audit.md`
- `ref/iscsi-os-initiator-compat-plan.md`
- `ref/v2-frontend-protocol-gap-audit.md`

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
  - status: local on `iscsi/frontend-completeness`, pending milestone PR.
  - includes rapid login/logout goroutine budget, concurrent target close
    idempotency, target same-address restart, and NOP-Out queued during
    Data-Out.
- iSCSI-P3 attach/detach loop tooling:
  - status: local on `iscsi/frontend-completeness`, pending milestone PR.
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
  - status: open in PR #40.
  - relevant to K8s cleanup and alpha install flow.

## Current Active Milestone: iSCSI-P2 Stability

- Goal:
  - prevent session breakage, leaks, and unbounded memory behavior under real
    OS-style traffic.
  - turn the recent large I/O debugging class into repeatable local and QA
    tests.

- Tasks:
  - add RX/TX stability test pack:
    - status: done in PR #26.
    - supplemental guards: local on `iscsi/frontend-completeness`, pending
      milestone PR.
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
  - status: next development milestone after P2 close.
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

## Milestone: iSCSI-P4 CHAP / Access Control

- Goal:
  - reach V2-level iSCSI auth behavior before any security-facing claim.

- Tasks:
  - status: planned.
  - CHAP config validation,
  - login success with correct secret,
  - wrong username/password fail closed,
  - missing CHAP fields fail closed,
  - replayed challenge rejected if supported by the protocol path,
  - Kubernetes Secret integration for CSI later.

- Close bar:
  - unauthenticated access fails when CHAP is required,
  - authenticated access succeeds,
  - failed auth leaves no partial session/device state.

- QA/tooling:
  - #QA needs CHAP-capable initiator script once implementation starts.
  - V2 CHAP tests are the reference coverage inventory.

## Milestone: iSCSI-P5 CSI Node Lifecycle

- Goal:
  - make kubelet retry/restart behavior safe enough for real clusters.

- Tasks:
  - status: planned.
  - NodeStage idempotency,
  - NodeUnstage idempotency,
  - login failure cleanup,
  - mkfs failure cleanup,
  - stale session detection,
  - plugin restart fallback,
  - repeated stage/unstage,
  - wrong volume at staging path fails closed.

- Close bar:
  - kubelet retries do not wedge the node plugin,
  - failed attach leaves no leaked device or session,
  - repeated create/delete works without manual host cleanup.

- QA/tooling:
  - #QA needs K8s scenario scripts or TestOps entries.
  - manual kubelet poking is allowed only for first reproduction.

## Milestone: iSCSI-P6 ALUA / MPIO / Mounted Failover

- Goal:
  - make mounted-volume failover a real frontend behavior instead of only a
    reconnect story.

- Tasks:
  - status: planned.
  - ALUA state model:
    - active,
    - standby,
    - unavailable,
    - transitioning.
  - standby metadata commands allowed,
  - standby write/read policy pinned,
  - REPORT TARGET PORT GROUPS,
  - VPD 0x83 target-port identity,
  - state change while I/O is in flight,
  - multipath initiator test,
  - primary failover while mounted,
  - old primary cannot serve stale successful I/O.

- Close bar:
  - real initiator sees correct ALUA/MPIO behavior,
  - mounted workload survives or fails/reconnects according to documented
    policy,
  - byte-equal data proof after failover,
  - no stale-primary success.

- QA/tooling:
  - #QA needs real initiator multipath setup script.
  - #QA needs mounted workload failover script.
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
