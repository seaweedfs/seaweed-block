# Current Plan: iSCSI OS-Initiator Compatibility Closure

Status: closed. Started after closing
`finished-plans/phase5_finishedplan_read_only_operations_status_report.md` on
2026-05-11. Closed after Linux and Windows OS initiator validation on
2026-05-11.

Archive target after closure:
`finished-plans/phase6_finishedplan_iscsi_os_initiator_compatibility.md`.

## Goal

Close the highest-risk alpha credibility gap: real OS iSCSI initiators must be
able to format, mount, write, read, and cleanly detach a meaningful Seaweed
Block volume without `DID_BAD_TARGET`, `I/O error`, dangling sessions, or hidden
harness-only success.

This plan is intentionally narrow. It is about iSCSI correctness with real
initiators. It is not a performance claim, production HA claim, multi-node
claim, NVMe claim, or operator-completeness claim.

## Why This Is Next

`product-management-plan.md` currently says:

```text
Do not start new feature work until the iSCSI OS-initiator compatibility issue
is verified.
```

The user-visible failure is severe: if a normal Linux or Windows initiator
cannot format or write a volume reliably, the alpha PVC/demo story is not
trustworthy even if component tests and protocol gates pass.

This contributes to the roadmap:

- Track B: iSCSI frontend stability.
- Alpha Stabilization: real OS initiator compatibility and larger filesystem
  writes.
- Beta Foundation: durable restart and failover only matter after the basic
  attached filesystem path is credible.

## Current Known Risk

Earlier findings point at possible iSCSI execution gaps:

- large write / mkfs compatibility has been active hardening,
- V2 has reference behavior for iSCSI execution paths,
- possible V3 gaps include:
  - multi Data-In read splitting,
  - CmdSN window and pending queue behavior,
  - Data-Out timeout handling,
  - larger write burst/R2T behavior,
  - OS initiator cleanup and reattach behavior.

Do not assume the problem is fixed because component tests pass. The gate must
use a real OS initiator.

## Delivery Gate

This plan is complete when:

1. A Linux OS initiator gate formats or mounts an iSCSI Seaweed Block volume,
   writes and reads a meaningful payload, verifies checksum, disconnects, and
   leaves no iSCSI sessions or V3 processes.
2. A Windows OS initiator check is either:
   - validated with explicit evidence, or
   - documented as deferred with a precise non-claim and QA assignment.
3. If the OS gate fails, the failure is reduced to one or more fast component
   or protocol tests before another long integration loop is added.
4. V2 comparison is documented only where it changes the fix:
   - what behavior V2 has,
   - what V3 lacks,
   - what test proves the port/fix.
5. The final evidence bundle includes:
   - initiator logs,
   - target logs,
   - dmesg or Windows event evidence when applicable,
   - fio or checksum output,
   - session cleanup proof,
   - product and runner commit provenance.

## Workstream A: Evidence Baseline

Purpose: determine whether the current branch already passes the real initiator
path.

Status: Linux/m02 baseline is green at product commit `9e8ffab`.

Evidence:

- Runner command:
  `swblock run testops/scenarios/iscsi-os-initiator-compat-chain.yaml`.
- Run ID: `20260511-014714-eca5`.
- Result: `PASS` in `1m13s`, `22/22` actions passed.
- Workload: one Linux open-iscsi attach, `mkfs.ext4`, mount, checksum
  write/read, `fio` randrw for 60 seconds against a 256 MiB target.
- Kernel device: `/dev/sda`, 65,536 4 KiB logical blocks.
- `fio.iter1.log`: `err= 0`, read/write completed for the full 60 seconds.
- `sha256-check.iter1.log`: payload checksum verified.
- `iscsi-sessions.after.txt`: `iscsiadm: No active sessions.`
- Process cleanup: `assert_no_processes` passed for `blockmaster` and
  `blockvolume`.
- `dmesg.new.txt`: only new attach, ext4 mount/unmount, cache sync, and ALUA
  detach lines; no `DID_BAD_TARGET`, `I/O error`, `Buffer I/O`, or rejecting
  I/O.

Harness note:

- Commit `eaba13c` added a dmesg-delta gate but compared the before/after
  files by common prefix, which misclassified old rotated kernel messages as
  new.
- Commit `8e220e5` fixed the gate by using dmesg timestamps and reran green.
- Commit `0da42ff` added target-only support for external initiator
  validation and reran the Linux gate green.
- Commit `9e8ffab` added `target-ready.env` for QA orchestration and reran
  the Linux gate green.

Tasks:

- Identify the current best Linux iSCSI OS smoke scenario and command.
- Run it from TestOps when possible so result/provenance is structured.
- Capture exact failure if it fails:
  - initiator error,
  - target-side log,
  - kernel/dmesg evidence,
  - last successful SCSI/iSCSI phase.
- Avoid changing product code until the failure shape is known.

Developer default:

- Run the Linux/m02 path directly when it is a single TestOps command.
- Ask QA only for independent Windows validation, ambiguous initiator behavior,
  or milestone repeatability.

## Workstream B: V2 Comparison

Purpose: avoid speculative rewrites by comparing only the failing behavior.

Status: no new V2 port is required for the current closure.

Decision:

- The Linux OS-initiator gate is green with the current V3 protocol executor.
- The existing V2 comparison docs already identify the historical deltas:
  Data-Out collection, bounded pending queue, Data-Out timeout, and multi-PDU
  Data-In.
- Those deltas are already implemented/tested in the current V3 track or are
  classified as future component stress coverage.
- Because there is no current red OS-initiator behavior, do not port broader V2
  session/txLoop architecture in this plan.

Forward carry:

- If a future OS or soak failure shows `expected Data-Out`, CmdSN/window,
  StatSN, or Data-In segmentation symptoms, reduce that failure to a focused
  component test first and compare that exact state transition against V2.

Tasks:

- Inspect V2 iSCSI handling for the specific failing path.
- Compare against V3:
  - command sequencing,
  - read/write segmentation,
  - R2T/Data-Out handling,
  - CmdSN/pending window,
  - timeout/error mapping.
- Write down the minimum behavioral delta needed for V3.

Non-goal:

- Do not port broad V2 architecture unless the failing evidence requires it.

## Workstream C: Fast Regression Tests

Purpose: keep long OS-initiator runs as milestone gates, not the only debugging
loop.

Tasks:

- For each concrete protocol bug, add a unit/component test around the smallest
  executable boundary.
- Prefer component tests for segmentation/windowing/timeout behavior.
- Use runner-native integration only for the final real initiator proof.

Rule:

```text
Long integration failure -> reduce to component/protocol test -> fix -> rerun
the OS gate once.
```

## Workstream D: Final Gate And Non-Claims

Purpose: produce product-facing evidence without overclaiming.

Status: Linux and Windows evidence are present.

QA assignment:

- `internal/docs/qa-assignments/iscsi-os-windows-initiator-validation.md`

Windows QA evidence:

- Result: PASS.
- Product commit on m02: `9e8ffab`.
- Windows host: Windows 11 Pro (`PING-R13`).
- m02 kernel: `6.17.0-19-generic`, Ubuntu 24.04.3 LTS.
- Target artifact:
  `/mnt/smb/work/share/g15d-k8s/20260511T085158Z-iscsi-windows-target/`.
- Target-only mode printed in `run.log`.
- `target-ready.env` had `SW_BLOCK_ISCSI_TARGET_READY=1`, IQN, and portal.
- `blockvolume.log` had iSCSI listener and no startup error.
- Windows discovered the target IQN and connected with
  `IsConnected=True`, `NumberOfConnections=1`.
- Exactly one new iSCSI disk came online.
- NTFS format succeeded: 240 MiB NTFS on `S:`.
- 4 MiB write/copy/read checksum matched.
- Disconnect succeeded.
- Target self-exited at hold window and cleaned up.
- `Get-IscsiSession` was empty after cleanup.
- m02 `pgrep` showed no lingering `blockmaster` or `blockvolume`.

Windows claim:

```text
Windows 11 Pro built-in iSCSI Initiator can discover, connect to, format NTFS,
write/read with byte-exact checksum, and cleanly disconnect from a V3 iSCSI
target at commit 9e8ffab over an SSH local-forward tunnel to a loopback-bound
m02 target.
```

Windows non-claims:

- Not performance.
- Not MPIO.
- Not CHAP.
- Not failover.
- Not a Windows Server matrix.
- Tested with a 4 MiB single-file round trip.

Support added:

- `scripts/run-iscsi-os-smoke.sh` now supports external initiators with:
  - `SW_BLOCK_ISCSI_LISTEN_HOST`,
  - `SW_BLOCK_ISCSI_INITIATOR_PORTAL_ADDR`,
  - `SW_BLOCK_ISCSI_TARGET_ONLY=1`,
  - `SW_BLOCK_ISCSI_TARGET_HOLD_SECONDS`.
- This keeps the Linux gate unchanged while allowing QA to hold a loopback V3
  target on m02 and drive it from the Windows built-in iSCSI Initiator through
  an SSH local port-forward. The product guard still refuses unauthenticated
  non-loopback target binds, which is the safer default.
- Target-only startup check passed on m02 with `SW_BLOCK_ISCSI_TARGET_ONLY=1`
  and `SW_BLOCK_ISCSI_TARGET_HOLD_SECONDS=1`; it emitted
  `target-ready.env` with IQN, portal, listener, and artifact path.

Final pass must state:

- Linux initiator result.
- Windows initiator result or explicit deferral.
- Volume size and workload profile.
- What cleanup was verified.
- What this does not prove:
  - performance,
  - HA,
  - broad distro matrix,
  - upgrade safety,
  - multi-node readiness,
  - Windows support if deferred.

Close checklist:

- If QA Windows validation passes:
  - record the Windows run evidence in this plan. Done.
  - archive this file as
    `internal/docs/finished-plans/phase6_finishedplan_iscsi_os_initiator_compatibility.md`. Done.
  - update `product-management-plan.md` from pending to closed. Done.
  - keep future iSCSI session/backend pressure work under a new plan.
- If QA Windows validation fails:
  - keep this plan active,
  - classify the first failing point from the QA assignment,
  - add the smallest component/protocol test that reproduces the failure,
  - rerun the Windows gate only after that fast test is green.
- If Windows validation is deferred:
  - state the reason and exact non-claim,
  - close only the Linux/open-iscsi claim,
  - keep Windows as a separate compatibility follow-up.

## Dev / QA Split

Developer handles:

- identifying/running single-command Linux TestOps gates,
- V2 code comparison for concrete failures,
- component/protocol tests,
- product fixes.

QA handles:

- independent milestone validation,
- Windows initiator validation,
- ambiguous lab behavior,
- repeatability claims after the first green.

Default rule:

```text
single TestOps command with clear pass/fail -> developer runs
cross-OS, ambiguous, or milestone repeatability -> QA validates
```

## Non-Claims

- This plan does not deliver NVMe changes.
- This plan does not deliver performance benchmarks.
- This plan does not claim production HA.
- This plan does not prove multi-node attach.
- This plan does not prove broad distro compatibility.
- This plan does not deliver an operator.
- This plan does not replace the beta-hardening suite.
