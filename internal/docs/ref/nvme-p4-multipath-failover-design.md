# NVMe P4 Multipath / Mounted Failover Design

Status: design record.
Branch: `frontend/nvme-ana-parity-plan`.

## Goal

- Bring NVMe-oF to the same frontend correctness bar that iSCSI reached in
  P6: real Linux multipath visibility first, mounted failover second.
- Keep this as correctness work. No performance, RoCE, or Kubernetes claim is
  made by P4.

## Starting State

- P1: Linux `nvme connect -> mkfs -> mount -> checksum -> disconnect` is QA
  green on M02.
- P3: ANA Identify fields and ANA log page are QA green on M02:
  - Identify advertises ANA only when `ANAProvider` is wired,
  - ANA log group id is dense `1`, matching `ANAGRPMAX=1` and `NANAGRPID=1`,
  - Linux accepts the controller into ANA-aware init.
- Missing:
  - no two-path NVMe/TCP script,
  - no Linux native NVMe multipath proof,
  - no mounted failover through `/dev/nvme...` or `/dev/disk/by-id/...`.

## Linux Multipath Model

- NVMe multipath is not `multipathd` for SCSI.
- Linux NVMe uses native multipath in the kernel when enabled, usually via:
  - `nvme_core.multipath=Y` kernel parameter, or
  - module parameter if supported by the distro.
- Evidence must record whether native NVMe multipath is enabled. A two-path
  test without native multipath only proves two independent controllers, not a
  usable multipath device.

## Identity Requirements

- Both paths for one volume must expose the same namespace identity:
  - same NGUID,
  - same EUI-64,
  - same NSID,
  - same SubNQN unless the host path model requires otherwise.
- The host must be able to tell path state apart through ANA:
  - active path reports optimized,
  - non-active but valid path reports non-optimized or inaccessible according
    to the chosen policy,
  - stale/degraded path must not report optimized.
- Current P3 uses one ANA group id (`1`) because there is one advertised group.
  P4 may need a dense group allocation policy if Linux requires different path
  states to live in distinct groups.

## Policy Question For First P4 Slice

- Option A: one ANA group per namespace.
  - simple,
  - works for single-path P3,
  - cannot represent two paths with different states in one log response.
- Option B: one dense ANA group per path state/path.
  - required if Linux native multipath expects active and standby paths to
    report different groups,
  - `ANAGRPMAX` and `NANAGRPID` must be the dense group count,
  - every group id in the ANA log must be `<= ANAGRPMAX`.

P4 should start with discovery evidence before deciding. If Linux groups two
paths with the current single group and both are optimized, that is not
failover-ready; it is only identity proof.

## Milestones

### P4-A: Two-Path Discovery / Identity

- Start one blockmaster and two blockvolume frontends for one volume.
- Expose two NVMe/TCP portals to the same Linux host.
- Connect both paths.
- Capture:
  - `nvme list`,
  - `nvme list-subsys -o json`,
  - `nvme id-ctrl`,
  - `nvme id-ns`,
  - `nvme get-log -i 0x0c`.
- Expected:
  - both paths connect,
  - no `nvme_parse_ana_log` warning,
  - same namespace identity across both paths,
  - cleanup disconnects both paths.

### P4-B: Native Multipath Grouping

- Preconditions:
  - Linux native NVMe multipath enabled.
- Expected:
  - host exposes one logical namespace with multiple paths, or reports a
    clearly documented reason it cannot.
  - Identify/log fields remain internally consistent.
- Non-claim:
  - no failover yet.

### P4-C: Mounted Failover

- Mount through the native multipath namespace.
- Write and checksum pre-failover data.
- Kill or close the active target path.
- Wait for authority/ANA state to move.
- Verify:
  - pre-failover data reads back,
  - post-failover write succeeds,
  - old primary cannot acknowledge stale writes,
  - cleanup leaves no NVMe connections or processes.

## Non-Goals

- No Kubernetes CSI protocol switch.
- No RoCE.
- No Windows NVMe claim.
- No performance claim.
- No long soak.
- No OAES ANA Change Notice until an async event producer exists.

## Open Technical Risks

- Linux may require multiple ANA groups to represent different states across
  paths. P3's single dense group is valid for one path, but may be insufficient
  for P4.
- Linux native NVMe multipath setup may require host reboot or kernel parameter
  changes. QA must report this explicitly.
- Two targets with the same SubNQN/NGUID must not collide in the controller
  registry or in host cleanup.
- Mounted failover depends on V3 authority/data-continuity timing, not only
  protocol metadata.
