# NVMe P4 Multipath / Mounted Failover Design

Status: fully validated on M02.
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
- P4: native multipath discovery and mounted failover are QA green on M02.

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

Decision after QA on M02:

- Single ANA group id `1` is sufficient for the current two-path native
  multipath identity model.
- Linux accepted:
  - one SubNQN,
  - distinct controller IDs,
  - CMIC multi-controller + ANA bits,
  - NMIC shared namespace,
  - common NGUID/EUI64,
  - ANA log group id `1`.
- Evidence:
  - branch `frontend/nvme-ana-parity-plan`,
  - commit `a5ef1a5`,
  - run `20260507T161800Z-test`,
  - final line:
    `[nvme-mpath] PASS: two NVMe/TCP paths expose one ANA-aware namespace`.

## Milestones

### P4-A: Two-Path Discovery / Identity

Status: PASS on M02 at `a5ef1a5`.

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

Status: PASS on M02 at `a5ef1a5`.

- Preconditions:
  - Linux native NVMe multipath enabled.
- Expected:
  - host exposes one logical namespace with multiple paths, or reports a
    clearly documented reason it cannot.
  - Identify/log fields remain internally consistent.
- Non-claim:
  - no failover yet.

### P4-C: Mounted Failover

Status: PASS on M02 at `e1e0e0c`.

- Mount through the native multipath namespace.
- Write and checksum pre-failover data.
- Kill or close the active target path.
- Wait for authority/ANA state to move.
- Verify:
  - pre-failover data reads back,
  - post-failover write succeeds,
  - old primary cannot acknowledge stale writes,
  - cleanup leaves no NVMe connections or processes.
- Evidence:
  - run ID `20260507T170000Z-nvme-p4-mounted-failover`,
  - final line:
    `[nvme-failover] PASS: mounted NVMe multipath workload read/wrote through r1->r2 failover`,
  - Linux native multipath merged two TCP paths to `/dev/nvme1n1`,
  - r2 promoted to `Epoch=2` with `FrontendPrimaryReady=true`,
  - pre-failover checksum read after failover,
  - post-failover write/read/verify succeeded,
  - final cleanup removed the test NQN and target processes.

## Non-Goals

- No Kubernetes CSI protocol switch.
- No RoCE.
- No Windows NVMe claim.
- No performance claim.
- No long soak.
- No OAES ANA Change Notice until an async event producer exists.

## Follow-Up Risks

- Longer soak is still required before any HA durability claim.
- Kubernetes CSI protocol switching is not covered by P4.
- RoCE and real network path behavior remain outside this milestone.
- OAES ANA Change Notice remains off until an async event producer exists.
