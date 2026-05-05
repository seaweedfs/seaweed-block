# iSCSI P6 ALUA / MPIO / Mounted Failover Design

Status: design draft.
Branch: `iscsi/csi-node-lifecycle`.

## Goal

Make mounted-volume failover a deliberate frontend behavior. P6 is not only
"the initiator can reconnect"; it must define what an OS initiator sees when
there are multiple paths, one path is active, another is standby, and authority
moves.

## Current State

- V3 iSCSI currently advertises no ALUA:
  - standard INQUIRY TPGS bits are zero,
  - VPD 0x83 emits one non-ALUA NAA designator,
  - REPORT TARGET PORT GROUPS is not implemented.
- G8/G9 authority and data-continuity tests prove failover below the frontend,
  but not mounted failover through a real OS initiator.
- P5 proves CSI node restart while a PVC is mounted, but not blockvolume
  failover or multipath.

## Design Rules

- Protocol state must consume frontend/authority facts. It must not decide
  authority, placement, or replica readiness.
- A standby path must not acknowledge stale writes.
- Authority movement alone must not imply data continuity.
- If we expose a multipath identity, it must be stable enough for Linux
  multipath and udev.
- In-process protocol tests are necessary but not sufficient; P6 closes only
  with a real Linux initiator/multipath run.

## P6 Milestones

### P6-A: ALUA Contract Pinning

- Define the exported state names:
  - `active_optimized`: current primary frontend may serve read/write.
  - `standby`: observed target path exists but must not serve normal I/O.
  - `unavailable`: target path should fail fast.
  - `transitioning`: authority or readiness is changing; normal I/O must not
    be silently accepted.
- Define command policy by state:
  - INQUIRY, VPD, REPORT LUNS, TEST UNIT READY: allowed on active and standby.
  - READ/WRITE/SYNCHRONIZE_CACHE: allowed only on active unless explicitly
    documented otherwise.
  - standby write must fail with a stable sense code.
- Red tests:
  - standby WRITE cannot return GOOD.
  - TPGS bits are not enabled until REPORT TARGET PORT GROUPS and VPD identity
    are implemented.

### P6-B: Protocol Surface

- Implement TPGS advertisement in standard INQUIRY.
- Implement REPORT TARGET PORT GROUPS.
- Extend VPD 0x83 with ALUA-safe target-port / target-port-group identity.
- Keep VPD 0x00 advertised pages equal to implemented pages.
- Red tests:
  - advertised ALUA pages and commands must be served.
  - target port identity is deterministic across restart for the same path.
  - different paths for the same volume are distinguishable.

### P6-C: Frontend State Wiring

- Add an explicit ALUA state provider interface near the iSCSI frontend.
- The provider reads current frontend facts; it does not call placement or
  authority decision code.
- Blockvolume feeds:
  - active when it owns the current primary frontend,
  - standby/unavailable when it is not the primary or is closing,
  - transitioning when authority changes are observed but local readiness is
    not closed.
- Red tests:
  - old primary after failover cannot serve WRITE as GOOD.
  - returned replica heartbeat does not make the path active.

### P6-D: Real Initiator MPIO Lab

- Linux initiator with `open-iscsi` and `multipath-tools`.
- Two target paths for one volume.
- Expected:
  - multipath sees one logical device,
  - active path accepts workload,
  - standby path is visible but not selected for normal writes,
  - path state changes are visible after authority movement.
- Non-goal for P6-D:
  - no automatic transparent failover claim until P6-E passes.

### P6-E: Mounted Failover

- Start a mounted workload through the multipath device.
- Kill or close the active primary path.
- Drive authority movement to the replica path.
- Verify either:
  - workload survives according to documented policy, or
  - workload fails and remount/retry succeeds according to documented policy.
- Required proof:
  - byte-equal read-back after failover,
  - old primary cannot acknowledge stale successful I/O,
  - no dangling sessions after cleanup.

## First Implementation Slice

Start with P6-A only:

- Write red tests for standby WRITE fail-closed behavior and ALUA advertisement
  discipline.
- Do not enable TPGS yet.
- Do not change K8s manifests yet.
- Do not claim MPIO.

This keeps the first PR reviewable and prevents enabling multipath-looking
metadata before the actual protocol surface exists.

## Non-Claims

- No NVMe-oF ANA.
- No performance claim.
- No production HA claim.
- No silent failover claim until P6-E passes with real OS initiator evidence.
