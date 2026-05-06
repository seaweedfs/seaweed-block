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

## V2 Alignment Inventory

This section is a coverage inventory only. Do not copy V2 state ownership or
promotion logic into V3. V3 must keep protocol reporting separate from
authority, placement, and replica readiness.

- V2 ALUA states that V3 must account for:
  - `active_optimized` (`0x00`),
  - `active_non_optimized` (`0x01`),
  - `standby` (`0x02`),
  - `unavailable` (`0x0e`),
  - `transitioning` (`0x0f`).
- V2 role-to-state mapping used as a behavioral reference:
  - standalone / primary -> active optimized,
  - replica -> standby,
  - stale -> unavailable,
  - rebuilding / draining -> transitioning.
- V3 equivalent mapping must be derived from V3 frontend facts:
  - current primary frontend -> active optimized,
  - valid non-primary path -> standby or active non-optimized, depending on
    the final MPIO policy,
  - stale/closed/unverified frontend -> unavailable,
  - authority or recovery movement not yet closed -> transitioning.
- V2 protocol surface that P6 must match at the test level:
  - standard INQUIRY advertises TPGS only when REPORT TARGET PORT GROUPS and
    ALUA VPD identity are implemented,
  - VPD 0x00 remains unchanged when ALUA is enabled,
  - VPD 0x83 has a non-ALUA branch and an ALUA branch,
  - REPORT TARGET PORT GROUPS handles all states, short allocation lengths,
    no-ALUA rejection, and concurrent state reads,
  - standby and transitioning paths reject writes but still serve discovery
    and path-probing reads.

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
  - `active_non_optimized`: path is usable but should not be preferred.
  - `standby`: observed target path exists but must not serve normal I/O.
  - `unavailable`: target path should fail fast.
  - `transitioning`: authority or readiness is changing; normal I/O must not
    be silently accepted.
- Define command policy by state:
  - INQUIRY, VPD, REPORT LUNS, TEST UNIT READY, READ CAPACITY, MODE SENSE,
    REQUEST SENSE, and REPORT TARGET PORT GROUPS: allowed on visible paths.
  - READ is allowed on standby/transitioning for OS path probing and must not
    leak stale data beyond what the underlying frontend provider allows.
  - WRITE and SYNCHRONIZE_CACHE are allowed only on active paths.
  - standby, unavailable, and transitioning WRITE must fail with stable NOT
    READY sense, not GOOD.
- Red tests:
  - standby WRITE cannot return GOOD.
  - standby READ/READ(16) is allowed for probing.
  - unavailable and transitioning WRITE cannot return GOOD.
  - state change from active -> standby -> active -> transitioning is observed
    immediately by command handling and REPORT TARGET PORT GROUPS.
  - TPGS bits are not enabled until REPORT TARGET PORT GROUPS and VPD identity
    are implemented.

### P6-B: Protocol Surface

- Implement TPGS advertisement in standard INQUIRY:
  - use implicit ALUA TPGS (`01b`) unless the design later explicitly adds
    explicit ALUA transitions,
  - preserve unrelated byte-5 bits and do not advertise unsupported features.
- Implement REPORT TARGET PORT GROUPS.
- Extend VPD 0x83 with ALUA-safe identity:
  - NAA-6 device designator remains stable per volume,
  - target port group designator identifies the path group,
  - relative target port designator distinguishes paths,
  - no-ALUA device still returns only the NAA branch.
- Keep VPD 0x00 advertised pages equal to implemented pages.
- Red tests:
  - advertised ALUA pages and commands must be served.
  - target port identity is deterministic across restart for the same path.
  - different paths for the same volume are distinguishable.
  - VPD 0x83 truncates safely for short allocation length.
  - VPD 0x83 NAA high nibble remains NAA-6 even with hostile volume ID bytes.
  - REPORT TARGET PORT GROUPS rejects no-ALUA devices.
  - REPORT TARGET PORT GROUPS handles short allocation lengths.
  - REPORT TARGET PORT GROUPS covers all five states.
  - concurrent REPORT TARGET PORT GROUPS calls during state changes are safe.

### P6-C: Frontend State Wiring

- Add an explicit ALUA state provider interface near the iSCSI frontend.
- The provider reads current frontend facts; it does not call placement or
  authority decision code.
- Provider fields:
  - ALUA state,
  - target port group ID,
  - relative target port ID,
  - stable device NAA or volume identity input.
- Blockvolume feeds:
  - active when it owns the current primary frontend,
  - standby/active-non-optimized when it is a valid non-primary path,
  - unavailable when the local frontend is closed, stale, or unverified,
  - transitioning when authority changes are observed but local readiness is
    not closed.
- Red tests:
  - old primary after failover cannot serve WRITE as GOOD.
  - returned replica heartbeat does not make the path active.
  - ALUA state provider cannot mint authority or mutate assignment.
  - no frontend fact means no active/standby ALUA claim.

### P6-D: Real Initiator MPIO Lab

- Linux initiator with `open-iscsi` and `multipath-tools`.
- Two target paths for one volume.
- Expected:
  - multipath sees one logical device,
  - active path accepts workload,
  - standby path is visible but not selected for normal writes,
  - path state changes are visible after authority movement.
- Evidence:
  - `sg_inq` standard INQUIRY shows TPGS enabled,
  - `sg_inq -p 0x83` shows stable NAA plus path-distinguishing descriptors,
  - `sg_rtpg` or equivalent shows target port group state,
  - `multipath -ll` shows one logical device with two paths.
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
