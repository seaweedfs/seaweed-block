# iSCSI ALUA Technical Note

Status: internal reference for P6 implementation.

## What ALUA Does

ALUA is the SCSI way to tell an initiator that multiple paths reach the same
logical device, but the paths are not equivalent.

- Active optimized: preferred path; can serve normal read/write.
- Active non-optimized: usable path but less preferred.
- Standby: visible path for probing/failover, but not writable.
- Unavailable: path exists but should fail fast.
- Transitioning: path state is moving; do not silently accept writes.

Linux `multipathd` uses standard INQUIRY TPGS bits, VPD 0x83 identifiers, and
REPORT TARGET PORT GROUPS to decide whether several iSCSI sessions are one
device with multiple paths.

## V3 Design

The iSCSI layer reports ALUA state. It does not decide authority.

- `core/frontend/iscsi` owns protocol shape:
  - TPGS in standard INQUIRY,
  - REPORT TARGET PORT GROUPS,
  - VPD 0x83 path identity,
  - write/SYNC rejection on non-writable paths.
- A read-only `ALUAProvider` feeds protocol state into `SCSIHandler`.
- The provider must consume V3 frontend facts only.
- It must not import authority, placement, or publisher code.

This keeps the boundary:

- authority decides who may be primary,
- frontend facts expose current path truth,
- iSCSI translates that truth into SCSI-visible ALUA behavior.

## Current P6 Local Protocol Slice

Implemented on `iscsi/csi-node-lifecycle` in commit `bfffbbe`.

- No provider:
  - TPGS remains off,
  - REPORT TARGET PORT GROUPS fails closed,
  - VPD 0x83 stays single NAA descriptor.
- With provider:
  - TPGS advertises implicit ALUA,
  - REPORT TARGET PORT GROUPS returns state, target port group ID, and
    relative target port ID,
  - VPD 0x83 adds target-port-group and relative-target-port descriptors,
  - active optimized / active non-optimized can write,
  - standby / unavailable / transitioning reject WRITE and SYNCHRONIZE_CACHE,
  - non-writable states still allow READ for path probing.

## State Mapping Target

V2 behavior is the coverage baseline, not the code model.

- standalone or current primary -> active optimized
- valid non-primary path -> standby or active non-optimized
- stale / closed / unverified path -> unavailable
- authority or recovery movement not closed -> transitioning

V3 still needs P6-C wiring to map real blockvolume/frontend facts into this
provider.

## Hard Parts

- Identity stability:
  - all paths for the same volume need the same device NAA,
  - paths must still be distinguishable by target port group / relative target
    port.
- Write safety:
  - old primary must never return successful WRITE after authority moved.
  - standby read probing must not become stale data leakage.
- Transition timing:
  - authority moved is not enough;
  - data continuity and frontend readiness must be closed before a path becomes
    active.
- Real initiator behavior:
  - in-process tests are not enough;
  - Linux `open-iscsi`, `sg_inq`, `sg_rtpg`, `multipath -ll`, mounted workload,
    and failover must be tested.

## Test Checklist

- Unit protocol:
  - TPGS off without provider.
  - REPORT TPG rejected without provider.
  - VPD 0x83 non-ALUA branch unchanged.
  - all five ALUA states report correctly.
  - standby / unavailable / transitioning reject WRITE/SYNC.
  - standby / transitioning READ works for path probing.
  - VPD 0x00 unchanged.
  - VPD 0x83 truncates safely.
- Product wiring:
  - blockvolume primary path reports active.
  - non-primary path reports standby/non-optimized.
  - stale or closed path reports unavailable/transitioning.
  - no frontend fact means no ALUA claim.
- Lab:
  - `sg_inq` confirms TPGS.
  - `sg_inq -p 0x83` confirms stable identity.
  - `sg_rtpg` confirms path states.
  - `multipath -ll` sees one device with multiple paths.
  - mounted failover proves byte-equal read-back and no stale-primary success.
