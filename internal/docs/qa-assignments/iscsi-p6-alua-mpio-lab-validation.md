# QA Assignment: iSCSI P6 ALUA / MPIO Lab Validation

Status: design draft; not ready for QA execution.
Branch: `iscsi/csi-node-lifecycle`.
Scope: real Linux initiator validation for ALUA/MPIO and mounted failover.

## Preconditions

- Linux initiator host with:
  - `open-iscsi`,
  - `multipath-tools`,
  - `sg3_utils` or equivalent tools for `sg_inq` and `sg_rtpg`,
  - `mkfs.ext4`,
  - `mount`,
  - `fio` or `dd`,
  - sudo access.
- Target host or Kubernetes lab capable of exposing two iSCSI paths for the
  same volume.
- P6-B/P6-C implementation must be present before execution.

## Test 1: ALUA Discovery And Multipath Identity

Status: blocked until P6-B.

Expected when unblocked:

- initiator discovers two target paths for one volume.
- `multipath -ll` shows one logical device.
- standard INQUIRY shows TPGS enabled.
- VPD 0x00 still advertises only implemented pages.
- VPD 0x83 identity is stable and path-distinguishing:
  - same volume -> same NAA,
  - different paths -> distinct target port / target port group identity.
- REPORT TARGET PORT GROUPS returns active, active-non-optimized if used,
  standby, unavailable, and transitioning states when forced by the test.
- standby path does not accept normal WRITE as GOOD.
- standby path still accepts metadata/path-probing commands and READ.

Evidence to collect:

- `iscsiadm -m discovery` output.
- `iscsiadm -m session -P 3` output.
- `sg_inq <device>` standard INQUIRY output.
- `sg_inq -p 0x00 <device>` supported VPD pages.
- `sg_inq -p 0x83 <device>` device identification.
- `sg_rtpg <device>` or equivalent REPORT TARGET PORT GROUPS output.
- `multipath -ll` output.

## Test 2: Mounted Workload Failover

Status: blocked until P6-C/P6-E.

Expected when unblocked:

- workload starts on mounted multipath device.
- active path failure triggers documented path-state change.
- authority moves only after data-continuity prerequisites are met.
- final read-back is byte-equal.
- old primary path cannot acknowledge stale successful I/O.
- cleanup leaves no sw-block iSCSI sessions and no multipath residue.

Evidence to collect:

- workload log before and after active-path failure.
- target logs showing old primary write rejection after state change.
- `multipath -ll` before failure, during transition, and after recovery.
- `sg_rtpg` before failure, during transition, and after recovery.
- checksum or byte-equal read-back proof.
- cleanup commands and final empty session/device state.

## Report Format

QA should report:

- branch and commit SHA,
- host/kernel,
- exact commands,
- `iscsiadm -m session`,
- `multipath -ll`,
- `sg_inq` standard/VPD evidence,
- `sg_rtpg` or equivalent REPORT TARGET PORT GROUPS evidence,
- workload result,
- failover event timing,
- cleanup state,
- any kernel `dmesg` I/O errors.

## Non-Claims

- No Windows MPIO claim.
- No NVMe-oF ANA claim.
- No performance claim.
- No production HA claim until soak/fault testing exists.
