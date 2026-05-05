# QA Assignment: iSCSI P6 ALUA / MPIO Lab Validation

Status: design draft; not ready for QA execution.
Branch: `iscsi/csi-node-lifecycle`.
Scope: real Linux initiator validation for ALUA/MPIO and mounted failover.

## Preconditions

- Linux initiator host with:
  - `open-iscsi`,
  - `multipath-tools`,
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
- VPD 0x83 identity is stable and path-distinguishing.
- REPORT TARGET PORT GROUPS returns active and standby path states.
- standby path does not accept normal WRITE as GOOD.

## Test 2: Mounted Workload Failover

Status: blocked until P6-C/P6-E.

Expected when unblocked:

- workload starts on mounted multipath device.
- active path failure triggers documented path-state change.
- authority moves only after data-continuity prerequisites are met.
- final read-back is byte-equal.
- old primary path cannot acknowledge stale successful I/O.
- cleanup leaves no sw-block iSCSI sessions and no multipath residue.

## Report Format

QA should report:

- branch and commit SHA,
- host/kernel,
- exact commands,
- `iscsiadm -m session`,
- `multipath -ll`,
- `sg_inq` or equivalent VPD evidence,
- workload result,
- failover event timing,
- cleanup state,
- any kernel `dmesg` I/O errors.

## Non-Claims

- No Windows MPIO claim.
- No NVMe-oF ANA claim.
- No performance claim.
- No production HA claim until soak/fault testing exists.
