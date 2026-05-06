# QA Assignment: iSCSI P6 ALUA / MPIO Lab Validation

Status: Test 1B and Test 2 QA green.
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

## Test 1: ALUA Discovery And Active Path Identity

Status: ready.

Run:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p6-alua-active"
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
bash scripts/run-iscsi-alua-os-smoke.sh "$PWD"
```

Expected:

- initiator discovers and logs into the V3 iSCSI target.
- standard INQUIRY shows TPGS enabled.
- VPD 0x00 still advertises only implemented pages.
- VPD 0x83 identity is stable and path-distinguishing:
  - NAA is present,
  - target port group and relative target port descriptors are present.
- REPORT TARGET PORT GROUPS returns active/optimized for the current path.
- `mkfs.ext4`, mount, checksum write/read, logout, and cleanup succeed.
- final `iscsiadm -m session` shows no active sw-block session.

Evidence to collect:

- `iscsiadm -m discovery` output.
- `iscsiadm -m session -P 3` output.
- `sg_inq <device>` standard INQUIRY output.
- `sg_inq -p 0x00 <device>` supported VPD pages.
- `sg_inq -p 0x83 <device>` device identification.
- `sg_rtpg <device>` or equivalent REPORT TARGET PORT GROUPS output.
- checksum output.
- final session cleanup.

Non-claim:

- Test 1 does not prove Linux `multipathd` grouping yet.
- Test 1 does not prove standby path login or mounted failover yet.

## Test 1B: Two-Path Multipath Identity

Status: PASS on `iscsi/csi-node-lifecycle@88e9301`.

Run:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p6-alua-mpath" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
bash scripts/run-iscsi-alua-multipath-smoke.sh "$PWD"
```

Expected:

- initiator discovers two target paths for one volume.
- `multipath -ll` shows one logical device.
- standard INQUIRY / VPD 0x83 / REPORT TARGET PORT GROUPS can be queried
  through both paths.
- one path reports active/optimized.
- one path reports standby.
- VPD 0x83 exposes volume identity plus target-port identity for each path.
- standby path does not accept normal WRITE as GOOD.
- cleanup leaves no sw-block iSCSI sessions.

QA evidence:

- host: M02, Ubuntu 24.04.3 LTS, kernel 6.17.0-22-generic.
- artifact:
  `/mnt/smb/work/share/g15d-k8s/20260506T093732Z-iscsi-p6-alua-mpath-fix`.
- final line:
  `[iscsi-mpath] PASS: two iSCSI paths report ALUA and multipath groups one logical device`.
- wire proof:
  - common NAA: `36bfc269594ef6492`.
  - r1: TPG `0x9866`, RTP `0x6b22`, AAS `0x00`.
  - r2: TPG `0xf866`, RTP `0xdff0`, AAS `0x02`.
  - `multipath -ll`: `mpatha` contains both `sda` and `sdb` with
    `hwhandler='1 alua'`.
  - standby path raw WRITE returned I/O error.
  - cleanup left no active iSCSI sessions.

Evidence to collect:

- two-path `iscsiadm -m session -P 3`.
- `multipath -ll`.
- `sg_inq -p 0x83` from each path.
- `sg_rtpg` from each path.
- standby write rejection evidence.
- final `iscsiadm -m session`.

Non-claim:

- Test 1B does not prove mounted workload failover.
- Test 1B does not prove Windows MPIO.
- Test 1B does not prove primary movement while the filesystem is mounted.

## Test 2: Mounted Workload Failover

Status: PASS on `iscsi/csi-node-lifecycle@d1025f1`.

Run:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p6-mounted-failover" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
bash scripts/run-iscsi-alua-mounted-failover-smoke.sh "$PWD"
```

Expected:

- workload starts on mounted multipath device.
- active path failure triggers authority movement to the replica path.
- authority moves only after data-continuity prerequisites are met.
- final read-back is byte-equal.
- old primary path cannot acknowledge stale successful I/O.
- cleanup leaves no sw-block iSCSI sessions and no multipath residue.

QA evidence:

- host: M02, Ubuntu 24.04.3 LTS, kernel 6.17.0-22-generic.
- artifact:
  `/mnt/smb/work/share/g15d-k8s/20260506T094503Z-iscsi-p6-mounted-failover`.
- final line:
  `[iscsi-failover] PASS: mounted multipath workload read/wrote through r1->r2 failover`.
- wire proof:
  - before failover: r1 AAS `0x00`, r2 AAS `0x02`.
  - after killing r1: r2 AAS `0x00`.
  - `multipath -ll`: same `mpatha` map served the mounted filesystem.
  - r2 status after failover: `Epoch=2`, `AuthorityRole=primary`,
    `Healthy=true`, `FrontendPrimaryReady=true`.
  - pre-failover `pre.bin` checksum read back after failover.
  - post-failover `post.bin` checksum written and verified.
  - r1 log showed stale primary lineage rejections for post-promotion
    WRITE/SYNCHRONIZE_CACHE attempts.
  - cleanup left no active iSCSI sessions, no multipath map, and no stray
    blockmaster/blockvolume processes.

Evidence to collect:

- pre-failover and post-failover checksum logs.
- r1/r2 target logs.
- r2 `/status` after failover showing `Healthy=true` and `Epoch>=2`.
- `multipath -ll` before and after failover.
- `sg_rtpg` before failure, during transition, and after recovery.
- `iscsiadm -m session -P 3` before and after failover.
- cleanup commands and final empty session/device state.

Non-claim:

- Test 2 is Linux multipath evidence only.
- Test 2 does not prove Kubernetes-mounted failover.
- Test 2 does not prove Windows MPIO.
- Test 2 does not claim production HA soak.
- Test 2 covers one volume and one RF=2 pair, not multi-volume soak or
  repeated fault injection.

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
