# QA Assignment: iSCSI P2/P3 Lab Validation

Status: QA green.

Branch under test: `iscsi/frontend-completeness`.
Verified commit: `e7c95ee44863f34b304995fa977a4ff57ab3e649`.
Host: M02 (`192.168.1.184`).

Purpose: validate the local iSCSI P2/P3 test/tooling work on a real Linux/K8s
lab. This is not a performance claim and not a failover claim.

## Scope

- #QA iSCSI P2 OS initiator repeat loop.
- #QA iSCSI P2 OS initiator fio mode.
- #QA iSCSI P3 Kubernetes fio smoke.
- #QA iSCSI P3 Kubernetes attach/detach loop.

## Preconditions

- Lab host: M02 unless QA states otherwise.
- Linux host has `sudo`, `iscsiadm`, `mkfs.ext4`, `mount`, `sha256sum`.
- K8s tests need working k3s/kubectl and privileged CSI node support.
- Images for K8s tests are either local:
  - `sw-block:local`
  - `sw-block-csi:local`
- Or QA explicitly records the GHCR image tags/digests used.

## Test 1: OS Initiator Repeat Loop

- Result: PASS.
- Artifact:
  `/mnt/smb/work/share/g15d-k8s/20260505T062122Z-iscsi-p2-test1-os-repeat`.
- Final line:
  `[iscsi-os] PASS: 5 x iscsiadm mkfs mount write/read logout`.
- Cleanup: `iscsi-sessions.final.txt` reports no active sessions.
- `expected Data-Out` errors in `blockvolume.log`: 0.

- Command:

  ```bash
  SW_BLOCK_ISCSI_ITERATIONS=5 \
  SW_BLOCK_ISCSI_STRESS=dd \
  bash scripts/run-iscsi-os-smoke.sh "$PWD"
  ```

- Pass criteria:
  - all 5 iterations pass,
  - every iteration logs discovery, login, mkfs, mount, checksum, logout,
  - `iscsi-sessions.final.txt` reports no active sessions,
  - no `expected Data-Out` session error in `blockvolume.log`.

- Required report:
  - branch and commit,
  - host,
  - command,
  - artifact dir,
  - final PASS line,
  - cleanup state.

## Test 2: OS Initiator fio Mode

- Result: PASS.
- Artifact:
  `/mnt/smb/work/share/g15d-k8s/20260505T062159Z-iscsi-p2-test2-os-fio60s`.
- fio summary:
  - read IOPS=122, BW=490KiB/s,
  - write IOPS=123, BW=494KiB/s,
  - runtime=60.004s,
  - err=0,
  - `fio.iter1.log` present.
- Cleanup: `iscsi-sessions.final.txt` reports no active sessions.
- `expected Data-Out` errors in `blockvolume.log`: 0.

- Command:

  ```bash
  SW_BLOCK_ISCSI_ITERATIONS=1 \
  SW_BLOCK_ISCSI_STRESS=fio \
  SW_BLOCK_ISCSI_FIO_RUNTIME=60 \
  bash scripts/run-iscsi-os-smoke.sh "$PWD"
  ```

- Pass criteria:
  - fio completes,
  - `fio.iter1.log` exists,
  - logout succeeds,
  - no active iSCSI sessions after cleanup,
  - no session errors in `blockvolume.log`.

## Test 3: Kubernetes fio Smoke

- Result: PASS.
- Artifact:
  `/mnt/smb/work/share/g15d-k8s/20260505T062334Z-iscsi-p3-test3-k8s-fio`.
- Pod evidence:
  - `pod.log` shows Alpine `apk` install of fio,
  - 60s randrw run,
  - READ BW=13.0MiB/s, 782MiB total,
  - WRITE BW=13.1MiB/s, 787MiB total,
  - err=0,
  - util=96.36%.
- Final line:
  `[alpha-fio] PASS: dynamic PVC create/delete completed checksum write/read and cleanup`.
- Cleanup: no PVC, no sw-block deployments, no active iSCSI sessions.

- Command:

  ```bash
  bash scripts/run-k8s-alpha-fio.sh "$PWD"
  ```

- Pass criteria:
  - pod reaches `Succeeded`,
  - `pod.log` contains fio output,
  - generated blockvolume Deployment is cleaned,
  - no active iSCSI sessions after delete,
  - no K8s residue for `sw-block`.

- Notes:
  - This test uses `alpine:3.20` and installs fio with `apk`.
  - If pod egress is blocked, report it as environment failure, not product
    failure.

## Test 4: Kubernetes Attach/Detach Loop

- Result: PASS.
- Artifact:
  `/mnt/smb/work/share/g15d-k8s/20260505T062606Z-iscsi-p3-test4-attach-detach`.
- Iterations:
  - iter-1 PASS: writer wrote and verified `/data/demo.bin`; reader read-back OK; no active iSCSI sessions after delete.
  - iter-2 PASS: same.
  - iter-3 PASS: same.
- Final line:
  `[attach-loop] PASS: 3 attach/detach app PVC cycles completed`.
- Cleanup: no PVC, no sw-block deployments, no active iSCSI sessions.

- Command:

  ```bash
  SW_BLOCK_ATTACH_DETACH_ITERATIONS=3 \
  bash scripts/run-k8s-attach-detach-loop.sh "$PWD"
  ```

- Pass criteria:
  - all iterations pass,
  - each iteration has writer and reader logs,
  - each reader verifies data written by its writer,
  - every iteration has no active iSCSI session after delete,
  - final summary line reports PASS.

## Failure Handling

- Preserve artifact directories.
- Do not rerun over a failed artifact directory.
- If a script cannot collect required evidence, report the script gap and stop.
- Do not replace the script with manual commands unless needed for first
  diagnosis.

## QA Report Template

```text
QA Assignment: iSCSI P2/P3 Lab Validation
Branch:
Commit:
Host:

Test 1 OS repeat:
  result:
  command:
  artifact:
  final line:
  cleanup:

Test 2 OS fio:
  result:
  command:
  artifact:
  fio summary:
  cleanup:

Test 3 K8s fio:
  result:
  command:
  artifact:
  pod evidence:
  cleanup:

Test 4 K8s attach/detach:
  result:
  command:
  artifact:
  iterations:
  cleanup:

Findings:
- ...
```

## Lab Notes

- M02 initially had no Go installation.
- QA installed Go 1.25.0 to `/usr/local/go` and symlinked
  `/usr/local/bin/go`.
- K8s tests used locally rebuilt images:
  - `sw-block:local` sha256 `dcb621e4447a`,
  - `sw-block-csi:local` sha256 `84fab78dc05a`.
- No GHCR images were used.
- Test 3 requires pod network egress for `apk add fio`; M02 had egress.
