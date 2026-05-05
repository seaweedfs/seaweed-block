# QA Assignment: iSCSI P2/P3 Lab Validation

Status: assigned.

Branch under test: `iscsi/frontend-completeness`.

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
