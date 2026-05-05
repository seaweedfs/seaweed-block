# QA Assignment: iSCSI P4 CHAP Lab Validation

Status: ready for QA.
Branch: `iscsi/frontend-completeness`.
Scope: target-side CHAP login auth through the real Linux `iscsiadm`
initiator. No Kubernetes Secret or CSI CHAP claim in this assignment.

Dev preflight:

- commit: `d0c62e6`
- host: M02 or equivalent Linux lab host
- command: Test 2 below
- artifact:
  `[artifact-root]/20260505T072921Z-iscsi-p4-chap`
- result: PASS, including wrong-secret failure, correct-secret login,
  `mkfs.ext4`, mount, checksum, 32 MiB dd, logout, and no active sessions.

## Preconditions

- Host: M02 or equivalent Linux host with `sudo`, `iscsiadm`, `mkfs.ext4`,
  `mount`, `sha256sum`, and optional `fio`.
- Worktree: local `seaweed-block` checkout on branch `iscsi/frontend-completeness`,
  or archived copy of the same commit on the Linux host.
- Lab must start clean:
  - `sudo iscsiadm -m session` reports no active sessions, or only unrelated
    sessions explicitly noted.
  - no stale `blockmaster` / `blockvolume` from prior runs.

## Test 1: Default Non-CHAP Regression

Command:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p4-default" \
SW_BLOCK_ISCSI_ITERATIONS=1 \
SW_BLOCK_ISCSI_STRESS=dd \
bash scripts/run-iscsi-os-smoke.sh "$PWD"
```

Expected:

- final line contains:
  `[iscsi-os] PASS: 1 x iscsiadm mkfs mount write/read logout`
- `iscsi-sessions.final.txt` reports no active sessions.
- no `expected Data-Out` session errors in `blockvolume.log`.

## Test 2: CHAP Positive + Negative Login

Command:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p4-chap" \
SW_BLOCK_ISCSI_ITERATIONS=1 \
SW_BLOCK_ISCSI_STRESS=dd \
SW_BLOCK_ISCSI_CHAP_USERNAME=swchap \
SW_BLOCK_ISCSI_CHAP_SECRET=swchap-secret \
SW_BLOCK_ISCSI_CHAP_BAD_SECRET=wrong-secret \
bash scripts/run-iscsi-os-smoke.sh "$PWD"
```

Expected:

- run log includes `chap=enabled username=swchap`.
- run log includes `verify wrong CHAP secret fails`.
- `login-bad.iter1.log` exists and shows the wrong-secret login failed.
- `iscsi-sessions.bad-auth.iter1.txt` does not show an established session
  for this IQN.
- the following correct-secret login succeeds.
- final line contains:
  `[iscsi-os] PASS: 1 x iscsiadm mkfs mount write/read logout`
- `iscsi-sessions.final.txt` reports no active sessions.
- no `expected Data-Out` session errors in `blockvolume.log`.

## Test 3: Optional CHAP fio

Run this only if Test 2 is green and the host has `fio`.

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p4-chap-fio" \
SW_BLOCK_ISCSI_ITERATIONS=1 \
SW_BLOCK_ISCSI_STRESS=fio \
SW_BLOCK_ISCSI_FIO_RUNTIME=60 \
SW_BLOCK_ISCSI_CHAP_USERNAME=swchap \
SW_BLOCK_ISCSI_CHAP_SECRET=swchap-secret \
SW_BLOCK_ISCSI_CHAP_BAD_SECRET=wrong-secret \
bash scripts/run-iscsi-os-smoke.sh "$PWD"
```

Expected:

- fio exits with `err=0`.
- final line contains PASS.
- no active sessions after cleanup.

## Report Format

QA should report:

- branch and commit SHA,
- host and kernel,
- exact commands,
- result for each test,
- artifact path for each run,
- final PASS line,
- `login-bad.iter1.log` result for Test 2,
- `iscsi-sessions.final.txt`,
- count of `expected Data-Out` in `blockvolume.log`,
- any host setup changes, such as installing `open-iscsi` or `fio`.

## Non-Claims

- No Kubernetes Secret integration.
- No CSI publish_context CHAP secret transport.
- No CHAP mutual authentication.
- No ALUA, MPIO, NVMe, or mounted failover claim.
