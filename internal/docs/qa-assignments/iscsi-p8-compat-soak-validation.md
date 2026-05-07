# QA Assignment: iSCSI P8 Compatibility Soak

Status: QA green on `iscsi/frontend-hardening@38ff850`.
Branch: `iscsi/frontend-hardening`.
Scope: repeatable soak evidence for the current iSCSI frontend stack.

Verified on M02:

- run ID: `20260506T223240Z-iscsi-p8-soak-38ff850`
- artifact:
  `/mnt/smb/work/share/g15d-k8s/20260506T223240Z-iscsi-p8-soak-38ff850`
- final line: `[iscsi-soak] PASS: compatibility soak completed`
- steps:
  - `os-fio-repeat`: PASS,
  - `k8s-fio`: PASS,
  - `k8s-attach-detach`: PASS.
- cleanup:
  - no active iSCSI sessions,
  - no sw-block PVC/deployment residue,
  - no stray `blockmaster` / `blockvolume` processes.

## Preconditions

- Linux host with:
  - `open-iscsi`,
  - `iscsi_tcp`,
  - `mkfs.ext4`,
  - `mount`,
  - `fio`,
  - sudo access.
- Optional K8s steps need:
  - k3s/kubectl access,
  - local `sw-block:local` and `sw-block-csi:local` images rebuilt from this
    branch,
  - pod egress for `apk add fio`.
- No active sw-block iSCSI sessions before the run.

## Run

OS-only soak:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p8-soak" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_P8_OS_ITERATIONS=2 \
SW_BLOCK_P8_OS_FIO_RUNTIME=120 \
SW_BLOCK_P8_OS_FIO_SIZE=128m \
bash scripts/run-iscsi-compat-soak.sh "$PWD"
```

Full lab soak with K8s fio and attach/detach:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p8-soak-full" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_P8_OS_ITERATIONS=2 \
SW_BLOCK_P8_OS_FIO_RUNTIME=120 \
SW_BLOCK_P8_OS_FIO_SIZE=128m \
SW_BLOCK_P8_RUN_K8S_FIO=1 \
SW_BLOCK_P8_RUN_ATTACH_LOOP=1 \
SW_BLOCK_P8_ATTACH_ITERATIONS=3 \
bash scripts/run-iscsi-compat-soak.sh "$PWD"
```

## Expected

- `summary.md` has PASS rows for every enabled step.
- OS fio step finishes all iterations.
- If enabled, K8s fio step reaches the existing `[alpha-fio] PASS` line.
- If enabled, attach/detach reaches the existing `[attach-loop] PASS` line.
- Final host has no active sw-block iSCSI sessions.
- K8s steps leave no sw-block PVC, pod, or generated blockvolume Deployment.

## Evidence To Report

- branch and commit SHA,
- host/kernel,
- exact command,
- artifact root,
- `summary.md`,
- fio read/write lines,
- any iSCSI session errors in daemon logs,
- cleanup state.

## Non-Claims

- This is not a performance claim.
- This is not a benchmark publication.
- This is not a multi-host, RoCE, NVMe, or long soak claim.
- K8s evidence is single-node unless the report explicitly says otherwise.
