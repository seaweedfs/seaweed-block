# QA Assignment: NVMe P1 OS Smoke Validation

Status: ready for QA.
Branch: `frontend/nvme-ana-parity-plan`.
Scope: first real Linux kernel NVMe/TCP initiator gate for V3.

## Goal

- Prove the current NVMe-oF frontend can survive a real OS path:
  `nvme connect -> mkfs.ext4 -> mount -> checksum write/read -> disconnect`.
- Produce artifacts good enough to decide whether NVMe-P2/P3 work starts from
  a working baseline or from a protocol bug.
- Keep this as correctness evidence only. No performance claim.

## Preconditions

- Linux host with:
  - `nvme-cli`,
  - loadable `nvme_tcp` kernel module,
  - `mkfs.ext4`,
  - `mount`,
  - `sha256sum`,
  - sudo access.
- `fio` installed only for Test 2.
- Go installed unless `SW_BLOCK_BIN_DIR` points at prebuilt `blockmaster` and
  `blockvolume` binaries.
- No active sw-block NVMe sessions before the run.

## Test 1: Basic OS Path

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p1-basic" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_NVME_ITERATIONS=1 \
SW_BLOCK_NVME_STRESS=none \
bash scripts/run-nvme-os-smoke.sh "$PWD"
```

Expected final line:

```text
[nvme-os] PASS: 1 x nvme connect mkfs mount write/read disconnect
```

## Test 2: FIO Sanity

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p1-fio" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_NVME_ITERATIONS=1 \
SW_BLOCK_NVME_STRESS=fio \
SW_BLOCK_NVME_FIO_RUNTIME=60 \
SW_BLOCK_NVME_FIO_SIZE=128m \
bash scripts/run-nvme-os-smoke.sh "$PWD"
```

Expected final line:

```text
[nvme-os] PASS: 1 x nvme connect mkfs mount write/read disconnect
```

## Acceptance

- `run.log` contains the final PASS line.
- `nvme-connect.iter1.log`, `nvme-id-ctrl.iter1.txt`, and
  `nvme-id-ns.iter1.txt` exist.
- `sha256.iter1.log` reports `OK`.
- Test 2 includes `fio.iter1.log` with `err= 0` or no fio error.
- `nvme-list-subsys.final.json` does not contain the test NQN.
- `nvme-list-subsys.after.json` does not contain the test NQN.
- `processes.after.txt` has no live `blockmaster` or `blockvolume` process
  from this run.
- `blockvolume.log` has no NVMe session panic and no repeated protocol errors.
- `blockvolume.log` contains one `nvme: stats ...` line with non-zero
  `writes`.
- Report whether the host used `inline_writes` or `r2t_writes` for the
  workload. This is evidence for NVMe-P2, not a pass/fail performance claim.

## Evidence To Report

- branch and commit SHA,
- host distro/kernel,
- `nvme version`,
- exact command,
- artifact root,
- final PASS or failure line,
- `sha256.iter1.log`,
- fio summary for Test 2,
- final `nvme list-subsys -o json` state,
- final `nvme: stats ...` line from `blockvolume.log`,
- cleanup state.

## Non-Claims

- This is not ANA.
- This is not multipath.
- This is not CSI.
- This is not RoCE or performance evidence.
- This is not a replacement for the later NVMe-P2 in-capsule/R2T audit.
