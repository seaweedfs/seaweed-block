# QA Assignment: NVMe P2 Inline / R2T Validation

Status: ready for QA.
Branch: `frontend/nvme-ana-parity-plan`.
Scope: classify Linux NVMe/TCP write transport behavior against V3.

## Goal

- Confirm small writes use in-capsule inline data.
- Force a larger write profile that exceeds the advertised 32 KiB
  `MaxH2CDataLength` / `IOCCSZ` inline payload and therefore exercises
  R2T/H2CData.
- Treat this as transport-path classification, not a performance claim.

## Preconditions

- Same host requirements as `nvme-p1-os-smoke-validation.md`.
- Use current branch HEAD, not the earlier P1-only commit. The current script
  exposes `SW_BLOCK_NVME_FIO_BS`, `SW_BLOCK_NVME_DD_BS`, and
  `SW_BLOCK_NVME_DD_COUNT`.

## Test 1: Inline Baseline

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p2-inline" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_NVME_ITERATIONS=1 \
SW_BLOCK_NVME_STRESS=fio \
SW_BLOCK_NVME_FIO_RUNTIME=30 \
SW_BLOCK_NVME_FIO_SIZE=128m \
SW_BLOCK_NVME_FIO_BS=4k \
bash scripts/run-nvme-os-smoke.sh "$PWD"
```

Expected:

- final PASS line,
- `blockvolume.log` stats show non-zero `inline_writes`,
- `r2t_writes=0` is acceptable for this test.

## Test 2: R2T Candidate

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p2-r2t" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_NVME_ITERATIONS=1 \
SW_BLOCK_NVME_STRESS=fio \
SW_BLOCK_NVME_FIO_RUNTIME=60 \
SW_BLOCK_NVME_FIO_SIZE=128m \
SW_BLOCK_NVME_FIO_BS=128k \
bash scripts/run-nvme-os-smoke.sh "$PWD"
```

Expected:

- final PASS line,
- fio exits with `err= 0`,
- `blockvolume.log` stats show non-zero `r2t_writes`,
- `h2c_pdus` and `h2c_bytes` are non-zero,
- no repeated NVMe protocol errors.

## Fallback If Test 2 Still Uses Inline Only

Run a larger direct-write profile:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p2-r2t-dd" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_NVME_ITERATIONS=1 \
SW_BLOCK_NVME_STRESS=dd \
SW_BLOCK_NVME_DD_BS=1M \
SW_BLOCK_NVME_DD_COUNT=32 \
bash scripts/run-nvme-os-smoke.sh "$PWD"
```

If both 128 KiB fio and 1 MiB dd still report `r2t_writes=0`, report that as
a host/initiator behavior finding. Do not fail the product solely for that; it
means the Linux initiator did not choose the R2T path under these profiles.

## Acceptance

- At least one run proves inline writes with non-zero `inline_writes`.
- At least one run either:
  - proves R2T with non-zero `r2t_writes`, `h2c_pdus`, and `h2c_bytes`, or
  - explicitly reports that this host did not trigger R2T under the tested
    larger profiles.
- All runs clean up:
  - no test NQN in final `nvme list-subsys -o json`,
  - no stray `blockmaster` / `blockvolume` processes.

## Evidence To Report

- branch and commit SHA,
- host distro/kernel,
- `nvme version`,
- exact commands,
- artifact roots,
- final PASS or failure lines,
- fio or dd summaries,
- final `nvme: stats ...` lines from every `blockvolume.log`,
- cleanup state.

## Non-Claims

- This is not a benchmark.
- This is not ANA.
- This is not multipath or failover.
- This is not CSI.
- This is not RoCE.
