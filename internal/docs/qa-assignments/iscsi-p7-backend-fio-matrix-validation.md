# QA Assignment: iSCSI P7 Backend FIO Matrix

Status: PASS on M02.
Branch: `iscsi/p7-performance-matrix-clean`.
Commit: `6826139ed9551df957a10a1cbc7368f63cccc36c`.
Scope: experimental Linux iSCSI fio comparison between `walstore` and
`smartwal`.

## Preconditions

- Linux host with:
  - `open-iscsi`,
  - `iscsi_tcp`,
  - `mkfs.ext4`,
  - `mount`,
  - `fio`,
  - sudo access.
- No active sw-block iSCSI sessions before the run.

## Run

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-iscsi-p7-backend-fio" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_BACKEND_FIO_RUNTIME=60 \
SW_BLOCK_BACKEND_FIO_SIZE=128m \
bash scripts/run-iscsi-backend-fio-matrix.sh "$PWD"
```

Optional network/backend controls:

```bash
SW_BLOCK_BACKEND_MATRIX="walstore smartwal"
SW_BLOCK_BACKEND_BASE_ISCSI_PORT=3290
```

## Expected

- `walstore` run completes.
- `smartwal` run completes.
- each run produces:
  - `run.log`,
  - `blockmaster.log`,
  - `blockvolume.log`,
  - `fio.iter1.log`,
  - `iscsi-sessions.final.txt`,
  - `cleanup.log`.
- top-level `summary.md` contains one row per backend.
- final `iscsiadm -m session` is empty.

## Evidence To Report

- branch and commit SHA,
- host/kernel,
- exact command,
- artifact root,
- `summary.md`,
- fio read/write lines for both backends,
- any iSCSI session errors in daemon logs,
- cleanup state.

## Non-Claims

- This is not a product performance claim.
- This is not a benchmark publication.
- This is single-host Linux iSCSI only.
- This does not compare RoCE/25GbE unless the report explicitly states the
  network path and portal addresses used.
- This does not prove Kubernetes performance.

## QA Result

- Host:
  - M02, `192.168.1.184`,
  - Ubuntu 24.04.3 LTS,
  - Linux 6.17.0-22-generic.
- Tooling:
  - `fio-3.36`,
  - `iscsiadm 2.1.9`.
- Path:
  - Linux iSCSI loopback only, `127.0.0.1`.
- Command:

```bash
SW_BLOCK_BACKEND_FIO_RUNTIME=60 SW_BLOCK_BACKEND_FIO_SIZE=128m \
  bash scripts/run-iscsi-backend-fio-matrix.sh "$PWD"
```

- Artifact root:
  - `/mnt/smb/work/share/g15d-k8s/20260506T215457Z-iscsi-p7-backend-fio`.
- Final line:
  - `[backend-matrix] PASS: backend fio matrix completed`.
- Results:
  - `walstore`: PASS, about 124 read IOPS / 124 write IOPS.
  - `smartwal`: PASS, about 124 read IOPS / 125 write IOPS.
- Observed session errors:
  - each backend logged the expected discovery/login-phase EOF/reset pattern,
  - no session errors during the fio body.
- Cleanup:
  - `iscsiadm -m session`: no active sessions,
  - no `blockmaster` or `blockvolume` processes,
  - no multipath device used by this test.
