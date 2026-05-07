# QA Assignment: NVMe P4 Multipath Lab Validation

Status: draft, blocked until P2 and P4-A script are ready.
Branch: `frontend/nvme-ana-parity-plan`.
Scope: real Linux NVMe/TCP two-path and native multipath validation.

## Goal

- Validate that Linux can connect two NVMe/TCP paths for one V3 volume.
- Determine whether native NVMe multipath groups those paths.
- Do not claim mounted failover until the dedicated failover test passes.

## Preconditions

- Linux host with:
  - `nvme-cli`,
  - `nvme_tcp`,
  - native NVMe multipath enabled or explicitly reported disabled,
  - `mkfs.ext4`,
  - `mount`,
  - `sha256sum`,
  - sudo access.
- Record:
  - distro/kernel,
  - `nvme version`,
  - `cat /sys/module/nvme_core/parameters/multipath` if present,
  - relevant `dmesg` warnings after the run.

## Test 1: Two-Path Discovery

Status: needs script.

Expected script shape:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p4-two-path" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
bash scripts/run-nvme-multipath-smoke.sh "$PWD"
```

Expected:

- two NVMe/TCP portals connect for the same test volume,
- `nvme list-subsys -o json` shows both paths,
- both paths expose the same NGUID / EUI-64 / NSID,
- ANA log can be read without kernel warning,
- cleanup disconnects both paths.

Evidence:

- `nvme list`.
- `nvme list-subsys -o json`.
- `nvme id-ctrl` per path if addressable.
- `nvme id-ns` per path if addressable.
- ANA log summary per path.
- `dmesg` delta for `nvme_parse_ana_log`, reset, or I/O errors.

## Test 2: Native Multipath Grouping

Status: blocked on Test 1 and host multipath setting.

Expected:

- If native NVMe multipath is enabled:
  - host presents one logical namespace with multiple paths, or
  - QA records why Linux did not group them.
- If native NVMe multipath is disabled:
  - report disabled state and stop here; do not fail product correctness.

Evidence:

- `/sys/module/nvme_core/parameters/multipath`.
- `nvme list-subsys -o json`.
- `/sys/class/nvme-subsystem/*` path data if available.

## Test 3: Mounted Failover

Status: future, do not run until Test 1/2 pass.

Expected:

- mount through the multipath namespace,
- pre-failover checksum write/read,
- kill active path,
- authority/ANA state moves,
- post-failover read/write succeeds,
- old primary rejects stale writes,
- cleanup leaves no NVMe connections or target processes.

## Non-Claims

- No CSI.
- No RoCE.
- No performance claim.
- No long-running HA claim.
- No OAES ANA Change Notice claim.
