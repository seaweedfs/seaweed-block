# QA Assignment: NVMe P4 Multipath Lab Validation

Status: Test 1/2/3 QA green.
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

Status: PASS on `frontend/nvme-ana-parity-plan@a5ef1a5`.

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
- final line:
  `[nvme-mpath] PASS: two NVMe/TCP paths expose one ANA-aware namespace`.

Observed evidence:

- run ID: `20260507T161800Z-test`.
- `wait_nvme_paths: ok at iter=1 count=2`.
- `nvme_namespace_devices=1`.
- ANA log: `group_count=1`, `group_id=1`, `state=0x01 optimized`,
  `nsid=1`.
- identity: `nguid=24634c35194743419febbb18e06446be`,
  `eui64=24634c3519474341`, `anagrpid=1`.
- cleanup: clean disconnect, `EXIT=0`.

## Test 2: Native Multipath Grouping

Status: PASS with Test 1 on `a5ef1a5`.

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

Status: PASS on `frontend/nvme-ana-parity-plan@e1e0e0c`.

Command:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p4-mounted-failover" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
bash scripts/run-nvme-mounted-failover-smoke.sh "$PWD"
```

Expected:

- mount through the multipath namespace,
- pre-failover checksum write/read,
- kill active path,
- authority/ANA state moves,
- post-failover read/write succeeds,
- old primary rejects stale writes,
- cleanup leaves no NVMe connections or target processes.

Acceptance:

- final line:
  `[nvme-failover] PASS: mounted NVMe multipath workload read/wrote through r1->r2 failover`.
- `pre-check-after-failover.log` reports `/pre.bin: OK`.
- `post-check.log` reports `/post.bin: OK`.
- `status-r2-primary.json` shows r2 primary at epoch `>=2`.
- `nvme-list-subsys.final.json` has no test NQN.
- `processes.after.txt` has no live `blockmaster` or `blockvolume`.

Observed evidence:

- run ID: `20260507T170000Z-nvme-p4-mounted-failover`.
- two TCP paths registered and Linux native multipath merged them to
  `/dev/nvme1n1`.
- `mkfs.ext4`, mount, pre-failover write, and SHA256 capture completed before
  failure injection.
- active r1 was killed; r2 promoted to `Epoch=2`, `AuthorityRole=primary`,
  `FrontendPrimaryReady=true`.
- `pre-check-after-failover.log` reported `/pre.bin: OK`.
- `post-check.log` reported `/post.bin: OK`.
- final cleanup disconnected the test NQN and left no
  `blockmaster`/`blockvolume` process.

Failure evidence:

- `blockvolume-r1.log`, `blockvolume-r2.log`, `blockmaster.log`.
- `nvme-list-subsys.before-failover.json`.
- `nvme-list-subsys.after-failover.json`.
- `path-summary.before-failover.txt`.
- `path-summary.after-failover.txt`.
- `dmesg` delta if the script captures one or if QA observes a kernel error.

## Non-Claims

- No CSI.
- No RoCE.
- No performance claim.
- No long-running HA claim.
- No OAES ANA Change Notice claim.
