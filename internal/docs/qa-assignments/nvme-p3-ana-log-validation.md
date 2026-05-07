# QA Assignment: NVMe P3 ANA Identify / Log Validation

Status: ready for QA.
Branch: `frontend/nvme-ana-parity-plan`.
Scope: first real Linux `nvme-cli` validation of V3 ANA Identify fields plus
ANA log page.

## Goal

- Prove a real Linux host can explicitly read NVMe ANA log page `0x0c` from
  `blockvolume`.
- Prove Identify Controller and Identify Namespace advertise ANA only after the
  log page and product state provider are wired.
- Confirm the target reports one valid ANA group with a non-zero group id,
  namespace count `1`, the expected NSID, and a recognized ANA state.
- This is not a multipath claim. It only validates the single-path primary
  state and Identify/log consistency.

## Preconditions

- Same host requirements as `nvme-p1-os-smoke-validation.md`.
- `python3` is required for parsing the raw ANA log.
- `nvme-cli` must support:

```bash
nvme get-log /dev/<device> -i 0x0c -l 40 -b
```

## Test 1: Basic ANA Log Capture

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-nvme-p3-ana" \
SW_BLOCK_ARTIFACT_DIR="/mnt/smb/work/share/g15d-k8s/${RUN_ID}" \
SW_BLOCK_NVME_ITERATIONS=1 \
SW_BLOCK_NVME_STRESS=none \
SW_BLOCK_NVME_COLLECT_ANA=1 \
bash scripts/run-nvme-os-smoke.sh "$PWD"
```

Expected final line:

```text
[nvme-os] PASS: 1 x nvme connect mkfs mount write/read disconnect
```

## Acceptance

- `run.log` contains:
  - `collect_ana=1`,
  - the final PASS line.
- `nvme-ana-log.iter1.bin` exists and is 40 bytes.
- `nvme-ana-log.iter1.summary` exists and contains:
  - `ana_group_count=1`,
  - `ana_group_id=<non-zero>`,
  - `ana_nsid_count=1`,
  - `ana_state=0x01 optimized` for the single primary path,
  - `ana_nsid=1`.
- `nvme-id-ctrl.iter1.txt` and `nvme-id-ns.iter1.txt` are captured.
- Identify ANA advertisement is on and consistent with the log page:
  - controller `cmic` shows ANA support,
  - controller `anacap` is non-zero,
  - controller `anagrpmax=1`,
  - controller `nanagrpid=1`,
  - namespace `anagrpid` matches `ana_group_id` from
    `nvme-ana-log.iter1.summary`.
- OAES ANA Change Notice must remain off unless an async event producer is
  implemented. Do not fail because OAES is zero.
- Cleanup state is the same as P1:
  - final subsystem list does not contain the test NQN,
  - no `blockmaster` / `blockvolume` processes from this run remain.

## Evidence To Report

- branch and commit SHA,
- host distro/kernel,
- `nvme version`,
- exact command,
- artifact root,
- final PASS or failure line,
- `nvme-ana-log.iter1.summary`,
- relevant `nvme id-ctrl` / `nvme id-ns` ANA lines,
- final `nvme: stats ...` line from `blockvolume.log`,
- cleanup state.

## Non-Claims

- This is not Linux NVMe multipath.
- This is not mounted failover.
- This does not validate ANA Change Notice events.
- This is not Kubernetes CSI.
- This is not a performance test.
