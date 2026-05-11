# Finished Plan: iSCSI OS-Initiator Compatibility

Status: historical reference. Closed on 2026-05-11 after both Linux/open-iscsi
and Windows built-in iSCSI Initiator validation passed.

Current work remains tracked in `../current-plan.md`.

## Goal

Prove V3 iSCSI can survive real OS initiators doing normal filesystem work,
not just in-process protocol clients:

```text
discover/login -> block device -> mkfs/format -> mount -> write/read verify
-> stress I/O -> disconnect -> no residue
```

This plan closes only OS-initiator compatibility. It does not claim
performance, HA, MPIO, broad distro coverage, multi-node Kubernetes attach,
upgrade safety, or production readiness.

## Current Close State

| Gate | Status | Evidence |
| --- | --- | --- |
| Linux open-iscsi OS initiator | PASS | `iscsi-os-initiator-compat-chain`, run `20260511-014714-eca5`, product `9e8ffab` |
| Windows built-in iSCSI Initiator | PASS | Target artifact `/mnt/smb/work/share/g15d-k8s/20260511T085158Z-iscsi-windows-target/`, product `9e8ffab` |
| External target hold mode | PASS | m02 target-only startup emitted `target-ready.env` at product `9e8ffab` |
| V2 comparison decision | Done | No broad V2 port while Linux OS gate is green; future failures reduce to focused component tests |

## Linux Evidence

Runner-native gate:

```text
swblock run testops/scenarios/iscsi-os-initiator-compat-chain.yaml
```

Latest green:

- Run ID: `20260511-014714-eca5`.
- Product commit: `9e8ffab`.
- Result: `PASS`, `22/22` actions, `1m13s`.
- Host: m02 Linux/open-iscsi.
- Target size: 65,536 x 4 KiB blocks, 256 MiB.
- Workload:
  - `iscsiadm` discovery/login,
  - kernel block device materialization,
  - `mkfs.ext4`,
  - mount,
  - random payload write/read checksum,
  - `fio` randrw for 60 seconds,
  - logout and node cleanup.

Artifact claims:

- `run.log`: final `[iscsi-os] PASS`.
- `mkfs.iter1.log`: ext4 format completed.
- `sha256-check.iter1.log`: payload checksum OK.
- `fio.iter1.log`: `err= 0`.
- `iscsi-sessions.after.txt`: `iscsiadm: No active sessions.`
- process assertions: no `blockmaster` or `blockvolume` residue.
- `dmesg.new.txt`: attach, mount, unmount, cache sync, ALUA detach only; no
  `DID_BAD_TARGET`, `I/O error`, `Buffer I/O`, or rejecting I/O.

## Windows Evidence

QA result: PASS.

Environment:

- Product commit on m02: `9e8ffab`.
- Windows host: Windows 11 Pro (`PING-R13`).
- m02 kernel: `6.17.0-19-generic`, Ubuntu 24.04.3 LTS.
- Target artifact:
  `/mnt/smb/work/share/g15d-k8s/20260511T085158Z-iscsi-windows-target/`.

Validation shape:

- Held a V3 iSCSI target on m02 in target-only mode.
- Kept the target loopback-only.
- Used SSH local port-forward from Windows to m02.
- Used Windows built-in iSCSI Initiator to discover/connect.
- Initialized/formatted NTFS.
- Wrote, copied, and read back a 4 MiB payload with byte-exact checksum.
- Disconnected and verified no session/process residue.

Evidence:

- `run.log`: target-only mode, portal `127.0.0.1:36260`, hold complete,
  cleanup.
- `target-ready.env`: `SW_BLOCK_ISCSI_TARGET_READY=1`, IQN, portal.
- `blockvolume.log`: iSCSI listener and per-LBA durable write dispatches during
  NTFS journal and payload/copy writes.
- Windows: target IQN discovered; connect succeeded with `IsConnected=True`,
  `NumberOfConnections=1`.
- Windows: exactly one new iSCSI disk online.
- Windows: NTFS format succeeded, 240 MiB on `S:`.
- Windows: 4 MiB write/copy/read checksum matched.
- Windows: disconnect succeeded; `Get-IscsiSession` empty after cleanup.
- m02: target self-exited at hold window; no lingering `blockmaster` or
  `blockvolume`.

Claim:

```text
Windows 11 Pro built-in iSCSI Initiator can discover, connect to, format NTFS,
write/read with byte-exact checksum, and cleanly disconnect from a V3 iSCSI
target at commit 9e8ffab over an SSH local-forward tunnel to a loopback-bound
m02 target.
```

QA found two documentation portability issues and the assignment was corrected:

- PowerShell 5.1 lacks static `RandomNumberGenerator.Fill`; use
  `RNGCryptoServiceProvider.GetBytes`.
- `Remove-IscsiTargetPortal -TargetPortalPortNumber` needs a `[UInt16]` cast on
  PowerShell 5.1.

## Harness Changes

Added runner scenario:

- `testops/scenarios/iscsi-os-initiator-compat-chain.yaml`

Added script support in `scripts/run-iscsi-os-smoke.sh`:

- timestamp-based dmesg delta gate,
- target-only hold mode for external initiators,
- loopback/external initiator portal separation,
- machine-readable `target-ready.env`.

Important security behavior:

- `blockvolume` still rejects unauthenticated non-loopback iSCSI binds.
- Windows validation uses SSH tunneling instead of weakening that product
  guard.

## V2 Comparison Decision

The V2 audit remains useful as a coverage inventory, but this plan does not
port broader V2 session architecture.

Reason:

- The Linux OS-initiator gate is green.
- Historical V2 deltas relevant to the original failure are already covered in
  V3 or tracked as future component stress coverage:
  - Data-Out collection,
  - bounded pending queue,
  - Data-Out timeout,
  - multi-PDU Data-In.

Future rule:

```text
If Windows or soak fails, reduce the first failure to a focused component or
protocol test before changing the long runner gate.
```

## Non-Claims

- No performance benchmark.
- No production HA.
- No MPIO.
- No CHAP claim from this plan.
- No NVMe claim.
- No multi-node Kubernetes claim.
- No broad distro or Windows Server matrix.
- No operator lifecycle claim.
