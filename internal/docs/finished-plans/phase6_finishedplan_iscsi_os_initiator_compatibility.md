# Finished Plan Draft: iSCSI OS-Initiator Compatibility

Status: draft, not closed. This file is staged as the eventual archive for
`../current-plan.md` after Windows validation either passes or is explicitly
deferred.

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
| Windows built-in iSCSI Initiator | Pending QA | `../qa-assignments/iscsi-os-windows-initiator-validation.md` |
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

Pending QA.

Expected validation shape:

- Hold a V3 iSCSI target on m02 in target-only mode.
- Keep the target loopback-only.
- Use SSH local port-forward from Windows to m02.
- Use Windows built-in iSCSI Initiator to discover/connect.
- Initialize/format NTFS.
- Write/read checksum.
- Disconnect and verify no session/process residue.

If this passes, update this section with run ID, host version, target artifact
path, and cleanup evidence. If it fails, do not close this plan; classify the
first failing point and add a fast component/protocol regression before another
long validation loop.

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
