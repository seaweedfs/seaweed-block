# Finished Plan: iSCSI Session And Backend Pressure Hardening

Status: historical reference. Closed on 2026-05-11 after fast protocol
pressure coverage, product-backed L2 durable restart coverage, review
hardening, and one Linux/open-iscsi milestone gate passed at commit `a0550be`.

Current work remains tracked in `../current-plan.md`.

## Goal

Harden the iSCSI frontend behind the now-green Linux and Windows OS initiator
gates by moving remaining risk into fast component and L2 tests first.

This plan deliberately avoided starting with another long m02 integration loop.
The rule was:

```text
component first -> product-backed gate only when the component boundary is green
```

## Close State

| Gate | Status | Evidence |
| --- | --- | --- |
| Fast iSCSI protocol pressure pack | PASS | `go test ./core/frontend/iscsi -run 'TestP2_ISCSI\|TestP1_ISCSI\|TestDataInWriter\|TestDataOut' -count=1`, `0.466s` |
| Product-backed durable restart L2 pack | PASS | `go test -tags subprocess ./cmd/blockvolume -run 'TestISCSI_L2Durable(RestartReconnect_(PreservesData\|RepeatedCycles)\|SyncCacheRestart_AcceptsSyncAndPreservesWrites)' -count=1 -v`, `64.151s` |
| Internal review | PASS | Reviewer found no blocking issues after readiness/test-claim fixes |
| Linux open-iscsi milestone gate | PASS | `iscsi-os-initiator-compat-chain`, run `20260511-091432-d7f6`, `22/22` actions, `1m13s`, product `a0550be` |

Milestone bundle:

```text
V:/share/g15d-k8s/testops-runs/iscsi-pressure-close/20260511-091432-d7f6
```

Remote product checkout used for the milestone gate:

```text
/tmp/seaweed-block-plan-roadmap-refresh-devrun
commit a0550be6a2fc02a4b33b76152d0deb47a5d8cef9
```

## Delivered Tests

### Durable Restart Reconnect

Added:

```text
TestISCSI_L2DurableRestartReconnect_PreservesData
```

Shape:

- start single-slot `blockmaster`,
- start `blockvolume` with `--durable-root`, `walstore`, and iSCSI,
- write one 4 KiB block through an iSCSI session,
- stop `blockvolume`,
- restart `blockvolume` with the same durable root and same iSCSI address,
- reconnect over iSCSI and read the same LBA,
- assert byte equality.

This pins the smallest product-backed restart/reconnect path without
Kubernetes or an OS initiator.

### Repeated Durable Reconnect Cycles

Added:

```text
TestISCSI_L2DurableRestartReconnect_RepeatedCycles
```

Shape:

- start the same single-slot product stack,
- write a distinct 4 KiB LBA in each cycle,
- read all prior LBAs,
- logout,
- restart `blockvolume` with the same durable root and iSCSI address,
- repeat for three cycles,
- final reconnect verifies every written LBA.

This turns one restart into a short pressure loop and catches durable-root or
session cleanup regressions that a single cycle can miss.

### Sync Cache Acceptance With Durable Restart

Added:

```text
TestISCSI_L2DurableSyncCacheRestart_AcceptsSyncAndPreservesWrites
```

Shape:

- start the same single-slot product stack,
- write 12 distinct 4 KiB LBAs through iSCSI,
- issue `SYNCHRONIZE CACHE(10)` every four writes plus a final sync,
- stop `blockvolume`,
- restart with the same durable root and iSCSI address,
- reconnect and verify all written LBAs.

Helper added:

```text
g8IscsiClient.syncCache10
```

Important claim boundary: this test proves the product iSCSI stack accepts
`SYNCHRONIZE CACHE(10)` and preserves acknowledged writes across a clean durable
restart. It is not crash, FUA, or power-loss durability evidence.

## Review Fixes

Internal review caught test-quality issues before close. The final commit
addressed them:

- Added explicit iSCSI listener readiness polling after product status reports
  the replica healthy. Status readiness alone is not a TCP/listener contract.
- Hardened single-slot master startup parsing with a ready-line timeout and
  read-error check.
- Narrowed the Sync Cache test name and plan wording to avoid overclaiming
  crash durability.
- Expanded restart verification from representative LBAs to all 12 written
  LBAs.

## Milestone Gate

Runner-native gate:

```text
swblock run --results-dir V:/share/g15d-k8s/testops-runs/iscsi-pressure-close \
  --env product_root=/tmp/seaweed-block-plan-roadmap-refresh-devrun \
  --env ssh_key=C:/work/dev_server/testdev_key \
  testops/scenarios/iscsi-os-initiator-compat-chain.yaml
```

Result:

```text
=== iscsi-os-initiator-compat-chain === PASS (1m13.531s)
22 actions: 22 passed, 0 failed
run bundle: V:\share\g15d-k8s\testops-runs\iscsi-pressure-close\20260511-091432-d7f6
```

The milestone gate re-proves the real Linux kernel initiator path after the
fast/L2 gates:

- `iscsiadm` discovery/login,
- ext4 format,
- mount,
- payload write/read checksum,
- `fio` stress with `err= 0`,
- logout,
- no active iSCSI sessions,
- no blockmaster/blockvolume process residue,
- dmesg delta gate clean.

## What This Adds To The Roadmap

This phase sits between protocol credibility and beta release gates:

- It keeps iSCSI debugging mostly in fast local/component/L2 loops.
- It adds product-backed durable restart evidence without Kubernetes.
- It keeps OS initiator and beta suites as milestone evidence, not the first
  debugging loop.
- It provides a pattern for future pressure hardening: narrow component test,
  product-backed L2 confirmation, then one milestone gate.

## Non-Claims

- Not performance evidence.
- Not HA or multi-replica failover evidence.
- Not MPIO evidence.
- Not broad OS matrix evidence.
- Not Kubernetes operator cleanup evidence.
- Not crash, FUA, or power-loss durability evidence.
- Not a replacement for beta-hardening suite validation.
