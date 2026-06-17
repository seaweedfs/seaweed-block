# QA Verification #3 - Phase 34 D4 SmartWAL (PASS)

Verdict: **PASS — no false Ready=True after SmartWAL corruption.** The
contract-level fix `09aa6fe master: require primary readiness evidence` works:
the corrupted volume now reports `Ready=Unknown / status=unknown` instead of
the false `Ready=True reason=first_volume_verified` seen in the three prior
cycles. All 8 required checks satisfied.

One scenario/lab-infra bug found and worked around (not a product issue) — see
"Scenario Gap" below; it must be fixed or the gate is non-deterministic.

Date: 2026-06-01

Source: branch `phase33-testops-failure-hardening` @ `d86a195`, with product
fix `09aa6fe` + test `7fa34a7`.
Passing run: `20260601-020747-5a1f`, 30/30 actions PASS.

## Required Checks

| # | Check | Result | Evidence |
|---|---|---|---|
| 1 | `target_offset_inside_wal=true` | PASS | corruption stdout |
| 2 | `target_offset_inside_extent=false` | PASS | corruption stdout (`mutated_offset=5823`) |
| 3 | blockvolume logs WAL integrity / durable recovery fault | PASS | `failing closed`; `WALIntegrity: CRC mismatch LSN=45 ... WAL integrity fault`; `durable recovery faulted; ... local readiness remains blocked`; `NOT applying primary assignment r1@1 to adapter` |
| 4 | surface does NOT show Ready=True | PASS | `ready_true_after_corruption=false`; operator-snapshot per-volume `status=unknown`, Condition `Ready=Unknown` |
| 5 | Preferred: Blocked/Unknown with `reason=wal_integrity_fault` | PARTIAL | It is **Unknown** (good) but `reason_after_corruption=unknown`, not `wal_integrity_fault`. See note below. |
| 6 | Acceptable: any non-ready generic reason, no Ready=True | **PASS** | `status=unknown`, `Ready=Unknown`, no `Ready=True` on any surface |
| 7 | `cleanup_status=ok` | PASS | cleanup-summary |
| 8 | iSCSI/node-DB/multipath/dmsetup/k8s/process residue clean | PASS | all counts 0; post-run host audit: helm none, no iSCSI sessions, no multipath, no dmsetup, no pods |

The gate passes on **check 6** (acceptable generic non-ready). Check 5
(preferred specific `wal_integrity_fault` reason) is **not yet met** — see the
non-blocking note.

## How the Fix Works

`09aa6fe` implemented the contract-level fix recommended in QA verify2
(Option 2): the ManagedVolume projection now refuses to claim `Ready=True` for
an assigned primary that has not positively confirmed local readiness.

The chain now:

```text
SmartWAL CRC mismatch
-> storage fails closed (WALIntegrity fault)              [85d9375]
-> blockvolume blocks local readiness, does not publish   [954083a]
-> blockmaster projection: no positive readiness => Unknown, NOT Ready  [09aa6fe]
-> operator-snapshot: status=unknown, Ready=Unknown
```

This is the correct negative-first behavior: a corrupted volume is no longer
confidently reported healthy. It closes the false-Ready bug (#51) that survived
three prior fix layers.

## Non-Blocking Note: reason is generic `unknown`, not `wal_integrity_fault`

The volume correctly reports NOT-Ready, but the status surface shows
`reason=unknown` rather than the specific `wal_integrity_fault`. The
`wal_integrity_fault` text lives in the blockvolume LOG, not in the
operator-snapshot `reason_code`. This is the "Option 1" half (carry the fault
reason through the heartbeat/status channel) that `09aa6fe` did not implement —
it took Option 2 (require positive readiness) only.

Per the assignment this is explicitly acceptable for this slice (check 6). The
preferred `reason=wal_integrity_fault` (check 5) remains a follow-up so a cold
operator sees WHY the volume is Unknown without reading blockvolume logs. Not a
blocker for D4 close.

## Scenario Gap (must fix — gate is non-deterministic without it)

The first verify-3 rerun (`20260601-015115-61a8`) FAILED at `helm_install_stack`
with a 10-minute timeout. Root cause was NOT the product:

```text
blockmaster: flag provided but not defined: -launcher-durable-impl
```

The `helm-smartwal-corrupt-restart-chain.yaml` build phase rebuilds and imports
`sw-block:local` only on `single_node` (m02). But the chart's blockmaster
Deployment is not pinned to m02, so k3s scheduled it on **m01**, which still had
a STALE `sw-block:local` (predating `8a6737e helm: expose launcher durable
impl`). The stale binary rejected the `--launcher-durable-impl` flag the chart
emits -> CrashLoopBackOff -> `helm install --wait` hung -> timeout.

Per-node image state at the failure:

```text
m01  (192.168.1.181): launcher-durable-impl flag = 0  (STALE)
m02  (192.168.1.184): launcher-durable-impl flag = 1  (fresh, built this run)
tp01 (192.168.1.188): launcher-durable-impl flag = 0  (STALE)
```

QA unblocked by exporting the fresh `sw-block:local` from m02 and importing it
to m01 + tp01, then re-running — which PASSED 30/30.

This means the prior "successful" D4 runs passed only because blockmaster
happened to schedule on m02. The gate is non-deterministic: it fails whenever
blockmaster lands on a node with a stale image.

Fix shape (pick one):

1. **Pin blockmaster to `single_node`** in the smartwal scenario values
   (nodeSelector kubernetes.io/hostname=m02), so it always runs the freshly
   built image. Smallest change, matches the single-node intent.
2. **Import `sw-block:local` to all schedulable nodes** in the build phase
   (the multi-volume scenarios already do `SW_BLOCK_IMPORT_K3S_NODES=...`).
3. **Pre-clean stale images**: have `pre_run_cleanup` remove `sw-block:local`
   from all nodes so a stale one can never be picked.

Option 1 is the cleanest for this single-node scenario.

## Lab State

Clean after the always-run cleanup + QA teardown: no helm release, no iSCSI
sessions, no multipath, no dmsetup, no sw-block pods. Temp image tars QA
distributed to m01/tp01 were removed.

## Bottom Line

- **D4 product assertion: PASS.** SmartWAL corruption no longer produces false
  Ready=True; the volume reports `Ready=Unknown` via the new
  require-positive-readiness projection (`09aa6fe`).
- **#51 is fixed** across all three layers (storage fail-closed, volume
  readiness block, projection positive-readiness requirement).
- **Two follow-ups, neither blocking D4 close:**
  - Surface `reason=wal_integrity_fault` on the status surface (currently
    generic `unknown`) — the Option-1 half.
  - Fix the scenario's stale-image non-determinism (pin blockmaster to
    single_node, or refresh/clean images on all nodes).
- Per the assignment, **D4 can close** and dev can write the D6 close report.
  Recommend filing the two follow-ups so they are tracked, not lost.
