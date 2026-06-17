# QA Finding - Phase 34 D4: False Ready=True After SmartWAL Corruption

Verdict: **GATE NOW WORKS, and it found a real product gap.** The D4 gate
reached its product assertion `assert_no_false_ready_after_corruption` and
**failed because the product reports `Ready=True` for a volume whose SmartWAL
contains a corrupted committed record.** Per the Phase 34 plan, this is a
valid gate result to file as a product gap, not a scenario failure.

Date: 2026-05-29

Source commit under test: `03fe9ae testops: keep smartwal corruption gate scoped to volume`
QA run: `20260529-232752-b23c`
Scenario: `testops/scenarios/helm-smartwal-corrupt-restart-chain.yaml`
Tracking issue: https://github.com/seaweedfs/seaweed-block/issues/51

This supersedes the orchestration blocker (issue 4) in
`phase34-d4-smartwal-corrupt-qa-status.md`. Dev's rewrite (scale only the
target blockvolume, keep blockmaster up) was correct and let the run reach the
real assertion.

## The Gate Now Reaches Its Product Question

| Phase | Result |
|---|---|
| pre_clean / build / generate / install | PASS |
| first_volume_before_corruption | PASS |
| corrupt_smartwal_and_reconcile | PASS (corruption injected + target restarted) |
| assert_no_false_ready_after_corruption | **FAIL: false Ready=True after SmartWAL corruption** |

28 actions, 27 pass, 1 fail. The single fail is the product assertion. This is
the gate doing its job.

## The Finding

### Corruption was real and landed in the WAL

```text
target_record_offset=6016
mutated_offset=6047
target_offset_inside_wal=true
target_offset_inside_extent=false
```

The layout-aware `sw-block-testutil` corrupted a genuine committed SmartWAL
record (not the extent region, not a torn tail it invented).

### The product DID detect the corruption

blockvolume recovery log after restart
(`corrupt/blockvolume-pods.log`):

```text
smartwal: recovery CRC mismatch LSN=45 LBA=0 expected=80824a23 actual=91e6685b — skipping
smartwal: recovery: 40 LBAs verified, 1 torn, frontier=59
blockvolume: durable recovered: recovered LSN=59
```

The SmartWAL CRC check works. It caught the mismatch at LSN=45.

### But it silently skipped the corrupted committed record and reported full health

- Recovery classified the corrupted record as `1 torn` and **skipped LSN=45**,
  then recovered forward to `frontier=59`.
- The blockvolume came up `1/1 Running`, 0 restarts,
  `deployment_rollout_ready=true`.
- The status surface
  (`after-corrupt/corruption-status-summary.txt`):

```text
smartwal_corruption_status_surface=failed
ready_true_after_corruption=true
blocked_true_after_corruption=false
reason_after_corruption=first_volume_verified
```

operator-snapshot.json for the volume:

```json
"status": "ready",
"reason_code": "first_volume_verified",
"conditions": [{"type": "Ready", "status": "True", ...}]
```

So a volume whose durable WAL has a detected CRC failure on a committed record
reports `Ready=True reason=first_volume_verified` — the exact same status as a
clean first volume. No `Blocked`, no `Recovered`-with-loss, no `Degraded`, no
`EvidenceStale`.

## Why This Matters — Two Layers

### Layer 1 (certain): status-surface negative-first violation

This is the gate's literal assertion and it is indisputable. The negative-first
contract from Phase 32 says the product must not claim `Ready=True` against
corrupted or contradictory evidence. Here a known CRC mismatch on a committed
record is fully swallowed and the surface claims healthy-Ready. At minimum the
status model needs a condition for "recovered with a detected WAL integrity
fault" so the user can tell this volume apart from a clean one.

### Layer 2 (needs engine-owner judgment): is the skip itself safe?

The corrupted record is `LSN=45`; recovery reached `frontier=59`. So records
LSN 46..59 are AFTER the corrupted one in the log. The product skipped LSN=45
but kept the later records and declared `recovered LSN=59`.

- If LSN=45's data was already checkpointed into stable extents before the
  corruption, skipping it during WAL replay is harmless — the data lives in the
  extent store.
- If LSN=45 was a committed-but-not-yet-flushed write, skipping it is **silent
  loss of an acknowledged write from the middle of the log**, while later writes
  (46..59) are kept — a consistency violation masked as `Ready=True`.

I cannot determine from logs alone which case applies; that is an engine /
durability-owner call. But the test correctly surfaces the question, and the
"torn write" classification of a *mid-history* corrupted record (not just a torn
tail) deserves scrutiny. Torn-tail discard is safe; mid-history skip is not
obviously safe.

## This Matches the Plan's Anticipated Outcome

The Phase 34 plan D4 explicitly allowed this:

> This gate is allowed to fail if the product does not yet surface a stable
> WAL-corruption reason. A green run is valid only if the corruption evidence
> exists and no status surface claims Ready=True afterward.

And D7's close criteria allowed D4 to be:

> PASS or explicitly blocked by missing product reason code with a concrete
> implementation issue filed.

This run is exactly that case. **D4 is "explicitly blocked by a confirmed
product gap," with this finding as the concrete issue.** The gate should stay
red until the product either:

1. surfaces a stable condition/reason for WAL-integrity-fault recovery (e.g.
   `Recovered=True reason=wal_record_skipped_crc` or
   `Degraded/EvidenceStale` with a `wal_corrupt` reason), so the status is no
   longer a false `Ready=True reason=first_volume_verified`; and
2. the engine owner confirms whether skipping a mid-history corrupted committed
   record is durability-safe, and if not, changes the recovery policy (refuse /
   fence / mark degraded rather than silently continue).

## Recommended Issue To File

Title: "blockvolume reports Ready=True after SmartWAL recovery skips a
CRC-failed committed record"

- Severity: high for the status-surface lie (Layer 1); to-be-determined for the
  durability question (Layer 2), pending engine-owner review.
- Repro: `helm-smartwal-corrupt-restart-chain.yaml` on a smartwal+hostPath
  install, run `20260529-232752-b23c`.
- Evidence: corruption stdout (offset-inside-wal), blockvolume recovery log
  (CRC mismatch / skipping / frontier), operator-snapshot (Ready=True).

## Process Note

The D4 scenario went through 4 issues before reaching this finding: 3 scenario
mechanics bugs (nested template, YAML indent munge, missing sudo — all fixed
during the prior QA cycle and folded in by dev) and 1 orchestration redesign
(blockmaster bounce -> scoped blockvolume scale, fixed by dev in `03fe9ae`).
Only after all four did the gate reach the product assertion. A "live gate"
should be run live once before being treated as ready; the mechanics churn was
avoidable.

That said, the end state is the best possible outcome for a hardening phase:
**a gate that is red for a real reason.** This is worth more than all the green
happy-path runs combined, because it found a correctness gap none of them
could.

## Lab State

Clean after the always-run cleanup: no helm release, no iSCSI sessions, no
multipath, no sw-block pods, no testops hostPath residue.

## Bottom Line

- D4 gate: **functional and red for a real product reason.** Keep it red.
- Product gap: blockvolume silently skips a CRC-failed committed WAL record and
  reports `Ready=True reason=first_volume_verified`. Status-surface lie is
  certain; durability safety of the skip needs engine-owner review.
- Do not mark Phase 34 D4 "passed." Mark it "gate live, product gap filed."
  Per the plan that is a valid D4 close state, but the underlying product issue
  should be tracked, not closed.
