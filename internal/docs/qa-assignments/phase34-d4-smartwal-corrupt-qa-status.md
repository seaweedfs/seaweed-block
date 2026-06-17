# QA Status - Phase 34 D4 SmartWAL Corruption Restart Gate

Verdict: **NOT PASS, NOT a product-Ready failure yet.** The gate has never
reached its actual product assertion. The scenario had not been run live
before push; QA fixed three sequential scenario-mechanics bugs, verified the
corruption-injection core is correct, then hit a fourth blocker that is dev's
to own.

Date: 2026-05-29

Source commit under test: `6bdb30f testops: add smartwal corruption restart gate`
Scenario: `testops/scenarios/helm-smartwal-corrupt-restart-chain.yaml`

Dev follow-up after this QA report: the three QA mechanics fixes were folded
into the scenario, and issue 4 was redesigned to keep blockmaster running. The
current scenario now scales only the target blockvolume Deployment to zero,
corrupts that volume's SmartWAL under `sudo -n`, scales the target Deployment
back to one replica, captures whether the target restarted/rolled out, and then
continues to the status-surface assertion. D4 still requires a fresh QA rerun;
this report remains the record of the failed `6bdb30f` cycle.

## What's Good (give credit)

My biggest worry in the Phase 34 plan review was that `corrupt_wal` would
target the V2 monolithic layout and silently miss the V3 SmartWAL — i.e. a
test that "runs corruption" but proves nothing. **Dev solved this correctly.**

`sw-block-testutil smartwal-corrupt-latest-record` reads the real SmartWAL
header layout and refuses to mutate unless the offset is genuinely inside the
WAL region. Live evidence from the QA run:

```text
wal_offset=4096
target_record_offset=5792
mutated_offset=5823
target_offset_inside_wal=true
target_offset_inside_extent=false
```

The corruption landed on a real WAL record (offset 5823, inside the WAL ring,
not the extent region). This is genuine L2 injection, not L0 self-proof. The
D4-0 prerequisite I asked for is satisfied.

## Where It Broke (4 sequential issues)

| # | Phase | Issue | Class | Fixed by |
|---|---|---|---|---|
| 1 | generate_smartwal_values | `state_hostpath` env contains nested `{{ run_id }}`; runner does not recursively expand it, so `--state-hostpath {{ state_hostpath }}` leaked literal `{{ run_id }}` tokens as stray args | scenario | QA (inlined run_id at 4 use sites, removed nested env var) |
| 2 | helm_install_stack | python YAML edit used 2-space marker `  stateHostPath:` matching the suffix of the generator's real 4-space `    stateHostPath:`, corrupting indentation (`durableImpl` at 4 spaces, `stateHostPath` at 2) -> invalid YAML | scenario | QA (rewrote python to insert `durableImpl` at correct indent right after `blockmaster:`) |
| 3 | corrupt_smartwal_and_reconcile | `sw-block-testutil` invoked without `sudo`; the hostPath store file is root-owned -> `permission denied` | scenario | QA (added `sudo -n`) |
| 4 | corrupt_smartwal_and_reconcile | after `scale blockmaster=0 -> corrupt WAL -> scale blockmaster=1`, blockmaster never returns to rollout-complete: `0 of 1 updated replicas are available`, 300s timeout | scenario-design OR product | **NOT fixed by QA — handed back** |

Three of four are unambiguous scenario typos. Their existence means the
scenario was pushed after `swblock validate` (syntax) + unit tests only, never
a live lab run. Worth noting as a process gap: a "live gate" should be run live
at least once before being handed to QA as a gate.

## Issue 4 — why QA stopped here

After the corruption succeeds, the scenario does:

```text
scale deploy/sw-blockmaster --replicas=0
corrupt the SmartWAL record
scale deploy/sw-blockmaster --replicas=1
rollout status deploy/sw-blockmaster --timeout=300s   <-- times out here
```

`corrupt/wait-blockmaster-after-corrupt.txt`:

```text
Waiting for deployment "sw-blockmaster" rollout to finish:
  0 of 1 updated replicas are available...
```

blockmaster does not own the corrupt WAL (that is blockvolume's durable data),
so in principle corrupting a blockvolume's WAL should not stop blockmaster from
rolling out. Two possibilities, and I cannot disambiguate them by patching:

- **(a) scenario-design fragility**: the scale-0 / corrupt / scale-1 blockmaster
  dance is the wrong way to inject. A cleaner injection would leave blockmaster
  running and only restart the single blockvolume Deployment whose WAL was
  corrupted, then observe whether THAT volume goes Ready.
- **(b) real product gap**: blockmaster genuinely fails to become ready after
  this sequence (e.g. its reconcile loop wedges when a blockvolume Deployment
  was deleted while its durable hostPath holds a corrupt WAL).

I did not patch issue 4 because:

1. It is no longer a typo; it is the scenario's K8s orchestration logic, which
   is dev authoring work, not QA mechanics.
2. It may BE the product signal the gate is meant to surface. Patching the wait
   could mask a real recovery gap.

The gate's actual product question — **does any status surface report
`Ready=True` for a volume whose WAL is corrupted?** — remains unanswered,
because the run never reaches the `assert_no_false_ready_after_corruption`
phase.

## QA Scenario Fixes Applied (for dev to fold in)

All three are in my working copy of
`testops/scenarios/helm-smartwal-corrupt-restart-chain.yaml`:

1. Removed `state_hostpath` env var; inlined
   `/var/lib/sw-block/testops-{{ run_id }}-smartwal-corrupt` at all 4
   references (lines ~44, ~86, ~162, ~271).
2. Rewrote the values-patch python to insert `    durableImpl: smartwal`
   (4-space, matching generator indent) immediately after the `blockmaster:`
   line, instead of the fragile 2-space suffix-match replace.
3. Added `sudo -n` to the `sw-block-testutil smartwal-corrupt-latest-record`
   invocation.

These three are pure mechanics; dev should accept them. They are not the
product fix.

## Recommended Next Step For Dev

Redesign issue-4 injection to NOT bounce blockmaster:

```text
keep blockmaster running
scale ONLY the target blockvolume Deployment to 0  (or delete it)
sudo corrupt that volume's SmartWAL record
let the launcher recreate the blockvolume workload
observe: the recreated blockvolume must refuse corrupt replay and the status
  surface must show NOT Ready (Blocked or Unknown with a stable reason),
  never Ready=True
```

This keeps the fault localized to the data path the gate cares about, removes
the blockmaster-rollout dependency entirely, and lets the run reach the actual
`assert_no_false_ready_after_corruption` assertion.

Open product question to confirm during that redesign: does the product even
HAVE a stable reason code for corrupt-WAL refusal yet? The plan's D4 escape
hatch (`wal_corrupt` / `recovery_evidence_invalid` / `durable_recovery_failed`
"may need implementation alignment") suggests it might not. If the blockvolume
crashloops without emitting a status-surface reason, that itself is the gap to
file.

## Lab State

Clean after the always-run cleanup phase: no helm release, no iSCSI sessions,
no multipath, no sw-block pods, no testops hostPath residue.

## Bottom Line

- D4-0 (V3-aware corruption injection): **dev did it right.** Verified live.
- D4 scenario: **3 mechanics bugs fixed by QA, 1 orchestration blocker
  remaining.** Never run live before push.
- D4 product claim (no false Ready on corrupt WAL): **still unproven** — the
  gate has not reached its product assertion.
- This is the correct kind of gate to have; it just is not green yet, and it is
  not green because of scenario plumbing, not (yet) because of a confirmed
  product behavior.
