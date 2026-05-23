# Phase 27 D5+ Follow-up Gaps

Date: 2026-05-23

QA validation of Phase 27 D1-D4 passed strict (see
`phase27-multi-volume-ha-independence-close-report.md`), but four product-claim
gaps surfaced during the audit. D5 and D6 are now closed by measured evidence;
D7-D8 remain follow-ups before "multi-volume HA independence" is published as a
release-grade claim beyond the alpha lab.

## D5 - Real Stale-Primary I/O Fencing Probe

**Original gap**: `old_primary_stale_io_success_count=0` was literally written
by `scripts/run-multi-volume-mounted-failover.sh`:

```bash
echo "old_primary_stale_io_success_count=0"
```

The scenario then asserts the line exists in the per-volume summary. This is
tautological - it proves the script wrote 0, not that the stale primary
actually rejected I/O. The same pattern is in
`scripts/run-alpha-app-demo.sh:1004,1100`.

**Why it matters**: the strongest single claim Phase 27 makes is "stale
primary fenced; old-primary stale I/O success count = 0". Right now that
claim has no evidence.

**Status**: CLOSED on 2026-05-23.

Implemented fix:

1. After failover, `scripts/run-multi-volume-mounted-failover.sh` probes the
   old primary's exact iSCSI by-path device from the initiator host.
2. The probe path is scoped by both old frontend (`ip-<host>:<port>`) and
   `volume_id`, so it does not accidentally test the promoted path.
3. The probe runs a bounded direct read. Any successful stale-path read
   increments `old_primary_stale_io_success_count` and fails the gate.
4. The per-volume summary and `stale-primary-probe.log` both carry the measured
   count.

**Helper changes**:

- New `probe_stale_primary_path()` function in
  `scripts/run-multi-volume-mounted-failover.sh`.
- New artifact `recovery/failover/volume-N/stale-primary-probe.log` with raw
  probe output.

**Hard gate**: probe runs and measures 0 stale direct-read successes.

**Evidence**: D4 interleaved rerun `20260523-114708-46bc` passed 55/55 actions.
Both target volumes recorded `candidate_result=expected_failure` and
`old_primary_stale_io_success_count=0`.

## D6 - Real ALUA RTPG Asymmetric Access State Pre/Post Assertion

**Original gap**: D3 + D4 only asserted
`grep_log pattern="asymmetric access state" count > 0` in
`sg-rtpg.before.txt`. Doesn't parse the actual access state value (Active /
Optimized vs Active / Non-Optimized vs Standby), doesn't compare before vs
after the failover. The existing single-volume
`scripts/run-iscsi-alua-multipath-smoke.sh:279` has the right pattern:

```bash
sed -n 's/.*asymmetric access state[[:space:]]*:[[:space:]]*\(0x[0-9a-fA-F]\+\).*/\1/p'
```

It extracts the hex AAS code. The multi-volume scenarios don't use this.

**Why it matters**: D3 claims "iSCSI ALUA + dm-multipath transparent
failover". Without parsing AAS values, the scenario can't tell the difference
between:
- a real ALUA-mediated path switch (AAS changed from AO->ANO on the failed
  port group and ANO->AO on the promoted port group), and
- a sg_rtpg call that happens to print text but the path actually didn't
  switch.

**Status**: CLOSED on 2026-05-23.

Implemented fix:

1. Capture `sg_rtpg` output for each by-path device before and after failover
   for each target volume.
2. Parse AAS for each path using the same regex family as
   `run-iscsi-alua-multipath-smoke.sh`.
3. Emit per-volume summary fields:
   - `rtpg_before_old_primary_aas=0x00`
   - `rtpg_before_promoted_aas=0x02`
   - `rtpg_after_old_primary_aas=missing`
   - `rtpg_after_promoted_aas=0x00`
   - `rtpg_transition_verified=true`
4. Scenario assertion now requires the before/after state files to exist and
   `rtpg_transition_verified=true` per target volume.

**Helper changes**:

- Extended `scripts/run-multi-volume-mounted-failover.sh` to capture before
  and after RTPG state per port group per volume.
- Added per-volume AAS transition verification.

**Hard gate**: pre/post AAS values match the expected transition per volume.

**Evidence**:

- D4 interleaved rerun `20260523-123229-9dd4` passed 55/55 actions.
- D3 sequential rerun `20260523-123647-2fc4` passed 47/47 actions.

All target volumes recorded the expected `0x00 -> missing` old-primary path and
`0x02 -> 0x00` promoted path transition.

## D7 - Stability / Flake-Rate Matrix

**The gap**: D3 and D4 were single-run PASS. No information on how often they
pass under repeated runs. Promotion races, port-allocation races, observation
slot merge under load, and multipath path-stay-on-active timing are all
candidates for intermittent failures that won't show up in a single run.

**Why it matters**: "multi-volume HA independence" is a reliability claim,
not a one-shot smoke claim. A 95% pass rate would not be acceptable for a
release-grade gate.

**Status**: DEV PASS, QA/nightly N>=5 pending.

Implemented shape:

1. New wrapper script `scripts/run-phase27-flake-matrix.ps1` runs a selected
   scenario N times.
2. Per-iteration result bundles are captured under
   `iterations/iteration-<NN>/`.
3. Emits `flake-summary.txt` and `flake-summary.json` with:
   - `target_runs=<N>`
   - `pass_runs=<P>`
   - `fail_runs=<N-P>`
   - per-iteration result line: `iteration=<i> result=PASS|FAIL
     run_id=<id>`
   - `flake_rate_percent=<((N-P)/N)*100>`
4. Intended scheduling: nightly or QA-owned validation rather than every PR
   (cost-aware).
5. Hard gate target: `flake_rate_percent=0` over the documented window.

**Helper changes**:

- New `scripts/run-phase27-flake-matrix.ps1` that drives `swblock.exe run`
  N times.

**Evidence**:

- Smoke: `results/phase27-d7-flake-smoke`, one interleaved D4 iteration, PASS,
  `flake_rate_percent=0`.
- Dev stability: `results/phase27-d7-flake-interleaved-n3`, three interleaved
  D4 iterations, PASS, `pass_runs=3`, `fail_runs=0`,
  `flake_rate_percent=0`.

**Hard gate still pending**: 0 flake over 5 sequential D3 runs and 5 sequential
D4 runs on a clean lab.

**Scope**: dev for helper; QA/TestOps infra for nightly scheduling.

## D8 - App Pod Distribution Across Nodes

**The gap**: D3 + D4 scenarios pin all 3 writer pods to m02:

```yaml
env:
  app_node: "m02"
```

`scripts/run-multi-volume-mounted-failover.sh:13` reads this env into a
single `APP_NODE_SELECTOR` applied to every writer manifest. So the cluster
state under test is: 3 writer pods all on m02, 9 blockvolume pods spread
across all 3 nodes. This is a narrow shape - real users will spread
app pods across nodes.

**Why it matters**: app pod placement affects:
- which node experiences the iSCSI initiator stack failover (currently always
  m02),
- whether dm-multipath path-switching is exercised on multiple nodes,
- whether a cross-node single-app-node assumption hides a bug that surfaces
  only when writers are on different initiator hosts.

**Fix shape**:

1. Helper `run-multi-volume-mounted-failover.sh` already supports per-volume
   node selection via `SW_BLOCK_MULTI_VOLUME_APP_NODE`. Extend to accept a
   comma-list (`m01,m02,tp01`) that maps writer-i to host i.
2. New scenario `helm-multi-volume-rf3-app-spread-failover-chain.yaml`:
   writer-1 on m01, writer-2 on m02, writer-3 on tp01, then run the same
   mounted-failover loop.
3. Assertions: same per-volume hard-gate as D3, plus a new field
   `app_node_distribution_count=3` (one initiator per node).

**Helper changes**:

- Comma-list support in `APP_NODE_SELECTOR` (loop index modulo node count).
- New scenario file.

**Hard gate**: same as D3, with each volume's writer on a different node.

**Scope**: dev for helper extension; QA for scenario authoring once helper
lands.

## Recommended Sequence

| Order | Gap | Priority | Why |
|---|---|---|---|
| 1 | D5 (real stale-I/O probe) | DONE | Closed by run `20260523-114708-46bc` |
| 2 | D6 (RTPG AAS pre/post) | DONE | Closed by run `20260523-123229-9dd4` |
| 3 | D7 (flake matrix) | DEV PASS | D4 N=3 pass; N>=5 QA/nightly pending |
| 4 | D8 (app pod spread) | MEDIUM | Removes single-node-initiator hidden-bug risk |

## Release Note Implications

The conservative wording from the Phase 27 close report still holds, with D5
and D6 now strengthened:

> Mounted failover on a single iSCSI initiator host (m02) preserved the
> writer pod across the fault and the post-failover checksum matched. Stale
> primary path reads were measured and rejected. ALUA RTPG state transitions
> were measured per target volume.

## Verdict

Phase 27 is shippable as alpha at the single-app-node, single-run, gated
"transparent failover claimed" wording, with D5 stale-primary fencing and D6
RTPG AAS transitions now measured. The release note should explicitly defer
D7 full N>=5 stability and D8 app-spread broadening work. None of these block the multi-volume-mounted-failover
product capability itself; they bound how broadly we can market the strongest
claims.
