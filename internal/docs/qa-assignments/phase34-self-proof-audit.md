# Phase 34 D1 - Self-Proof Audit

Status: draft on 2026-05-29.

Purpose: identify TestOps assertions where a helper writes a summary value and
the scenario only greps that same summary value. These checks are useful smoke
signals, but hard product claims need an independent evidence source.

## Rule

```text
If a field gates a product claim, validate it against at least one independent
source: Kubernetes state, operator-snapshot JSON, product event stream, host
probe, direct IO, or a stricter verifier.
```

## Summary

The scenario library has many `grep_log` assertions. Most are acceptable as
artifact-shape checks. The risk is concentrated in a smaller set of hard-claim
summary fields:

- multi-volume counts
- writer/reader verification counts
- restart/failover status summaries
- cross-volume isolation booleans
- cleanup status
- stale-primary rejection and ALUA transition fields

Not all of these are bad. Some fields already come from real probes
(`stale-primary-probe.log`, RTPG captures, direct workload logs). The main gap
is that several scenarios still treat helper summaries as the final authority
without cross-checking an independent source.

## Representative Findings

| ID | Field / Assertion | Current Source | Risk | Classification | Required Cross-Check |
|---|---|---|---|---|---|
| A1 | `multi_volume_status=ok` in `helm-multi-volume-day1-chain.yaml` | `run-multi-volume-example.sh` summary | Can hide partial helper failure if summary is wrong | acceptable smoke | Keep, but not a hard claim by itself |
| A2 | `writer_verified_count=3` in multi-volume gates | Helper counts writer logs | Log count can be correct while pods/PVCs differ from expected topology | needs independent cross-check | `kubectl get pods` by writer labels plus each writer log checksum |
| A3 | `reader_verified_count=3` in multi-volume gates | Helper counts reader logs | Same-source count does not prove 3 distinct PVCs were read | needs independent cross-check | Operator snapshot has 3 distinct volumes; each reader references a distinct PVC |
| A4 | `managed_volume_count=3` in multi-volume gates | Helper counts `managed_volume=` lines in report summary | Report summary and helper may share the same product snapshot assumptions | needs independent cross-check | `operator-snapshot.json` volume array length and distinct `volume_id`/`pvc_name` |
| A5 | `cleanup_status=ok` in user-loop helpers | Helper cleanup result | Acceptable only if verifier is stricter than uninstall | acceptable if verifier-backed | Require `verify-helm-cleanup.sh` residue counters all zero |
| A6 | `pod_recreate_used=false` in mounted-failover gates | Helper summary | Could be self-proof if not tied to pod UID evidence | needs independent cross-check | Compare `writer_pod_uid_before` and `writer_pod_uid_after` artifacts |
| A7 | `old_primary_stale_io_success_count=0` in mounted-failover gates | Direct stale-path probe summary | Lower risk; now probe-backed | acceptable with probe artifact | Require non-empty `stale-primary-probe.log` with `candidate_result` lines |
| A8 | `rtpg_transition_verified=true` | Helper summary from RTPG parsing | Lower risk if before/after AAS files are non-empty and values are asserted | acceptable with artifacts | Require `rtpg-before-states.txt`, `rtpg-after-states.txt`, and AAS value checks |
| A9 | `cross_interference_observed=false` | Helper summary | High-level boolean can hide missing untouched-volume evidence | needs independent cross-check | Untouched volume workload log + unchanged volume status in operator snapshot |
| A10 | `restart_promotion_status=ok` | Scenario-generated restart summary | Can pass without proving status surface convergence | needs independent cross-check | Post-restart report/operator-snapshot eventually Ready for 3 consecutive polls |
| A11 | `multi_volume_restart_status=ok` | Helper summary | Can hide per-volume identity mixup if only summary is read | needs independent cross-check | 3 distinct volume IDs, no duplicate publish target, no primary mixup in operator snapshot |
| A12 | `status_endpoint_unreachable` replay assertions | Synthetic replay bundle | Valid L1 replay, not live fault evidence | should be upgraded | D2 F2b live status-port block; assert Unknown, not Blocked |

## High-Value Hardening Picks

Prioritize these first because they support user-visible release claims.

1. Multi-volume count identity
   - Current weak check: `managed_volume_count=3`
   - Add cross-check: parse `operator-snapshot.json` and require exactly 3
     distinct `volume_id` values and 3 distinct `pvc_name` values.

2. Writer/reader verification count
   - Current weak check: `writer_verified_count=3` and `reader_verified_count=3`
   - Add cross-check: each writer/reader log must map to a distinct PVC, and
     the corresponding pod must have reached a terminal verified state.

3. Mounted no-recreate claim
   - Current weak check: `pod_recreate_used=false`
   - Add cross-check: compare pod UID before/after for every target volume.
     A summary boolean alone is not enough.

4. Restart convergence
   - Current weak check: restart summary says status ok.
   - Add cross-check: after restart, status surfaces may temporarily show
     Unknown, but must converge to Ready for 3 consecutive polls within the
     bounded window.

5. Status endpoint unreachable
   - Current weak check: L1 synthetic replay.
   - Add cross-check: D2 F2b live injection blocks only the status endpoint,
     then verifies `Ready=Unknown` and not `Blocked`.

## Acceptable Existing Patterns

These are not blockers if kept as smoke or artifact-shape checks:

- Grepping `summary.txt` for report artifact presence.
- Grepping release hygiene fields after an independent Helm command has
  already succeeded.
- `cleanup_status=ok` when it is produced by `verify-helm-cleanup.sh` and the
  residue counters are also asserted.
- Stale-primary count checks when `stale-primary-probe.log` is non-empty and
  contains direct read results.
- RTPG transition checks when raw before/after AAS captures are also asserted.

## Anti-Patterns To Avoid

- Do not count a helper's summary field as the only proof of a product claim.
- Do not accept a chaos primitive as proof unless the artifact shows the fault
  hit the intended layer.
- Do not let synthetic replay replace live injection for P0 failure claims.
- Do not treat a single transient `Ready=True` after restart as convergence.
- Do not create broad RF/node-count matrices to compensate for weak evidence.

## D1 Acceptance

This audit satisfies D1 of `phase34-test-realism-plan.md`:

- At least 10 representative summary-grep assertions are listed.
- Each is classified as acceptable smoke, needs independent cross-check, or
  should be upgraded/replaced.
- Five high-value assertions are selected for first hardening.

Next implementation step: D2 F2b live status-endpoint-unreachable, while
carrying A4/A6/A10 cross-checks into the next affected scenario edits.
