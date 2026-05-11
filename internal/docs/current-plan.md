# Current Plan: Main Merge PR Readiness

Status: active. Started after closing
`finished-plans/phase7_finishedplan_iscsi_session_backend_pressure.md` on
2026-05-11.

## Goal

Prepare `plan-roadmap-refresh` for a meaningful PR to `main`.

This is not a new feature phase. The development work has already produced the
beta hardening, operations observability, iSCSI/NVMe/CSI gates, and iSCSI
pressure hardening evidence. This plan is the closeout layer that turns that
work into a reviewable integration PR.

## PR Scope

The PR should be framed as:

```text
testops: add beta hardening gates and storage readiness evidence
```

Main claims:

- runner-native protocol and beta hardening gates,
- CSI lifecycle and RF1 durable restart coverage,
- iSCSI/NVMe protocol release gates,
- returned-replica and failover evidence gates,
- read-only operations status report support,
- Linux and Windows iSCSI OS initiator validation path,
- fast iSCSI L2 durable restart/reconnect coverage,
- planning docs organized into roadmap, finished phases, and current plan.

## Required Close Checks

Before opening or updating the PR:

1. Branch points at `main`.
2. Local branch is pushed.
3. Working tree has no tracked modifications.
4. Current finished-plan chain is coherent:
   - phase1 frontend protocol readiness,
   - phase2 beta hardening seed,
   - phase3 beta seed stabilization,
   - phase4 fast gates and operations contract prep,
   - phase5 read-only operations status report,
   - phase6 iSCSI OS initiator compatibility,
   - phase7 iSCSI session/backend pressure hardening.
5. Core fast checks still pass or are explicitly listed as not rerun.
6. PR description is simple and human-readable.
7. Merge strategy is squash merge, so `main` does not receive the full
   development commit train.

## Suggested Validation Before PR

Fast local checks:

```text
go test ./core/frontend/iscsi -run 'TestP2_ISCSI|TestP1_ISCSI|TestDataInWriter|TestDataOut' -count=1
go test -tags subprocess ./cmd/blockvolume -run 'TestISCSI_L2Durable(RestartReconnect_(PreservesData|RepeatedCycles)|SyncCacheRestart_AcceptsSyncAndPreservesWrites)' -count=1
```

Branch/PR checks:

```text
git status --short --branch
git rev-list --count origin/main..HEAD
git diff --stat origin/main..HEAD
```

Optional if time allows:

```text
swblock validate-bundle --profile beta-hardening <latest known green bundle>
```

Do not rerun long suites unless review asks for fresh evidence. The beta and
protocol suites already have real-hardware green evidence in the finished
plans.

## PR Description Draft

```md
## Summary

Adds the beta-hardening test and operations foundation for Seaweed Block.

This includes runner-native protocol and beta gates, CSI lifecycle and durable
restart coverage, iSCSI/NVMe protocol release gates, returned-replica evidence,
read-only operations status reports, OS initiator compatibility gates, and fast
iSCSI L2 durable restart/reconnect coverage.

The branch also reorganizes planning docs into roadmap, finished plans, and a
current plan so future work has a clear PM trail.

## Validation

- beta-hardening-gate: PASS twice back-to-back on m02
- protocol-release-gate: PASS on m02
- iSCSI OS initiator compatibility: Linux PASS, Windows 11 PASS
- iSCSI L2 restart/reconnect pack: PASS
- fast iSCSI protocol pressure pack: PASS

## Notes

Please squash-merge this PR into main.
```

## Non-Claims

- This plan does not add new runtime behavior.
- This plan does not add new test coverage beyond final closeout edits.
- This plan does not require another full beta suite run unless reviewers ask.
- This plan does not decide the next product feature phase after merge.
