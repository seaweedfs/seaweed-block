# Finished Plan: Main Merge PR Readiness

Status: historical reference. Closed on 2026-05-11 after PR #46 was opened,
reviewed, fixed, and squash-merged into `main`.

Current work remains tracked in `../current-plan.md`.

## Goal

Prepare `plan-roadmap-refresh` for a meaningful PR to `main`.

This was not a feature phase. It was the closeout layer that turned the beta
hardening, operations observability, iSCSI/NVMe/CSI gates, and iSCSI pressure
hardening work into a reviewable integration PR.

## Close State

| Item | Status | Evidence |
| --- | --- | --- |
| PR opened | Done | #46, `testops: add beta hardening gates and storage readiness evidence` |
| Target branch | Done | `main` |
| Merge strategy | Done | squash/admin merge |
| Merge commit | Done | `246de4954d31151df010f2f2f7b734452b30226f` |
| Review follow-up | Done | `cc1fa74 testops: address review hardening comments` |
| CodeRabbit | Done | review completed, check passed |

PR:

```text
https://github.com/seaweedfs/seaweed-block/pull/46
```

## PR Scope

The PR added the beta-hardening test and operations foundation:

- runner-native protocol and beta hardening gates,
- CSI lifecycle and RF1 durable restart coverage,
- iSCSI/NVMe protocol release gates,
- returned-replica and failover evidence gates,
- read-only operations status report support,
- Linux and Windows iSCSI OS initiator validation path,
- fast iSCSI L2 durable restart/reconnect coverage,
- planning docs organized into roadmap, finished phases, and current plan.

## Review Fixes

The final review-fix commit addressed concrete CodeRabbit/Gemini findings:

- reconciled finished-plan archive workflow,
- fixed Markdown table rendering,
- made iSCSI L2 failure diagnostics safe,
- checked setup `MkdirAll` errors,
- normalized launcher hostPath values with `path.Clean`,
- scoped manifest state-volume assertions,
- escaped launcher hostPath injection in shell scripts,
- relaxed brittle RF1 durable epoch assertions to positive epoch,
- scoped `emptyDir` checks to the state volume,
- used fixed-string grep for blockvolume log waits.

Deferred intentionally:

- operations-report scenario grep/assert consolidation,
- demo pod security context hardening,
- `--env` versus `-env` example unification.

These are polish/follow-up items, not merge blockers.

## Validation Recorded In PR

Real-hardware / runner evidence:

- `beta-hardening-gate`: PASS twice back-to-back on m02.
- `protocol-release-gate`: PASS on m02.
- `iscsi-os-initiator-compat-chain`: PASS on Linux/open-iscsi.
- Windows 11 built-in iSCSI Initiator: PASS through target-ready validation.

Focused local gates:

- `go test ./core/frontend/iscsi -run 'TestP2_ISCSI|TestP1_ISCSI|TestDataInWriter|TestDataOut' -count=1`
- `go test -tags subprocess ./cmd/blockvolume -run 'TestISCSI_L2Durable(RestartReconnect_(PreservesData|RepeatedCycles)|SyncCacheRestart_AcceptsSyncAndPreservesWrites)' -count=1 -v`

Review-fix validation:

- `go test ./cmd/blockmaster -run 'TestG15d_BlockmasterLauncherTickCanRenderHostPathState' -count=1`
- `go test ./core/launcher -count=1`
- `bash -n scripts/install-k8s-alpha.sh scripts/run-g15d-k8s-dynamic.sh scripts/run-alpha-app-demo.sh`
- `swblock validate testops/scenarios/csi-rf1-durable-restart-chain.yaml`

## Non-Claims

- This phase did not add new product behavior after the PR review fixes.
- This phase did not claim production HA.
- This phase did not claim performance readiness.
- This phase did not replace future operations, soak, upgrade, or operator
  hardening plans.
