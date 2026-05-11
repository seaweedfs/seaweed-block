# Current Plan: Operations Layer Usability Seed

Status: active. Started after closing
`finished-plans/phase8_finishedplan_main_merge_pr_readiness.md` on 2026-05-11.

## Goal

Turn the newly merged read-only status/report surfaces into a small,
operator-facing workflow.

The beta-hardening PR gave us evidence and diagnostics primitives. The next
step is not another protocol bring-up. The next step is making a developer or
early user answer basic operational questions without spelunking through raw
logs:

```text
what volumes exist -> what frontend targets exist -> what is unhealthy ->
what artifact bundle should I attach to an issue
```

## Why This Is Next

The roadmap now has protocol, CSI, failover, returned-replica, and iSCSI OS
initiator gates recorded. The remaining weakness is usability:

- operations are still split across scripts, TestOps, status endpoints, and
  manual artifact paths,
- the read-only status report exists but is not yet a simple operator command,
- users need a small workflow before they can give useful feedback,
- enterprise operations can come later, but basic diagnostics should be open and
  easy to run.

This contributes to roadmap Track F: Operations Layer.

## Scope

Build a minimal operations layer seed around existing read-only data.

In scope:

- a simple command or script that collects volume status report JSON,
- a human-readable summary view for the same data,
- a diagnostic bundle shape suitable for bug reports,
- TestOps scenario coverage for the command,
- docs showing when to use it and what it does not prove.

Out of scope:

- Kubernetes operator,
- web UI,
- mutating admin actions,
- upgrade/uninstall automation,
- fleet agent,
- cloud-scale test controller,
- performance dashboards.

## Candidate Delivery

Preferred first slice:

```text
sw-block ops status --volume <id> --master <addr> --out <dir>
```

Equivalent script is acceptable if the product CLI surface is not ready:

```text
scripts/collect-ops-status-report.sh
```

The command should produce:

- `volume-status-report.json`,
- `volume-status-summary.txt`,
- optional raw source snapshots,
- clear exit code:
  - `0`: report collected, parsed, and classified clean,
  - `1`: report collected but unhealthy/incomplete/residue or collection-error
    evidence detected,
  - `2`: required input is invalid, artifact writing failed, or the report
    identity/schema is invalid.

## Test Strategy

Component first:

- unit tests for summary rendering from fixed JSON,
- unit tests for exit-code classification,
- component tests for missing fields / unknown states,
- no Kubernetes required.

Product-backed only after component green:

- run the existing operations status report component gate,
- add one runner-native scenario that invokes the operator-facing command and
  asserts the JSON plus summary artifact exists.

Milestone gate only if needed:

- include the command in `beta-hardening-gate` only after the command is stable.

## Progress

2026-05-11:

- Added the component-level summary/classifier slice for
  `VolumeStatusReport`.
- Delivery shape:
  - `RenderVolumeStatusSummary(report)` emits deterministic operator-readable
    text.
  - `ClassifyVolumeStatusReport(report)` maps report evidence to exit-code
    intent: `0` clean, `1` unhealthy/needs inspection, `2` invalid report
    identity or schema.
  - `VolumeStatusReportIssues(report)` exposes the exact reasons behind
    non-zero classification for future CLI/script output.
- Validation:
  - `go test ./core/ops -count=1`
- Non-claim:
  - This slice does not collect live state and does not add a CLI yet. It only
    defines the stable rendering/classification contract the operator-facing
    command will use.

2026-05-11 follow-up:

- Added `WriteVolumeStatusArtifacts(ctx, dir, collector)` as the reusable
  command/script seam.
- It writes:
  - `volume-status-report.json`
  - `volume-status-summary.txt`
- It preserves partial report artifacts when a read-only source fails, records
  the collection error in JSON, and includes the same error in the summary
  issue list.
- Validation:
  - `go test ./core/ops -count=1`
- Non-claim:
  - This still does not open network connections or collect from a live
    blockvolume/master. Live source wiring is the next slice.

2026-05-11 live-source slice:

- Added `cmd/sw-block` with:
  - `sw-block --version`
  - `sw-block ops status --volume <id> --master <addr> --status-addr <addr|url>
    --out <dir>`
- Added live read-only sources for:
  - blockmaster `EvidenceService.QueryVolumeStatus`,
  - blockvolume `/status`,
  - blockvolume `/status/peers`,
  - blockvolume `/status/durable`.
- The command writes the report/summary artifact pair and prints the summary to
  stdout.
- The first command version requires both `--master` and `--status-addr` so a
  clean exit does not hide missing master or local blockvolume evidence.
- The live command now collects host-side iSCSI and NVMe initiator residue via
  local read-only commands. Process, Kubernetes, and storage-path residue are
  reported as unchecked in this CLI slice and remain covered by TestOps gates.
- Validation:
  - `go test ./core/ops ./cmd/sw-block ./cmd/sw-testops -count=1`
- Non-claim:
  - This is still diagnostic-only. It does not mutate volume state, perform
    cleanup, replace TestOps, or provide a web UI/operator.

2026-05-11 CLI TestOps gate:

- Added `operations-volume-status-cli-gate`.
- The gate runs the `sw-block ops status` command contract through
  `cmd/sw-block` against in-test fake master/status endpoints and writes the
  resulting operator artifacts into the TestOps artifact tree.
- It asserts:
  - `volume-status-report.json` exists and carries schema/version/volume facts,
  - `volume-status-summary.txt` reports `status: ok`,
  - the summary preserves volume/replica/epoch identity,
  - the summary reports `issues: none`.
- Validation:
  - `go test ./cmd/sw-block -run TestOpsStatusWritesArtifactsAndReturnsClean -count=1`
  - `swblock validate testops/scenarios/operations-volume-status-cli-gate.yaml`
- Non-claim:
  - This is a command-boundary component gate, not a live product deployment
    gate. It prevents CLI/artifact drift before spending lab time.

## Delivery Gate

This plan is complete when:

1. An operator-facing status collection command or script exists.
2. It emits both machine-readable JSON and human-readable summary output.
3. It has component tests for summary and exit-code classification.
4. A TestOps scenario captures the command artifacts.
5. The operator guide documents usage and non-claims.
6. No mutating control-plane action is added under this plan.

## Non-Claims

- Not a full UI.
- Not an operator.
- Not upgrade/uninstall.
- Not automated repair.
- Not performance monitoring.
- Not enterprise fleet automation.
