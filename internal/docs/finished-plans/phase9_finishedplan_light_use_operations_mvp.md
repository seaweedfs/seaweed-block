# Finished Plan: Light-Use Operations MVP

Status: finished. Closed after QA validation at product commit
`8beaaa7ca83cc3a9ebbf6873a42eb9b3bd3505be`.

This plan reframed the smaller `Operations Layer Usability Seed` into a
user-visible light-use operations loop. The goal was not to finish the full
enterprise operations story. The goal was to make one useful product workflow
real and gated:

```text
PVC/app problem -> run status command -> read summary -> attach bundle -> dev/QA
can triage without asking for raw logs first
```

## Product Question

Can an early user or developer use Seaweed Block as a functional light block
product without reading internal scripts and logs?

The answer after this plan is narrower but materially better:

- The datapath is close to a functional MVP: CSI dynamic PVC, iSCSI/NVMe
  frontend paths, mounted failover, restart, returned-replica evidence, and
  beta-hardening gates are real.
- The operations layer now has a first closed read-only loop: a product CLI can
  collect status, classify issues, write a support bundle, and TestOps can
  prove the command/bundle contract.
- The product is still not a complete light-use product. Install, lifecycle
  ownership, cluster-wide observation, and UI/API operations remain future
  work.

## Delivered User Experience

As a user or developer, I can now:

1. Run a product-owned status command for one volume.
2. Read a human summary that includes product revision, volume identity,
   authority, replication, residue, and issue classification.
3. Attach a self-describing support bundle with machine-readable JSON,
   human-readable summary, bundle metadata, artifacts, unchecked classes,
   collection errors, and non-claims.
4. Use TestOps control data to see active/history run records, resource locks,
   and shared-lab ownership instead of guessing whether a test is running or
   stale.

This is the first functional operations loop. It is intentionally read-only.

## Delivered Scope

### Product CLI

`sw-block ops status` was added with the intended shape:

```text
sw-block ops status \
  --volume <id> \
  --master <host:port> \
  --status-addr <host:port|url> \
  --out <dir>
```

Exit code contract:

- `0`: report collected, parsed, and classified clean,
- `1`: report collected but unhealthy/incomplete/residue or collection-error
  evidence was detected,
- `2`: required input is invalid, artifact writing failed, or the report
  identity/schema is invalid.

### Support Bundle

The command writes:

- `volume-status-report.json`,
- `volume-status-summary.txt`,
- `ops-status-bundle.json`.

The bundle records schema version, command, capture time, volume id, product
revision, exit classification, artifacts, unchecked residue classes, collection
errors, and non-claims.

### TestOps Gates

Fast local gates cover the report and CLI boundary:

- `operations-volume-status-report-component-gate`,
- `operations-volume-status-cli-gate`.

These gates validate schema, summary/classifier behavior, artifact writing,
bundle metadata, CLI command boundary, exit code behavior, outlier cases, and
non-claims.

### Shared-Lab Control Data

TestOps gained simple shared-lab control data:

```text
testops-control/
  active/<run_id>.json
  history/<run_id>.json
  locks/<resource>.lock
  events.jsonl
```

The first version is not a scheduler. It provides visibility and safety:

- create an active record at run start,
- update state at terminal exit,
- record scenario, artifact dir, commits, and known resource ownership,
- refuse conflicting resource locks,
- move terminal runs into history,
- keep stale active records visible if a runner crashes.

## QA Close Evidence

QA validated the close gate at product commit
`8beaaa7ca83cc3a9ebbf6873a42eb9b3bd3505be`.

Validated parts:

- Part A local tests: PASS for
  `go test ./core/ops ./cmd/sw-block ./internal/testops ./cmd/sw-testops -count=1`.
- Part B CLI gate: PASS. `operations-volume-status-cli-gate.yaml` validates
  schema, volume id, product revision, status/authority, bundle command,
  exit code, artifact list, unchecked classes, collection errors, and
  non-claims.
- Part C artifact inspect: PASS. `ops-status-bundle.json`,
  `volume-status-summary.txt`, and `volume-status-report.json` all have the
  required fields and parse correctly.
- Part D CLI outliers: PASS. Missing `--volume`, bad master/status addresses,
  and unwritable `--out` return exit code `2` without false success claims.
- Part E control data: PASS. Terminal run moved from `active/` to `history/`,
  locks were released, `events.jsonl` carried start and complete records, and
  `--control-list` showed the expected fields.
- Part F lock conflict: PASS. A second run against an exclusive resource exits
  `2`, reports the lock owner, creates no active record/artifact dir, and does
  not disturb the first run.
- Part G hygiene: N/A. The close gate used local execution only, not M01/M02.

QA verdict:

```text
PASS to close current plan.
The light-use operations loop -- observe one volume -> read a useful summary ->
attach a self-describing bundle -> see TestOps control data for shared lab
ownership -- is exercised end to end with all required guarantees enforced by
code, not by exit-0 alone.
```

## Informational Notes From QA

These are portability notes, not close blockers:

- In custom registries on Windows, Unix-style `/c/work/...` paths are treated
  as relative by `filepath.IsAbs`. Use Windows absolute paths such as
  `C:/work/...`.
- On Windows, `ShellDriver` invokes the driver path directly with `os/exec`.
  `.sh` drivers need a `.bat` shim that calls `bash.exe`, or the driver should
  be `.bat`/`.exe`.

## Non-Claims Carried Forward

This plan does not claim:

- productized install/upgrade/uninstall,
- Kubernetes operator/controller ownership,
- mutating admin commands,
- automatic repair,
- cluster-wide UI/dashboard,
- performance monitoring,
- fleet agent,
- full TestOps scheduler.

## Next Roadmap Pull

The next usability work should move from observation to product-owned lifecycle.
The main remaining user-visible gap is:

```text
install/create/delete/retry/cleanup should feel like product behavior, not a
collection of internal scripts plus TestOps cleanup discipline
```

Candidate next plan: `Light-Use Install And Lifecycle MVP`.
