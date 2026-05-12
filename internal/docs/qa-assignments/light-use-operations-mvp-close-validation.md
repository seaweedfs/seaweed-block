# QA Assignment: Light-Use Operations MVP Close Validation

## Goal

Validate the current plan's user-facing operations loop before it moves to
`finished-plans/`.

This is a spec-driven close gate. Do not validate only that commands exit 0.
Validate whether an early user/developer can:

```text
observe one volume -> read a useful summary -> attach a self-describing bundle
-> see TestOps control data for shared lab ownership
```

## Scope

Validate two surfaces:

1. Product CLI support bundle:
   - `sw-block ops status`
   - `volume-status-report.json`
   - `volume-status-summary.txt`
   - `ops-status-bundle.json`

2. Product-local TestOps control data:
   - `sw-testops --control-dir`
   - `active/<run_id>.json`
   - `history/<run_id>.json`
   - `locks/<resource>.lock`
   - `events.jsonl`
   - `sw-testops --control-list`

## Build

From the product repo under test:

```text
go build -o sw-block ./cmd/sw-block
go build -o sw-testops ./cmd/sw-testops
```

Record:

- product branch,
- product commit,
- whether worktree is dirty,
- OS/controller host,
- target host if using M01/M02.

## Part A: Fast Local Contract Validation

Run:

```text
go test ./core/ops ./cmd/sw-block ./internal/testops ./cmd/sw-testops -count=1
```

Expected:

- exit 0,
- no skipped failure relevant to ops status or TestOps control data.

## Part B: CLI Bundle Contract

Run the command-boundary gate:

```text
swblock validate testops/scenarios/operations-volume-status-cli-gate.yaml
```

Expected:

- scenario validates,
- the gate asserts:
  - report schema,
  - volume id,
  - product revision,
  - summary status/authority identity,
  - `ops-status-bundle.json` command,
  - exit code,
  - status,
  - artifact list,
  - `unchecked`,
  - `collection_errors`,
  - `non_claims`.

If the runner binary is unavailable, run the equivalent `go test` path and
manually inspect the output directory from `SW_BLOCK_OPS_STATUS_CLI_ARTIFACT_DIR`.

## Part C: Bundle Artifact Inspection

From an actual generated bundle, verify:

### `ops-status-bundle.json`

Required:

- `schema_version == "1.0"`
- `command == "sw-block ops status"`
- `volume_id` is populated
- `product_revision` is populated
- `exit_code` is present
- `status` is one of:
  - `ok`
  - `unhealthy`
  - `invalid`
- `artifacts` includes:
  - `volume-status-report.json`
  - `volume-status-summary.txt`
  - `ops-status-bundle.json`
- `unchecked` exists, even if empty
- `collection_errors` exists, even if empty
- `non_claims` is non-empty

### `volume-status-summary.txt`

Required:

- starts with `status:`
- includes `product_revision:`
- includes `volume:`
- includes `authority:`
- includes `replication:`
- includes `residue:`
- includes either `issues: none` or explicit issue lines.

### `volume-status-report.json`

Required:

- parses as JSON,
- includes `schema_version`,
- includes `source`,
- includes `volume`,
- includes `authority`,
- includes `replication`,
- includes `durable`,
- includes `residue`.

## Part D: CLI Outlier Checks

Run at least these adversarial cases:

1. Missing required args:

```text
sw-block ops status --volume v1
```

Expected:

- exit 2,
- stderr explains missing `--volume`/`--out` or missing live sources,
- no misleading artifact success line.

2. Bad master/status address against an output dir:

```text
sw-block ops status \
  --volume v1 \
  --master 127.0.0.1:1 \
  --status-addr 127.0.0.1:2 \
  --out <tmp-dir>
```

Expected:

- non-zero exit,
- if partial artifacts are written, `ops-status-bundle.json` and
  `volume-status-summary.txt` reflect unhealthy/error state,
- collection error is visible in either summary or bundle.

3. Output path invalid or unwritable.

Expected:

- exit 2,
- no false `status: ok` claim.

## Part E: TestOps Control Data

Use a shared/control directory. Prefer a shared-drive path if available.

```text
CONTROL=<shared-or-temp>/testops-control
ART=<shared-or-temp>/artifacts

sw-testops \
  --repo-root <seaweed_block_repo> \
  --scenario g15b-manifest \
  --artifact-dir "$ART/run-1" \
  --run-id qa-control-run-1 \
  --commit <product_commit> \
  --control-dir "$CONTROL"

sw-testops --repo-root <seaweed_block_repo> --control-dir "$CONTROL" --control-list
```

Expected after terminal exit:

- `active/qa-control-run-1.json` absent,
- `history/qa-control-run-1.json` present,
- `locks/` empty,
- `events.jsonl` has start and complete events,
- `--control-list` shows:
  - state,
  - run id,
  - scenario,
  - source commit,
  - updated timestamp,
  - locks column,
  - artifact dir.

## Part F: Control Lock Conflict

Validate resource-lock collision behavior. Use either:

- a tiny custom registry with a shell script that sleeps, or
- an existing resource-declaring scenario if it is safe to run.

Expected:

- first run creates active record and lock files,
- second run using the same resource exits before driver execution,
- second run does not create an active record,
- first run's lock is not removed by the conflicting run,
- terminal first run moves to history and releases locks.

## Part G: Shared Lab Hygiene

If running on M01/M02, verify after the test:

- no stale TestOps locks for completed runs,
- no active record for completed runs,
- no unexpected iSCSI session,
- no unexpected NVMe SeaweedFS subsystem,
- no blockmaster/blockvolume process left by this validation.

## Report Template

Return one report with:

```text
QA Report — Light-Use Operations MVP Close Validation

Product commit:
Runner/swblock commit or binary:
Host/controller:
Target node(s):
Artifact/control dirs:

Part A local tests: PASS/FAIL
Part B CLI gate: PASS/FAIL
Part C artifact inspection: PASS/FAIL
Part D CLI outliers: PASS/FAIL
Part E control data: PASS/FAIL
Part F lock conflict: PASS/FAIL
Part G hygiene: PASS/FAIL

Findings:
1. ...

Verdict:
PASS to close current plan / FAIL with blockers
```

## Non-Claims

This validation does not prove:

- production HA,
- performance,
- install/upgrade/uninstall,
- Kubernetes operator ownership,
- UI/dashboard,
- remote TestOps agent,
- queueing or stale-lock stealing.
