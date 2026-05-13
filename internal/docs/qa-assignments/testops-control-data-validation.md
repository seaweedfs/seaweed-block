# QA Assignment: TestOps Control Data Validation

## Goal

Validate that product-local `sw-testops` writes useful shared-lab control data
for M01/M02-style runs.

This is not a product datapath test. It validates operator visibility for
running/stale tests:

- active run record exists while a run owns resources,
- conflicting resource locks refuse a second run,
- terminal runs move to history,
- locks release on terminal exit,
- `--control-list` shows enough information for dev/QA to know what ran.

## Build

From the product repo:

```text
go build -o sw-testops ./cmd/sw-testops
```

## Suggested Local Validation

Use a temporary control directory and a fast scenario:

```text
CONTROL=<tmp>/testops-control
ART=<tmp>/artifacts

./sw-testops \
  --repo-root <seaweed_block_repo> \
  --scenario g15b-manifest \
  --artifact-dir "$ART/run-1" \
  --run-id qa-control-run-1 \
  --commit <current_commit> \
  --control-dir "$CONTROL"

./sw-testops --repo-root <seaweed_block_repo> --control-dir "$CONTROL" --control-list
```

Expected:

- command exits `0`,
- `active/qa-control-run-1.json` is absent after terminal exit,
- `history/qa-control-run-1.json` exists,
- `events.jsonl` contains start and complete events,
- `--control-list` prints:
  - state,
  - run id,
  - scenario,
  - source commit,
  - updated timestamp,
  - locks/resources,
  - artifact dir.

## Conflict Validation

Use a small custom registry entry or an existing resource-declaring scenario.
Start one run that holds a lock, then attempt another run with the same
resource. If using a fast local custom registry, both scenarios can use a shell
script that sleeps.

Expected:

- first run creates `active/<run_id>.json`,
- lock file appears under `locks/`,
- second run exits non-zero before driver execution,
- second run does not create an active record,
- after first run exits, lock files are gone and the first run is in history.

## Report Back

Return:

- runner/product commit,
- control directory path,
- command lines used,
- `--control-list` output,
- active/history/lock tree before and after terminal exit,
- whether conflict refusal happened before driver execution.

## Non-Claims

- This does not validate standalone `swblock` yet.
- This does not validate remote agent mode.
- This does not validate stale-lock stealing or queueing.
- This does not validate per-phase heartbeat; the product-local runner records
  start and terminal ownership only.
- This does not validate product storage behavior.
