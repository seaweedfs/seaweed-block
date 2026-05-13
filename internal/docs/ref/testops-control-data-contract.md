# TestOps Control Data Contract

Status: first shared-lab visibility contract for product-local TestOps. This is
not a scheduler, queue, or remote agent.

## Purpose

M01/M02 are shared test resources. Before this contract, a developer or QA
operator could see result bundles after a run, but could not reliably answer:

- what is running now,
- which scenario owns the lab,
- which commit is under test,
- where the artifacts are,
- which global resources are locked,
- whether a run is stale after a crash.

The first control-data layer makes that visible through files on the shared
drive.

## Directory Shape

```text
testops-control/
  active/<run_id>.json
  history/<run_id>.json
  locks/<resource>.lock
  events.jsonl
```

`active/` records are created before driver execution. On terminal exit, the
record moves to `history/` and locks are released. If the runner crashes, the
active record and lock files remain visible for manual diagnosis.

## Record Shape

Each run record includes:

- `schema_version`
- `run_id`
- `scenario`
- `state`
  - `running`
  - `pass`
  - `fail`
  - `error`
- `source_commit`
- `artifact_dir`
- `resources`
- `locks`
- `started_at`
- `updated_at`
- `ended_at`, for terminal records
- `error_summary`, when terminal with an error

## Resource Metadata

Scenario registrations may declare:

```json
{
  "resources": {
    "group": "m02-block-lab",
    "exclusive": ["node:m02", "iscsi:m02", "k3s:m02"],
    "ports": [3260, 4420]
  }
}
```

The product-local runner converts these into lock files. A conflicting lock
causes the run to fail before driver execution.

## CLI Surface

Run with control data:

```text
sw-testops --control-dir <dir> --scenario <name> ...
```

List known control records:

```text
sw-testops --control-dir <dir> --control-list
```

The list output is intentionally simple:

```text
state  run_id  scenario  source_commit  updated_at  locks  artifact_dir
```

## Non-Claims

- No queueing.
- No stale-lock stealing.
- No periodic heartbeat inside a long phase.
- No per-phase progress in the product-local runner; active records show
  start/terminal ownership only.
- No remote agent on M01/M02.
- No product cleanup or repair action.
- No guarantee that external `swblock` has this exact implementation yet; this
  is the product-local contract to port into the standalone runner.
