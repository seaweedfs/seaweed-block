# Phase 32 D7 Stale Evidence QA Assignment

Date: 2026-05-25

Owner: QA.

Purpose: validate that report/dashboard/operator replay does not silently use
older cluster evidence when a newer restart snapshot exists in the same bundle.

## Source Commit Under Test

Use the dev commit that includes the D7 replay-precedence fix in:

- `core/ops/observation_bundle.go`
- `core/ops/observation_bundle_test.go`

## Scoped Unit Tests

Run:

```bash
go test ./core/ops ./cmd/sw-block
```

Expected: PASS.

## Bundle Replay Gate

Use the D5 restart/promotion bundle from the latest Phase 32 QA run, or rerun:

```text
testops/scenarios/helm-rf3-promotion-restart-persistence-chain.yaml
```

Then run:

```bash
sw-block ops report --from-bundle <D5-bundle-dir> --out <tmp-report-dir>
```

Required checks:

- `summary.txt` names the post-restart primary and publish target from
  `restart/cluster-after-restart.json`, not the older setup snapshot.
- For the current D5 shape, this means:
  - no stale `primary=r1@m01 frontend=192.168.1.181:3260` in the regenerated
    report when the restart summary says r2 is authoritative,
  - the regenerated report names `primary=r2@m02` and
    `frontend=192.168.1.184:3260`, or an equivalent current post-restart
    frontend from the run.
- `operator-snapshot.json` and dashboard `/operator-snapshot.json` agree with
  the regenerated summary.
- No surface publishes `Ready=True` for an older primary if newer evidence
  contradicts it.

## Dashboard Probe

Run:

```bash
sw-block ops dashboard --from-bundle <D5-bundle-dir> --listen 127.0.0.1:<port>
```

Required checks:

- `GET /operator-snapshot.json` returns HTTP 200.
- The JSON uses the same post-restart primary/publish-target evidence as
  `summary.txt`.
- `POST`, `PUT`, `PATCH`, and `DELETE` still return HTTP 405.

## Expected Sign-off

Write:

```text
internal/docs/qa-assignments/phase32-d7-stale-evidence-qa-signoff.md
```

Include:

- source commit,
- D5 bundle/run ID used,
- before/after primary evidence from the bundle,
- regenerated `summary.txt` and `operator-snapshot.json` checks,
- dashboard route checks,
- any remaining stale-evidence or replay ambiguity.
