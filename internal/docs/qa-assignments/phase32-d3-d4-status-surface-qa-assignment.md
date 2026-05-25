# Phase 32 D3/D4 Status Surface QA Assignment

Date: 2026-05-25

Owner: QA.

Purpose: validate that happy-path and blocked-path status projection agree
across report, dashboard, operator snapshot, and support evidence.

## Source Commit Under Test

Use the dev commit that includes:

- `core/ops/phase32_status_surface_test.go`
- D2 `EvidenceStale` contract from commit `97a8027`

## D3 Happy-Path Status Projection

Run the canonical happy-path scenario:

```text
testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml
```

Required checks:

- Scenario PASS from clean lab.
- `summary.txt` includes:
  - `managed_volume=<id> status=ready reason=first_volume_verified`
  - `managed_volume_condition=Ready status=True reason=first_volume_verified severity=info`
  - `read_only=true`
- `operator-snapshot.json` includes:
  - `read_only=true`
  - `mutation.mutation_allowed=false`
  - `cluster.ready_volume_count=1`
  - per-volume `status.status=ready`
  - per-volume `status.reason_code=first_volume_verified`
- Dashboard `/operator-snapshot.json` returns HTTP 200 and the same reason
  code.

## D4 Blocked / Negative Status Projection

Run the canonical blocked bundle scenario:

```text
testops/scenarios/helm-support-bundle-diagnostics-chain.yaml
```

Required checks:

- Scenario PASS or expected blocked-bundle PASS, depending on existing runner
  semantics.
- Blocked evidence uses `reason=csi_node_image_pull_failed`.
- `summary.txt` includes:
  - `managed_volume=<id> status=blocked reason=csi_node_image_pull_failed`
  - `managed_volume_condition=Ready status=False reason=csi_node_image_pull_failed severity=warning`
  - `managed_volume_condition=Blocked status=True reason=csi_node_image_pull_failed severity=warning`
  - `managed_volume_action=safe_k8s.import_csi_image mode=dry_run`
- `operator-snapshot.json` includes:
  - `cluster.blocked_volume_count>=1`
  - no `Ready=True` for the blocked volume
  - per-volume `Blocked=True`
  - every action has `mutation_allowed=false`
- Dashboard `/operator-snapshot.json` returns HTTP 200 and the same reason.

## Scoped Unit Tests

Run:

```bash
go test ./core/ops ./cmd/sw-block
```

The unit tests are not a substitute for the scenarios, but they prove the
surface agreement contract:

- Ready path: summary, HTML, snapshot, dashboard JSON agree.
- Blocked path: summary, HTML, snapshot, dashboard JSON agree.

## Failure Rule

Fail D3/D4 if any product surface reports:

```text
Ready=True
```

for the blocked CSI image-pull path.

## Expected Sign-off

```text
internal/docs/qa-assignments/phase32-d3-d4-status-surface-qa-signoff.md
```

