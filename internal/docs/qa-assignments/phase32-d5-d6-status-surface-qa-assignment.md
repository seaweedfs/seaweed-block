# Phase 32 D5/D6 Status Surface QA Assignment

Date: 2026-05-25

Owner: QA.

Purpose: validate restart/promotion and multi-volume status consistency across
report, dashboard, and operator-snapshot surfaces.

## Source Commit Under Test

Use the dev commit that includes D5/D6 additions to:

- `core/ops/phase32_status_surface_test.go`

## D5 Restart / Promotion Status Consistency

Run:

```text
testops/scenarios/helm-rf3-promotion-restart-persistence-chain.yaml
```

Required checks:

- Scenario PASS from clean, serially-owned lab.
- Existing Phase 31 product fields still hold:
  - `restart_promotion_status=ok`
  - `before_restart_primary == after_restart_primary`
  - `after_restart_epoch >= before_restart_epoch`
  - `post_restart_primary_count=1`
  - reader checksum after restart passes
- Status-surface checks:
  - `summary.txt` names the promoted primary and publish target after restart.
  - ManagedVolume status is `recovered` or `ready` with a stable reason.
  - `operator-snapshot.json` has `ready_volume_count=1`, `blocked_volume_count=0`.
  - No surface shows old primary as Ready.
  - Dashboard `/operator-snapshot.json` returns HTTP 200 with the same reason.

## D6 Multi-Volume Independence Status

Run:

```text
testops/scenarios/helm-multi-volume-rf3-restart-smoke-chain.yaml
```

or, if QA wants stronger fault coverage:

```text
testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml
```

Required checks:

- Scenario PASS from clean, serially-owned lab.
- `managed_volume_count=3`.
- `reader_verified_count=3` when using restart-smoke.
- `cross_volume_authority_mixup=false`.
- `duplicate_publish_target_for_distinct_volume=false`, unless a scenario
  explicitly documents shared target semantics.
- Status-surface checks:
  - `operator-snapshot.json cluster.volume_count=3`.
  - all three per-volume entries retain distinct `volume_id` / `pvc_name`.
  - no per-volume status/reason is copied from another volume.
  - dashboard `/operator-snapshot.json` returns HTTP 200 and preserves all
    three volume entries.

## Scoped Unit Tests

Run:

```bash
go test ./core/ops ./cmd/sw-block
```

These unit tests prove:

- D5 promoted restart status surface keeps primary/target/epoch evidence
  attached to the same ManagedVolume.
- D6 multi-volume snapshot keeps volume identity and publish targets distinct.

## Expected Sign-off

```text
internal/docs/qa-assignments/phase32-d5-d6-status-surface-qa-signoff.md
```

