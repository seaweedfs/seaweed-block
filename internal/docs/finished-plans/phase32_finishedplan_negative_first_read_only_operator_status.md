# Finished Plan: Phase 32 - Negative-First Read-Only Operator Status Surface

Status: **PASS, 100%**.

Dates: started and closed on 2026-05-25.

## Goal

Make the Kubernetes-facing operations surface truthful under both normal and
bad states.

The delivered loop is:

```text
Helm install / PVC lifecycle / recovery / restart
-> ManagedVolume projection
-> report, dashboard, operator-snapshot, Condition, Event, support bundle
-> Ready only when current evidence supports Ready
-> Blocked/Unknown/EvidenceStale when evidence is missing, stale, or contradictory
```

## What Shipped

- Negative-first status contract and failure matrix.
- TestOps product-grade validation layer:
  - scenario inventory,
  - negative-status evidence review,
  - runner action backlog addendum,
  - failure snapshot standard.
- Alpha read-only CRD / Condition / Event contract.
- `EvidenceStale` ManagedVolume projection:
  - `status=unknown`,
  - `reason_code=evidence_stale`,
  - `Ready=Unknown`,
  - `EvidenceStale=True`,
  - stale evidence maps to a Kubernetes Warning event.
- Status-surface agreement across:
  - report summary,
  - report HTML,
  - `operator-snapshot.json`,
  - dashboard `/operator-snapshot.json`,
  - support-bundle replay.
- Bundle replay precedence hardening:
  - `cluster-after-restart.json` is included as cluster evidence,
  - newest `captured_at` is selected before path rank,
  - older pre-promotion snapshots no longer override post-restart truth.

## Validation

Scoped tests:

```text
go test ./core/ops ./cmd/sw-block
```

Result: PASS.

QA gates:

| Scope | Run / artifact | Result |
|---|---:|---|
| D2 CRD / Condition / Event contract | `phase32-d2-crd-condition-event-qa-signoff.md` | PASS |
| D3 happy first-volume status | `20260525-141234-8a89` | 34/34 PASS |
| D4 blocked CSI-image-pull status | `20260525-141341-9c7d` | 38/38 PASS |
| D5 restart / promotion status | `20260525-143121-3aaa` | 34/34 PASS |
| D6 multi-volume restart smoke | `20260525-143400-e085` | 36/36 PASS |
| D6 stronger interleaved failover | `20260525-143747-f93a` | 56/56 PASS |
| D7 stale evidence replay | `20260525-172250-bf28` | PASS |

## Product Claim Now Supported

Seaweed Block's read-only operations status can explain:

- a healthy first PVC,
- a blocked CSI/image-pull path,
- a promoted RF3 volume after restart,
- three independent RF3 volumes,
- stale or reconverging post-restart evidence without false Ready.

The same status vocabulary appears in report, dashboard, operator-snapshot, and
support-bundle replay.

## Explicit Non-Claims

- No mutating operator action.
- No promote/repair/rebuild/failback/delete/cleanup execution.
- No backup/snapshot/restore.
- No NVMe ANA feature work.
- No broad production SLO.
- No claim that `Ready=Unknown` is a failure when the freshest evidence is
  still reconverging.

## Follow-Ups

- Add bounded refresh probe or scenario settle wait for immediate post-restart
  reconvergence windows.
- Continue improving TestOps runner-native primitives:
  - JSONPath wait,
  - completed pod wait,
  - Helm install/uninstall,
  - product report/dashboard capture,
  - failure snapshot collection.
- Keep mutating operator workflows out until executor policy and safety
  invariants are explicitly gated.

## Close Artifacts

- `internal/docs/qa-assignments/phase32-negative-first-read-only-operator-status-close-report.md`
- `internal/docs/qa-assignments/phase32-d2-crd-condition-event-qa-signoff.md`
- `internal/docs/qa-assignments/phase32-d3-d4-status-surface-qa-signoff.md`
- `internal/docs/qa-assignments/phase32-d5-d6-status-surface-qa-signoff.md`
- `internal/docs/qa-assignments/phase32-d7-stale-evidence-qa-signoff.md`
