# QA Sign-off - Phase 35 D6 Stable Operator Event Identity

Verdict: **PASS, no findings.** Repeated write-mode reconciles against a
persistent blocked volume now produce a **single, stable** Kubernetes Event that
does not grow per reconcile. Three consecutive one-shot reconciles all exit 0
and the distinct Event count stays at **1**. This closes the event-hygiene
follow-up filed in the D4/D5 sign-offs (the timestamped name that minted a new
Event every iteration).

Date: 2026-06-04

Source commit: `1b22ccc phase35: stabilize operator event identity`
(branch `phase33-testops-failure-hardening`; HEAD, on top of
`d6506dc docs: close phase35 d5 stale status gate`)

## The fix (verified in code)

`kubernetesEventName` dropped its `at time.Time` parameter and the
`.UnixNano()` suffix. The name is now stable per **object + type + reason**:

```go
func kubernetesEventName(event OperatorKubernetesEvent) string {
    base := kubernetesName(event.InvolvedObject.Name + "-" + event.Type + "-" + event.Reason)
    if base == "unknown-volume" { base = "sw-block-event" }
    return base
}
```

So the same persistent condition resolves to the same Event name on every
reconcile → the existing idempotent `409 Conflict` handling (D4 `a2714c8`)
deduplicates it instead of creating a fresh Event. Including `event.Type` keeps
`Normal` and `Warning` for the same object/reason on distinct names.

Only `core/ops/kubernetes_status_writer.go` changed (product) vs `a2714c8`;
fresh images built + imported to m01/m02/tp01. `go test ./core/ops` ok.

## Live results — repeated reconcile, no Event growth

Persistent blocked volume driven via a one-volume `cluster-evidence.json`
(`pvc-walfault`, `reason=wal_integrity_fault`), one-shot reconcile run **3×**
inside the operator-status pod (as the constrained SA):

```text
RUN #1 exit=0 | operator_status=write_status volumes=1 events=2 | distinct_events=1
RUN #2 exit=0 | operator_status=write_status volumes=1 events=2 | distinct_events=1
RUN #3 exit=0 | operator_status=write_status volumes=1 events=2 | distinct_events=1
```

The one Event, after all three runs:

```text
NAME                                       TYPE      REASON                COUNT
pvc-walfault-warning-wal-integrity-fault   Warning   wal_integrity_fault   1
```

| Check | Result | Evidence |
|---|---|---|
| Repeated reconcile exits 0 | PASS | RUN #1/#2/#3 all `exit=0` |
| Second/third reconcile creates **no new** Event for same object/type/reason | PASS | `distinct_events` stays `1` across all runs |
| Event name stable, no timestamp suffix | PASS | name = `pvc-walfault-warning-wal-integrity-fault` (object-type-reason) |
| Type embedded (Normal vs Warning distinct names) | PASS | `…-warning-…`; a Normal event for the same object/reason would be `…-normal-…` |
| Status surface still correct | PASS | `status=blocked reason=wal_integrity_fault Ready=False Blocked=True`, `Ready=True` count 0 |
| SA mutation boundary unchanged | PASS | `create events: yes`, `patch …/status: yes`; `patch swblockvolumes (spec): no`, `create pods: no`, `delete pvc: no` |

Before this fix (D4/D5), the name embedded `observedAt.UnixNano()`, so three
reconciles would have left **three** distinct Events for the same condition.
Now it is one.

## Minor observations (non-blocking, not regressions)

1. **Dedupe, not aggregate.** The repeated Event keeps `COUNT=1` and its
   original `lastTimestamp` — the `409` short-circuits before any update. The
   D6 goal (no unbounded Event growth) is fully met; if richer telemetry is ever
   wanted, the conventional core/v1 pattern is to `PATCH count++` /
   `lastTimestamp` on the 409 path so a long-running blocker reads as "seen N
   times, still active." The CRD `.status.observedAt` already carries live
   freshness, so this is optional polish, not a gap.
2. **Within one reconcile, the two same-type+reason conditions still collapse
   to one Event.** A blocked volume emits `Ready=False` and `Blocked=True`, both
   `Warning` + `wal_integrity_fault`, so they share the (now stable) name and
   dedupe to a single Event (`events=2` reported, 1 distinct). This is the same
   D4 nuance #1 and is acceptable — the CRD status conditions carry both Ready
   and Blocked detail; the Event is a breadcrumb. D6 intentionally only splits
   on `type` (Normal vs Warning), which is the meaningful split.

## Lab State

Clean — `SwBlockVolume`/`SwBlockCluster` stubs deleted, the Event deleted, helm
uninstalled, both CRDs deleted; 0 sw-block pods, 0 CRDs, 0 iSCSI sessions.

## Bottom Line

- **D6 PASS.** Operator Event identity is stable per object+type+reason; repeated
  write-mode reconciles against a persistent blocked volume exit 0 and produce
  no new Event objects (distinct count stays 1), the status surface stays
  correct, and the SA still has zero storage/workload mutation power.
- This **closes the D4/D5 event-hygiene carry-forward** (per-reconcile Event
  growth). The remaining items are optional polish only: count++/lastTimestamp
  aggregation on the 409 path, and per-condition Event splitting — neither
  blocks D6 or anything downstream.
- **D6 can close.**
