# QA Sign-off - Phase 35 D5 Stale / Unreachable Status Projections

Verdict: **PASS, no findings.** Driven live through the write-mode
operator-status controller, both stale-evidence projections publish exactly the
right CRD status + Warning Events, with no false `Ready=True` and no
inappropriate `Blocked=True`:

- `status_endpoint_unreachable` → `status=unknown`, `Ready=Unknown`,
  `EvidenceStale=True`, **no Blocked condition** — a "we don't know" surface,
  not a claim of health and not a hard block.
- `wal_integrity_fault` → `status=blocked`, `Ready=False`, `Blocked=True` — a
  real, known blocker; never `Ready=True`.

Date: 2026-06-04

Source commit: `6859be1 phase35: cover stale operator status projections`
(branch `phase33-testops-failure-hardening`)

Note: `6859be1` is **test-only** relative to `a2714c8` (the diff is
`operator_status_controller_test.go` + two docs; zero product `.go`). The
product binary is therefore the D4-validated `a2714c8` binary; the a2714c8
images already on m01/m02/tp01 were reused (no rebuild). The D5 unit additions
pass: `go test ./core/ops` → ok.

## How the gate was driven

Both reasons are reachable through the bundle projection: a `VolumeEvidence`
carries `reason` straight into `facts.ProductReason`
(`managedVolumeFactsFromVolumeEvidence`: `ProductReason = volume.Reason`), and
`classifyManagedVolume` maps them (`managed_volume_model.go:526` wal_integrity →
Blocked; `:547` status_endpoint_unreachable → Unknown; `:753` the
EvidenceStale condition).

So I crafted **one** `cluster-evidence.json` with **two** volumes and ran a
single write-mode reconcile inside the operator-status pod (as the constrained
SA):

```json
{ "schema_version":"1.0", "captured_at":"2026-06-04T00:00:00Z", "status":"degraded",
  "nodes":[], "volumes":[
    {"volume_id":"pvc-unreachable","pvc_name":"pvc-unreachable","replication_factor":1,
     "status":"degraded","reason":"status_endpoint_unreachable","replicas":[]},
    {"volume_id":"pvc-walfault","pvc_name":"pvc-walfault","replication_factor":1,
     "status":"blocked","reason":"wal_integrity_fault","replicas":[]} ] }
```

```text
sw-block ops operator-status --from-bundle /tmp/bb2 --namespace kube-system --cluster-name sw-block
-> operator_status=write_status cluster=kube-system/sw-block volumes=2 events=4 mutation_allowed=false
   EXIT=0
```

Two volumes in one reconcile also re-confirms the D4 best-effort fix: both
statuses are written and the reconcile exits 0.

## Results

| Surface | pvc-unreachable | pvc-walfault |
|---|---|---|
| `.status.status` | `unknown` | `blocked` |
| `.status.reasonCode` | `status_endpoint_unreachable` | `wal_integrity_fault` |
| Ready condition | `Ready=Unknown` (sev warning) | `Ready=False` (sev warning) |
| Second condition | `EvidenceStale=True` (sev warning) | `Blocked=True` (sev warning) |
| Blocked condition present? | **no** (correct) | yes |
| `Ready=True` anywhere? | **no** (0) | **no** (0) |
| Warning Event | `Warning / status_endpoint_unreachable` | `Warning / wal_integrity_fault` |

Checklist:

| Check | Result | Evidence |
|---|---|---|
| Unreachable → CRD status published | PASS | `status=unknown`, `reasonCode=status_endpoint_unreachable` |
| Unreachable → `Ready=Unknown`, not True/False | PASS | condition `Ready=Unknown` |
| Unreachable → `EvidenceStale=True` | PASS | condition `EvidenceStale=True` |
| Unreachable → NOT `Blocked=True` | PASS | no Blocked condition on the volume (count 0) |
| WAL fault → CRD status published | PASS | `status=blocked`, `reasonCode=wal_integrity_fault` |
| WAL fault → `Ready=False`, `Blocked=True`, never `Ready=True` | PASS | conditions `Ready=False` + `Blocked=True`; `Ready=True` count 0 |
| Warning Events for both | PASS | one Warning event per volume, reason-matched |
| No `Ready=True` on any surface | PASS | CRD (both), report dir `grep -ic ready.*true`=0, operator-snapshot/summary show unknown+blocked |
| SA mutation boundary unchanged | PASS | `create events: yes`, `patch …/status: yes`; `patch swblockvolumes (spec): no`, `create pods: no`, `delete pvc: no` |

All report surfaces agree (summary.txt):

```text
managed_volume=pvc-unreachable status=unknown reason=status_endpoint_unreachable
managed_volume=pvc-walfault    status=blocked reason=wal_integrity_fault
```

## The Distinction That Matters (and is correct)

This gate validates the negative-first contract's hardest call: telling apart
"the evidence is stale / I can't reach the status endpoint" from "the volume is
genuinely broken."

- **Unreachable** is `Unknown` + `EvidenceStale`, **never** `Blocked`. The
  surface does not claim the volume is healthy (no `Ready=True`) and does not
  claim it is broken (no `Blocked=True`) — it honestly says "I don't know." An
  operator is not paged for a false outage, and is not falsely reassured.
- **WAL integrity fault** is `Blocked` + `Ready=False` — a documented blocker
  the operator should act on.

Both behave exactly as specified.

## Carry-forward (non-blocking, unchanged from D4)

The same-reason event-name collapse and per-reconcile event minting noted in the
D4 sign-off still apply (each volume's two same-reason conditions emit two
events that share a name → one distinct event lands per volume; the name embeds
`observedAt`). `events=4` was reported while 2 distinct events landed (one per
volume). Not a D5 regression — already filed. (Of note: the event name suffix
here was `…1780531200000000000` = the bundle's `captured_at`, confirming the
name is keyed on the snapshot's observedAt.)

## Lab State

Clean — both `SwBlockVolume` stubs + `SwBlockCluster` deleted, Warning events
deleted, helm uninstalled, both CRDs deleted; 0 sw-block pods, 0 CRDs, 0 iSCSI
sessions.

## Bottom Line

- **D5 PASS, no findings.** `status_endpoint_unreachable` projects
  `Unknown`/`EvidenceStale=True` (not Blocked, not Ready), and
  `wal_integrity_fault` projects `Blocked`/`Ready=False` (never Ready=True);
  both publish a matching Warning event; the reconcile exits 0 for a
  two-volume snapshot; and the SA still has zero storage/workload mutation power.
- **D5 can close.** The only open items are the pre-existing, non-blocking D4
  event-hygiene follow-ups (stable per-(object,reason) event name so
  cross-iteration emits dedupe/aggregate; per-condition `type` so the two
  same-reason conditions emit distinct events).
