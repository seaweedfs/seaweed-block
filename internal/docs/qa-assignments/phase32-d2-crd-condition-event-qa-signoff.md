# QA Sign-off - Phase 32 D2 CRD / Condition / Event Alpha Contract

Verdict: **PASS**

Date: 2026-05-25

Validated source commit: `97a8027 ops: add phase32 stale evidence status contract`

## Scope

Contract-level QA on the D2 read-only Kubernetes status surface. No scenario
rerun. Verifies the new `EvidenceStale` Condition + `evidence_stale` reason
project end-to-end and that the CRD contract RBAC / verb / forbidden-action
boundary is held.

## Scoped Tests

```text
go test ./core/ops ./cmd/sw-block
```

Result on synced D2 tree: **PASS** (`ok core/ops`, `ok cmd/sw-block` in 2s).

## Contract Checks

### CRD read-only + RBAC boundary

`core/ops/managed_volume_crd_contract.go`:

| Field | Required | Observed |
|---|---|---|
| `read_only` | `true` | `true` |
| `rbac.mutating_storage_verbs_allowed` | `false` | `false` |
| `rbac.allowed_verbs` | `get,list,watch,update_status,patch_status,create_event` | exactly these 6, in that order |
| `rbac.forbidden_actions` | `promote,repair,rebuild,failback,delete_storage,cleanup_live_state` | exactly these 6, in that order |

No mutating storage verb is permitted. PASS.

### Condition vocabulary

```go
ConditionReady           = "Ready"
ConditionBlocked         = "Blocked"
ConditionRecovering      = "Recovering"
ConditionRecovered       = "Recovered"
ConditionCleanupRequired = "CleanupRequired"
ConditionEvidenceStale   = "EvidenceStale"
```

All 6 required Condition types defined. PASS.

(Note: `ConditionInvalid` is also present, used for the
`ManagedVolumeStatusInvalid/Unsafe` projection. Not in the assignment's
required list but harmless — it carries `Severity=error` and clearly is not
a Ready surface.)

### `EvidenceStale` Projection Chain

Traced end-to-end in code:

| Step | Code site | Evidence |
|---|---|---|
| Trigger | `core/ops/managed_volume_model.go` `classifyManagedVolume` | `if facts.EvidenceStale \|\| facts.ProductReason == ReasonEvidenceStale { return ManagedVolumeStatusUnknown, defaultString(facts.EvidenceStaleReason, ReasonEvidenceStale) }` |
| Reason code | constants | `ReasonEvidenceStale = "evidence_stale"` |
| Status field | `classifyManagedVolume` return | `ManagedVolumeStatusUnknown` (= `"unknown"`) |
| Ready Condition | `managedVolumeConditionsForProjection` default branch | `Type=Ready Status=Unknown Severity=warning Reason=evidence_stale Message="managed volume evidence is stale or unreachable; readiness is not claimed"` |
| Second Condition | same function | `Type=EvidenceStale Status=True Severity=warning Reason=evidence_stale Message="bounded probe or fresh evidence is required before claiming readiness"` |
| Operator event mapping | `ManagedVolumeEventRule` table | `ConditionSeverity=warning -> KubernetesType=Warning` |
| Operator snapshot cluster aggregate | `core/ops/operator_snapshot.go` | `StaleVolumeCount int json:"stale_volume_count"`; incremented when per-volume `ReasonCode == ReasonEvidenceStale` OR `hasCondition(ConditionEvidenceStale, "True")`; cluster-level `EvidenceStale=True` Condition added when count > 0 |

Direct test coverage: `core/ops/operator_snapshot_test.go` sets
`EvidenceStale: true, EvidenceStaleReason: ReasonEvidenceStale` on a fact
input and asserts the cluster Condition `Type=EvidenceStale Status="True"
Reason=ReasonEvidenceStale` appears in the resulting snapshot.

All six steps of the chain verified. PASS.

## Hard-Gate Acceptance

| Requirement | Result |
|---|---|
| `go test ./core/ops ./cmd/sw-block` PASS | PASS |
| `crd_contract.read_only=true` | PASS |
| `crd_contract.rbac.mutating_storage_verbs_allowed=false` | PASS |
| Allowed verbs exact match | PASS |
| Forbidden actions exact match | PASS |
| Six required Condition types defined | PASS |
| Stale-evidence facts → `status=unknown` | PASS |
| Stale-evidence projection → `Ready=Unknown severity=warning` | PASS |
| Stale-evidence projection → `EvidenceStale=True severity=warning` | PASS |
| Warning event mapping for severity=warning | PASS |
| `operator-snapshot.json cluster.stale_volume_count` field present + incremented + cluster Condition added | PASS |

## Non-Claim Boundary

The contract retains the negative-first discipline:

- No mutating Kubernetes verb on storage state (`forbidden_actions` covers
  `promote/repair/rebuild/failback/delete_storage/cleanup_live_state`).
- No controller manager is implied; the contract documents a snapshot shape,
  not a reconciliation loop.
- No new product action is exposed; the only writes are status/event
  publication.
- Stale evidence forces `Ready=Unknown` rather than `Ready=True` — the
  PM-visible rule from the parent assignment holds.

## Blocking Findings

**None.**

## Non-Blocking Findings

### N1: `Severity=warning` is a string, not an enum

`ObservationCondition.Severity` is a free-form string today (`info`,
`warning`, `error`). The event-rule mapping does substring matching. Worth
introducing a named type when D2 hardens into a CRD ship-grade contract,
so a typo (e.g. `Severity="warn"`) doesn't silently fall back to the
`info -> Normal` mapping.

Not blocking because the current call sites (`managedVolumeConditionsForProjection`)
only emit the documented three values and the unit tests cover them.

### N2: `ConditionInvalid` is in vocabulary but not in the assignment's required list

`ConditionInvalid` (and `ManagedVolumeStatusUnsafe`) appear in the
projection switch with severity=error. They are correctly forbidden from
ever co-existing with `Ready=True`, but the assignment's required
Condition list (Ready/Blocked/Recovering/Recovered/CleanupRequired/EvidenceStale)
doesn't mention them. Worth either documenting `Invalid` as part of the
alpha contract or removing it from the public emission path.

Not blocking because `Invalid` only fires on safety-invariant violations and
is consistent with the negative-first rule.

## Verdict

Phase 32 D2 contract **PASS** on independent QA review.

The new `EvidenceStale` Condition + `evidence_stale` reason project
correctly through `ManagedVolumeFacts` → projection → Ready/EvidenceStale
Conditions → operator event type=Warning → operator-snapshot
`cluster.stale_volume_count`. CRD contract holds the read-only / RBAC
boundary with explicit forbidden mutating actions.

D2 unblocks D3-D7 scenario authoring against the stable Condition + reason
vocabulary. The Phase 32 D1a Workstream A inventory's "D7 stale-evidence
gate missing" gap can now be filled with a scenario that asserts
`status=unknown reason=evidence_stale` on the report/dashboard/operator-snapshot
surfaces.
