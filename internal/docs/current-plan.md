# Current Plan: Phase 32 - Negative-First Read-Only Operator Status Surface

Status: active, 85% complete. Started on 2026-05-25 after Phase 31 restart
persistence closed.

## Product Goal

Make the Kubernetes-facing operations surface truthful under both normal and
bad states.

The target user experience is:

```text
Helm install Seaweed Block
-> create one or more PVCs
-> status is visible through Kubernetes-native objects, Conditions, Events,
   report, dashboard, and support bundle
-> failures are reported as Blocked/Unknown/CleanupRequired with stable reasons
-> no read-only surface invents Ready when evidence is stale, missing, or
   contradictory
```

This phase is intentionally negative-first. Happy-path status is not enough.
The gate must prove that known failure classes are visible, bounded, and
non-mutating.

## Source Inputs

- External failure lessons:
  `C:/work/seaweedfs/sw-block/design/external-failure-taxonomy.md`
- Semantic constraints:
  `C:/work/seaweedfs/sw-block/design/v3-semantic-constraint-checklist.md`
- Control model:
  `internal/docs/ref/phase30-control-state-dependency-review.md`
- Restart persistence:
  `internal/docs/ref/phase31-restart-persistence-claim-and-qa-checklist.md`
- Evidence vocabulary:
  `internal/docs/ref/multi-volume-ha-support-evidence-contract.md`
- Phase 32 mapping:
  `internal/docs/ref/phase32-negative-first-operator-status-plan.md`

## Scope Contract

| In | Out |
|---|---|
| read-only Kubernetes status surface for `SwBlockCluster` / `SwBlockVolume` or equivalent alpha CRD shape | mutating promote / repair / rebuild / failback / delete actions |
| ManagedVolume projection to Conditions and Events | backup / snapshot / restore |
| stable reason codes for Ready, Blocked, Recovered, CleanupRequired, Unknown, EvidenceStale | NVMe ANA feature work |
| negative-first QA using existing failure scenarios | production SLO or broad compatibility matrix |
| status/dashboard/report/operator-snapshot vocabulary alignment | operator-owned lifecycle mutation |
| bounded active probe policy for stale or high-impact state | replacing TestOps with a full controller/agent implementation |

## D1: Negative-First Contract And Failure Matrix

Goal: turn the external failure taxonomy into executable operator-status
requirements.

Acceptance:

- Each major external failure class maps to at least one status rule:
  - liveness misjudgment,
  - stale epoch/state,
  - address-derived identity,
  - stale health/durability,
  - partial cleanup,
  - recovery starvation / thundering herd.
- Each status rule maps to:
  - Condition type,
  - reason code,
  - evidence path,
  - existing or new TestOps scenario.
- The matrix explicitly says which failures are covered now and which remain
  future work.

Status: PASS on 2026-05-25.

Artifact:

- `internal/docs/ref/phase32-negative-first-operator-status-plan.md`
- QA/TestOps assignment:
  `internal/docs/qa-assignments/phase32-testops-product-grade-validation-assignment.md`

## D1a: TestOps Product-Grade Validation Layer

Goal: make TestOps the external auditor for Phase 32 status truthfulness.

Acceptance:

- QA classifies existing scenarios into happy, negative, restart,
  multi-volume, cleanup, and runner-native buckets.
- QA produces a negative-status evidence review for at least one blocked path.
- Runner primitive gaps are stated with acceptance cases, not only complaints.
- Failure snapshot standard is defined.
- The runner action backlog reflects Phase 32 needs.

Status: PASS on 2026-05-25.

Artifacts:

- `internal/docs/qa-assignments/phase32-testops-product-grade-validation-assignment.md`
- `internal/docs/qa-assignments/phase32-testops-scenario-inventory.md`
- `internal/docs/qa-assignments/phase32-negative-status-evidence-review.md`
- `internal/docs/qa-assignments/phase32-runner-action-backlog-addendum.md`
- `internal/docs/qa-assignments/phase32-failure-snapshot-standard.md`
- `internal/docs/ref/testops-runner-action-backlog.md`

## D2: CRD / Condition / Event Alpha Contract

Goal: define the Kubernetes-native read model without adding mutating operator
behavior.

Acceptance:

- Define alpha `SwBlockCluster` and `SwBlockVolume` status fields, or document
  the exact equivalent if the implementation first ships as
  `operator-snapshot.json`.
- Conditions include at minimum:
  - `Ready`,
  - `Blocked`,
  - `Recovering`,
  - `Recovered`,
  - `CleanupRequired`,
  - `EvidenceStale`.
- Kubernetes Events use stable reason codes already present in report/timeline.
- Status fields classify stable vs provisional vs test-only.
- RBAC is read-only except status/event publication.

Status: PASS on 2026-05-25.

Implementation:

- `ConditionEvidenceStale` and `ReasonEvidenceStale` are first-class.
- `ManagedVolumeFacts{EvidenceStale:true}` projects to:
  - `status=unknown`,
  - `reason_code=evidence_stale`,
  - `Ready status=Unknown`,
  - `EvidenceStale status=True severity=warning`.
- Operator events map stale evidence to a Kubernetes `Warning`.
- `operator-snapshot.json` cluster status includes `stale_volume_count`.
- CRD contract includes:
  - `read_only=true`,
  - `mutating_storage_verbs_allowed=false`,
  - read/status/event verbs only,
  - forbidden mutating storage actions.

Validation:

- `go test ./core/ops`
- `go test ./cmd/sw-block`

QA assignment:

- `internal/docs/qa-assignments/phase32-d2-crd-condition-event-qa-assignment.md`

QA evidence:

- `internal/docs/qa-assignments/phase32-d2-crd-condition-event-qa-signoff.md`

## D3: Happy-Path Status Projection Gate

Goal: prove the normal user path produces Kubernetes-native status that matches
report/dashboard/support evidence.

Gate:

```text
Helm install
-> first PVC writer/reader
-> operator/status export
-> kubectl-style status says Ready=True
-> report/dashboard/operator-snapshot agree
```

Acceptance:

- `Ready=True` with reason `first_volume_verified` or equivalent.
- PVC, PV, volume_id, primary, epoch, publish target, RF, ACK profile agree
  across status, report, dashboard, and bundle.
- No mutating action appears in status, dashboard, or operator snapshot.

Status: PASS on 2026-05-25.

Implementation:

- Added scoped surface-agreement tests for the Ready path:
  - report `summary.txt`,
  - report HTML,
  - `operator-snapshot.json`,
  - dashboard `/operator-snapshot.json`.

Validation:

- `go test ./core/ops ./cmd/sw-block`

Existing scenario seed:

- `helm-first-volume-via-sw-block-cli-chain.yaml`
- `helm-first-volume-chain.yaml`

QA sign-off:

- `internal/docs/qa-assignments/phase32-d3-d4-status-surface-qa-signoff.md`

## D4: Blocked / Negative Status Projection Gate

Goal: prove common user-visible failures become explicit Blocked/Unknown status
instead of silent timeout or false Ready.

Candidate negative cases:

- CSI node image pull failure.
- missing publish target.
- loopback publish target rejected in multi-node mode.
- writer pod mount failure.
- cleanup residue present.
- blockmaster unreachable or stale evidence.

Acceptance:

- Failure status is not `Ready=True`.
- Condition and reason code identify the blocker.
- Evidence path points to logs/events/bundle facts.
- `sw-block ops explain` and dashboard use the same reason.

Status: PASS on 2026-05-25.

Implementation:

- Added scoped surface-agreement tests for the blocked CSI image-pull path:
  - `Ready=False`,
  - `Blocked=True`,
  - `reason=csi_node_image_pull_failed`,
  - dry-run `safe_k8s.import_csi_image`,
  - no mutating action in operator snapshot.

Validation:

- `go test ./core/ops ./cmd/sw-block`

QA assignment:

- `internal/docs/qa-assignments/phase32-d3-d4-status-surface-qa-assignment.md`

QA sign-off:

- `internal/docs/qa-assignments/phase32-d3-d4-status-surface-qa-signoff.md`

Existing scenario seeds:

- `helm-support-bundle-diagnostics-chain.yaml`
- `same-node-alpha-attach-negative-chain.yaml`
- `csi-rf1-durable-restart-failure-chain.yaml`
- `light-use-first-volume-breaks-chain.yaml`

## D5: Restart / Promotion Status Consistency Gate

Goal: carry Phase 31 restart guarantees into the operator/status surface.

Gate:

```text
RF3 promotion to r2
-> k3s/product restart
-> status still shows r2 primary, epoch non-rollback, one primary
-> reader verifies data
```

Acceptance:

- CRD/status or operator snapshot preserves:
  - primary,
  - epoch,
  - publish target,
  - `post_restart_primary_count=1`.
- Old primary is not resurrected as Ready.
- Event timeline explains restart reload and authority persistence.

Status: PASS on 2026-05-25.

Implementation:

- Added scoped surface-agreement tests for promoted-authority restart status:
  - primary remains `r2`,
  - publish target remains the promoted replica frontend,
  - epoch does not roll back,
  - `Recovered=True` and reason `csi_reattach_recovered` agree across report
    summary and operator snapshot.

Validation:

- `go test ./core/ops ./cmd/sw-block`

QA assignment:

- `internal/docs/qa-assignments/phase32-d5-d6-status-surface-qa-assignment.md`

QA sign-off:

- `internal/docs/qa-assignments/phase32-d5-d6-status-surface-qa-signoff.md`

Follow-up carried into D7:

- D5 report regeneration can pick an older pre-promotion `cluster-evidence.json`
  while the post-restart evidence is present under a different filename. This
  is a stale-evidence precedence issue, not a D5 product-claim failure.

Existing scenario seeds:

- `helm-rf3-promotion-restart-persistence-chain.yaml`
- `helm-multi-volume-rf3-restart-smoke-chain.yaml`

## D6: Multi-Volume Independence Status Gate

Goal: prove status isolation for multiple PVCs.

Gate:

```text
3 RF3 PVCs
-> independent writers/readers
-> per-volume failover or restart smoke
-> each SwBlockVolume / ManagedVolume remains distinct
```

Acceptance:

- `managed_volume_count=3`.
- no cross-volume authority mixup.
- no duplicate publish target for distinct volumes unless explicitly expected.
- failed target volume status does not poison untouched volume status.

Status: PASS on 2026-05-25.

Implementation:

- Added scoped surface-agreement tests for three independent ManagedVolume
  projections:
  - `managed_volume_count=3`,
  - each volume has a distinct PVC and volume ID,
  - each Ready condition uses reason `first_volume_verified`,
  - publish targets remain distinct across the three volumes.

Validation:

- `go test ./core/ops ./cmd/sw-block`

QA assignment:

- `internal/docs/qa-assignments/phase32-d5-d6-status-surface-qa-assignment.md`

QA sign-off:

- `internal/docs/qa-assignments/phase32-d5-d6-status-surface-qa-signoff.md`

Existing scenario seeds:

- `helm-multi-volume-rf3-readiness-chain.yaml`
- `helm-multi-volume-rf3-interleaved-failover-chain.yaml`
- `helm-multi-volume-rf3-restart-smoke-chain.yaml`

## D7: Stale Evidence And Bounded Probe Gate

Goal: prove observation is honest when passive state is stale or missing.

Acceptance:

- Status can enter `EvidenceStale=True` or `Unknown` with reason.
- Bounded active probes are allowed only for:
  - high-impact promotion/failover status,
  - stale or missing status evidence,
  - cleanup residue close,
  - support/report replay.
- Every probe has timeout, evidence, and no mutation.
- Failure to probe does not become `Ready=True`.
- Report replay chooses current evidence deterministically or marks the replay
  `EvidenceStale=True`; it must not silently publish an older primary when a
  newer restart snapshot exists in the same bundle.

Status: pending.

## D8: Close Gate

Goal: close Phase 32 only when happy path, negative path, restart path, and
multi-volume path agree across all read-only operations surfaces.

Acceptance:

- D1-D7 complete.
- QA independently reruns D3-D6, including at least one negative case.
- PM reviews claim wording and non-claims.
- README/quickstart/release note only claim read-only status/operator surface.
- Close report and finished plan are written.

Status: pending.

## Progress

- D1: PASS
- D1a: PASS
- D2: PASS
- D3: PASS
- D4: PASS
- D5: PASS
- D6: PASS
- D7: pending
- D8: pending

Overall: 85%.
