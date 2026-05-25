# Product Roadmap

This is the short internal roadmap. Keep it current, priority-driven, and tied
to runnable gates.

## Product Goal

- Build a small Kubernetes block storage service that is easier to try and
  reason about than a large distributed-storage stack.
- Target early users running lab or small Kubernetes clusters.
- Keep alpha/beta claims narrow: dynamic PVC, mounted app write/read, clean
  teardown, read-only evidence, and explicit non-claims.
- Do not claim production HA, broad distro compatibility, performance, upgrade
  safety, or scale beyond the separately gated multi-volume HA lab boundary.

## Product Vision (North Star)

Seaweed Block should become a **simple, stable, observable Kubernetes block
platform** for teams that need real stateful workload reliability without
operating a full Ceph-scale control plane from day one.

Target product shape:

```text
install in one path (Helm/operator)
-> provision many PVC volumes safely
-> survive common failures automatically
-> explain every recovery decision with evidence
-> upgrade/rollback without state ambiguity
```

Competitive intent versus Ceph/OpenEBS:

- We are not trying to out-feature them immediately.
- We are trying to reach a trustworthy beta faster with a narrower, explicit
  contract.
- Every release must reduce "unknown behavior under failure", not just add
  protocol/features.

Maturity principle:

- New capabilities are only "real" when they are
  **functional + operational + core-stability** complete.
- If a behavior cannot be diagnosed or cleaned up, it is not a product claim.

## Maturity Gap To Ceph/OpenEBS

Already proven in our gated path:

- RF3 failover and interleaved multi-volume fault isolation.
- iSCSI ALUA/dm-multipath transparent failover.
- Helm lifecycle and cleanup evidence.
- support bundle and read-only ops/dashboard surface.
- ManagedVolume/Condition/event semantics as read-only foundation.

Still behind mature platforms:

- Mutating operator lifecycle (day-2 control plane).
- returned-replica rebuild/reintegration/failback full loop.
- backup/snapshot/restore and DR workflow.
- broad compatibility matrix and long-horizon upgrade guarantees.
- production SLO/performance characterization at scale.

Vision-driven sequencing rule:

1. First close reliability/control gaps that protect user data semantics.
2. Then add lifecycle automation (operator mutating workflows).
3. Then add enterprise data services (backup/restore/DR) and broad matrix/SLO.

## Roadmap Taxonomy

Use these labels in plans and release notes:

| Type | Meaning | Examples |
|---|---|---|
| Functional | User-visible storage capability | PVC create/mount, RF3 recovery, multipath failover, multi-volume HA |
| Operational | Install, observe, diagnose, package, operate | Helm, dashboard/report, support bundle, cleanup, future operator |
| Core Stability | Semantics and invariants that make functional claims safe | placement identity, port allocation, authority epoch, ACK frontier, fencing |

Rule: functional claims require core stability evidence and operational evidence.
If a feature works but cannot be diagnosed or cleaned up, it is not release
ready.

## Current Priority Map

| Priority | Work | Type | Status | Why It Matters |
|---|---|---|---|---|
| P0 | Phase 31 Kubernetes Restart Persistence | Core Stability + Operational + Functional | PASS | Storage must not forget data, authority, or promoted primary after Kubernetes/product restart |
| P0 | Phase 32 Negative-First Read-Only Operator Status Surface | Operational + Core Stability | active | Make Kubernetes-native status truthful under happy, blocked, restart, and multi-volume paths |
| P0 | Phase 29 Product-Owned Lifecycle And Cleanup Reliability | Operational + Core Stability | PASS | Cleanup/lifecycle is deterministic and auditable across active alpha loops |
| P0 | Phase 30 Control Model / ManagedVolume Hardening | Core Stability + Operational | PASS | Stabilizes state ownership before mutating operator, rebuild/failback, NVMe ANA, or backup work |
| P0 | Phase 28 Productized Operations And Operator Foundation | Operational + Core Stability | PASS | Turn Helm/scripts/evidence/model into one Kubernetes product operations loop before the next feature expansion |
| P0 | Publish v0.3.3 images and update doc pins | Operational | PASS | Phase 28 D12 is gated; users have a consumable immutable GHCR SHA for the operator-foundation surface |
| P0 | Multipath stale-map cleanup verifier | Operational + Core Stability | PASS in Phase 28 D1 | QA found orphan dm-multipath maps after sessions were gone; cleanup evidence now covers this |
| P0 | Phase 27 Multi-Volume HA Independence | Functional + Core Stability | PASS | Proves RF3 multi-volume readiness, CSI reattach, mounted transparent failover, and interleaved failover isolation |
| P0 | ManagedVolume / CRD / Condition contract | Core Stability + Operational | PASS in Phase 28 D12 | Operator, dashboard, report, and support bundle must consume one state model |
| P1 | Read-only operator foundation | Operational | PASS in Phase 28 D12 | Kubernetes-native day-2 loop starts with status, Conditions, Events, and no mutating storage actions |
| P1 | Product-owned cleanup/lifecycle ownership | Operational + Core Stability | seed closed in Phase 29 | Scripts still own execution, but ownership/evidence contracts are now explicit |
| P1 | Control Model Stabilization Gate | Core Stability | required before operator-grade operations | Operation layer and future operator need stable state, action, and evidence contracts |
| P1 | ManagedVolume model hardening for operator dependency | Core Stability | seed closed, folds into model gate | Operator/dashboard must consume one semantic model, not invent truth |
| P1 | Returned-replica rebuild/reintegration/failback | Functional + Core Stability | pending | Required for credible sustained HA after recovery |
| P2 | NVMe ANA Kubernetes parity | Functional | pending | Protocol parity after iSCSI multipath path is stable |
| P2 | Backup/snapshot/restore workflow | Functional + Operational | pending | Enterprise expectation, not part of current alpha |
| P2 | Metrics/alerts/production dashboard | Operational | pending | Needed for beta operations, but depends on stable model |
| P3 | Broad compatibility/performance/SLO matrix | Core Stability | pending | Production hardening, too early for current claim |

## Release Ladder

- `v0.1-alpha`: Kubernetes block foundation. CSI/PVC path, product-owned
  blockvolume lifecycle, inventory, recovery gates, iSCSI ALUA/dm-multipath,
  node-loss recovery, and read-only control-plane observation are proven in
  lab gates. This is an engineering alpha, not a polished install product.
- `v0.2-alpha`: Day-1 activation. A supported user can run the script-based
  activation path, create a first PVC, verify writer/reader data, generate a
  local read-only report, and clean up. This remains the script fallback
  boundary.
- `v0.3-alpha`: Helm activation. Package the same supported Day-1 path as a
  normal Kubernetes add-on: chart values, immutable images, CHAP/external
  iSCSI configuration, StorageClass, readiness output, first-volume smoke,
  local read-only report/dashboard, and uninstall hygiene. Phase 25 closed this
  release target on 2026-05-22.
- `v0.3.1-alpha`: Helm lifecycle hardening. Phase 26 proved chart hygiene,
  install/upgrade/rollback smoke, multi-PVC Day-1 smoke, support bundle replay,
  and strict cleanup. QA replay passed on 2026-05-22. Before external release,
  publish a new immutable GHCR SHA and update docs.
- `v0.3.2-alpha`: Multi-volume HA independence. Phase 27 closed on
  2026-05-23 with independent QA reruns across D1-D4. It proves three RF3
  PVC-backed volumes can coexist, recover independently through CSI reattach,
  recover independently through iSCSI ALUA/dm-multipath without pod recreate,
  and tolerate two interleaved volume-primary failures while the third volume
  stays stable and writable. Before external release, publish a new immutable
  GHCR SHA and update docs.
- `v0.4-beta-candidate`: Productized operations and operator foundation. Add a
  stable ManagedVolume model, Kubernetes-native CRD/Condition/Event contract,
  read-only operator foundation, and aligned CLI/report/dashboard/support
  evidence. Mutating repair/rebuild/failback workflows remain later unless they
  are separately gated.

Do not skip from scripts directly to an operator. Helm stabilizes the install
contract; Phase 27 stabilizes multi-volume HA semantics; then an operator can
own day-2 lifecycle without hiding unstable behavior.

## Active Work

### Phase 32 - Negative-First Read-Only Operator Status Surface

Type: Operational + Core Stability

Current status: active, 25%. Started on 2026-05-25 after Phase 31 restart
persistence closed.

Goal:

```text
Helm install / PVC lifecycle / recovery / restart
-> ManagedVolume projection
-> Kubernetes-native status, Conditions, Events, report, dashboard, support bundle
-> Ready only when current evidence supports Ready
-> blocked or stale states are explicit and explainable
```

This phase is negative-first. The main product risk is not "missing a status
page"; it is publishing a stable-looking state when evidence is stale, missing,
or contradictory.

Gate plan:

- D1 negative-first contract and failure matrix: PASS on 2026-05-25. Artifact:
  `internal/docs/ref/phase32-negative-first-operator-status-plan.md`.
- D1a TestOps product-grade validation layer: PASS on 2026-05-25. Assignment:
  `internal/docs/qa-assignments/phase32-testops-product-grade-validation-assignment.md`.
- D2 CRD / Condition / Event alpha contract: pending.
- D3 happy-path status projection gate: pending.
- D4 blocked / negative status projection gate: pending.
- D5 restart / promotion status consistency gate: pending.
- D6 multi-volume independence status gate: pending.
- D7 stale evidence and bounded probe gate: pending.
- D8 close gate: pending.

Non-goals:

- no mutating operator promote, repair, rebuild, failback, delete, or cleanup,
- no backup/snapshot/restore,
- no NVMe ANA feature work,
- no broad production SLO.

### Phase 31 - Kubernetes Restart Persistence (Closed)

Type: Core Stability + Operational + Functional

Current status: PASS, 100%. Started and closed on 2026-05-25 after Phase 30
control model hardening closed.

Goal:

```text
PVC writes data
-> product / k3s restarts
-> persisted authority/lifecycle/durable state is reloaded
-> CSI reattaches to the current publish target
-> reader verifies the same PVC data
-> promotion state is not forgotten
```

Gate status:

- D1 restart persistence contract review: PASS on 2026-05-25.
- D2 durable Helm values / install contract: PASS on 2026-05-25.
- D3 single-node restart gate: PASS on 2026-05-25.
- D4 RF3 restart after promotion gate: PASS (strict) on 2026-05-25.
- D5 multi-volume restart smoke: PASS (strict) on 2026-05-25.
- D6 close gate: PASS on 2026-05-25.

Claim wording and QA checklist:

- `internal/docs/ref/phase31-restart-persistence-claim-and-qa-checklist.md`

Non-goals:

- no fresh-cluster restore,
- no backup/snapshot/restore,
- no returned-replica rebuild/failback,
- no host disk loss survival,
- no broad production SLO.

### Phase 30 - Control Model / ManagedVolume Hardening

Type: Core Stability + Operational

Current status: PASS, 100%. Started on 2026-05-24 after Phase 29 lifecycle
cleanup reliability closed. Closed on 2026-05-25.

Goal:

```text
PVC/PV + ManagedVolume + Launcher + CSI + Authority + HostPath + Cleanup
-> one explicit state dependency model
-> each fact has an authority
-> each action has an executor
-> each status/condition has evidence
```

Gate status:

- D1 control-state dependency review: PASS on 2026-05-24. Artifact:
  `internal/docs/ref/phase30-control-state-dependency-review.md`.
- D2 ManagedVolume field/action contract tightening: PASS on 2026-05-25.
- D3 cleanup projection ownership cleanup: PASS on 2026-05-25.
- D4 regression gates: PASS on 2026-05-25.
- D5 close gate: PASS on 2026-05-25.

Close artifacts:

- `internal/docs/qa-assignments/phase30-control-model-managed-volume-hardening-close-report.md`
- `internal/docs/finished-plans/phase30_finishedplan_control_model_managed_volume_hardening.md`

Non-goals:

- no rebuild/failback implementation,
- no mutating operator action,
- no NVMe ANA implementation,
- no backup/snapshot/restore implementation,
- no broad production SLO.

### Phase 29 - Product-Owned Lifecycle And Cleanup Reliability

Type: Operational + Core Stability

Current status: PASS, 100%. Started on 2026-05-24 after Phase 28 D13
published-image release packaging passed.

Goal:

```text
install -> run multi-volume HA loops -> cleanup
-> residue check is deterministic
-> evidence vocabulary is stable
-> no helper TOCTOU race masks real state
```

Phase 29 makes lifecycle and cleanup less dependent on helper timing and more
product-owned. The seed issue is the Phase 28 D12 non-blocking multi-volume
cleanup TOCTOU race. The exit target is deterministic cleanup evidence across
the documented RF3 multi-volume loops.

Gate status:

- D1 cleanup ownership matrix (product-owned vs helper-owned steps): PASS on
  2026-05-24. Artifact:
  `internal/docs/ref/phase29-cleanup-ownership-matrix.md`.
- D2 helper TOCTOU cleanup fixes (`run-multi-volume-*`): PASS for the primary
  `scripts/run-multi-volume-example.sh` target on 2026-05-24. Evidence:
  `20260524-140609-c204` plus N=3 regression
  `20260524-141408-35e3`, `20260524-141615-7be6`,
  `20260524-141814-83f6`.
- D3 lifecycle evidence contract parity (summary/report/dashboard/operator
  snapshot): PASS on 2026-05-24. Artifact:
  `internal/docs/ref/phase29-lifecycle-evidence-contract.md`; `core/ops`
  and `cmd/sw-block` tests validate bundle cleanup evidence propagation.
- D4 deterministic cleanup reruns across RF3 multi-volume gates: PASS on
  2026-05-24. QA replay: 181/181 actions across five scenarios.
  Validation report:
  `internal/docs/qa-assignments/phase29-deterministic-cleanup-qa-validation.md`.
- D5 phase close with independent QA replay: PASS. Close report:
  `internal/docs/qa-assignments/phase29-lifecycle-cleanup-reliability-close-report.md`.

D5 hardening:

- `scripts/verify-helm-cleanup.sh` now emits `iscsi_residue_count` directly in
  `cleanup-summary.txt`.
- Quick validation: `cleanup-residue-chain.yaml` run `20260524-215539-4285`,
  PASS, 13/13 actions.

Non-goals:

- no mutating operator cleanup or lifecycle actions,
- no rebuild/failback implementation,
- no backup/snapshot/restore implementation,
- no new NVMe ANA claim,
- no production lifecycle SLO.

### Phase 28 - Productized Operations And Operator Foundation

Type: Operational + Core Stability

Current status: PASS, 100%. Started on 2026-05-23 after Phase 27 D5/D6/D8
independent QA reruns passed. Expanded on 2026-05-23 to include the
productized operations / operator foundation loop.

Goals:

- close cleanup residue gaps, starting with orphan dm-multipath maps,
- make Phase 27 repeatability measurable through flake matrices,
- convert TestOps runner pain into concrete action backlog and reference
  scenarios,
- align support/report/dashboard evidence for multi-volume HA failures,
- stabilize the ManagedVolume model as the operations read model,
- define the CRD/Condition/Event contract for Kubernetes-native status,
- establish a read-only operator foundation with no mutating storage actions.

Closed foundation gates:

- D1 multipath cleanup verifier: `20260523-182000-41ee`, 13/13 PASS.
- D2 Phase 27 flake matrix:
  - D3 mounted failover N=5, 5/5 PASS, `flake_rate_percent=0`.
  - D4 interleaved failover N=5, 5/5 PASS, `flake_rate_percent=0`.
- D3 TestOps action backlog: `internal/docs/ref/testops-runner-action-backlog.md`.
- D4 multi-volume support evidence contract:
  `internal/docs/ref/multi-volume-ha-support-evidence-contract.md`.
- D5-D8 structure/model/readiness review:
  `internal/docs/ref/phase28-structure-model-readiness-review.md`.
- Operational reliability QA validation:
  `internal/docs/qa-assignments/phase28-operational-reliability-qa-validation.md`
  confirms D1 plus D2 N=5/N=5 flake matrices with 0 failures and 0 flakes.

Closed operator-foundation gates:

- D9 ManagedVolume operational model contract: PASS in D12 close on
  2026-05-24.
- D10 Kubernetes CRD / Condition / Event contract: PASS in D12 close on
  2026-05-24.
- D11 read-only operator foundation gate: PASS in D12 close on 2026-05-24.
  `sw-block ops report` writes `operator-snapshot.json`; dashboard serves
  `/operator-snapshot.json`; operator snapshot is read-only and explicitly
  declares mutation non-claims.
- D12 productized operations close gate: PASS on `bf7281b` after final rerun.
  Final run IDs: G1 `20260524-103052-beb2`, G2 `20260524-103143-7c41`,
  G3 `20260524-103350-901d`, G5 `20260524-103511-d329`.

Closed release gate:

- D13 release packaging: draft v0.3.3 release note and doc alignment prepared;
  immutable GHCR images published and final pins recorded. Published-image
  release-path QA `20260524-124413-829a`, PASS, 34/34 actions.

Closed carryover:

- The non-blocking `scripts/run-multi-volume-example.sh` cleanup TOCTOU race
  found during D12 rerun cycle is closed in Phase 29 D2.

Non-goals:

- no new NVMe ANA claim,
- no backup/snapshot/restore implementation,
- no mutating operator lifecycle implementation,
- no large model refactor without gates.

### Phase 27 - Multi-Volume HA Independence

Type: Functional + Core Stability

Current status: PASS, 100%. Phase 27 closed on 2026-05-23. D5/D6/D8
independent QA reruns also passed on 2026-05-23.

Closed gates:

- D1 RF3 readiness: `20260523-094437-c24c`, 35/35 PASS.
- D2 per-volume CSI reattach recovery: `20260523-094707-bbf5`, 29/29 PASS.
- D3 mounted transparent failover: `20260523-155700-9a63`, 47/47 PASS with
  D5/D6 instrumentation.
- D4 interleaved multi-volume failover: `20260523-160109-cd3d`, 55/55 PASS
  with D5/D6 instrumentation.
- D8 app-spread mounted failover: `20260523-160348-6cc2`, 32/32 PASS.

Open follow-ups moved into Phase 28:

- D7 N>=5 flake matrix.
- Multipath stale-map cleanup verifier is closed in Phase 28 D1.

## Functional Capability Backlog

### Availability And Recovery

Closed:

- RF3 `sync-quorum` recovery through CSI/pod recreate.
- RF3 Kubernetes node-loss recovery through CSI reattach.
- RF3 iSCSI ALUA + Linux dm-multipath transparent mounted failover for one
  volume.

Closed:

- Multi-volume HA independence, Phase 27, for N=3 RF3 PVCs in the gated lab.

Later:

- Returned-replica rebuild/reintegration/failback.
- Stronger committed-frontier publication and audit.
- Longer soak under failure.
- NVMe ANA Kubernetes multipath parity.

### Protocols And Backends

Closed / current:

- iSCSI is the default frontend.
- NVMe-oF exists behind explicit protocol selection and gates.
- `walstore` remains the MVP durable backend.
- Protocol hardening docs live under `internal/docs/protocol/`.

Later:

- NVMe ANA parity after iSCSI multipath semantics are stable.
- Storage-engine boundary tests and backend pressure gates.
- RDMA/KV-backed experiments only if they preserve block semantics:
  ACK profile, durable frontier, fencing, retry behavior, and supportable
  failure evidence.

### Data Services

Later:

- Backup/snapshot/restore.
- Cross-cluster DR contract.
- Capacity/quota enforcement with operator-readable errors.

These are important product features, but they should not interrupt the current
multi-volume HA and operator readiness tracks.

## Operational Product Backlog

### Packaging And Install

Closed:

- Script activation Day-1 loop.
- Helm first-volume loop.
- Helm lifecycle hardening: chart hygiene, upgrade/rollback smoke,
  multi-PVC Day-1, support bundle replay, cleanup.

Current:

- Publish v0.3.2 immutable GHCR images and update README / quickstart /
  release note pins.

Next:

- Operator lifecycle design after Helm and Phase 27 semantics are stable.

### Observation And Support

Closed:

- `sw-block ops inventory`.
- `sw-block ops cluster`.
- `sw-block ops report`.
- `sw-block ops dashboard` local read-only surface.
- `sw-block ops explain`.
- Product-owned timeline / event evidence.
- Support bundle replay from saved artifacts.

Next:

- Keep report/dashboard/reason-code output aligned as Phase 27 adds
  multi-volume failover evidence.
- Add concise runbook mappings from stable reason codes to operator actions.

Later:

- Metrics and alerts.
- Production hosted dashboard.
- Audit/compliance reports.

### Operator

Preconditions:

- Helm lifecycle stable.
- ManagedVolume model is the single read model.
- Phase 27 does not reveal unresolved multi-volume semantics.

Initial operator scope:

- CRD status only.
- Conditions and Events from ManagedVolume projection.
- Install/readiness/cleanup visibility.
- No mutating promote/repair/rebuild buttons in the first operator release.

Later:

- Safe repair/rebuild/failback workflows with RBAC, audit, dry-run, and hard
  gates.

## Core Stability Backlog

### State Model And Fact Authority

Closed / current:

- ManagedVolume Operations Model seed.
- Truth-domain and invariant docs under `internal/docs/protocol/`.
- Layered Participant / Fact Authority / Master / Executor model for reusable
  control-plane decomposition.
- Observation slots merge by `(volume, replica)` with independent freshness.
- Materialized workload ports persist so new volumes cannot reshuffle existing
  ports.
- Phase 27 proved per-volume independence under sequential and interleaved
  faults for N=3 RF3 PVCs.

Next:

- Run a `Control Model Stabilization Gate` before operator-grade operations:
  - freeze the ManagedVolume fact model used by report/dashboard/explain,
  - define Fact Authorities for Kubernetes placement, CSI publish/stage,
    authority, recovery, host path, workload, cleanup, and evidence,
  - separate fact publication, orchestration decisions, executor actions, and
    timeline evidence,
  - define dual-mode aggregation: passive fact streams for steady state,
    bounded active probes for high-impact decisions,
  - define state priority when multiple automata overlap, such as node loss
    affecting authority, CSI, host path, and cleanup at the same time,
  - keep mutating actions as dry-run/read-only contracts until the model has
    gates and audit evidence.
- Extend ManagedVolume to express multi-volume failover isolation facts:
  target volume, non-target volume stability, primary_count, stale I/O result,
  and host-path recovery method.
- Keep local controllers small: participants emit observations, Fact
  Authorities publish authoritative facts, masters make global decisions,
  executors perform allowed actions, evidence records why.

### Cleanup And Idempotence

Closed / current:

- Helm cleanup verification.
- Multi-volume helper waits for generated blockvolume Deployments before
  claiming cleanup success.

Next:

- Extend cleanup verification to catch orphan dm-multipath maps after mounted
  failover tests.
- Make more cleanup product-owned instead of script-owned.
- Preserve test/debug artifacts while guaranteeing no live sessions/processes.

### TestOps And Lab Control

Current:

- TestOps scenarios are release evidence.
- Runner still relies heavily on SSH and shared lab convention.

Next:

- TestOps controller/agent design for node snapshots, logs, locks, and
  artifact collection.

Later:

- Scenario library, matrix scheduling, hosted validation, and enterprise test
  automation.

## Release Rules

- Every user-facing claim needs a gate, run ID, and support artifact.
- Do not promote TestOps-only behavior into docs unless a user path exists.
- Prefer one coherent milestone PR, not one PR per tiny fix.
- Keep `internal/docs/current-plan.md` as the active execution pointer.
- When a plan closes, add a finished plan under `internal/docs/finished-plans/`.
- Keep long audits and historical references under `internal/docs/ref/`.
