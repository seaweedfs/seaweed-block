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
| P0 | Phase 28 Operational Reliability And TestOps Hardening | Operational + Core Stability | active | Cleanup, repeatability, runner primitives, and support evidence must stabilize before the next feature expansion |
| P0 | Publish v0.3.2 images and update doc pins | Operational | next | Phase 27 uses local TestOps images; users need a consumable immutable GHCR SHA |
| P0 | Multipath stale-map cleanup verifier | Operational + Core Stability | PASS in Phase 28 D1 | QA found orphan dm-multipath maps after sessions were gone; cleanup evidence now covers this |
| P0 | Phase 27 Multi-Volume HA Independence | Functional + Core Stability | PASS | Proves RF3 multi-volume readiness, CSI reattach, mounted transparent failover, and interleaved failover isolation |
| P1 | Operator lifecycle design and first CRD/Condition shape | Operational | after Phase 28 model review | Kubernetes-native day-2 loop starts here |
| P1 | Product-owned cleanup/lifecycle ownership | Operational + Core Stability | partial | Scripts still own too much lifecycle and cleanup |
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
- `v0.4-beta-candidate`: Operator lifecycle. Add Kubernetes-native CRDs,
  Conditions, Events, safe cleanup, and eventually gated repair/rebuild
  workflows. This is the first release boundary that can feel like a complete
  Kubernetes product loop rather than scripts plus Helm.

Do not skip from scripts directly to an operator. Helm stabilizes the install
contract; Phase 27 stabilizes multi-volume HA semantics; then an operator can
own day-2 lifecycle without hiding unstable behavior.

## Active Work

### Phase 28 - Operational Reliability And TestOps Hardening

Type: Operational + Core Stability

Current status: active, 30%. Started on 2026-05-23 after Phase 27 D5/D6/D8
independent QA reruns passed.

Goals:

- close cleanup residue gaps, starting with orphan dm-multipath maps,
- make Phase 27 repeatability measurable through flake matrices,
- convert TestOps runner pain into concrete action backlog and reference
  scenarios,
- align support/report/dashboard evidence for multi-volume HA failures,
- review structure/model dependencies before operator-grade operations.

Closed gates:

- D1 multipath cleanup verifier: `20260523-182000-41ee`, 13/13 PASS.

Non-goals:

- no new NVMe ANA claim,
- no backup/snapshot/restore implementation,
- no operator/CRD implementation,
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

### State Model And Truth Ownership

Closed / current:

- ManagedVolume Operations Model seed.
- Truth-domain and invariant docs under `internal/docs/protocol/`.
- Observation slots merge by `(volume, replica)` with independent freshness.
- Materialized workload ports persist so new volumes cannot reshuffle existing
  ports.
- Phase 27 proved per-volume independence under sequential and interleaved
  faults for N=3 RF3 PVCs.

Next:

- Run a `Control Model Stabilization Gate` before operator-grade operations:
  - freeze the ManagedVolume fact model used by report/dashboard/explain,
  - define truth owners for Kubernetes placement, CSI publish/stage, authority,
    recovery, host path, workload, cleanup, and evidence,
  - separate fact publication, orchestration decisions, executor actions, and
    timeline evidence,
  - define state priority when multiple automata overlap, such as node loss
    affecting authority, CSI, host path, and cleanup at the same time,
  - keep mutating actions as dry-run/read-only contracts until the model has
    gates and audit evidence.
- Extend ManagedVolume to express multi-volume failover isolation facts:
  target volume, non-target volume stability, primary_count, stale I/O result,
  and host-path recovery method.
- Keep local controllers small: truth owners publish facts, orchestration
  entities make global decisions, executors perform allowed actions, evidence
  records why.

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
