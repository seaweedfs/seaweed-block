# Phase 28 Structure, Model, And Readiness Review

Date: 2026-05-23

Purpose: decide what must be stabilized before the next feature expansion
after v0.3.x multi-volume HA and operational hardening.

## D5 Structure Review

| Area | Current Shape | Desired Shape | Risk If Left Mixed | Next Action |
|---|---|---|---|---|
| Helm / install lifecycle | Helm values, shell scripts, and TestOps each encode install assumptions. | Helm owns declarative install intent; scripts only generate values and collect evidence. | Docs, chart, and scripts drift. | Keep Helm as primary install path; add schema/values validation before operator work. |
| Launcher / BlockVolume lifecycle | Launcher reconciles Deployments and carries placement/port details. | Launcher is executor for placement intents; ManagedVolume/controller owns lifecycle state. | Reconcile logic becomes hidden state machine. | Extract lifecycle facts: desired replicas, observed replicas, placement, ports, readiness. |
| CSI publish/stage | CSI asks master and reports reattach events; some evidence is gathered by scripts. | CSI remains executor/observer; master model records publish generation and attach events. | Dashboard cannot distinguish CSI reattach vs host multipath switch reliably. | Stabilize `publish_target`, `epoch`, `endpoint_version`, `reattach_observed` fields. |
| Recovery / promotion | Authority and promotion logic is product-owned; scenarios still assemble proof. | Authority publishes facts; recovery orchestrator records why promotion was allowed/refused. | Future operator could duplicate promotion reasoning. | Keep mutation out of CLI; expose reason/evidence in read model. |
| Host path / multipath | Scripts and sg_rtpg probes prove path behavior. | Host-path evidence becomes a named evidence source with stable fields. | Transparent failover claims depend on ad-hoc parsing. | Promote AAS/stale-I/O checks to runner actions, then product evidence where possible. |
| Cleanup | Cleanup is mostly script/TestOps-owned; verifier now detects sessions, processes, K8s, multipath, dmsetup. | Product-owned lifecycle should reduce residue; TestOps remains final auditor. | Repeated tests and user labs become polluted. | Keep strict verifier; later map cleanup state into operator Conditions. |
| ManagedVolume model | Exists as projection and report/explain surface. | Becomes the common read model for dashboard, CLI, support bundle, and operator. | Each surface invents its own truth. | Classify fields stable/provisional/test-only before CRD/operator. |
| TestOps helpers | Helper scripts contain real orchestration logic. | Keep helpers for complex flows; move repeated primitives into runner actions. | YAML becomes unreadable or shell failures stay opaque. | Follow `testops-runner-action-backlog.md`. |

## D6 Model Dependency Map

### Stable Fields

These can be consumed by docs, report, dashboard, and future operator
Conditions:

- `volume_id`
- `pvc`
- `replication_factor`
- `ack_profile`
- `primary`
- `primary_node`
- `publish_target`
- `epoch`
- `endpoint_version`
- `status`
- `reason`
- `condition`
- `evidence_ref`
- `candidate_ready`
- `frontier_covered`
- `required_frontier_lsn`
- `candidate_frontier_lsn`
- `post_failure_primary_count`
- `pod_recreate_used`
- `transparent_failover_claimed`

### Provisional Fields

These are useful but should not become public API until model ownership is
clear:

- per-scenario `target_index`
- `interleaved_fault_window_seconds`
- raw RTPG AAS fields
- stale direct-read probe counters
- helper-script cleanup internals
- app-node distribution counters

### Test-Only Fields

These should remain in TestOps/support bundles unless promoted deliberately:

- local run IDs
- scenario names
- local artifact paths
- exact pod UIDs
- generated manifest paths
- raw `multipath -ll` text
- raw `sg_rtpg` text

## Overlapping Automata Rule

Many failures affect several small state machines at once. Example: node loss
touches authority, CSI publish, host path, kubelet mount, cleanup, and support
evidence.

Required rule:

```text
Truth owner publishes facts.
Orchestration entity makes global decision.
Executor performs allowed action.
Evidence records why action was allowed/refused.
```

This prevents simple local listeners from taking actions without global
visibility.

## D7 Model Tightening Proposal

Next major internal model work should create an explicit control-plane entity
for volume lifecycle projection. It should not replace the data engine.

Responsibilities:

- Observe PVC/PV volume identity.
- Observe desired RF / ack profile / placement.
- Observe replica readiness and publish target state.
- Project a ManagedVolume status and Conditions.
- Emit read-only evidence for report/dashboard/operator.
- Request allowed executor actions only through explicit orchestration paths.

Non-responsibilities:

- It should not perform data-plane writes.
- It should not directly promote replicas.
- It should not bypass authority epoch rules.
- It should not expose unsafe user mutating actions.

First useful boundary:

```text
ManagedVolumeController = projection + condition engine
Authority/Recovery = mutation decision owner
Launcher/CSI/K8s = executors and observers
TestOps = external auditor
```

## D8 Next Feature Readiness

| Feature | Start When | Blocking Dependency |
|---|---|---|
| Operator / CRD | ManagedVolume stable/provisional/test-only fields are classified and Conditions are mapped. | D6 model map. |
| NVMe ANA | iSCSI ALUA/multipath cleanup and repeatability remain stable. | D1/D2 plus host-path evidence contract. |
| Rebuild / reintegration / failback | Multi-volume HA remains repeatable and stale-primary fencing stays measured. | D2 flake matrix + D5 stale-I/O action. |
| Backup / snapshot / restore | Volume identity and lifecycle model are stable enough to avoid orphaned backups. | ManagedVolume ownership and cleanup rules. |
| TestOps controller/agent | SSH-only gates become bottleneck for fault injection and node snapshots. | Runner action backlog and agent design. |

## Recommendation

Do not start NVMe ANA, backup, or operator mutation before the model dependency
map is reflected in code-level ownership. The next safest work is operational:

1. publish the Phase 27/28 image SHA and docs,
2. add first-class runner cleanup/wait actions,
3. then start ManagedVolume/controller extraction behind read-only gates.

