# Operations-State Dependency Review

Date: 2026-05-20

Status: Phase 22 scope review

## Purpose

This review decides what Phase 22 should be.

The question is not whether operations or protocol model is more important in
the abstract. The product question is:

```text
Which operations can safely be exposed now because their facts are already
stable, and which operations require the `ManagedVolume` read model before they
become user-facing product surfaces?
```

This avoids two failure modes:

- shipping a nicer CLI/dashboard that is backed by fragile grep/script logic,
- spending a whole phase on internal modeling without improving the user path.

## Decision Summary

Phase 22 should be:

```text
ManagedVolume Operations Model + read-only operations alignment
```

It should not be:

```text
pure dashboard work
pure protocol refactor
operator lifecycle
mutating admin workflow
```

The right vertical slice is:

```text
define the PVC-backed ManagedVolume entity
then align ops/report/explain/dashboard foundations with that entity
```

This is deliberately larger than a dashboard polish pass. It is also narrower
than an operator. The goal is to stop PVC, CSI, authority, host-path, and
recovery facts from being recomposed differently by every command, scenario, or
future UI panel.

Boundary clarification:

```text
ManagedVolume does not own PVC lifecycle.
ManagedVolume does not own generated blockvolume Deployment lifecycle.
ManagedVolume does not own authority/promotion lifecycle.
ManagedVolume owns the product correlation layer and action contract that
explains those lifecycles together. Phase 22 keeps execution read-only/dry-run,
but future mutating controllers should consume this action contract instead of
inventing local shortcuts.
```

## Stability Levels

| Level | Meaning | Product Action |
|---|---|---|
| `stable` | Live gates have repeatedly passed and facts are product-owned or directly observable. | Can expose in user-facing read-only ops. |
| `gated` | A scenario proves it, but evidence still depends partly on TestOps shape or narrow lab assumptions. | Expose carefully with claim boundary and evidence refs. |
| `model-first` | The operation would combine multiple domains where ad-hoc logic can mislead users. | Build ManagedVolume/projection first. |
| `missing` | Required facts or invariants are not implemented. | Keep out of product surface. |

## Operation Dependency Matrix

| Operation / Surface | User Question | Required Facts | Current Source | Stability | Decision |
|---|---|---|---|---|---|
| Helm values generation | What should I pass to Helm for this cluster? | Ready schedulable nodes, InternalIP, selected node count, RF, ACK profile, image refs | `sw-block ops generate-helm-values`, `kubectl get nodes` | `stable` for alpha | Ship as Phase 21/22 user path. |
| Helm install readiness | Did sw-block install correctly? | Helm release status, blockmaster ready, CSI controller ready, CSI node ready, StorageClass exists | Helm/kubectl rollout, chart values | `stable` for alpha | Ship read-only summary. |
| First PVC smoke | Can I create a PVC and write/read data? | PVC Bound, PV provisioned, writer checksum, reader checksum, volume ID, cleanup status | `run-basic-app-example.sh`, inventory/report | `stable` | Ship as first user loop. |
| `ops cluster` | What volumes/nodes/events does sw-block see? | master event stream, volume evidence, node evidence | master ClusterEvidence API / inventory fallback | `stable` for supported alpha | Ship. |
| `ops report` | Can I see a readable local status page? | cluster evidence, timeline, summary | product-owned report artifacts | `stable` | Ship read-only report. |
| `ops explain volume` healthy path | Is my volume ok and why? | volume status, primary, frontend, RF, ACK profile, events | product evidence / inventory | `stable` for simple path | Ship. |
| Blocked first PVC diagnosis | Why is my first PVC or writer pod stuck? | PVC phase, pod scheduling, CSI mount events, publish target, blockvolume readiness, image pull state | currently scripts/TestOps diagnostics and kubectl describe | `model-first` | Add ManagedVolume K8s/CSI facts before making dashboard/operator claims. |
| Node-loss recovery explain | What happened when the primary node failed? | before primary, failed node, candidate readiness, authority publish, CSI reattach, reader checksum, non-claims | node-loss D4/D5 bundle and product events | `gated` | Expose as read-only evidence, but model composed facts before broader UI/operator. |
| Transparent multipath failover explain | Did failover happen without pod recreate? | same pod UID, ALUA state before/after, multipath path state, stale I/O blocked, checksum | Stage 2 D4 bundle, host commands | `gated` | Keep as gated report; needs host path projection before general dashboard. |
| Repair/rebuild/failback status | Is a returned replica being rebuilt safely? | replica durable frontier, recovery session, catch-up/rebuild progress, terminal close, peer state | partial protocol internals | `model-first` / `missing` | Do not productize until ManagedVolume/recovery model and gates exist. |
| Dashboard cluster page | What is the cluster health? | stable cluster evidence and reason codes | `ops cluster/report` | `stable` for read-only summary | Can build read-only v1 over existing evidence. |
| Dashboard recovery page | What is recovering or blocked? | composed K8s + authority + host path + workload facts | mixed | `model-first` | Needs ManagedVolume model. |
| Operator Conditions | What should Kubernetes show via CRDs? | all lifecycle and recovery facts with stable reason codes | not yet CRD-owned | `model-first` | Do after Phase 22. |
| Mutating admin actions | Can I promote/repair/cleanup safely? | full authority, fencing, recovery, audit, rollback facts | not productized | `missing` | Out of Phase 22. |

## Existing Stable Facts We Can Use Now

These can back Phase 22 read-only operations without inventing new protocol:

- Helm values: selected nodes, network mode, CHAP, RF, ACK profile.
- Install readiness: blockmaster, CSI controller, CSI node, StorageClass.
- First-volume: PVC Bound, writer checksum, reader checksum, cleanup status.
- Cluster evidence: volumes, replicas, nodes, product-owned events.
- Static report: HTML, JSON, JSONL timeline, summary.
- Recovery evidence for gated scenarios: authority published, CSI reattach
  observed, reader checksum passed.

## Facts That Need ManagedVolume Before Broader Productization

These are currently too scattered or too cross-layer to expose broadly:

- K8s blocked states:
  - ImagePullBackOff,
  - FailedMount,
  - PVC Pending,
  - CSI node not ready,
  - publish target loopback on cross-node attach.
- Host path states:
  - iSCSI session present/absent,
  - dm-multipath map,
  - ALUA target port group state,
  - failed/faulty/stale path.
- Recovery lifecycle:
  - candidate facts,
  - durable frontier,
  - required frontier,
  - session terminal truth,
  - rebuild/catch-up progress,
  - returned replica reintegration.

## Phase 22 Proposed Scope

### D1: ManagedVolume Entity Spec

Define the PVC-backed volume entity and small Go structs/JSON schema for facts
that Phase 22 operations need:

- `ManagedVolume`
- `KubernetesNodeFact`
- `PVCFact`
- `PodFact`
- `CSIStageFact`
- `VolumeAuthorityFact`
- `HostPathFact`
- `WorkloadCheckFact`

Acceptance:

- facts include source, observed_at, generation when known,
- no fact mints authority outside its owner,
- lifecycle ownership is explicit for PVC/PV, generated blockvolume
  Deployments, authority/recovery, CSI attach, and host path,
- JSON output is stable enough for CLI/report/dashboard.

### D2: Cross-Controller Model Tests

Create table tests for composed states:

- Helm first-volume healthy.
- First-volume blocked by loopback cross-node publish.
- CSI node image pull blocked.
- Node-loss recovery through pod recreate.
- Stage 2 transparent multipath failover.

Acceptance:

- event order does not change final projection,
- reason codes are stable,
- non-claims are generated from facts,
- invariant rows are referenced.

### D3: Operations v1 Improvements

Expose stable ManagedVolume facts in user-facing read-only operations:

- `sw-block ops generate-helm-values`
- `sw-block ops cluster`
- `sw-block ops report`
- `sw-block ops explain volume`
- optional `sw-block ops first-volume-summary` or report integration if useful.

Acceptance:

- no mutating actions,
- no SSH required for normal read path,
- output is human-readable by default and JSON/JSONL-capable for AI/dashboard.
- action hints, if present, are explicitly `dry_run` / `not_executed`.

### D3b: Action Contract Seed

Define the product-level action contract while keeping execution disabled:

- `observe.collect_bundle`
- `safe_k8s.apply_missing_owned_object`
- `safe_k8s.update_condition`
- `disruptive_k8s.recreate_workload`
- `authority.request_promotion`
- `repair.start_rebuild`
- `cleanup.cleanup_residue`

Acceptance:

- every action names preconditions, owner executor, side-effect class,
  invariant rows, and audit/evidence output,
- model tests prove actions are not suggested when required global facts are
  missing,
- no Phase 22 path executes mutating actions.

### D4: Blocked-State Explain Seed

Add model-backed explanations for the most common Day-1 failures:

- image missing / ImagePullBackOff,
- PVC Pending,
- writer FailedMount,
- loopback publish target across nodes,
- CSI node not ready.

Acceptance:

- explanations cite facts and sources,
- support bundle contains the needed artifacts,
- no hidden TestOps-only grep dependency.

### D5: Phase 22 Close Gate

Run:

- CLI unit/model tests,
- Helm single-node first-volume,
- Helm multi-node first-volume,
- one blocked-first-volume fixture,
- one recovery evidence replay from existing bundle.

Close when all user-facing operations in scope are backed by ManagedVolume facts or
explicitly marked as gated/non-claims.

## Deferred

- Operator CRDs and Conditions.
- Mutating admin actions.
- Repair/rebuild/failback workflow.
- Hosted dashboard.
- Broad performance or production HA claims.

## Phase 22 One-Line Goal

```text
Make Seaweed Block's read-only operations useful to users while establishing
the ManagedVolume model that future dashboard/operator/recovery workflows can
safely depend on.
```
