# Layered Participant / Fact Authority / Master / Executor Model

Status: working protocol methodology for Phase 28 D9-D11.

Purpose: define a reusable control-plane shape for Seaweed Block domains that
need more discipline than local `if/else` listeners, but should not become one
giant global state machine.

This document deepens the current truth-domain and `ManagedVolume` model. It is
intended to be reused outside PVC/Kubernetes, including recovery, repair, NVMe
ANA, backup, and future operator workflows.

## Core Thesis

Seaweed Block should be modeled as a layered network of small control systems:

```text
Participants emit observations
  -> Fact Authorities publish authoritative facts
      -> Domain Master computes domain state
          -> Domain Master publishes domain facts upward
              -> Higher Master computes cross-domain product state
                  -> Executor performs only allowed actions
                      -> Evidence records why the action was allowed or refused
```

Do not build:

```text
one super master that is authoritative for all facts
```

Do not drift into:

```text
many local listeners each making product decisions independently
```

The correct middle shape is:

```text
participants publish observations
fact authorities publish authoritative facts
domain masters compute bounded domain state
higher masters consume domain-master output as authority facts
executors perform side effects only after the relevant master allows them
```

## Terms

### Participant

A Participant is any process, controller, probe, or adapter that contributes observations or local state.

A Participant:

- publishes observations or local facts,
- must not claim global semantics,
- attaches generation, epoch, timestamp, or evidence where needed,
- does not publish authoritative facts unless it is also the Fact Authority for that domain,
- does not take cross-domain product actions directly.

Examples:

| Participant | Emits |
|---|---|
| replica process | local health, durable-store observation, replication observation |
| CSI node plugin | local stage/publish observation |
| host-path probe | iSCSI session, multipath, RTPG/AAS observation |
| Kubernetes watcher | PVC/PV/pod/node observation |
| cleanup verifier | residue observation |

### Fact Authority

A Fact Authority is the component or domain output that is accepted as
authoritative for one fact domain.

A Participant can be a Fact Authority, but not every Participant is one. This
distinction keeps the model useful: "I observed X" is not the same as "X is
the authoritative product fact".

Examples:

| Fact Authority | Publishes |
|---|---|
| `AuthorityLineAuthority` | primary, epoch, endpoint_version |
| `ReplicaDurabilityAuthority` | durable_frontier, latched, operational |
| `PlacementAuthority` | replica node, frontend address, Deployment readiness |
| `CSIAttachAuthority` | staged node, staged target, attach generation |
| `HostPathAuthority` | iSCSI sessions, dm-multipath paths, RTPG/AAS, stale-path probe |
| `CleanupAuthority` | resources gone, sessions gone, multipath maps gone |

Rule: if two components can publish the same fact with different answers, the
fact authority is broken.

### Master

A Master is an information gatherer and decision maker for one control domain.

A Master:

- consumes Fact Authority facts,
- checks whether required facts are present and fresh,
- computes a collective state,
- decides whether an action is allowed, refused, or still pending,
- emits reason codes and evidence references,
- fails closed when required facts are missing or contradictory.

A Master must not forge lower-domain facts. It can only derive state from facts
that are authoritative elsewhere or authoritative inside its own domain.

Examples:

| Master | Consumes | Publishes upward |
|---|---|---|
| `EngineMaster` | authority, replica durability, replication/ACK facts | primary line, promotion decision, required frontier |
| `ManagedVolumeMaster` | engine state, placement, CSI attach, host path, K8s, cleanup | Ready/Degraded/Recovering/Blocked, allowed actions, evidence |
| `RepairMaster` future | ManagedVolume state, durability, placement, policy | rebuild/reintegrate/failback eligibility |
| `BackupMaster` future | volume identity, quiescence/freeze, backend snapshot facts | backup eligibility and result state |

### Executor

An Executor performs side effects. It should not decide cross-domain semantics.

An Executor:

- accepts an allowed action from the relevant Master,
- performs the side effect,
- reports result facts or errors back to a Fact Authority or Master,
- produces audit evidence.

Examples:

| Executor | Does |
|---|---|
| `LauncherExecutor` | apply/delete generated runtime objects |
| `CSIExecutor` | stage/publish volume paths |
| `KubeExecutor` | apply/delete Kubernetes objects |
| `CleanupExecutor` | logout sessions, flush multipath, remove artifacts |
| `ReportExecutor` | write bundle/report/dashboard artifacts |
| `RepairExecutor` future | rebuild/reintegrate/failback side effects |

Rule: an Executor that starts deciding safety is becoming an accidental Master.

## Recursive Role Rule

Fact Authority and Master are relative roles.

A domain Master can become a Fact Authority for the next higher layer:

```text
ReplicaDurabilityAuthority
  -> EngineMaster
      publishes authority.primary=r2, epoch=2, promotion=committed
          -> ManagedVolumeMaster consumes that as an authority fact
```

From inside the engine domain, `EngineMaster` is the master.

From the product-control domain, `EngineMaster` is a Fact Authority for authority-domain facts.

This recursive rule is what lets the system scale without one huge controller.

## Two Valid Shapes

### Small Shape: Domain State Aggregated To One Master

Use this when the domain boundary is narrow and all required facts are local to
one protocol or resource family.

Example: engine identity / authority.

```text
participants:
  primary, replicas, durable backend, replication transport

fact authorities:
  AuthorityLineAuthority
  ReplicaDurabilityAuthority

master:
  EngineMaster

state:
  primary, epoch, endpoint_version, promotion eligibility, required frontier

actions:
  publish authority
  refuse promotion
  advance epoch
  fence stale primary
```

Here a relatively pure state machine is possible because the domain is bounded.

### Large Shape: Collective State Across Multiple Masters

Use this when a product state spans several domains.

Example: PVC-backed Kubernetes volume recovery.

```text
domain facts:
  EngineMaster publishes authority and promotion facts
  PlacementAuthority publishes replica/node/frontend facts
  CSIAttachAuthority publishes stage/publish facts
  HostPathAuthority publishes iSCSI/multipath/RTPG facts
  KubernetesObjectAuthority publishes PVC/PV/pod/node facts
  CleanupAuthority publishes residue facts

higher master:
  ManagedVolumeMaster

collective state:
  Ready
  Degraded
  Recovering
  Blocked
  CleanupRequired

actions:
  observe.collect_bundle
  wait_for_csi_reattach
  request_restage future
  refuse_ready_claim
  propose_repair future
```

The large shape cannot be safely implemented by any one local listener. The
ManagedVolumeMaster must compute the product state from facts.

## Action Rule

Every cross-domain action must come from the relevant Master.

Required action fields:

| Field | Meaning |
|---|---|
| `action_type` | observe, safe_k8s, disruptive_k8s, authority_mutating, repair_mutating, destructive |
| `allowed` | true, false, pending |
| `master` | the Master that made the decision |
| `required_facts` | facts that must exist and be fresh |
| `reason_code` | stable reason for allow/refuse/pending |
| `evidence_ref` | bundle/event/report pointer |
| `executor` | component allowed to perform the side effect |
| `policy_gate` | read-only, dry-run, explicit-user-policy, admin-only, disabled |

Bad pattern:

```text
CSI listener sees old target fail -> directly promotes r2
```

Correct pattern:

```text
CSI listener publishes attach/path fact
EngineMaster publishes promotion fact
ManagedVolumeMaster computes Recovering or Ready
Executor performs only the action allowed by that state
```

## Priority And Conflict Rule

Large product states often overlap. For example, one physical or Kubernetes
node loss can simultaneously affect:

- authority,
- replica durability,
- CSI attach,
- host path,
- pod readiness,
- cleanup.

Local domains should not race to publish a final product conclusion.

ManagedVolumeMaster should apply a deterministic priority order:

| Priority | State | Rule |
|---|---|---|
| 1 | `Blocked` | Required safety facts missing, contradictory, or unsafe |
| 2 | `CleanupRequired` | Live product function may be done, but residue remains |
| 3 | `Recovering` | Authority/path/pod recovery in progress |
| 4 | `Degraded` | Workload usable but redundancy/path health reduced |
| 5 | `Ready` | All required facts satisfy the claim boundary |
| 6 | `Unknown` | Insufficient observation, no action except collect evidence |

This order prevents "one green local automaton" from hiding a higher-severity
product state.

## Evidence Rule

No Master should publish a final state without evidence.

Minimum evidence for a derived state:

| Derived state | Evidence required |
|---|---|
| `Ready` | authority, placement, attach/path, and workload or readiness evidence |
| `Recovering` | failure fact, candidate/authority fact, and pending attach/path/workload evidence |
| `Blocked` | missing or unsafe fact, stable reason code, and safe next step |
| `CleanupRequired` | residue fact and cleanup executor recommendation |
| `Degraded` | usable workload path plus explicit reduced redundancy/path fact |

Evidence should be referenced, not copied, so a higher layer can point to lower
layer artifacts:

```text
ManagedVolume condition evidence_ref:
  engine:event/master-276
  csi:node-stage/m02/pvc-...
  hostpath:rtpg-after/pvc-...
  testops:reader-log/pvc-...
```

## Relation To ManagedVolume

`ManagedVolume` is the first product-level application of this model.

Its shape:

```text
participants, fact authorities, and masters publish facts
ManagedVolumeMaster computes product state and allowed actions
ops/report/dashboard/operator consume ManagedVolume state
executors remain behind policy gates
```

Therefore D9 is not only a field-list exercise. It must define:

- which facts exist,
- which Fact Authority publishes each fact,
- which domain-master outputs are accepted as upward Fact Authority facts,
- how ManagedVolumeMaster computes Conditions,
- which actions require ManagedVolumeMaster authorization,
- which evidence refs prove each state.

## Relation To Engine

The engine directory remains a protocol/domain implementation:

```text
engine = identity / authority / recovery protocol automata
```

It is authoritative for primary, epoch, endpoint version, promotion
eligibility, and frontier semantics. It should not be authoritative for PVC,
CSI, host-path, dashboard, or cleanup truth.

The engine output becomes an authority fact for ManagedVolume:

```text
EngineMaster publishes:
  authority.primary=r2
  authority.epoch=2
  promotion.status=committed
  promotion.reason=candidate_covers_required_frontier

ManagedVolumeMaster consumes that plus:
  csi.staged_target=192.168.1.184:3260
  hostpath.rtpg_after_promoted=0x00
  k8s.writer_pod_uid_same=true
  cleanup.pending=false

ManagedVolumeMaster publishes:
  condition Ready=True
  reason=mounted_workload_failover_verified
```

This preserves engine authority while preventing product-state logic from
spreading across CSI, launcher, scripts, and reports.

## Design Review Checklist

For any new behavior, ask:

1. What fact changed?
2. Which Fact Authority publishes that fact?
3. Is there a domain Master that computes the local state?
4. Does that domain Master publish an upward fact?
5. Does a higher Master need this fact before allowing an action?
6. Which Executor performs the side effect?
7. What evidence proves allow/refuse/pending?
8. What invariant prevents a lower layer from forging an upper-layer truth?
9. What happens when required facts are stale, missing, or contradictory?
10. Which test proves the local transition and which test proves the composed
    product state?

If these answers are unclear, do not add the behavior to product claims.

## Open Work

This document defines the hierarchy and roles. It does not yet finish the full
invariant ledger for every domain.

Phase 28 D9-D11 should add:

- ManagedVolume fact-authority table,
- Condition derivation rules,
- action contracts,
- cross-domain invariant rows,
- golden model tests for healthy and blocked volume cases,
- read-only operator contract that consumes the model instead of inventing a
  parallel state view.
