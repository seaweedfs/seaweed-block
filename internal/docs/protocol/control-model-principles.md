# Control Model Principles

## Product Boundary

Seaweed Block is not just a set of protocol frontends. In Kubernetes it is a
multi-layer product:

```text
Kubernetes intent and facts
-> CSI attach/stage facts
-> block authority and recovery facts
-> host path facts
-> workload data-check facts
-> operator-facing evidence
```

The protocol model must preserve those layers without letting one layer mint
truth for another.

## Core Rule

Use:

```text
one ManagedVolume-centered product model, many local controllers
```

Do not implement:

```text
one monster engine
```

and do not drift into:

```text
many unrelated state machines
```

The global model defines entities, facts, invariants, and allowed commands. For
Kubernetes block storage, the central product entity is `ManagedVolume`: the
PVC-backed volume the user created and expects to attach, recover, observe, and
clean up. Local controllers maintain projections of that model.

## Truth Domains

Each fact should have one owner.

| Truth | Owner | Consumers | Must Not Do |
|---|---|---|---|
| Desired topology | Product controller / future operator | master, dashboard, placement report | silently rewrite authority |
| Current epoch / endpoint version / primary | master / authority publisher | blockvolume, CSI, observation | depend on heartbeat timing alone |
| Local replica health and durable frontier | blockvolume | master, recovery evaluator | mint primary |
| Frontend write/read eligibility | adapter projection | iSCSI, NVMe, mounted workloads | bypass stale-primary fence |
| Remote durable ack / ship frontier | replication transport and peer | ACK evaluator, promotion gate | count acks without lineage |
| Recovery decision | recovery engine | executor, observation | infer terminal success from ack arrival |
| Kubernetes PVC/PV/Pod/Node facts | K8s adaptor / future operator | master observation, dashboard, support bundle | select primary directly |
| CSI stage/publish facts | CSI node/controller | observation, K8s adaptor | decide promotion |
| Host path facts | host-path observer | K8s adaptor, dashboard | claim data recovery without block authority evidence |

Review rule: a new second owner for any truth domain is architecture drift.

## Global Context

The global context is a typed fact store, not a giant enum. Its first concrete
shape should be the `ManagedVolume` read model. It should express mixed or
conflicting evidence without collapsing it into one boolean too early.

Example:

```json
{
  "fact_type": "node_reachability",
  "node": "m01",
  "kubernetes_ready": true,
  "storage_path_reachable": false,
  "primary_replica": "r1",
  "source": ["k8s", "iscsi", "blockmaster"],
  "confidence": "mixed",
  "generation": 42
}
```

This is important for support. A user does not ask which internal automaton is
green. They ask why their PVC-backed volume is blocked and what the next safe
action is.

## Projection Controllers

Projection controllers are small automata over the global facts.

Examples:

- Authority projection: one current primary per volume, epoch, endpoint
  version, stale-primary fence.
- Recovery projection: candidate frontier, durable coverage, catch-up/rebuild
  state, promotion eligibility.
- K8s adaptor projection: PVC/PV/pod/node/CSI stage, attach blocked reasons,
  pod recreate vs transparent failover.
- Host path projection: iSCSI sessions, dm-multipath paths, ALUA state,
  failed/stale path evidence.
- Observation projection: timeline, reason codes, support bundle, read-only
  report.

Each controller should:

- consume facts,
- emit derived state and events,
- optionally emit safe commands,
- never create truth owned by another domain.

## Invariants Before States

Before adding a new state, ask:

1. Is this a new semantic distinction?
2. Or is it a workaround for event timing, retry order, or log shape?

Timing workaround states should be rejected unless they express a product
truth. Prefer facts, generations, epochs, session IDs, and deterministic
decision functions.

## Composed Example: Kubernetes Node Loss

Input facts:

```text
NodeReachability(m01)=lost
Replica(r1).node=m01
Volume.primary=r1
Replica(r2).durable_frontier >= required_frontier
CSIStage(old_target)=192.168.1.181:3260
```

Expected global projection:

```text
volume status=recovering
primary_suspect=r1
candidate_ready=r2
authority_published primary=r2 epoch=2
publish_target=192.168.1.184:3260
csi_reattach_expected=true
transparent_failover_claimed=false
physical_host_loss_claimed=false
```

This projection is not owned by one local state machine. It is the composed
ManagedVolume story. The local engines are only useful if they produce this
story consistently.

## Testing Shape

Use three layers:

1. Local automata tests: one controller, one transition table.
2. Cross-controller model tests: simulated facts across K8s, authority,
   recovery, CSI, and host path; assert global projection and invariants.
3. E2E gates: real Kubernetes/CSI/iSCSI/NVMe/multipath scenarios with support
   bundles.

Unit tests alone are not enough for product confidence. E2E gates alone are
too slow and can hide protocol drift. The middle layer is the missing product
hardening layer.
