# ManagedVolume Operations Model

## Purpose

`ManagedVolume` is the internal product entity for one Kubernetes PVC-backed
Seaweed Block volume.

It exists because the user's mental model is not "a Deployment, some CSI logs,
one master event, and a few iSCSI sessions". The user created a PVC and wants
to know whether that volume is usable, recoverable, blocked, or degraded.

The model should let operations surfaces answer that question consistently.

## Boundary

`ManagedVolume` is:

- a read model,
- a fact composition point,
- an operations/dashboard/explain foundation,
- the first phase of a future orchestration engine,
- a future operator Conditions source.

`ManagedVolume` is not yet:

- a replacement for Kubernetes PVC,
- the owner of PVC create/delete/bind lifecycle,
- the owner of generated `blockvolume` Deployment lifecycle,
- a new mutating volume API,
- a second authority publisher,
- a CSI primary selector,
- a repair/rebuild executor.

## What It Manages

Use the word "manage" carefully. In Phase 22, `ManagedVolume` manages the
product state and action contract, not the underlying object lifecycles.

| Layer | Lifecycle Owner | Examples | ManagedVolume Role |
|---|---|---|---|
| Kubernetes PVC/PV | Kubernetes + CSI external-provisioner/controller | PVC create/delete, PV binding, StorageClass parameters, reclaim policy | Observe intent and binding; correlate PVC/PV to sw-block volume identity. |
| sw-block generated runtime | blockmaster launcher / future operator | generated `blockvolume` Deployments, replica slots, node placement, status/frontend args | Observe desired/observed replica runtime and surface drift/blockers. |
| Authority/recovery | blockmaster authority/recovery engine | primary, epoch, endpoint_version, promotion, fail-closed reasons | Consume authority facts; never publish or override authority. |
| CSI attach path | CSI controller/node | ControllerPublish, NodeStage, NodePublish, re-stage after pod recreate | Record attach facts and explain target/path mismatches. |
| Host path | host initiator + kernel multipath + protocol frontend | iSCSI sessions, dm-multipath, ALUA, future NVMe ANA | Correlate host path to the active frontend and recovery claim. |
| Workload evidence | test/app/user workload | writer checksum, reader checksum, same pod UID | Prove the user-visible data claim. |

So the precise sentence is:

```text
ManagedVolume is the read-side product model for a PVC-backed sw-block volume.
It correlates Kubernetes intent, sw-block runtime, authority, CSI, host-path,
and workload evidence. In Phase 22 it emits state, blockers, evidence, and
allowed/recommended actions, but does not execute create/delete/promotion/repair.
```

## Entity Sketch

```text
ManagedVolume
  identity:
    namespace
    pvc_name
    pv_name
    volume_handle
    storage_class

  desired:
    replication_factor
    ack_profile
    protocol
    claim_profile

  topology:
    replicas[]
    block_nodes[]
    frontend_endpoints[]
    status_endpoints[]

  authority:
    primary_replica
    epoch
    endpoint_version
    required_frontier
    promotion_candidates[]

  kubernetes:
    pvc_phase
    bound_pv
    pods_using_volume[]
    csi_publish_target
    csi_stage_observations[]

  host_path:
    iscsi_sessions[]
    multipath_maps[]
    alua_states[]
    nvme_paths[]       # future
    ana_states[]       # future

  workload:
    writer_verified
    reader_verified
    same_pod_uid
    checksum_artifacts[]

  evidence:
    events[]
    reason_codes[]
    blockers[]
    evidence_refs[]
```

## Why A Model Instead Of More Status Helpers

Status helpers worked while the product was small. They become unsafe when one
question crosses several domains:

```text
PVC Pending
  could be StorageClass/RBAC,
  or provisioner,
  or master unavailable,
  or unsafe loopback publish target,
  or blockvolume Deployment churn,
  or CSI node image pull,
  or host iSCSI failure.
```

Putting that logic directly into CLI commands, TestOps scenarios, dashboard
components, or CSI listeners creates inconsistent answers. A model lets each
domain contribute facts once, then lets projection code derive the user-facing
story.

## Small Automata Still Matter

The model does not remove local state machines. It gives them a shared context.

Examples:

- authority engine owns primary/epoch/endpoint_version,
- recovery engine owns promotion eligibility and fail-closed reasons,
- CSI adaptor owns publish/stage observations,
- host-path adaptor owns iSCSI/multipath/ALUA or later NVMe/ANA observations,
- K8s adaptor owns PVC/PV/Pod/Node facts,
- observation engine owns timeline/report/explain projection.

Each automaton should consume facts and emit facts/events. It should not reach
across domains to create another domain's truth.

## Action Contract

The long-term reason to extract this model is not only prettier status. It is
to stop small local entities from taking simple actions without global
visibility.

Phase 22 should therefore define actions as data, even if execution remains
disabled:

```text
ManagedVolume facts
  -> product state
  -> blockers
  -> allowed_actions / recommended_actions
  -> evidence refs and required policy gate
```

Example action classes:

| Class | Examples | Phase 22 Behavior | Future Executor |
|---|---|---|---|
| `observe` | explain, report, bundle | emit and execute read-only | ops CLI/dashboard |
| `safe_k8s` | apply missing owned object, update Condition | emit dry-run/action hint | operator |
| `disruptive_k8s` | recreate workload, detach/reattach | emit only with explicit policy marker | operator with user policy |
| `authority_mutating` | promote, fence, refresh endpoint | emit only as gated request shape | master/recovery executor |
| `repair_mutating` | rebuild, reintegrate, failback | out of initial execution scope | future repair engine |
| `destructive` | cleanup data, delete residue | out of initial execution scope | future audited admin workflow |

Every action must name:

1. preconditions,
2. required facts,
3. invariant rows,
4. owning executor,
5. side-effect class,
6. audit/evidence output.

The design goal is:

```text
local engine proposes facts
ManagedVolume computes product-level allowed action
owning executor performs the action only through a policy gate
```

not:

```text
local entity sees one symptom and performs a product-level action directly
```

## Protocol Rule

Before adding a new field to `ManagedVolume`, name:

1. owner domain,
2. source command/API/event,
3. update timing,
4. generation or epoch if applicable,
5. whether it is control truth or derived projection,
6. invariant or test that prevents misuse.

If those six answers are unclear, the field is probably a timing workaround or
UI convenience, not a product fact.

## Kubernetes Product States

Initial derived states should be boring and user-facing:

| State | Meaning |
|---|---|
| `installing` | Helm/script install is not ready yet. |
| `ready_no_volume` | control plane is ready, no managed PVC observed. |
| `volume_pending` | PVC exists but not usable yet. |
| `volume_ready` | PVC is bound and writer/reader path can pass. |
| `degraded` | volume is usable but a replica/path/node is unhealthy. |
| `recovering` | failover or reattach is in progress. |
| `recovered` | documented recovery path completed and data check passed. |
| `blocked` | product cannot safely proceed; reason code required. |

These states are projections. They are not authority.

## Required Blocker Reasons

Phase 22 should model at least:

- `csi_node_image_pull_failed`
- `blockmaster_unavailable`
- `pvc_unbound`
- `blockvolume_not_ready`
- `publish_target_loopback_cross_node`
- `writer_mount_failed`
- `candidate_not_promotion_ready`
- `required_frontier_not_covered`
- `csi_reattach_timeout`
- `host_path_not_multipathed`

Each blocker should have:

- summary,
- evidence refs,
- user-safe next step,
- claim boundary.

## Future NVMe / ANA Fit

NVMe ANA should not add a second operations model. It should plug into the
host-path projection:

```text
iSCSI:
  session -> portal/IQN -> dm-multipath path -> ALUA state

NVMe:
  controller/path -> subsystem/NQN -> kernel multipath path -> ANA state
```

The product question is the same:

```text
Which path can serve this ManagedVolume now, and what evidence proves the
workload survived the transition?
```

If NVMe needs parallel status/explain code, the model failed.

## First Implementation Slice

Phase 22 should start with tests over synthetic facts, not with a new live
cluster gate:

1. healthy Helm first-volume,
2. loopback cross-node mount blocked,
3. image-pull blocked,
4. node-loss CSI reattach recovered,
5. Stage 2 transparent iSCSI failover recovered.

Then wire `sw-block ops explain/report` to use the model for those same cases.
