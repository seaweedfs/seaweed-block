# Engine Design Guidelines

## Purpose

This note turns the recent control-plane discussion into reviewable rules.

Seaweed Block already has several engine-like components: authority, recovery,
replication, launcher, CSI adaptor, host-path observation, and operations
reporting. The risk is not "too many engines". The risk is unclear ownership:
local services make product-level decisions from partial state.

## Core Rule

```text
Engine owns vocabulary: facts, states, invariants, actions.
Executor owns side effects.
```

An engine may compute allowed actions. It should not execute an action unless
it is also the named executor for that action.

## Engine Shape

Every engine should be reviewable through the same lens:

```text
Facts
  -> Context
  -> State projection
  -> Invariant checks
  -> Allowed actions
  -> Evidence / timeline
```

Definitions:

- `Fact`: owned observation from a truth domain.
- `Context`: identity, generation, epoch, topology, policy, and claim boundary
  needed to interpret facts.
- `State`: derived status for one concern.
- `Invariant`: rule that must hold for a product claim.
- `Action`: operation that may be recommended or executed.
- `Executor`: component allowed to perform the action.
- `Evidence`: durable explanation of why the state/action was valid.

## Truth Owner, Orchestration, Executor

Use three roles:

| Role | Responsibility | Example |
|---|---|---|
| Truth owner | Publishes facts it owns. | blockvolume publishes durable frontier. |
| Orchestration engine | Computes composed state and allowed action. | ManagedVolume says `request_promotion(r2)` is allowed. |
| Executor | Performs the side effect. | master recovery executor publishes new authority. |

Bad pattern:

```text
local listener sees a symptom -> performs product-level action
```

Correct pattern:

```text
local listener emits fact
orchestration engine evaluates context
executor performs gated action
```

## Multi-State Overlay

A product object rarely has one state. A PVC-backed block volume may have
several state dimensions at once:

```text
install_state=ready
kubernetes_state=volume_bound
authority_state=primary_available
replica_state=degraded
host_path_state=multipath_ready
recovery_state=none
workload_state=verified
claim_state=usable_degraded
```

Do not collapse these into one enum too early. A single `status=ok` field is
useful only after the dimensions are preserved.

Recommended state dimensions for Phase 22:

- install state,
- Kubernetes/PVC state,
- CSI attach state,
- authority state,
- replica/durable state,
- frontend/host-path state,
- recovery state,
- workload verification state,
- cleanup/residue state,
- claim boundary state.

## Priority Rules

When states conflict, use deterministic priority.

Suggested priority order:

1. `invalid`: facts contradict required identity/generation/epoch.
2. `unsafe`: stale primary, split-brain risk, unknown authority, or unsafe
   publish target.
3. `blocked`: user path cannot proceed and needs action or config change.
4. `recovering`: safe automated or manual recovery is in progress.
5. `degraded`: usable, but redundancy/path/replica is impaired.
6. `ready`: current documented claim is satisfied.
7. `unknown`: insufficient evidence.

Priority must not hide lower-priority context. Example:

```text
overall=blocked
blocked_reason=publish_target_loopback_cross_node
also:
  pvc_phase=Bound
  blockvolume_ready=true
  writer_mount_failed=true
```

## Context Rules

Facts without context are dangerous. Every fact should carry enough context to
avoid stale or cross-volume interpretation:

- volume identity,
- replica identity,
- Kubernetes namespace/name/UID when relevant,
- node identity,
- epoch and endpoint_version when authority-related,
- LSN/frontier when durability-related,
- session/path ID when host-path-related,
- observed_at,
- source,
- confidence.

If a fact cannot be attached to the correct context, it should be kept as
evidence but not used for control decisions.

## Action Rules

Every action should be a typed object:

```text
action_type
target
side_effect_class
owner_executor
preconditions
required_facts
invariant_refs
policy_gate
evidence_output
dry_run / executable
```

Side-effect classes:

- `observe`
- `safe_k8s`
- `disruptive_k8s`
- `authority_mutating`
- `repair_mutating`
- `destructive`

Action priority:

1. prefer observation/bundle when facts are incomplete,
2. prefer wait when a bounded convergence condition exists,
3. prefer safe K8s reconciliation only for owned objects,
4. require explicit policy for disruptive K8s actions,
5. require authority invariants for promotion/fencing,
6. require audit and rollback story for repair/destructive actions.

## State Machine Discipline

Before adding a new state, answer:

1. Is this a product semantic state or a timing workaround?
2. What facts enter the state?
3. What facts leave the state?
4. Is the transition deterministic over the fact set?
5. What invariant prevents a false claim?
6. What action, if any, becomes allowed?
7. What evidence is emitted?

If the state only exists because an event arrived late, model generation,
epoch, owner UID, session ID, or observed_at instead.

## Testing Methodology

Use three layers:

1. Local engine tests.
   - one engine,
   - table-driven facts,
   - state/action/invariant assertions.
2. Cross-engine model tests.
   - multiple fact domains,
   - same facts in different order,
   - same final product state and allowed actions.
3. Live gates.
   - prove host/Kubernetes/protocol behavior,
   - capture evidence bundle,
   - close the product claim.

Live gates should not be the first place where composed semantics are tested.

## Per-Step Review Discipline

Every model/control-plane D-step should report three things:

1. TDD.
   - What red/green or table tests were added?
   - What negative case prevents over-claiming?
2. Internal review.
   - Who owns each new fact?
   - Is this local truth or composed product decision?
   - Which state priority rule applies?
   - Which invariant protects the claim?
   - Which executor owns any action?
   - Is the action read-only, dry-run, or mutating?
3. Regression check.
   - Which package tests ran?
   - Was any live/TestOps gate needed?
   - What non-claim remains?

If a D-step cannot answer these, it is not ready to close.

## Phase 22 Application

For Phase 22, `ManagedVolume` should be treated as:

```text
orchestration-level engine
with execution disabled except read-only observe actions
```

It should define:

- fact schema,
- context fields,
- multi-state projection,
- priority rules,
- invariant refs,
- dry-run allowed actions,
- evidence refs.

It should not yet execute:

- promotion,
- repair/rebuild,
- workload recreate,
- destructive cleanup.

Future operator/controller work can enable execution by consuming the same
action contract.
