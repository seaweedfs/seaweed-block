# Engine Automata Design Note

Status: draft design note after the RF3 sync-quorum recovery slice.

## Why This Note Exists

The RF3 recovery work exposed a design smell:

```text
master promoted r2 -> CSI reattached to r2 -> ext4 mount still failed
```

The failure was not authority selection and not CSI. The missing semantic was a
post-promotion replication handoff:

```text
new primary r2 must know which surviving peers already cover the required
frontier and seed live shipping from that frontier before accepting RW mount
writes under sync-quorum
```

The narrow fix was correct for the current slice: the promoted primary runs a
barrier against newly installed peers at its local frontier, seeds the live ship
cursor only for peers that prove coverage, and leaves unsafe peers degraded. The
RF3 gate then passed.

But the architectural lesson is larger: "the engine" should not be treated as
one monolithic automaton. Block storage recovery is a composition of several
small automata with explicit contracts between them.

## Product Principle

Seaweed Block should not become a toy by making one green-path state machine
absorb every concern.

The product needs small, cold, auditable automata:

- each owns one semantic boundary,
- each emits durable or inspectable facts,
- each can fail closed with a reason,
- no automaton should infer another automaton's state from a convenient side
  effect.

## Proposed Automata

### 1. Identity Automaton

Owns:

- volume ID,
- replica ID,
- epoch,
- endpoint version,
- current assignment line.

It answers:

```text
Who am I for this volume line?
Is this process still assigned to this identity?
Has my identity changed?
```

It must not decide:

- whether the replica is caught up,
- whether a peer can be promoted,
- whether CSI should attach.

### 2. Authority Automaton

Owns:

- primary selection,
- promotion/refusal decision,
- old-primary supersede/fencing intent,
- authority epoch movement.

It answers:

```text
Which replica is allowed to be primary?
Why was promotion allowed or refused?
What authority epoch is published?
```

It should consume promotion evidence, not manufacture data-plane truth.

### 3. Durability / Frontier Automaton

Owns:

- local durable frontier,
- retained WAL frontier,
- head LSN,
- ACK profile interpretation: `best-effort`, `sync-quorum`, `sync-all`,
- required frontier for the current recovery claim.

It answers:

```text
What data has this replica durably covered?
What data has the product promised to preserve?
Does candidate durable_lsn cover required_lsn?
```

This is the automaton that prevents "largest LSN" from becoming an accidental
product promise. Largest LSN is a candidate ranking signal; required frontier is
the safety threshold.

### 4. Recovery Automaton

Owns:

- catch-up vs rebuild decision,
- recovery session lifecycle,
- base/WAL transfer,
- recovery completion evidence.

It answers:

```text
Can this replica catch up from retained WAL?
Does it need rebuild?
Has the session completed through target LSN?
```

It should not by itself publish the replica as primary. Completion becomes
evidence for authority, not authority itself.

### 5. Replication Handoff Automaton

Owns:

- primary-side peer runtime state,
- live ship cursor,
- post-promotion peer activation,
- quorum write admission after promotion.

It answers:

```text
Which peers are live enough to receive the next write?
At what LSN should live shipping resume?
Can sync-quorum/sync-all writes be acknowledged now?
```

This is the automaton that was implicit before the RF3 fix. The better contract
is:

```text
PromotionCommitted(volume, new_primary, epoch, required_frontier, covered_peers)
  -> for each covered peer: prove barrier >= required_frontier
  -> seed live cursor at required_frontier
  -> only seeded/healthy peers can satisfy quorum
  -> unproven peers stay degraded and trigger recovery/rebuild
```

The current implementation derives this from local frontier + barrier proof.
That is acceptable as an S1 product unblocker. A later design should make the
handoff event explicit.

### 6. Frontend Publish Automaton

Owns:

- iSCSI/NVMe frontend readiness,
- frontend endpoint,
- publish target generation,
- host-visible target identity.

It answers:

```text
What target should a host attach to now?
Is this target valid for the current authority line?
```

It should not decide promotion. It should reflect authority.

### 7. Host Attach Automaton

Owns:

- CSI ControllerPublish lookup,
- NodeStage / NodePublish behavior,
- portal/device/session matching,
- pod recreate reattach path,
- future multipath path selection.

It answers:

```text
Which target did Kubernetes ask me to stage?
Which device/session did I stage?
Did the mounted app path recover?
```

Stage 1 uses CSI/pod recreate. Stage 2 should add protocol multipath:

```text
iSCSI ALUA + dm-multipath
or
NVMe ANA + native multipath
```

## Message / Event Shape To Prefer

Avoid adding broad fields to every heartbeat because it is easy to create a
control-plane database that is stale, noisy, and semantically ambiguous.

Prefer narrow events/facts:

```text
IdentityAssigned(volume, replica, epoch, endpoint_version)
DurableFrontierObserved(volume, replica, durable_lsn, retained_lsn, head_lsn)
PromotionEvaluated(volume, candidate, ack_profile, required_lsn, candidate_lsn, result, reason)
PromotionCommitted(volume, old_primary, new_primary, epoch, endpoint_version, required_lsn)
ReplicationHandoffProved(volume, primary, peer, required_lsn, achieved_lsn)
ReplicationHandoffRejected(volume, primary, peer, required_lsn, achieved_lsn_or_unknown, reason)
FrontendTargetPublished(volume, replica, endpoint, epoch, endpoint_version)
HostPathRecovered(volume, pvc, target, method, checksum_result)
```

The key is not the exact names. The key is that each fact names the automaton
boundary and the product reason.

## Near-Term Guidance

For the current Stage 1 plan:

- Keep the narrow barrier-seeded handoff fix.
- Keep RF3 promotion gated by on-demand probe evidence.
- Keep CSI as a consumer of master-published target, not a promotion decider.
- Do not add a large heartbeat protocol expansion yet.
- Add support-bundle evidence for replication handoff when it becomes part of
  the close gate.

For the next design pass:

- Make promotion handoff an explicit event, not an incidental side effect of
  `UpdateReplicaSet`.
- Separate "current primary is healthy" from "candidate is promotable."
- Separate "replica process alive" from "replica durable frontier covers the
  committed frontier."
- Separate "frontend target published" from "host path recovered."

## Product Gate Implication

Future gates should not pass on a single line like:

```text
role=primary
```

They should require the automata chain:

```text
candidate durable frontier covers required frontier
authority commits new primary
replication handoff proves at least one quorum peer is live
frontend target is published
CSI/node stages that target
mounted app verifies data
```

If any link is missing, the product should fail closed and name the missing
automaton boundary.

