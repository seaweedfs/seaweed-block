# Protocol Anti-Patterns

This is the Seaweed Block working copy of the V3 protocol anti-pattern list.
Use it during design and code review.

## A1: Timer Defines Semantics

Bad:

```text
heartbeat missed for N seconds -> replica is semantically dead -> promote
```

Correct:

```text
timer fires -> collect facts -> facts determine action
```

Timers may trigger probes. They must not be the sole source of data truth,
promotion truth, or recovery type.

Kubernetes implication: kubelet timeout, pod phase, or node heartbeat should
not by itself prove data loss or promotion safety. It triggers observation.

## A2: Transport Error Defines Recovery Type

Bad:

```text
TCP reset -> rebuild
iscsi discovery timeout -> data lost
CSI stage timeout -> promotion needed
```

Correct:

```text
transport error -> reachability fact
probe / durable frontier / authority facts -> recovery decision
```

Transport failure and data-plane recovery are different truth domains.

## A3: Ack Arrival Defines Terminal Success

Bad:

```text
remote ack arrived -> recovery complete
barrier response arrived -> product recovered
CSI stage returned -> workload recovered
```

Correct:

```text
ack updates observed progress
terminal success requires explicit session close / barrier contract /
workload data check / support-bundle evidence depending on the claim
```

For product claims, terminal success must match the user-visible contract. A
reader checksum claim requires a reader checksum artifact, not just a control
ack.

## A4: Event Ordering Determines Semantics

Bad:

```text
same facts, different result depending on whether pod event arrived before
master heartbeat
```

Correct:

```text
same facts -> same decision
arrival order affects latency, not semantic outcome
```

Use epoch, endpoint_version, LSN, session ID, and generation to order facts.
Decision functions should be deterministic over the fact set.

## A5: Projection Drives Control Truth

Bad:

```text
status=healthy -> safe to promote
pod phase=Running -> volume is usable
inventory_status=ok -> no action needed
```

Correct:

```text
control decisions read underlying facts
projection reports facts for humans and machines
```

Projection is output. It must not create authority.

## A6: Timing Workaround States Accumulate

Bad:

```text
add WaitingForMaybeOldPodButMaybeNewMaster because one scenario raced
```

Correct:

```text
model the real fact: generation, epoch, owner UID, session ID, observed_at
```

New states must express product semantics, not a patch history of races.

## A7: Transport Mechanics Leak Into Engine

Bad:

```text
engine knows TCP connection, goroutine, kubectl wait, grep output
```

Correct:

```text
runtime adapter translates transport events to facts
engine consumes facts and emits decisions
executor performs transport actions
```

This matters for K8s: TestOps, kubectl, SSH, Helm, and CSI logs are evidence
collection mechanisms. They should not become the product protocol.

## A8: Local Automata Without Global Invariants

Bad:

```text
K8s adaptor green
authority engine green
host path engine green
but composed product story is contradictory
```

Correct:

```text
small controllers are tested locally and against cross-controller invariants
```

Example invariant:

```text
If transparent_failover_claimed=true, then:
  same pod UID before/after
  host path switched to promoted target
  old primary stale_io_success_count=0
  post_failure_primary_count=1
  post-failure workload checksum passed
```

Without global invariants, small automata are just logic scattered across new
files.
