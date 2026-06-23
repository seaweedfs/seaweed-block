# Phase 54 D5 QA: Authority Executor Negative And Hold Matrix

Status: implemented and validated live.

Runner scenario:

```text
testops/scenarios/authority-executor-negative-chain.yaml
```

Gate script:

```text
scripts/run-phase54-authority-executor-negative-gate.sh
```

## Goal

Validate that the authority executor does not mutate ACK eligibility unless the
D4 terminal-evidence contract is complete and exactly one target object matches
the volume/replica identity.

## Cases

- blocked preflight holds,
- stale/frontier-behind evidence holds,
- unsafe frontend state holds,
- ambiguous target holds,
- cross-volume identity mismatch holds,
- mixed multi-contract reconcile can write one eligible target while holding an
  unsafe target and ignoring a no-contract volume.

## Required Properties

- Unsafe/ambiguous/mismatched cases must show `mutation_attempts=0`.
- Held targets must not gain `status.reasonCode`.
- Mixed reconcile must report `authority_executor=partial`.
- The eligible volume in the mixed reconcile must write
  `ack_eligibility_recorded`.
- The blocked volume in the mixed reconcile must remain unchanged.
- The no-contract volume must not create or mutate a target.
- RBAC boundary remains D3/D4 narrow:
  - can patch `swblockreplicaeligibilities/status`,
  - cannot patch `SwBlockVolume.status`,
  - cannot create Events,
  - cannot mutate workloads/storage resources.
