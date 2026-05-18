# Mounted Failover And Reattach Contract

Date: 2026-05-13
Plan: `Basic Mounted Failover And Reattach MVP`

## User-Facing Claim Target

For the documented alpha topology and ACK profile, a controlled primary
`blockvolume` failure is either:

1. recovered through the Kubernetes PVC app path with data verified after
   reattach, or
2. refused safely with a support bundle that explains why promotion is not
   allowed.

This is a beta availability exercise, not a production HA claim.

## Supported MVP Topology

Initial supported topology:

- Kubernetes alpha path,
- iSCSI PVC,
- RF=2,
- product-owned `blockvolume` Deployments,
- durable hostPath per replica,
- loopback frontend constrained by same-node attach rules,
- one app workload writing data before failure and one app workload reading data
  after reattach/restart.

RF=3 is not part of this plan's live Kubernetes claim unless it falls out
naturally and is separately gated. NVMe remains protocol-gated but is not the
first Kubernetes mounted failover target.

## ACK Profile

The gate must record the exact ACK profile in both run artifacts and inventory:

```text
ack_profile=best-effort | sync-quorum | sync-all
```

Default MVP profile is conservative:

- If using `best-effort`, the claim is limited to bytes explicitly written,
  flushed, and verified after the controlled failure.
- If using `sync-quorum`, the gate must run blockvolumes with
  `--replication-ack=sync-quorum` and prove that writes fail closed when quorum
  acknowledgement is unavailable.
- If using `sync-all`, the gate must prove both replicas acknowledge the writes
  before claiming all-ack durability.

No plan text or runbook may say "all acknowledged writes survive failover"
unless the gate runs a non-best-effort ACK profile and validates the failure
mode.

## Failure Injection

The primary failure is controlled and named:

```text
failure_class=primary-blockvolume-controlled-stop
failed_replica=<replica_id>
failed_epoch=<epoch>
failed_endpoint_version=<endpoint_version>
```

The preferred injection is deleting or scaling down the primary
`blockvolume` Deployment after the writer has completed and after the artifacts
record the pre-failure authority line.

The failure must not be an unscoped process kill that could hide which resource
was targeted.

## Expected Successful Recovery

A successful recovery must show:

- pre-failure data checksum written through the PVC,
- pre-failure primary replica and authority lineage,
- old primary no longer serving frontend I/O,
- new primary replica and authority lineage,
- epoch or endpoint-version advancement,
- app restart/reattach method,
- post-reattach checksum match,
- inventory support bundles for every visible replica.

Minimum evidence lines:

```text
failover_status: recovered
ack_profile: <profile>
old_primary: replica=<rN> epoch=<N> endpoint_version=<N> fenced=true
new_primary: replica=<rM> epoch=<N+> endpoint_version=<N+> frontend_ready=true
reattach: method=pod-restart|pod-recreate|node-restage
data_check: before=<sha256> after=<sha256> match=true
```

Transparent in-place I/O continuation is not claimed. Pod restart/recreate is a
valid MVP reattach method if it is documented and gated.

## Expected Safe Refusal

If the product cannot safely promote another replica, the gate must still PASS
only if the refusal is explicit and safe:

```text
failover_status: refused
reason=<specific_issue_class>
old_primary_safe=false|unknown
candidate_ready=false
data_check_after_failover=not_claimed
```

Valid refusal reasons include:

- `insufficient_replica_coverage`
- `candidate_not_ready_for_primary`
- `durable_frontier_missing`
- `replication_ack_profile_unmet`
- `status_endpoint_unreachable`
- `stale_primary_not_fenced`

Invalid refusal behavior:

- silent timeout with no bundle,
- promoting a heartbeat-only replica,
- marking a returned replica healthy without durable/recovery evidence,
- keeping the old primary looking like a valid frontend writer after authority
  moved.

## Inventory And Support Bundle Requirements

`sw-block ops inventory` or a scenario-produced companion bundle must expose a
failover timeline:

```text
failover_timeline:
  - phase: before_failure
    primary: <replica>
    epoch: <epoch>
    endpoint_version: <ev>
  - phase: failure_injected
    failed_replica: <replica>
    failure_class: <class>
  - phase: after_failover
    status: recovered|refused
    primary: <replica>|none
    epoch: <epoch>|none
    endpoint_version: <ev>|none
```

Per-replica status bundles must include:

- authority role,
- frontend readiness,
- local healthy bit,
- durable latch/operational state,
- peer/replication state when available,
- reason wording when the rollup is degraded.

The wording must distinguish:

- "replica process is alive",
- "replica is eligible as primary",
- "replica is serving frontend I/O",
- "replica is degraded because a higher-level authority/durable condition is
  missing."

## Fast-Test Requirements

Before the runner gate, fast tests should cover:

- RF=2 lifecycle placement does not silently publish only stale authority when
  another eligible slot exists.
- old-primary supersede state is visible and maps to a non-serving frontend
  projection.
- failover inventory issue wording is non-contradictory.
- returned replica is not primary-eligible from heartbeat alone.
- missing durable frontier or unmet ACK profile causes safe refusal.

## Runner Gate Requirements

The runner-native gate must include:

```text
pre_clean
preflight
pin/build alpha images
create RF=2 PVC
wait for two blockvolume replicas
write/check data before failure
capture before-failure inventory/status
inject controlled primary failure
observe authority move or safe refusal
restart/reattach app if recovery path is claimed
read/check data after reattach if recovered
capture after-failure inventory/status
collect_and_cleanup(always)
```

If the product cannot create a real RF=2 PVC path safely, the gate must say so
through the safe-refusal branch and the operations manual must keep live RF=2
Kubernetes recovery as a non-claim.

## Non-Claims

This plan does not claim:

- transparent zero-disruption I/O,
- production HA,
- arbitrary node loss,
- remote-node attach to loopback frontends,
- RF=3 live Kubernetes failover,
- NVMe Kubernetes failover,
- automatic rebuild completion,
- returned-replica reintegration as a broad product claim,
- performance SLOs,
- upgrade safety,
- backup/restore,
- UI/operator-grade remediation.

## Close Rule

The plan can close only when the runner gate proves either recovered or refused
safely, and QA can read the bundle without implementation context and answer:

```text
which replica was primary before,
which resource failed,
whether authority moved,
whether the old primary was fenced,
whether data was verified after reattach,
or exactly why recovery was refused.
```
