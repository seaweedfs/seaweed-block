# Stage 1 Mounted Recovery ACK Profile Contract

Date: 2026-05-13
Plan: `Stage 1 Mounted Recovery ACK Profile MVP`
Positioning input: `internal/docs/ref/product-positioning-v1.md`

## Product Question

Can a small-cluster Kubernetes user write data to a Seaweed Block PVC, see a
peer become promotion-ready, stop the current primary, and then reattach a pod
that reads the same data without relying on internal logs, under an ACK profile
that honestly matches the product claim?

If yes, the product earns one of two explicit Stage 1 claims: RF=2
controlled-best-effort recovery demo, or RF=3 sync-quorum durable HA recovery.
If no, it must publish a specific safe blocker and keep recovery as a
non-claim.

Master-side promotion is not, by itself, user-visible recovery. A recovered
claim in this plan requires the Stage 1 host-path mechanism:

```text
CSI/node reattach on pod recreate
```

If the run cannot prove CSI/node reattach, the only valid result is safe refusal
even if the master selected a new primary. Transparent protocol multipath
switching through iSCSI ALUA or NVMe ANA is the next plan.

Stage 1 is the minimum HA-like product line. A close report may not mark this
plan recovered unless the Kubernetes app path recovers without manual promote,
manual repair, direct storage inspection, or direct blockvolume reads.

Stage 2 is a separate future contract for transparent multipath host failover.
It must not be used as a substitute for Stage 1 CSI/pod-recreate evidence.

## Supported MVP Topology

Supported for this contract:

- Kubernetes alpha path,
- product-owned blockvolume lifecycle,
- iSCSI PVC,
- RF=2 best-effort controlled demo,
- RF=3 sync-quorum durable HA gate,
- two logical Seaweed Block server identities,
- same physical Kubernetes node under the current loopback attach constraint,
- durable hostPath per replica,
- pod recreate/reattach after failure.

Out of topology:

- remote-node attach,
- arbitrary node loss,
- NVMe Kubernetes recovery,
- transparent in-place I/O continuation,
- production HA.

## User-Visible Contract

Given:

```text
topology=alpha-k8s-same-node-rf2
protocol=iscsi
ack_profile=<best-effort|sync-quorum|sync-all>
host_path_method=csi-node-reattach
reattach_method=pod-recreate
```

When:

```text
writer pod writes and verifies /data/demo.bin
non-primary peer candidate reaches promotion_ready=true
the current primary blockvolume Deployment is stopped
the documented host path recovers or safe-refuses
reader pod reads the same PVC if recovery is legitimately claimed
```

Then recovered behavior requires:

```text
failover_status: recovered
before_primary_replica=<rN>
failed_replica=<same rN>
new_primary_replica=<rM>
old_primary_fenced=true
candidate_ready=true
host_path_recovered=true
host_path_method=csi-node-reattach
reattach_method=pod-recreate
data_check_after_failover=claimed
before_sha256=<sha>
after_sha256=<same sha>
match=true
manual_operator_repair=false
```

If `r2` does not reach promotion readiness, the only valid pass is safe
refusal:

```text
failover_status: refused
candidate_ready=false
data_check_after_failover=not_claimed
reason=<specific blocker>
```

Recovered behavior is invalid if any of these are true:

- promotion is manual,
- recovery uses direct blockvolume or durable-store reads,
- CSI/node reattach target generation is not captured,
- reader checksum is missing,
- old primary still appears as a valid writer,
- the ACK/frontier basis is absent or weaker than the claim.

## ACK Profile

The run must emit:

```text
ack_profile=<profile>
```

Allowed profile meanings:

- `best-effort`: only the exact bytes caught up and verified by the gate are
  claimed. This does not claim quorum durability for all acknowledged writes.
- `sync-quorum`: foreground writes must fail closed when quorum acknowledgement
  is unavailable. If used, the gate must prove that failure mode.
- `sync-all`: all replicas acknowledge writes before success. If used, the gate
  must prove both replicas acknowledge the data before failure.

Default implementation behavior may remain `best-effort`. An RF=2 run may be a
controlled recovery demonstration only when the bundle says
`claim_profile=controlled-best-effort-demo`. A beta-facing writable HA recovery
claim requires RF=3 with `sync-quorum` unless a later product spec defines a
different degraded-write policy. The support bundle must make this visible so
operators do not infer stronger durability.

## Promotion-Ready Evidence

A replica is not promotion-ready just because it is observed.

Minimum promotion-ready evidence:

```text
candidate_replica=<rN>
candidate_ready=true
candidate_reason=caught_up
candidate_observed=true
candidate_reachable=true
candidate_frontend_ready=false
candidate_authority_role=<non-primary/supporting/superseded vocabulary>
candidate_replication_role=replica_ready
candidate_durable_latched=true
candidate_durable_operational=true
candidate_epoch=<E>
candidate_endpoint_version=<EV>
required_frontier_lsn=<writer acknowledged/flushed boundary>
candidate_frontier_lsn=<N>
frontier_covered=true
primary_peer_state=healthy
primary_peer_probe_in_flight=false
```

If the current implementation cannot expose every field yet, it must expose an
equivalent cold-readable line and document the mapping in this file before the
runner gate is accepted.

Invalid substitutes:

- heartbeat exists,
- Deployment Ready,
- `observed=1`,
- status endpoint reachable,
- local process healthy while `replication=not_ready`,
- component-only catch-up without the Kubernetes mounted path.

## Durable Frontier

Recovery must name the durable frontier that justifies promotion. Acceptable
evidence shapes:

```text
required_frontier_lsn=<writer acknowledged/flushed boundary>
durable_frontier: replica=<rN> lsn=<N> source=<status|recovery|peer>
frontier_covered=true
recovery_decision: none reason=caught_up R=<N> S=<N> H=<N>
```

or a stronger equivalent.

Promotion is valid only when:

```text
candidate_frontier_lsn >= required_frontier_lsn
```

If the bundle cannot identify a durable frontier, recovery must be refused:

```text
reason=durable_frontier_missing
candidate_ready=false
```

## Controlled Failure

The failure must be derived from live inventory, not hard-coded:

```text
failure_class=primary-blockvolume-controlled-stop
before_primary_replica=<rN>
failed_replica=<same rN>
failed_epoch=<E>
failed_endpoint_version=<EV>
target_deployment=<deployment>
target_ready_replicas=0
```

The gate must fail if `failed_replica != before_primary_replica`.

## Authority Movement

Recovered behavior requires:

```text
old_primary: replica=<rN> fenced=true frontend_ready=false
new_primary: replica=<rM> role=primary frontend_ready=true epoch=<E2> endpoint_version=<EV2>
authority_transition: old=<rN>@<E>/<EV> new=<rM>@<E2>/<EV2>
```

The transition must satisfy a concrete monotonic rule:

```text
E2 > E OR (E2 == E AND EV2 > EV)
```

If the implementation has a different documented authority generation rule, the
bundle must name it and prove it. If the transition cannot be proven, recovery
is invalid:

```text
failover_status: refused
reason=authority_promotion_missing
data_check_after_failover=not_claimed
```

If the old primary still looks frontend-ready, recovery is also invalid:

```text
failover_status: refused
reason=stale_primary_not_fenced
data_check_after_failover=not_claimed
```

## Reattach Method

Transparent in-place I/O continuation is not claimed.

The allowed Stage 1 MVP host-path method is:

```text
host_path_method=csi-node-reattach
reattach_method=pod-recreate
```

Evidence for CSI/node reattach:

```text
writer_pod=<name>
reader_pod=<name>
writer_sha256=<sha>
reader_sha256=<same sha>
publish_target_before=<replica>@<epoch>/<endpoint_version>
publish_target_after=<replica>@<epoch>/<endpoint_version>
staged_target_before=<replica-or-target>
staged_target_after=<replica-or-target>
manual_operator_repair=false
```

The reader pod must use the same PVC and read through the Kubernetes CSI/iSCSI
path. A direct blockvolume, filesystem, or component-store read is not valid for
the mounted recovery claim.

## Control-Plane Observation

Every recovered or refused run must emit a cold-readable control-plane timeline.
The current artifact name is:

```text
control-plane-timeline.txt
```

Minimum event vocabulary:

```text
event=primary_observed replica=<rN> evidence=<inventory-replica-line>
event=candidate_evaluated replica=<rM> candidate_ready=<true|false> reason=<reason>
event=primary_failure_injected replica=<rN> deployment=<deployment> failure_class=primary-blockvolume-controlled-stop
event=authority_published from=<rN> to=<rM> primary=<rM> primary_count=1 evidence=<inventory-replica-line>
event=safe_refusal replica=<rM> candidate_ready=false reason=<reason> evidence=<inventory-issue-line>
event=csi_reattach_observed reader_pod=sw-block-demo-reader method=pod-recreate log=<blockcsi-node-log>
event=data_check reader_verified=true result=reader_checksum_passed log=<reader-log>
```

Recovered runs require `authority_published`, `csi_reattach_observed`, and
`data_check`. Refused runs require `safe_refusal` and must not emit a successful
data check. This timeline is the Stage 1 control-plane observation surface until
the product grows a first-class `sw-block ops timeline` command.

If CSI/node reattach is not proven, recovery must be refused:

```text
failover_status: refused
reason=host_path_recovery_not_verified
data_check_after_failover=not_claimed
```

## Safe Refusal Reasons

Valid refusal reasons:

- `candidate_not_ready_for_primary`
- `replication_role_not_ready`
- `durable_frontier_missing`
- `replication_ack_profile_unmet`
- `status_endpoint_unreachable`
- `authority_promotion_missing`
- `stale_primary_not_fenced`
- `host_path_recovery_not_verified`
- `reattach_not_verified`

Safe refusal requires:

```text
failover_status: refused
reason=<one of above or a more specific subclass>
candidate_ready=false
data_check_after_failover=not_claimed
after_issue_evidence=<inventory/support-bundle line>
```

## Inventory And Bundle Requirements

The runner bundle must include:

- before-failure inventory summary,
- before-failure nested per-replica status bundles,
- promotion-readiness evidence,
- failure target evidence,
- after-failure inventory summary,
- after-failure nested per-replica status bundles,
- safe-refusal or recovery decision file,
- host-path method evidence if recovery is claimed,
- writer and reader logs if recovery is claimed,
- cleanup audit.

The summary must let a cold operator answer:

1. Which replica was primary before failure?
2. Was the peer promotion-ready before failure?
3. Which exact resource was stopped?
4. Did authority move or refuse?
5. Did the old primary stop looking like a writer?
6. Which host-path recovery method was used, or why was it not claimed?
7. Was data read after reattach, or was that explicitly not claimed?

## Fast Guards Required Before Runner Gate

Add or retain fast tests for:

- heartbeat-only candidate is not sufficient for promotion readiness,
- observed-but-`replication=not_ready` peer is not eligible,
- durable frontier missing causes safe refusal,
- ACK profile is emitted and cannot be silently defaulted,
- old primary supersede maps to non-serving frontend projection,
- inventory wording distinguishes process health from promotion readiness.

## Runner Gate Shape

Required phases:

```text
pre_clean
preflight
pin_build_alpha_images
install_rf2_alpha_stack
rf2_mounted_write_boundary
wait_for_promotion_ready_or_blocker
inventory_before_failure
inject_primary_failure
observe_recovery_or_refusal
prove_csi_node_reattach_if_recovered
reattach_reader_if_recovered
inventory_after_failure
assert_contract
collect_and_cleanup(always)
```

The gate may close in one of two ways:

1. `recovered`: reader checksum after pod recreate matches writer checksum, and
   authority/fencing evidence plus CSI/node reattach evidence are present.
2. `refused`: no reader success is claimed, and the blocker names the missing
   condition.

Any other outcome is fail.

## Non-Claims

This contract does not claim:

- production HA,
- node loss survival,
- transparent in-place I/O continuation,
- remote-node attach,
- NVMe Kubernetes recovery,
- Kubernetes CSI multipath host-path failover,
- transparent or near-transparent protocol failover,
- RF=2 quorum recovery after one replica loss,
- quorum durability unless RF=3 `sync-quorum` is gated,
- broad rebuild/reintegration automation,
- performance SLOs,
- upgrade or uninstall safety.

## Close Rule

This plan may close only when the implementation either:

- proves the recovered branch with QA-owned runner evidence, or
- proves a sharper safe blocker and updates user-facing non-claims.

It must not close by renaming `observed` or `reachable` as promotion-ready.
