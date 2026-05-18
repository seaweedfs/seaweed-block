# QA Assignment: Node-Loss Survival MVP Close Hard Gate

Status: draft gate for the active `Node-Loss Survival MVP` plan.

This gate is intentionally strict. Stage 2 proved same-node logical-replica
transparent failover. This plan must prove real Kubernetes node-loss recovery
semantics, starting with the conservative CSI/pod-recreate path.

## Product Contract Under Test

```text
protocol=iscsi
replication=RF3
ack_profile=sync-quorum
frontends=non-loopback
topology=3 Kubernetes nodes; physical-host/fault-domain sharing disclosed
failure=controlled primary Kubernetes node loss or equivalent node isolation
recovery=CSI/pod recreate reattach to surviving promoted frontend
transparent_failover_claimed=false
node_loss_recovery_claimed=true
```

Current lab note: this gate is LAN TCP/iSCSI over `192.168.1.x`. `tp01` is
non-RoCE, so QA must not treat this as RDMA/RoCE, NVMe/RDMA, or performance
evidence. Any `10.0.0.x` fabric claim is out of scope for this close.

## Required Runner Scenario

Expected scenario name:

```text
testops/scenarios/node-loss-survival-rf3-reattach-chain.yaml
```

If the scenario name changes, the report must state the replacement name and
why.

## Hard-Gate Clauses

Any single `FAIL` blocks close.

### HG-0 Documentation Entry

Pass:

- `docs/operations-v1.md` has a node-loss survival section,
- it distinguishes same-node Stage 2 failover from real node-loss recovery,
- it names the recovery mechanism as CSI/pod recreate for this close,
- it lists non-claims: no transparent node-loss, no NVMe ANA node-loss, no
  rebuild/failback, no RTO/SLO.

Fail:

- docs imply Stage 2 same-node multipath proves node loss,
- docs claim transparent mounted node-loss recovery without a gate.

### HG-1 Multi-Node Topology

Pass:

- artifacts show three Kubernetes nodes participating,
- RF3 desired placement spans three distinct Kubernetes nodes,
- `replicas_on_distinct_nodes=true`,
- `node-placement.before.txt` maps replica -> server -> Kubernetes node,
- `node-placement.before.txt` records physical/fault-domain shape and whether
  physical-host loss is claimed.

Fail:

- fewer than three Kubernetes nodes participate,
- node identity is inferred only from server IDs without Kubernetes node proof.

Note: three Kubernetes nodes may share two physical machines for this MVP if the
report explicitly says `physical_host_loss_claimed=false`. Full physical-host
loss requires a later or stricter gate with distinct physical fault domains.

### HG-2 Non-Loopback Frontends

Pass:

- each candidate frontend used for cross-node attach is non-loopback,
- artifact records `frontends_non_loopback=true`,
- inventory shows frontend addresses reachable from the CSI node.

Fail:

- any cross-node attach target is `127.0.0.1` or localhost-only,
- the scenario succeeds by same-node loopback attach.

### HG-3 Pre-Failure App Write

Pass:

- writer pod writes and verifies `/data/demo.bin` through the PVC,
- `writer.log` contains checksum evidence,
- pre-failure inventory identifies primary replica and primary node.

Fail:

- data was written outside the mounted PVC,
- primary identity is not captured before failure.

### HG-4 Scoped Primary Node Failure

Pass:

- failed node is derived from live inventory,
- `failed_node == before_primary_node`,
- `failed_replica == before_primary_replica`,
- failure is scoped to the primary node/path and does not globally kill all
  replicas.

Fail:

- failed node is hard-coded without inventory proof,
- failure injection kills all blockvolumes or all nodes.

### HG-5 Authority Movement

Pass:

- post-failure evidence shows a promoted replica on a surviving node,
- `post_failure_primary_count=1`,
- promoted replica covers the sync-quorum frontier,
- no `conflicting_primary_replicas` issue.

Fail:

- dual primary observed,
- promoted replica is on the failed node,
- authority movement is inferred only from logs without inventory/support
  evidence.

### HG-6 CSI Reattach Uses Surviving Target

Pass:

- replacement pod is allowed for this plan,
- artifact records `pod_recreate_used=true`,
- CSI publish/stage evidence before/after shows target moved from failed-node
  frontend to surviving-node frontend,
- reader attaches to the promoted target.

Fail:

- reader attaches to the old failed-node frontend,
- target movement is not visible.

### HG-7 Data Verification After Node Loss

Pass:

- `reader.log` verifies the pre-failure `/data/demo.bin`,
- artifact records `data_check_after_node_loss=reader_checksum_passed`,
- recovery boundary records `node_loss_recovery_claimed=true`.

Fail:

- checksum is missing or only validates newly written data,
- recovery is marked successful without reader evidence.

### HG-8 Stale Primary Fenced

Pass:

- old primary cannot return successful data WRITE or SYNC after failure, or the
  scoped failure leaves no ready old-primary endpoint for stale I/O,
- artifact records `old_primary_stale_io_success_count=0`,
- inventory/support bundle names the failed node/replica unavailable or fenced.

Fail:

- old primary returns GOOD for stale data I/O,
- stale primary is not tested or not observable.

### HG-9 Bounded Waits

Pass:

- `bounded-waits.txt` exists,
- attach, failure injection, authority promotion, CSI reattach, reader data
  check, and cleanup are all bounded,
- success records `bounded_waits=pass`,
- failure records a stable blocker reason.

Fail:

- PVC/pod/session/reattach/data check hangs until the outer runner timeout.

### HG-10 Support Bundle Self-Explains

Pass:

- bundle answers, without raw internal log spelunking:
  - which node failed,
  - which replica was primary before failure,
  - which surviving replica was promoted,
  - which frontend CSI used before/after,
  - whether data verified,
  - whether stale primary was fenced.

Fail:

- a reviewer must inspect blockmaster/blockvolume raw logs to understand the
  result.

### HG-11 Cleanup Hygiene

Pass:

- no active iSCSI sessions for the test IQN,
- no stale iSCSI node DB entries for the test IQN unless explicitly attributed
  and removed,
- no `blockmaster`/`blockvolume`/`blockcsi`/`iscsi-target` processes,
- no `kubectl port-forward svc/blockmaster`,
- no `app=sw-blockvolume` Deployments,
- no run-scoped `/var/lib/sw-block/testops-*` paths.

Fail:

- any unexplained residue remains.

### HG-12 Non-Claims Honest

Pass:

- docs and bundle explicitly do not claim transparent node-loss, NVMe ANA
  node-loss, arbitrary network partition tolerance, rebuild/failback, RTO/SLO,
  production HA outside the tested topology, or full physical-host loss when
  Kubernetes nodes share a physical host.

Fail:

- the close report uses this gate to imply broader HA.

## Report Template

QA report must include:

```text
Verdict: PASS|FAIL (strict)
Product commit:
Runner commit:
Run id:
Scenario:
Result:

HG table:
HG-0 ...
...
HG-12 ...

Key evidence:
- topology:
- frontends:
- before_primary_replica/node:
- failed_replica/node:
- promoted_replica/node:
- pod_recreate_used:
- CSI target before/after:
- data_check_after_node_loss:
- stale_primary_fencing:
- bounded_waits:

Residue audit:
- iSCSI sessions:
- iSCSI node DB:
- sw-block processes:
- port-forwards:
- k8s resources:
- run-scoped host paths:

Blocking findings:
Non-blocking findings:
QA needed next:
```

## Close Recommendation Rule

Only recommend close if all 13 clauses pass.

If the proof uses only same-node logical replicas or loopback frontends, report
`FAIL` even if the checksum passes.
