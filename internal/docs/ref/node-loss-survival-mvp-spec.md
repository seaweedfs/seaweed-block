# Node-Loss Survival MVP Spec

Status: D1 cold product spec for `current-plan.md` Node-Loss Survival MVP.

## Product Question

Can a Kubernetes user lose the node that hosts the active Seaweed Block primary
and still recover the same PVC data through a documented path, with evidence
that authority moved safely, stale paths were fenced, and the data check used a
surviving replica on another node?

This is the next product step after Stage 2. Stage 2 proved transparent
failover when multiple logical replicas run on one Kubernetes node and the
primary blockvolume pod is stopped. Node-loss survival must prove the failure
mode users actually care about:

```text
one Kubernetes node becomes unavailable
-> the old primary is unavailable
-> a surviving replica on another Kubernetes node becomes primary
-> the workload recovers through the declared mechanism
-> data verifies from the same PVC
```

## Product Contract Under Test

First close targets the conservative recovery path:

```text
protocol=iscsi
replication=RF3
ack_profile=sync-quorum
topology=3 logical replicas on 3 Kubernetes nodes; physical-host/fault-domain
sharing must be disclosed
frontends=non-loopback, reachable from CSI nodes
failure=controlled primary Kubernetes node loss or equivalent node isolation
recovery=CSI/pod recreate reattach to surviving promoted frontend
data_check=reader verifies pre-failure bytes from the same PVC
```

The current lab proof uses LAN TCP/iSCSI addresses on `192.168.1.x`. It does
not use the `10.0.0.x` RoCE fabric; `tp01` is not RoCE-capable and must not be
used to support RDMA, NVMe/RDMA, or performance claims for this plan.

Transparent multipath node-loss is a follow-up gate, not the first close gate,
unless the conservative path proves too little product value. The first close
may use pod recreate / CSI reattach, but it must be honest:

```text
pod_recreate_used=true
transparent_failover_claimed=false
node_loss_recovery_claimed=true
```

## Non-Negotiable Semantics

- Replicas must land on distinct Kubernetes nodes for the node-loss proof.
- Frontends must be reachable from the consuming CSI node; loopback frontends
  are not valid for cross-node attach.
- The failed primary must be derived from live inventory, not hard-coded.
- Master controls authority. CSI consumes the published target after recovery;
  CSI does not choose or promote a primary.
- Promotion must require sync-quorum frontier coverage, using the Stage 1 RF3
  promotion gate semantics.
- The old primary must not be treated as a valid writer after node loss.
- If candidate readiness cannot be proven, the product must fail closed with a
  support bundle and must not create a false reader-checksum success.
- Every attach, promotion, reattach, and cleanup wait must be bounded with a
  stable blocker reason.

## Explicit Non-Claims

- No transparent mounted I/O continuation in the first node-loss close.
- No NVMe ANA node-loss claim.
- No Windows MPIO claim.
- No broad multi-distro compatibility claim.
- No RTO/RPO/SLO claim beyond the bounded gate timings.
- No automatic rebuild, reintegration, or failback claim.
- No arbitrary multi-failure or network-partition tolerance unless a later gate
  explicitly proves it.
- No production HA claim outside the documented lab topology.
- No full physical-host-loss claim when multiple Kubernetes nodes share one
  physical machine; that is an intermediate Kubernetes-node-loss lab only.

## Required Evidence

Minimum artifacts:

```text
cluster-topology.txt
node-placement.before.txt
writer.log
inventory.before/
primary-failure-recovery.txt
inventory.after-failure/
csi-publish-before.txt
csi-publish-after.txt
reader.log
node-loss-recovery-boundary.txt
node-loss-recovery-summary.txt
bounded-waits.txt
cleanup-audit.txt
```

Required stable lines:

```text
replicas_on_distinct_nodes=true
frontends_non_loopback=true
before_primary_replica=<rN>
before_primary_node=<node-a>
failed_node=<same node-a>
failed_replica=<same rN>
promoted_replica=<rM>
promoted_replica_node=<node-b>
post_failure_primary_count=1
pod_recreate_used=true
transparent_failover_claimed=false
node_loss_recovery_claimed=true
data_check_after_node_loss=reader_checksum_passed
old_primary_stale_io_success_count=0
bounded_waits=pass
physical_host_loss_claimed=false
```

## D-Slice Plan

### D1: Topology And Placement Audit

Audit current code and lab reality:

- whether TestOps has at least two Kubernetes nodes available,
- how `blockmaster` cluster-spec names physical nodes and server IDs,
- whether launcher can place replicas on distinct Kubernetes nodes,
- whether generated frontends can use non-loopback addresses,
- whether CSI publish lookup can select a target reachable from the consuming
  node,
- whether inventory can prove node placement and non-loopback frontends.

Exit: written audit section in `current-plan.md` and any required spec updates.

D1 audit result:

- The current local Kubernetes context is single-node and is not eligible for
  the node-loss live gate.
- The alpha installer currently renders all logical servers onto one
  Kubernetes node and uses loopback data/control addresses.
- The lifecycle planner has a base for multi-physical-node placement, but the
  Kubernetes renderer still emits loopback iSCSI/NVMe/status frontends.
- `blockvolume` intentionally rejects external iSCSI/NVMe binds today.
- CSI receives a Kubernetes node ID but publish lookup does not yet use it to
  validate target reachability.
- Master promotion evidence also needs routable status/probe access to
  surviving replicas; loopback status endpoints are not enough for true
  node-loss recovery.

Therefore D3 must first make cross-node placement and target eligibility real.
Recovery code is out of scope until that gate passes.

### D2: Strict QA Hard Gate

Create `qa-assignments/node-loss-survival-mvp-close-hard-gate.md`.

The gate must fail if:

- all replicas are on one physical node,
- frontends are loopback,
- the proof uses only a pod stop instead of node failure/isolation,
- reader succeeds only because it still used the old primary,
- recovery happens without visible authority movement,
- cleanup leaves sessions/processes/k8s resources/run-scoped host paths.

### D3: Placement / Non-Loopback Slice

Make the product produce and inventory:

```text
RF3 placements across distinct nodes
non-loopback replication data/control addresses
non-loopback iSCSI frontends
promotion/status probe access to surviving nodes
node-aware publish target evidence
```

This slice does not need to inject node loss yet. It proves the topology is
eligible.

Three Kubernetes nodes may run on two physical machines for the MVP lab if the
bundle records the physical/fault-domain shape. The wording must remain
Kubernetes-node-loss, not full physical-host-loss, unless the artifacts prove
three distinct physical fault domains.

The first implementation should preserve the existing safe default:

```text
external iSCSI bind = explicit opt-in
external iSCSI bind without CHAP = rejected for node-loss gate
loopback/unspecified node-spec host in external mode = rejected before render
CSI NodeStage secret delivery = required when target CHAP is enabled
CSI loopback publish targets = rejected under node-loss profile
external status bind = explicit opt-in and concrete node address only
master promotion probes = use surviving replica node-address status endpoints
master status frontends = current primary first, with all assigned replica
frontends preserved for multipath evidence
NVMe external bind = not claimed
loopback frontend in node-loss profile = hard fail
```

### D4: Conservative Node-Loss Recovery Gate

Runner-native scenario:

```text
writer writes /data/demo.bin
inventory identifies primary replica and primary node
controlled failure isolates or drains/stops that node path
master promotes a surviving replica
replacement pod reattaches through CSI to surviving frontend
reader verifies /data/demo.bin
bundle records node_loss_recovery_claimed=true and transparent_failover_claimed=false
cleanup proves no residue
```

Implemented scenario name:

```text
testops/scenarios/node-loss-survival-rf3-reattach-chain.yaml
```

The scenario derives the primary node from inventory, cordons that Kubernetes
node, scales the primary blockvolume Deployment to zero, and schedules the
replacement reader on the survivor node selected by
`scripts/preflight-node-loss-lab.sh`. This is intentionally not a
physical-host-loss proof unless the placement artifacts show distinct physical
fault domains.

### D5: Transparent Multipath Node-Loss Gate

Only after D4 passes, decide whether to extend the plan or open the next plan
for:

```text
same mounted pod
primary node loss
host multipath path switch to another node
no pod recreate
post-failure data check
```

This requires real multi-node pathing and should not be mixed into D4 unless the
lab/product path is already stable.

## Success Statement

After this plan, Seaweed Block can make a narrow beta-facing availability
statement:

```text
For the documented RF3 sync-quorum Kubernetes topology with non-loopback
frontends, Seaweed Block can recover a PVC after controlled primary-node loss
through CSI/pod recreate reattach, and the support bundle proves authority
movement, surviving-node target selection, stale-primary fencing, and data
integrity.
```
