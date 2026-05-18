# Control-Plane Observation API MVP

Status: reference spec for the next operations/usability slice.

This document defines the product shape for a dashboard-grade read-only
observation surface. It is not an admin-action API. It exists so operators can
answer "what is happening to my PVC?" without knowing Seaweed Block internals.

The shape should follow the SeaweedFS operations pattern:

```text
shared observation core
-> CLI for humans, support, QA, and AI
-> JSON/JSONL output for automation and bundles
-> master read-only API for dashboard/UI
```

Do not build separate truth sources for CLI, dashboard, TestOps, and AI.

## Product Question

Can a Kubernetes user open one dashboard or run one command and understand:

```text
which volumes exist
where each replica runs
which replica is primary
whether failover/recovery is progressing
which frontend CSI should attach to
why a volume is blocked or degraded
what evidence to send support
```

Current `sw-block ops inventory` already proves these facts in support bundles,
but it assembles them by polling Kubernetes, master `QueryVolumeStatus`, and
per-replica status endpoints. That is good for support bundles, but it is not a
clean long-term operations plane. The next product step is to factor the same
facts into one observation core and expose it through CLI, JSON/JSONL, and a
master read-only API.

## User Experience Target

The user-facing flow should be:

```bash
sw-block ops cluster
sw-block ops volumes
sw-block ops describe volume pvc-...
sw-block ops timeline volume pvc-...
sw-block ops timeline volume pvc-... -o jsonl
sw-block ops explain volume pvc-...
sw-block ops bundle volume pvc-... --out /tmp/sw-block-support
sw-block ops events --since 1h
```

Default output should be concise human-readable text. Every command that emits
state should also support `-o json`; timeline/events should support
`-o jsonl`. JSON/JSONL is the stable contract for AI, CI, future dashboard, and
support automation.

Dashboard shape:

```text
Cluster
  Volumes: 3 total, 2 ready, 1 degraded
  Nodes:   m01 ready, m02 ready, tp01 ready
  Alerts:  volume pvc-a recovering; stale primary fenced

Volume pvc-a
  PVC: default/mysql-data
  RF: 3 sync-quorum
  Primary: r2 on m02
  Frontend: 192.168.1.184:3260
  Status: Recovering
  Reason: primary_node_lost_reattached
  Data check: reader_checksum_passed
  Next action: none; continue watching

Replicas
  r1 m01 unavailable, stale-primary fenced
  r2 m02 primary, epoch=2, endpoint_version=1
  r3 tp01 replica_ready, durable_frontier_lsn=44
```

The dashboard must not require users to understand `AssignmentFact`,
`EndpointVersion`, or TestOps scenario internals. Those fields can still appear
in an advanced evidence panel.

## Current Surfaces

Master gRPC APIs today:

- `ObservationService.ReportHeartbeat`: blockvolume -> master local facts.
- `AssignmentService.SubscribeAssignments`: master -> blockvolume authority
  assignment stream.
- `EvidenceService.QueryVolumeStatus`: read-only per-volume status, used by
  CSI and `sw-block ops status`.
- `LifecycleService.CreateVolume/DeleteVolume`: desired lifecycle intent from
  CSI/operator.

CLI/support surfaces today:

- `sw-block ops status`: one replica/volume status bundle.
- `sw-block ops inventory`: cluster inventory assembled from Kubernetes,
  master status, and per-replica status bundles.
- TestOps control-plane timeline files in recovery scenarios.

Gap:

- no master API to list volumes,
- no master API to list nodes,
- no cluster-wide status rollup,
- no product-owned event stream,
- no stable timeline API for failover/recovery,
- no dashboard-ready condition model.

## Three-Layer Operations Surface

### Layer 1: CLI

CLI is the fastest product entry and should land first. It should look like the
rest of the SeaweedFS family: one binary, readable subcommands, explicit JSON
mode when automation needs it.

Commands:

```text
sw-block ops cluster
sw-block ops volumes
sw-block ops describe volume <id>
sw-block ops timeline volume <id>
sw-block ops timeline volume <id> -o jsonl
sw-block ops events --since 1h
sw-block ops events --watch
sw-block ops explain volume <id>
sw-block ops bundle volume <id>
```

CLI principles:

- default output is readable by humans and AI,
- reason codes are stable,
- JSON/JSONL output is schema-versioned,
- no command mutates state,
- bundle output includes the same JSON evidence the CLI used.

### Layer 2: Master Read-Only API

The API is the long-term product boundary. The dashboard must not shell out to
`kubectl`, SSH, or CLI commands across nodes. It should read the master API.

The API should call the same observation core as the CLI.

### Layer 3: Dashboard

Dashboard is a consumer, not a new source of truth. It should show the same
volume status, replica status, reason codes, events, and support-bundle hints
that the CLI returns.

First dashboard scope:

- cluster health,
- volume table,
- selected volume detail,
- selected volume timeline,
- support-bundle capture instructions.

No promote/repair/rebuild buttons in the first dashboard.

## API Contract

Add a read-only service. Name is provisional but semantics are not:

```proto
service ClusterEvidenceService {
  rpc GetClusterStatus(GetClusterStatusRequest)
      returns (ClusterStatusResponse);
  rpc ListVolumes(ListVolumesRequest)
      returns (ListVolumesResponse);
  rpc GetVolumeStatus(GetVolumeStatusRequest)
      returns (VolumeEvidence);
  rpc GetVolumeTimeline(GetVolumeTimelineRequest)
      returns (VolumeTimelineResponse);
  rpc WatchClusterEvents(WatchClusterEventsRequest)
      returns (stream ClusterEvent);
}
```

No method may mutate authority, placement, lifecycle, or replica state.

### ClusterStatusResponse

Minimum fields:

```text
schema_version
captured_at
cluster_id
product_revision
nodes[]
volumes[]
conditions[]
non_claims[]
```

Cluster rollup status:

```text
ok | degraded | recovering | blocked | unavailable
```

This rollup must be conservative. If a status source is missing, the response
should say `degraded` or `blocked` with a reason, not `ok`.

### NodeEvidence

Minimum fields:

```text
node_name
kubernetes_node
physical_host
internal_ip
schedulable
ready
last_heartbeat_at
replica_count
required_images[]
missing_images[]
conditions[]
```

Important condition reasons:

```text
node_not_ready
node_cordoned
heartbeat_stale
external_frontend_unreachable
image_missing_on_node
```

For Kubernetes install/attach failures, node evidence must include image
inventory for the product images the scenario or install selected. A dashboard
or CLI should be able to say:

```text
node=m02 condition=image_missing_on_node image=sw-block-csi:local
impact=CSI node pod ImagePullBackOff; PVC attach cannot proceed on this node
next_action=import image to node or use a registry reachable by all nodes
```

### VolumeEvidence

Minimum fields:

```text
volume_id
namespace
pvc_name
pv_name
replication_factor
ack_profile
claim_profile
desired_replicas
observed_replicas
status
primary_replica
primary_node
publish_target
epoch
endpoint_version
replicas[]
conditions[]
next_actions[]
support_bundle_hint
```

Volume status values:

```text
ok
degraded
recovering
blocked
invalid
```

The UI should show `status` and `reason` first, then show advanced fields only
when expanded.

### ReplicaEvidence

Minimum fields:

```text
replica_id
server_id
kubernetes_node
physical_host
observed
role
replication_role
durable_latched
durable_frontier_known
durable_frontier_lsn
candidate_ready
candidate_ready_reason
frontend_protocol
frontend_addr
status_addr
stale_primary_fenced
conditions[]
support_bundle_path
```

Important replica condition reasons:

```text
primary
replica_ready
replica_degraded
status_endpoint_unreachable
authority_not_assigned
replication_role_not_ready
durable_frontier_missing
candidate_frontier_behind
stale_primary_fenced
generated_deployment_missing
observed_replicas_below_desired
loopback_frontend_rejected
```

### ClusterEvent

Events are append-only evidence. They are not commands.

Minimum fields:

```text
event_id
event_time
volume_id
replica_id
node_name
type
severity
message
reason
old_value
new_value
epoch
endpoint_version
correlation_id
evidence_ref
```

Required event types for the first dashboard release:

```text
volume_created
placement_verified
replica_heartbeat_seen
replica_stale
promotion_candidate_evaluated
authority_published
publish_target_changed
stale_primary_fenced
csi_reattach_observed
volume_recovered
volume_blocked
support_bundle_collected
pod_image_pull_failed
csi_node_unavailable
```

TestOps-only events such as `data_check_result` may be included when a recovery
gate runs, but normal production must not pretend it ran an application-level
checksum unless it actually did.

## Tutorial: What A User Should See

### Healthy Volume

Command:

```bash
sw-block ops describe volume pvc-123
```

Output shape:

```text
volume pvc-123  status=ok  rf=3  ack=sync-quorum
pvc default/mysql-data
primary r1 on m01  frontend=192.168.1.181:3260
replicas desired=3 observed=3

r1 m01 primary       ready     durable_lsn=44
r2 m02 replica_ready ready     durable_lsn=44 candidate_ready=true
r3 tp01 replica_ready ready    durable_lsn=44 candidate_ready=true

next action: none
support bundle: sw-block support bundle --volume pvc-123 --out /tmp/sw-block-pvc-123
```

### Recovering After Primary Node Loss

Output shape:

```text
volume pvc-123  status=recovering  reason=primary_node_lost
old primary r1 on m01 is unavailable
candidate r2 on m02 is promotion_ready
authority moved r1 -> r2  epoch=2 endpoint_version=1
CSI publish target changed 192.168.1.181:3260 -> 192.168.1.184:3260
reattach method: pod_recreate

next action: wait for reader/app pod to become Ready
non-claim: transparent in-place I/O continuation is not claimed for this path
```

### Blocked Recovery

Output shape:

```text
volume pvc-123  status=blocked  reason=no_promotion_ready_candidate
primary r1 on m01 is unavailable
r2 on m02 candidate_ready=false reason=durable_frontier_missing
r3 on tp01 candidate_ready=false reason=candidate_frontier_behind

next action:
  collect support bundle
  do not force promote unless support confirms data frontier safety
```

## Dashboard Panels

First dashboard should have only four panels:

1. Volumes

   Shows PVC, status, RF, primary, frontend, and reason.

2. Replicas

   Shows replica placement, role, durable frontier, candidate readiness, and
   stale/fenced state.

3. Timeline

   Shows the last N events for selected volume: placement, heartbeat loss,
   candidate evaluation, authority publish, publish-target change, CSI
   reattach, and recovery.

4. Support Bundle

   One button or command copy block that captures exactly the evidence support
   needs.

5. Kubernetes Runtime Evidence

   Shows pod phase, container waiting reason, recent Kubernetes events, selected
   pod logs, and image presence by node for Seaweed Block components. This panel
   is mandatory for "PVC stuck attaching" cases because many failures are
   Kubernetes runtime issues before storage recovery begins.

Avoid advanced controls in the first dashboard. No promote, repair, rebuild,
or cleanup buttons until the read-only model is stable and gated.

## Implementation Slices

### D1: Observation Core And CLI From Bundle

- Define `ClusterEvidence`, `VolumeEvidence`, `ReplicaEvidence`, and
  `ClusterEvent` structs in ops/control code.
- Add renderers for text, JSON, and JSONL.
- Add golden examples for healthy, recovering, and blocked volumes.
- Add CLI commands that can read an existing inventory/support bundle first:
  `sw-block ops describe volume --from-bundle <dir>`,
  `sw-block ops timeline volume --from-bundle <dir>`, and
  `sw-block ops explain volume --from-bundle <dir>`.

Pass condition:

```text
three golden examples render user-readable output with stable reason codes
```

### D2: Live CLI Over Current Inventory Sources

- Wire `sw-block ops cluster`, `volumes`, `describe volume`, `timeline`, and
  `explain` to the current live inventory collector.
- Keep text output short; write detailed evidence to JSON.

Pass condition:

```text
node-loss D4 artifacts can be explained by CLI without reading raw TestOps files
```

### D3: Master Snapshot API

- Add `ClusterEvidenceService.GetClusterStatus`.
- Source data from lifecycle stores, placement stores, observation store,
  authority publisher current lines, and known frontend facts.
- Keep it read-only.

Pass condition:

```text
one live RF3 volume appears with primary, replicas, frontend, epoch, and reason
```

### D4: Timeline API

- Add an in-memory bounded event ring with optional persisted JSONL tail.
- Append events from product-loop transitions:
  placement verified, candidate evaluated, authority published, publish target
  changed, stale primary fenced.
- Add `GetVolumeTimeline`.

Pass condition:

```text
node-loss D4 produces the same six-event story from master API, not only from TestOps files
```

### D5: Watch API

- Add `WatchClusterEvents` server stream.
- Dashboard can subscribe without polling.
- Events include monotonic IDs and reconnect cursor.

Pass condition:

```text
client disconnect/reconnect does not miss authority_published or volume_blocked events
```

### D6: User Tutorial And Support Bundle

- Add `sw-block ops bundle volume`.
- Add docs showing how to diagnose healthy, recovering, and blocked.
- Keep all mutation commands explicitly non-existent.
- Include Kubernetes runtime evidence:
  `kubectl get pods -A -o wide`, `kubectl describe pod` for Seaweed Block and
  app pods, namespace events, CSI node/controller logs, blockmaster logs, and
  per-node product image inventory.

Pass condition:

```text
a user can collect one bundle and identify the next action without reading TestOps artifacts
```

### D7: Attach/Install Failure Explanation

- Detect `ImagePullBackOff`, `ErrImagePull`, CSI node unavailable, missing
  DaemonSet pod on the selected app node, and missing product image on a k3s
  node.
- Render the failure as an explanation, not just raw events.

Pass condition:

```text
if sw-block-csi:local is missing on one selected node, `sw-block ops explain`
names that node, image, pod, event reason, and next action
```

## Non-Claims

- No mutating admin controls.
- No repair/promote/rebuild button.
- No performance metrics dashboard.
- No hosted UI.
- No external alert-manager integration.
- No claim that application-level data checks ran unless the user/TestOps ran
  them.
- No replacement for Kubernetes events; this API explains Seaweed Block state.

## Hard Gate

The close gate for this MVP should fail if:

- any read API can mutate state,
- a recovering/blocked state lacks a stable reason code,
- node-loss recovery evidence is available only in TestOps files and not via
  master API,
- dashboard output requires users to know internal epoch semantics,
- support bundle misses the timeline or per-replica evidence,
- support bundle misses Kubernetes pod events/logs for attach/install failures,
- image missing on a selected node is not reported as a first-class reason,
- stale primary and promoted primary cannot be distinguished,
- event stream can lose `authority_published` across reconnect.

## Product Rule

This API is a trust feature. For block storage, a stuck PVC is a product
failure until the operator can quickly answer:

```text
what failed, what the system did, whether data is safe, and what to do next
```

The observation API should make that answer obvious.
