# Mounted Failover And Reattach Audit

Date: 2026-05-13
Branch: `basic-mounted-failover-reattach`
Plan: `Basic Mounted Failover And Reattach MVP`

## Question

Can the current V3 product path support a user-visible RF=2 Kubernetes mounted
failover and reattach claim, or do we need to build new wiring before making
that claim?

Short answer: the substrate exists, but the product-owned Kubernetes path is not
yet a mounted failover product claim. V2 and the earlier V3 protocol gates prove
important frontend and authority behaviors, but they do not wire the current
Kubernetes lifecycle path end to end.

## Existing Green Substrate

The following pieces are already present and should be reused rather than
rewritten:

- iSCSI ALUA and mounted failover are covered by
  `testops/scenarios/iscsi-p6-alua-failover-chain.yaml`.
- Returned-replica behavior after iSCSI failover is covered by
  `testops/scenarios/iscsi-returned-replica-chain.yaml`.
- NVMe ANA and multipath failover are covered separately by the NVMe P4/P5
  protocol gates.
- RF=1 Kubernetes durable restart/reattach is covered by the durable restart
  plan.
- Product-owned lifecycle can create generated `blockvolume` Deployments.
- Same-node iSCSI attach can run through CSI and a PVC.
- Cluster inventory can expose PVC/PV/replica/frontend/status/support-bundle
  evidence.

These are necessary but not sufficient for a Kubernetes mounted failover claim.

## V2 Reference Value

V2 is useful as a behavior reference, not a direct port target.

Use V2 to compare:

- iSCSI host behavior under ALUA and mounted failover,
- NVMe multipath/ANA behavior,
- broad protocol stress and compatibility cases,
- failure vocabulary and runbook expectations.

Do not port V2 authority shortcuts into V3:

- no heartbeat-as-authority,
- no heartbeat-as-ready,
- no promotion without durable/ready evidence,
- no target-LSN completion shortcuts,
- no bypass around the V3 publisher/epoch model.

The V3 code already intentionally separates observation, supportability,
authority minting, frontend projection, and replication. That separation is the
right product direction.

## Current V3 RF And Failover Wiring

### Lifecycle And Launcher

`core/lifecycle/planner.go` and `core/lifecycle/workload_plan.go` can represent
RF=2/RF=3 placement intent. The launcher can render one `blockvolume`
Deployment per replica, with distinct replica IDs and node-local ports.

`core/launcher/k8s_renderer.go` renders:

- one Deployment per replica,
- `--replica-id=<rN>`,
- `--durable-root=/var/lib/sw-block/<volume>/<replica>`,
- `--recovery-mode=dual-lane` by default,
- optional loopback status endpoints,
- iSCSI or NVMe frontend args based on protocol.

This means the deployment substrate can host multiple replicas.

### Product Loop To Authority

`core/host/master/product_loop.go` calls
`assignmentRequestsFromVerifiedPlacement`.

`core/host/master/authority_request_bridge.go` currently selects only the first
verified slot:

```text
RF>1 slots are placement/recovery inputs; they are not multiple competing Bind asks.
Pick the first verified slot deterministically until a later placement policy grows
an explicit primary-candidate field.
```

Impact: the product-owned Kubernetes lifecycle path can place RF=2 workloads, but
it does not yet publish an RF=2 failover candidate through the lifecycle bridge.
The authority controller can fail over when it receives a supported RF snapshot,
but the product loop currently treats additional lifecycle slots as inputs, not
as an active promotion policy.

### Authority Controller

`core/authority/controller.go` supports the failover decision model:

- initial bind when no current authority exists,
- retain current primary when it remains acceptable,
- reassign to another acceptable replica when current primary is not acceptable,
- refuse to mint when no eligible candidate exists.

Tests such as `g8_0_authority_failover_test.go` and
`g9a_reintegration_policy_test.go` pin the core safety rule: a returned or
observed replica is not enough; the candidate must be ready and eligible.

This is the correct substrate for mounted failover, but the K8s product path
must feed it the right RF=2 supportability snapshot and then prove the app path.

### Volume-Side Fencing

`core/host/volume/subscribe.go` is volume-scoped: a volume process receives
authority lines for the whole volume, not just its own replica.

When master publishes a new primary line for another replica, the old replica
records the supersede event. `core/host/volume/projection_bridge.go` uses that
event to fail closed:

```text
Healthy = local engine healthy AND not superseded AND not supporting-replica-ready
```

Impact: stale-primary fencing has a concrete V3 mechanism. The mounted failover
gate should assert it through status/inventory artifacts, not only through logs.

### Replication And ACK Profile

`cmd/blockvolume/main.go` exposes:

- `--replication-ack=best-effort`
- `--replication-ack=sync-quorum`
- `--replication-ack=sync-all`

`core/frontend/durable/storage_adapter.go` defaults to
`WriteAckBestEffort`. In best-effort mode, local durable write success is enough
for foreground write success; observer fan-out errors are logged/degrade peers
but do not fail the write.

`sync-quorum` and `sync-all` switch the durable write path to
`WriteAckRequireObserverAck`, and `core/replication/volume.go` evaluates the
replica acknowledgement profile.

Impact: the mounted failover plan must choose and document the profile. A
best-effort run can prove the exact bytes that survived the injected failure,
but it cannot claim general quorum durability for all acknowledged writes.

### CSI Attach And Reattach

`core/csi/master_backend.go` reads `QueryVolumeStatus` and returns the current
assigned frontend target. It does not control authority, placement, or failover.
It also does not automatically restage an already-mounted workload after a
primary move.

Impact: the MVP should define reattach as a documented restart/restage path
unless and until transparent in-place I/O continuation is proven. The app path
can be:

```text
writer writes and syncs -> primary failure -> authority move or safe refusal
-> app pod is restarted/reattached -> reader verifies data
```

It should not claim zero-disruption I/O.

### Inventory And Support Bundles

`core/ops` already exposes volume/replica/status/support-bundle facts and issue
vocabulary. The recent inventory plans made missing placement, orphan workloads,
unreachable status endpoints, degraded replicas, and non-claims visible.

The mounted failover plan needs additional timeline-oriented evidence:

- before-failure primary replica and authority lineage,
- failure target,
- after-failure primary replica and authority lineage,
- stale/old primary state,
- reattach method,
- ACK profile,
- durable evidence for the bytes verified after reattach,
- explicit safe-refusal issue if promotion is not allowed.

## Gap Summary

| Area | State | Gap |
| --- | --- | --- |
| V2 protocol behavior | Strong reference | Not product-owned K8s wiring |
| V3 authority failover | Substrate exists | Need lifecycle/K8s feed and live proof |
| V3 stale-primary fencing | Mechanism exists | Need user-visible status/inventory proof |
| RF=2 K8s placement | Workload rendering exists | Product loop only binds first slot |
| CSI reattach | Current target lookup exists | No transparent restage claim |
| ACK profile | Flags exist | Plan must choose and gate exact claim |
| Inventory | Strong base | Needs failover timeline and safe-refusal wording |

## Implementation Direction

Do not start by porting V2. Start by composing the V3 pieces through the user
path:

1. Add fast tests for the lifecycle-to-authority RF=2 seam, old-primary
   supersede visibility, and inventory failover timeline wording.
2. Add a runner-native RF=2 iSCSI Kubernetes scenario that writes data through a
   PVC, injects controlled primary failure, then either reattaches and verifies
   data or refuses promotion safely with a specific support-bundle issue.
3. Keep ACK semantics explicit. Prefer a conservative best-effort MVP claim
   unless the scenario runs with `--replication-ack=sync-quorum` and proves that
   profile end to end.
4. Use V2 only for protocol expectations and host-side failover sanity.

## Current Audit Verdict

The current product is close enough to build the mounted failover MVP without an
architecture rewrite, but not close enough to claim it today.

The first real code slice should focus on the product-owned RF=2 lifecycle
bridge and evidence contract, not on frontend protocol parity.
