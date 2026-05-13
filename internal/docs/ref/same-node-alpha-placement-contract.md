# Same-Node Alpha Placement Contract

Date: 2026-05-12

Plan: Multi-Node Attach And Placement MVP

## Contract

The current alpha Kubernetes attach model is:

```text
RF=1 iSCSI PVC -> one generated blockvolume Deployment -> app pod attaches on
the same Kubernetes node through a loopback iSCSI frontend
```

This contract is intentionally narrower than remote-node attach. It is the
only honest model while generated blockvolumes publish frontend endpoints as
`127.0.0.1:<port>`.

## Supported User Experience

A user can:

- install the alpha stack,
- create one or more RF=1 PVCs,
- let blockmaster reconcile generated blockvolume Deployments,
- run an app pod on the same node as the generated blockvolume,
- write/read through the normal CSI path,
- run `sw-block ops inventory` and see where the PVC is backed.

The inventory/support bundle must let a reader answer:

- which PVC/PV owns the volume,
- which Kubernetes node hosts the blockvolume,
- which server ID the blockvolume reports,
- which frontend endpoint was published,
- which status endpoint was used for nested `sw-block ops status`,
- whether desired RF equals observed replicas,
- where the per-replica support bundle lives,
- whether the attach model is same-node loopback or an unsupported placement.

## Required Fields

Inventory summary and JSON should expose these facts for each live RF=1
blockvolume. Summary lines may use short aliases; JSON should use the canonical
inventory contract field names:

- `namespace`
- `pvc` summary alias for JSON `pvc_name`
- `pv` summary alias for JSON `pv_name`
- `volume_id`
- `rf` summary alias for JSON `replication_factor`
- `desired` summary alias for JSON `desired_replicas`
- `observed` summary alias for JSON `observed_replicas`
- `replica_id`
- `server`
- `node`
- `frontend`
- `status_addr`
- `lifecycle_owner`
- `owner_ref`
- `support_bundle`

For the same-node attach proof, the scenario must also capture the app pod node
for writer and reader pods and compare it with the blockvolume node.

## Endpoint Rules

Current supported endpoint rule:

```text
frontend is loopback -> app pod must schedule to the blockvolume node
```

Loopback frontends include:

- `127.0.0.1:<port>`
- `[::1]:<port>`
- `localhost:<port>`

If a frontend is loopback and the app pod is on a different Kubernetes node from
the blockvolume, the product/test bundle must not describe the placement as
supported. The issue should use a stable class such as:

```text
unsupported_cross_node_loopback_attach
```

with these facts:

- `app_node=<node>`
- `blockvolume_node=<node>`
- `frontend=<addr>`
- `volume_id=<id>`
- `replica_id=<id>`

Future remote-node attach requires a routable frontend strategy, for example:

- host-IP iSCSI frontend publishing,
- Service-backed iSCSI endpoint,
- node-local proxy,
- in-cluster attach helper.

That is future work and is not claimed by this contract.

## Placement Rules

Generated blockvolume workloads:

- must carry `nodeSelector: kubernetes.io/hostname=<replica.ServerID>`,
- must expose the selected node/server in inventory,
- must allocate node-local frontend/status ports without collision,
- must remain product-owned and PVC-owned when owner refs are enabled.

App workload for this contract:

- must either be explicitly scheduled to the same node, or
- must be verified after scheduling and fail with an actionable bundle if it
  lands on a different node while the frontend is loopback.

The preferred user-facing path is explicit scheduling. A hidden lucky schedule
is not a product claim.

Current script controls:

- `SW_BLOCK_ALPHA_NODE_NAME=<node>` selects the node used to render the
  blockmaster cluster spec and generated blockvolume placement input. If unset,
  the alpha scripts use the first Kubernetes node, matching the historical
  single-node path.
- `SW_BLOCK_DEMO_APP_NODE_NAME=<node>` selects the writer/reader app node. If
  unset, it defaults to `SW_BLOCK_ALPHA_NODE_NAME`.
- `SW_BLOCK_DEMO_PIN_APP_NODE=1` is the default. It renders the demo writer and
  reader pods with `nodeSelector: kubernetes.io/hostname=<app node>`.
- `SW_BLOCK_DEMO_PIN_APP_NODE=0` is reserved for negative fixtures and custom
  scheduling tests; it is not the supported happy path.

## Negative Fixture

The negative fixture should deliberately create one unsupported placement:

```text
blockvolume node != app node AND frontend is loopback
```

Expected result:

- no broad timeout-only failure,
- inventory/support bundle is still collected,
- the issue class includes `unsupported_cross_node_loopback_attach`,
- residue cleanup covers every participating node.

If the current lab has only one Kubernetes node, the negative fixture can be a
component/fixture test until a second node is available. The live gate must not
claim two-node behavior without a two-node lab.

## Non-Claims

This contract does not claim:

- remote-node attach,
- app rescheduling across nodes while the PVC remains mounted,
- blockvolume rescheduling,
- node loss or host loss survival,
- RF=2/RF=3 live Kubernetes lifecycle,
- failover while mounted,
- rebuild or returned-replica reintegration,
- performance SLOs,
- upgrade or broad uninstall safety,
- UI or production operator behavior.

## QA Checkpoint

QA is needed after:

- fast tests pin renderer/inventory/scheduling evidence, and
- a runner-native same-node gate exists, or
- a true two-node lab is available for the stricter live gate.

QA is not needed for this contract-only slice.
