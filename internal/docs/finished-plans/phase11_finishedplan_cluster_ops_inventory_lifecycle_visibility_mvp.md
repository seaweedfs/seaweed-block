# Finished Plan: Cluster Operations Inventory And Lifecycle Visibility MVP

Status: closed. Opened after closing
`finished-plans/phase10_finishedplan_light_use_install_lifecycle_operations_mvp.md`.
Closed after strict QA PASS in
`internal/docs/qa-assignments/cluster-ops-inventory-mvp-close-report.md`.

Final state: D1 defined the multi-volume/RF-
aware inventory contract and summary shape. D2 added the operator-facing
command and live Kubernetes discovery. D3 reuses the existing one-volume
`ops status` collector from the inventory command when a replica exposes a
status endpoint. D4 added the user-facing quickstart inspection flow. D5 is now
strict PASS: QA run `20260512-162943-77fe` at product commit `2e521b3` proved
two concurrent live PVCs on the alpha k3s path, with distinct volume IDs,
generated Deployments, iSCSI frontend ports, status endpoint ports, and nested
support bundles. D6 fast RF/missing-replica fixtures are in place and use the
same machine-readable issue vocabulary as the QA hard gate. The current cleanup
slice tightens HG-10 residue labels, HG-14 non-claim labels, and the HG-6/HG-7
degraded-replica wording so the close report can validate them mechanically.
The live inventory chain rejects stale Deployment-only status in rows
classified as `ok`; OK rows must be backed by nested product status evidence.
QA close report `cluster-ops-inventory-mvp-close-report.md` passed HG-0 through
HG-14 and the five required break fixtures at product commit `c662bc7`.

QA slice assignment for D5 live validation:
`internal/docs/qa-assignments/cluster-ops-inventory-chain-validation.md`.

## Product Question

Can an early developer or operator look at a running Seaweed Block alpha cluster
and answer the basic lifecycle questions without reading generated manifests,
pod logs, or TestOps artifacts?

The previous plan proved the first-volume user loop:

```text
install/launch -> create PVC -> app write/read -> delete -> cleanup evidence
```

This plan moves the operations layer from one known volume to cluster-visible
inventory:

```text
list volumes -> map PVC/PV/frontends -> show lifecycle/health -> point to a
support bundle -> identify stale or missing cleanup
```

If this plan closes, the claim is still narrow:

```text
On the supported alpha Kubernetes path, an operator can run a read-only command
to see Seaweed Block volumes and replicas, their Kubernetes ownership,
frontend/status endpoints, lifecycle health, residue hints, and support-bundle
pointers. The first live gate may use single-node k3s, but the inventory schema
and tests cover multiple volumes and RF=1/2/3 shapes.
```

It is not a full operator, repair controller, UI, metrics pipeline, upgrade
story, multi-node scheduling, or mutating admin surface.

## Current Honest State

What already works:

- `sw-block ops status` can produce a useful support bundle for one known
  volume when the caller already has the volume id and status address.
- `sw-block ops inventory` can discover supported alpha Kubernetes resources
  without TestOps artifact paths and emit a cluster inventory bundle.
- The first-volume quickstart and TestOps chain produce line-level cleanup
  attribution.
- TestOps can prove happy path, retry, failure bundle, and break-class behavior.
- The generated `blockvolume` manifests include useful identity and endpoint
  flags.
- The live D5 gate proves two concurrent PVCs on the alpha k3s path without
  cross-volume identity or port collision.
- Fast fixtures cover RF=1/RF=2/RF=3, missing replica slots, orphan PVCs, and
  Kubernetes-visible orphan blockvolume Deployments.

What is still weak:

- The close report still needs a QA-owned cold run against the full hard gate.
- Read-only behavior needs the HG-12 repeated-run proof at close time.
- Per-replica nested bundle reuse needs the HG-11 normalized comparison at
  close time.
- Pure standalone master heartbeat listing would still need a master
  observation-list API, but the inventory now covers the close-gate practical
  fixture: a host-local `blockvolume` process without PVC/PV placement is
  reported as `blockvolume-process-without-placement` plus
  `heartbeat-without-placement`.
- The product still has no API/UI; CLI is the right first step for this plan.

## Master / RF Reality Check

Current code already has dynamic process registration, but not open-ended
placement.

- A `blockvolume` joins a running master by starting with `--master`,
  `--server-id`, `--volume-id`, `--replica-id`, `--data-addr`, and
  `--ctrl-addr`; it heartbeats through `ReportHeartbeat` and subscribes to
  assignments through `SubscribeAssignments`.
- Master accepts heartbeat observations, but heartbeat alone is not authority.
  The accepted slot set comes from `--topology`, `--cluster-spec`, or lifecycle
  placement stores. This is intentional safety: observed process does not mean
  safe placement.
- `SubscribeAssignments` is volume-scoped. If the volume is not in accepted
  topology or lifecycle placement, the master refuses the stream instead of
  silently assigning it.
- RF=2 operation is already represented by tests and smokes: join lifecycle,
  ALUA/multipath, mounted failover, and replicated write paths all exercise
  two-slot behavior. The alpha Kubernetes first-volume path still defaults to
  RF=1.
- Therefore this plan must not build a one-volume/single-replica view. The live
  usability gate can start with single-node execution, but the contract, tests,
  and summaries must represent multiple volumes and RF=1/2/3.

## Product Value

This plan affects user experience directly. After it, an operator should be
able to run a small number of commands:

```text
sw-block ops list ...
sw-block ops inspect --volume <id> ...
sw-block ops bundle ...
```

and understand:

- which PVC/PV maps to which Seaweed Block volume,
- whether the generated `blockvolume` workload exists,
- which protocol and frontend/status address it exposes,
- whether authority/replication/frontend status is healthy,
- whether cleanup left active residue,
- where the shareable support bundle lives.

## Scope

In scope:

- Define a stable read-only inventory schema.
- Make the schema multi-volume and replica-aware.
- Add a product CLI surface for cluster/volume inventory.
- Reuse the existing one-volume `ops status` report where possible.
- Keep the command fail-closed: partial reports are allowed, false OK is not.
- Add fast unit/component tests before integration.
- Add a runner-native scenario that creates at least two real PVCs where the
  alpha path supports it and validates inventory output without cross-volume
  confusion.
- Add a QA assignment focused on new-user/operator usability and adversarial
  stale-state cases.

Out of scope:

- Mutating repair commands.
- Automatic cleanup.
- Full Kubernetes operator reconciliation.
- Web UI.
- Prometheus metrics.
- Multi-node scheduler or placement policy.
- Upgrade/uninstall safety.
- Claiming live RF=2/RF=3 Kubernetes lifecycle until a runner gate proves that
  path; RF=2/RF=3 inventory shapes are still required in contract tests.

## Top Blocking Issues

### P0: Inventory Discovery Must Not Depend On Test Artifacts

Today the best evidence is in scenario artifact directories. A real operator
starts from the cluster, not from a successful test run.

Close requirement: the product CLI discovers live alpha resources from
Kubernetes/master/status endpoints and emits an inventory even when no TestOps
artifact path is provided.

### P0: Inventory Must Be Multi-Volume And Replica-Aware

Single-node execution is not the same as a single-volume model. Operators need
to distinguish volumes, replicas, expected RF, observed RF, and primary/replica
health.

Close requirement: the inventory contract supports at least RF=1, RF=2, and
RF=3 fixture shapes, reports multiple volumes in one bundle, and marks missing
or stale replicas without collapsing them into one volume-level status.

### P0: Volume Identity Must Be Human-Mappable

Volume id alone is not enough. Users think in PVC namespace/name, PV name, app
pod, and generated workload.

Close requirement: inventory rows include Kubernetes owner identity when
available: namespace, PVC, PV, generated deployment, protocol, and endpoint
addresses.

### P0: Partial Failure Must Be Actionable

Inventory will often run while a PVC is half-created, a pod is crashlooping, or
a status endpoint is unreachable.

Close requirement: partial rows carry `status`, `issues`, `unchecked`, and
`collection_errors`; the command exits non-zero only when the command itself
cannot produce a trustworthy report.

### P1: Keep Long Integration Small

Most inventory logic should be tested with fixtures. The hardware/k3s gate
should only prove the real Kubernetes/resource discovery boundary.

Close requirement: schema parsing, issue classification, and bundle writing
have fast tests; the TestOps scenario is a final user-path gate.

## Deliverables

### D1: Operations Inventory Contract

Define `volume-inventory.json` and a human summary format.
Reference: `internal/docs/ref/operations-volume-inventory-contract.md`.

Minimum volume fields:

- `volume_id`,
- `namespace`,
- `pvc_name`,
- `pv_name`,
- `replication_factor`,
- `desired_replicas`,
- `observed_replicas`,
- `primary_replica_id`,
- `protocols`,
- `product_revision`,
- `status`,
- `residue`,
- `issues`,
- `unchecked`,
- `collection_errors`,
- `support_bundle`,
- `replicas`.

Minimum replica fields:

- `replica_id`,
- `server_id`,
- `node_name`,
- `generated_deployment`,
- `protocol`,
- `frontend_address`,
- `status_address`,
- `support_bundle`,
- `data_addr`,
- `ctrl_addr`,
- `observed`,
- `status`,
- `authority_role`,
- `healthy`,
- `frontend_primary_ready`,
- `replication_role`,
- `epoch`,
- `endpoint_version`,
- `residue`,
- `issues`,
- `collection_errors`.

The contract must state non-claims: read-only observation, not repair; alpha
single-cluster scope; best-effort partial discovery; RF=2/RF=3 Kubernetes live
operation only when a live gate explicitly proves it.

### D2: Product CLI Surface

Add the first discoverable command shape, expected to be one of:

```text
sw-block ops list --namespace <ns> --out <dir>
sw-block ops inventory --namespace <ns> --out <dir>
```

The exact name can change during implementation, but the user experience must
stay simple:

- default to the alpha namespace/resource labels where possible,
- print a compact table or summary to stdout,
- write JSON and bundle metadata to `--out`,
- do not require TestOps-specific paths.

### D3: Status Reuse / Per-Volume Inspect

For each discovered live replica with a status endpoint, call or reuse the
existing `ops status` collector and attach the per-replica report to the
inventory bundle. Aggregate those reports into a volume-level view.

Expected behavior:

- healthy live volume: volume row is `status=ok` and replicas show their roles;
- unreachable replica endpoint: replica remains present with an issue and
  collection error;
- missing generated workload: volume remains present if PVC/PV identity exists;
- missing or stale RF slot: volume marks `observed_replicas < desired_replicas`
  and names the affected replica;
- stale residue: row marks residue without claiming product cleanup.

### D4: Runbook Update

Update `docs/quickstart-kubernetes.md` with a short operator section:

```text
After the demo starts, run:
  sw-block ops list ...

If something fails, attach:
  <inventory bundle dir>
  <per-volume ops status bundle>
```

The docs must show expected output for:

- normal one-volume demo,
- after PVC delete,
- one partial/failure state.

### D5: Runner-Native Inventory Gate

Add a TestOps scenario that:

```text
pre_clean
pin/build alpha
run first-volume demo to live volume boundary
create a second PVC/volume when supported by the alpha path
run ops inventory/list
assert at least two volume rows where supported
assert PVC/PV/volume/protocol/status endpoint fields
assert per-replica fields and desired/observed replica counts
delete PVC
run inventory/list again
assert cleanup state or documented absence
collect_and_cleanup(always)
```

The scenario should be short enough to run as part of the light-use suite.
If RF=2 live Kubernetes creation is not yet reliable in the alpha path, keep the
live scenario RF=1 multi-volume and require RF=2/RF=3 contract/component gates
in D6. Do not imply RF=2/RF=3 live support until a runner gate proves it.

### D6: Fast Tests And Review

Use TDD before wiring the live path:

- fixture tests for Kubernetes object parsing,
- fixture tests for generated manifest parsing,
- fixture tests for multiple volumes in one inventory,
- fixture tests for RF=1, RF=2, and RF=3 volume shapes,
- fixture tests for missing/stale replica slots,
- inventory schema tests,
- partial-error classification tests,
- bundle writer tests.

Use an internal review agent before merge for the schema and CLI semantics.

### D7: QA Close Assignment

Ask QA to validate as an operator, not just as a command executor:

- run the documented first-volume path,
- run the inventory command without looking at TestOps artifacts,
- confirm a stranger can map PVC -> Seaweed Block volume -> frontend/status,
- confirm two volumes do not collapse into one row or cross-link ownership,
- confirm RF-aware output is honest: either live RF=2 is proven or RF=2/RF=3
  remain contract-tested non-claims,
- create one stale/partial state and verify inventory names it clearly,
- verify no false OK when endpoints are unreachable,
- report confusing output or over-claims.

## Gates To Close

This plan closes only when:

1. The inventory schema is documented and covered by fast tests.
2. The product CLI can discover at least the supported alpha resources without
   TestOps artifact inputs.
3. Fast tests cover multiple volumes, RF=1, RF=2, RF=3, and missing/stale
   replica slots.
4. A live run produces useful inventory rows with PVC/PV, generated deployment,
   protocol, status endpoint, replica count, and health fields. The preferred
   gate has at least two volumes; if the current alpha path cannot create two
   volumes reliably, that limitation must be explicit.
5. RF=2/RF=3 are not claimed as live Kubernetes operation unless a runner gate
   proves them.
6. A partial/failure state produces actionable issues and collection errors.
7. The command writes a support bundle suitable for issue reports.
8. The quickstart shows how and when an operator runs the command.
9. QA validates the operator experience independently and reports no blocking
   usability issue.

## Success Statement

After this plan, Seaweed Block can make a narrow operations claim:

```text
On the supported alpha Kubernetes path, an operator can discover Seaweed Block
volumes and replicas in the cluster, map them to Kubernetes PVC/PV objects, see
per-volume and per-replica health and endpoint status, and collect a shareable
read-only support bundle without digging through generated YAML or TestOps
artifacts.
```

This is the next step from "a first volume works" to "a user can operate and
debug the alpha product." It is not a claim of open node auto-placement:
new `blockvolume` processes can dynamically register with master, but master
only operates on replicas that are admitted by topology, cluster-spec, or
lifecycle placement.
