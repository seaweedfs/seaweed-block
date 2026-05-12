# Current Plan: Cluster Operations Inventory And Lifecycle Visibility MVP

Status: active. Opened after closing
`finished-plans/phase10_finishedplan_light_use_install_lifecycle_operations_mvp.md`.

Current task: D1 operations inventory contract. The first slice is to define
the operator-facing view before adding more CLI or scenario automation.

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
On the supported single-node Kubernetes alpha path, an operator can run a
read-only command to see Seaweed Block volumes, their Kubernetes ownership,
frontend/status endpoints, lifecycle health, residue hints, and support-bundle
pointers.
```

It is not a full operator, repair controller, UI, metrics pipeline, upgrade
story, multi-node scheduling, or mutating admin surface.

## Current Honest State

What already works:

- `sw-block ops status` can produce a useful support bundle for one known
  volume when the caller already has the volume id and status address.
- The first-volume quickstart and TestOps chain produce line-level cleanup
  attribution.
- TestOps can prove happy path, retry, failure bundle, and break-class behavior.
- The generated `blockvolume` manifests include useful identity and endpoint
  flags.

What is still weak:

- A user cannot ask "what Seaweed Block volumes exist?" through one product
  command.
- `sw-block ops status` is not discoverable; it needs volume/status endpoint
  inputs from artifacts or generated YAML.
- Kubernetes ownership, PVC/PV identity, frontend protocol, status endpoint,
  and cleanup residue are scattered across different files and commands.
- There is no compact cluster support bundle for "send me the current state of
  the alpha install."
- The product has no read-only operational API/UI; CLI is the right first step.

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
- Add a product CLI surface for cluster/volume inventory.
- Reuse the existing one-volume `ops status` report where possible.
- Keep the command fail-closed: partial reports are allowed, false OK is not.
- Add fast unit/component tests before integration.
- Add a runner-native scenario that creates at least one real PVC and validates
  inventory output.
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

## Top Blocking Issues

### P0: Inventory Discovery Must Not Depend On Test Artifacts

Today the best evidence is in scenario artifact directories. A real operator
starts from the cluster, not from a successful test run.

Close requirement: the product CLI discovers live alpha resources from
Kubernetes/master/status endpoints and emits an inventory even when no TestOps
artifact path is provided.

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

Minimum row fields:

- `volume_id`,
- `namespace`,
- `pvc_name`,
- `pv_name`,
- `generated_deployment`,
- `protocol`,
- `frontend_address`,
- `status_address`,
- `authority_role`,
- `healthy`,
- `replication_role`,
- `product_revision`,
- `residue`,
- `issues`,
- `unchecked`,
- `collection_errors`.

The contract must state non-claims: read-only observation, not repair; alpha
single-cluster scope; best-effort partial discovery.

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

For each discovered live volume with a status endpoint, call or reuse the
existing `ops status` collector and attach the per-volume report to the
inventory bundle.

Expected behavior:

- healthy live volume: row is `status=ok`;
- unreachable endpoint: row remains present with an issue and collection error;
- missing generated workload: row remains present if PVC/PV identity exists;
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
run ops inventory/list
assert PVC/PV/volume/protocol/status endpoint fields
delete PVC
run inventory/list again
assert cleanup state or documented absence
collect_and_cleanup(always)
```

The scenario should be short enough to run as part of the light-use suite.

### D6: Fast Tests And Review

Use TDD before wiring the live path:

- fixture tests for Kubernetes object parsing,
- fixture tests for generated manifest parsing,
- inventory schema tests,
- partial-error classification tests,
- bundle writer tests.

Use an internal review agent before merge for the schema and CLI semantics.

### D7: QA Close Assignment

Ask QA to validate as an operator, not just as a command executor:

- run the documented first-volume path,
- run the inventory command without looking at TestOps artifacts,
- confirm a stranger can map PVC -> Seaweed Block volume -> frontend/status,
- create one stale/partial state and verify inventory names it clearly,
- verify no false OK when endpoints are unreachable,
- report confusing output or over-claims.

## Gates To Close

This plan closes only when:

1. The inventory schema is documented and covered by fast tests.
2. The product CLI can discover at least the supported alpha resources without
   TestOps artifact inputs.
3. A live first-volume run produces a useful inventory row with PVC/PV,
   generated deployment, protocol, status endpoint, and health fields.
4. A partial/failure state produces actionable issues and collection errors.
5. The command writes a support bundle suitable for issue reports.
6. The quickstart shows how and when an operator runs the command.
7. QA validates the operator experience independently and reports no blocking
   usability issue.

## Success Statement

After this plan, Seaweed Block can make a narrow operations claim:

```text
On the supported single-node Kubernetes alpha path, an operator can discover
the Seaweed Block volumes in the cluster, map them to Kubernetes PVC/PV
objects, see health and endpoint status, and collect a shareable read-only
support bundle without digging through generated YAML or TestOps artifacts.
```

This is the next step from "a first volume works" to "a user can operate and
debug the alpha product."
