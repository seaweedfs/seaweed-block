# QA Assignment: Product-Owned Blockvolume Lifecycle MVP Close Gate

Status: ready for QA close validation.

Product branch/commit for dev evidence: `docs/post-merge-plan` at
`89cfaec` or newer.

Dev live gate evidence:

- Scenario: `testops/scenarios/cluster-ops-inventory-chain.yaml`
- Run id: `20260512-202811-1aa7`
- Bundle: `V:/share/g15d-k8s/testops-runs/lifecycle-gate/20260512-202811-1aa7`
- Result: PASS, 9/9 phases, 73/73 actions

## Product Question

Can an early Kubernetes user create and delete Seaweed Block PVCs without
running a separate manifest-apply script, while still getting observable
inventory and cleanup evidence?

## QA Scope

Validate the user-facing lifecycle path, not the implementation internals.

The supported path is:

```text
preflight -> install alpha stack -> create PVC -> product-owned blockvolume
reconcile -> app I/O -> inventory -> delete one PVC -> inventory proves scoped
cleanup
```

## Required Environment

- m02 single-node k3s lab, or equivalent single-node Kubernetes lab.
- Built `swblock`/`sw-block` tooling available.
- Product checkout at the target commit.
- No pre-existing Seaweed Block iSCSI sessions, blockmaster/blockvolume/blockcsi
  processes, or `app=sw-blockvolume` Deployments before the run.

## HG-0: Documentation Entry

Pass:

- `docs/overview.md` links `docs/operations-v1.md`.
- `docs/quickstart-kubernetes.md` links `docs/operations-v1.md`.
- A user can find the lifecycle path without reading internal docs.

Fail:

- The primary user path is only discoverable from `internal/docs`.

## HG-1: Operations Manual Follows Research Ladder

Pass:

- `docs/operations-v1.md` contains the ladder:
  preflight, image build/select, install, readiness checks, first PVC/I/O,
  inventory, delete/cleanup, failure collection, retry, uninstall.
- Non-claims are explicit: no production HA, no live RF=2/RF=3 Kubernetes
  lifecycle, no multi-node scheduling, no upgrade/uninstall safety, no repair,
  no UI.

Fail:

- The manual skips install/readiness/inventory/cleanup, or over-claims product
  readiness.

## HG-2: No Manual Apply In Primary Happy Path

Pass:

- Running `scripts/run-k8s-demo.sh` with default env does not invoke
  `scripts/apply-k8s-alpha-blockvolumes.sh`.
- Demo artifact `apply-generated-blockvolume.log` states product-owned
  lifecycle reconciliation.
- `SW_BLOCK_DEMO_MANUAL_APPLY_BLOCKVOLUMES=1` remains only as an explicit
  fallback.

Fail:

- The normal path still requires or silently invokes the apply helper.

## HG-3: Product-Owned Reconcile Creates Workloads

Pass:

- After a PVC is created, the generated `blockvolume` Deployment appears and
  becomes Ready without manual `kubectl apply -f generated-blockvolume.yaml`.
- For two PVCs on one node, both generated Deployments become Ready.
- The two Deployments have distinct volume IDs, status ports, and iSCSI ports.

Fail:

- Only one PVC reconciles, host ports collide, or the second Deployment is
  missing.

## HG-4: Inventory Names Lifecycle Ownership

Pass:

- `sw-block ops inventory` summary and JSON include `lifecycle_owner` and
  `owner_reference` for each observed replica.
- For the supported alpha path, owner evidence is:

```text
lifecycle_owner=pvc-owner-ref
owner_ref=PersistentVolumeClaim/default/<pvc-name>
```

Fail:

- Inventory shows a live generated workload but cannot say whether it is
  PVC-owned or launcher-managed.

## HG-5: Inventory Still Proves Multi-Volume Shape

Pass:

- Inventory shows two volume rows for two PVCs.
- Each row has:
  volume ID, PVC name, PV name, desired/observed replicas, generated
  Deployment, frontend/status endpoint, support bundle.
- No row inherits ports or owner reference from the other.

Fail:

- Rows collapse, duplicate, cross-contaminate identity, or hide one PVC.

## HG-6: Scoped Delete

Pass:

- Delete `sw-block-demo-pvc-2`.
- Inventory after delete shows exactly one remaining volume row for
  `sw-block-demo-pvc`.
- Inventory after delete does not show:

```text
pvc=sw-block-demo-pvc-2
owner_ref=PersistentVolumeClaim/default/sw-block-demo-pvc-2
generated_deployment_missing
orphan-blockvolume-deploy
```

Fail:

- Deleting one PVC removes or corrupts the other, or leaves an unexplained
  orphan/missing workload.

## HG-7: Read-Only Inventory And Support Bundles

Pass:

- Inventory command exits 0 for trustworthy ok/unhealthy state.
- Nested per-replica `ops-status-bundle.json` files exist for observed
  replicas when `--master` is provided.
- Inventory remains read-only: no PVC, Deployment, iSCSI session, or process
  changes attributable to the inventory command itself.

Fail:

- Inventory mutates state, returns false OK on broken evidence, or cannot write
  nested bundles.

## HG-8: Cleanup Hygiene

Pass after close validation:

- No active iSCSI sessions with `io.seaweedfs`.
- No `blockmaster`, `blockvolume`, `blockcsi`, or `iscsi-target` processes.
- No `kubectl port-forward svc/blockmaster` process.
- No `app=sw-blockvolume` Deployment remains unless intentionally left by the
  test and named in the report.

Fail:

- Residue remains without a documented non-claim or cleanup attribution.

## HG-9: Close Report

QA should write:

```text
internal/docs/qa-assignments/product-owned-blockvolume-lifecycle-mvp-close-report.md
```

The report must include:

- product commit,
- runner commit or binary source,
- run IDs and bundle paths,
- HG-0 through HG-8 PASS/FAIL table,
- residue audit,
- blocking findings,
- non-blocking findings.

## Recommended Commands

Runner-native gate:

```powershell
swblock run `
  -results-dir V:/share/g15d-k8s/testops-runs/lifecycle-gate `
  -env product_root=/tmp/seaweed-block-lifecycle-devrun `
  -env ssh_key=C:/work/dev_server/testdev_key `
  testops/scenarios/cluster-ops-inventory-chain.yaml
```

Manual cold-read:

```bash
bash scripts/preflight-k8s-alpha.sh --local-k3s
bash scripts/install-k8s-alpha.sh "$PWD"
bash scripts/run-k8s-demo.sh "$PWD"
```

Inventory:

```bash
kubectl -n kube-system port-forward svc/blockmaster 9333:9333
sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out /tmp/sw-block-inventory
cat /tmp/sw-block-inventory/volume-inventory-summary.txt
```

## Close Rule

All HG clauses must PASS. Any strict FAIL blocks moving the current plan to
`finished-plans/`.
