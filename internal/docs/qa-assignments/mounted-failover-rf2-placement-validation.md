# QA Assignment: Mounted Failover RF=2 Placement Gate

Status: requested for the active plan `Basic Mounted Failover And Reattach MVP`.

This is not the final mounted failover close gate. It validates the next D4
stepping stone: the alpha stack can opt into two logical Seaweed Block server
identities on one Kubernetes node, create an RF=2 PVC, render two distinct
`blockvolume` Deployments, and expose both replicas in inventory.

## Command

Use the current product branch and current `swblock`/testrunner binary.

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/mounted-failover-rf2-placement `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/mounted-failover-rf2-placement-chain.yaml
```

Adjust `product_root` only if the checked-out product path on m02 differs.

## Expected Result

The scenario should PASS.

Required evidence:

- Install log shows `logical_servers=2` and `expected_slots_per_volume=2`.
- Rendered block stack includes `server_id: m02-r1`, `server_id: m02-r2`, and
  `--expected-slots-per-volume=2`.
- RF=2 PVC `sw-block-rf2-placement-pvc` binds.
- Exactly two `app=sw-blockvolume` Deployments become available.
- Inventory exits 0 and writes the inventory bundle.
- Inventory summary contains the RF=2 row with `rf=2 desired=2 observed=2`.
- Replica rows preserve distinct logical server IDs while scheduling to m02:
  `replica=r1 server=m02-r1 node=m02` and
  `replica=r2 server=m02-r2 node=m02`.
- Nested per-replica `sw-block ops status` bundles are collected.
- Cleanup leaves no active iSCSI sessions, sw-block processes, or blockmaster
  port-forwards.

## Acceptance Checks

In `volume-inventory-summary.txt`, verify:

```text
pvc=sw-block-rf2-placement-pvc
rf=2 desired=2 observed=2
replica: ... replica=r1 ... server=m02-r1 node=m02
replica: ... replica=r2 ... server=m02-r2 node=m02
```

Also verify the summary does **not** contain:

```text
generated_deployment_missing
observed_replicas=0 desired_replicas=2
replica_slot_missing=unknown
```

## Fail Conditions

Fail the assignment if any of these occur:

- The installer silently ignores `SW_BLOCK_ALPHA_LOGICAL_SERVERS=2`.
- Only one `blockvolume` Deployment is rendered or becomes available.
- Either replica row collapses `server_id` to `m02` instead of preserving
  `m02-r1` / `m02-r2`.
- Inventory reports `rf=1` or `observed=0`.
- Inventory exits invalid/non-zero due to a broken bundle.
- Cleanup leaves an active session, sw-block process, or blockmaster
  port-forward.

## Report Template

```text
QA Report - Mounted Failover RF=2 Placement Gate

Product commit:
Runner commit:
Run id:
Result:

Phase table:
- pre_clean:
- preflight:
- pin_build_alpha_images:
- install_rf2_alpha_stack:
- rf2_placement_boundary:
- inventory_rf2_placement:
- inventory_asserts:
- collect_and_cleanup:

Placement evidence:
- logical_servers:
- expected_slots_per_volume:
- deployment count:
- inventory rf/desired/observed:
- r1 row:
- r2 row:
- nested bundle count:

Residue audit:
- iSCSI sessions:
- sw-block processes:
- blockmaster port-forward:

Verdict:
Blocking findings:
Non-blocking findings:
```
