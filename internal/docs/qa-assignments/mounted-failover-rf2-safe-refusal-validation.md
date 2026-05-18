# QA Assignment: Mounted Failover RF=2 Safe-Refusal Gate

Status: requested for the active plan `Basic Mounted Failover And Reattach MVP`.

This is not the final plan close gate. It validates the first D4 runner-native
gate: on the current single-node alpha Kubernetes topology, an RF=2 mounted
failover setup must be refused safely and explained by inventory instead of
launching a partial unsafe workload.

## Why QA Is Needed

The local unit tests prove the schema and controller bridge. This assignment
needs the real m02 alpha Kubernetes lab because the question is operational:

```text
If a user asks for RF=2 on the current single-node alpha topology, does the
product avoid unsafe partial placement, and can an operator see why?
```

## Command

Use the current product branch and current `swblock`/testrunner binary.

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/mounted-failover-rf2-safe-refusal `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/mounted-failover-rf2-safe-refusal-chain.yaml
```

Adjust `product_root` only if the checked-out product path on m02 differs.

## Expected Result

The scenario should PASS.

Required phase evidence:

- `preflight` PASS with `summary status=PASS`.
- `pin_build_alpha_images` records `SW_BLOCK_IMAGE_ID` and
  `SW_BLOCK_CSI_IMAGE_ID`.
- `install_alpha_stack` installs the product-owned alpha stack.
- `rf2_safe_refusal_boundary` creates and binds
  `sw-block-rf2-safe-refusal-pvc`, but no `app=sw-blockvolume` Deployment is
  created.
- `inventory_safe_refusal` exits 0 and writes the inventory bundle.
- `inventory_asserts` proves the refusal is visible and actionable.
- `collect_and_cleanup` leaves no active iSCSI sessions, sw-block processes, or
  blockmaster port-forwards.

## Acceptance Checks

In `volume-inventory-summary.txt`, verify:

```text
inventory_status: unhealthy
pvc=sw-block-rf2-safe-refusal-pvc
rf=2 desired=2 observed=0
generated_deployment_missing
observed_replicas=0 desired_replicas=2
replica_slot_missing=unknown
```

In the RF=2 boundary artifacts, verify:

```text
blockvolumes.after-rf2.txt has no sw-blockvolume rows
apply-rf2.log shows persistentvolumeclaim/sw-block-rf2-safe-refusal-pvc created
```

Post-run residue audit:

```text
iscsiadm -m session    # no active sessions
pgrep -af 'blockmaster|blockvolume|blockcsi|iscsi-target'    # no rows
pgrep -af 'kubectl.*port-forward.*svc/blockmaster'           # no rows
```

## Fail Conditions

Fail the assignment if any of these occur:

- The RF=2 PVC creates a `blockvolume` Deployment on the single-node alpha lab.
- Inventory reports `rf=1` for the RF=2 PVC.
- Inventory exits invalid/non-zero due to a broken bundle instead of exiting 0
  with `inventory_status: unhealthy`.
- The bundle lacks `generated_deployment_missing`,
  `observed_replicas=0 desired_replicas=2`, or `replica_slot_missing=unknown`.
- Cleanup leaves an active session, sw-block process, or blockmaster
  port-forward.

## Report Template

```text
QA Report - Mounted Failover RF=2 Safe-Refusal Gate

Product commit:
Runner commit:
Run id:
Result:

Phase table:
- pre_clean:
- preflight:
- pin_build_alpha_images:
- install_alpha_stack:
- rf2_safe_refusal_boundary:
- inventory_safe_refusal:
- inventory_asserts:
- collect_and_cleanup:

Inventory evidence:
- inventory_status:
- PVC:
- rf/desired/observed:
- issue lines:

Residue audit:
- iSCSI sessions:
- sw-block processes:
- blockmaster port-forward:

Verdict:
Blocking findings:
Non-blocking findings:
```
