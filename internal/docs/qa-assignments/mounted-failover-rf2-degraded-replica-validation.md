# QA Assignment: Mounted Failover RF=2 Degraded Replica Gate

Status: requested for the active plan `Basic Mounted Failover And Reattach MVP`.

This is not the final mounted failover close gate. It validates a D5 negative
fixture: after RF=2 placement succeeds, one generated `blockvolume` replica is
scaled to zero. Inventory must report that replica as degraded and must not
turn the state into a healthy recovery-looking claim.

## Product Question

```text
If one RF=2 replica workload exists but is unavailable, does the product keep
the RF=2 topology visible while refusing to call the volume healthy?
```

## Command

Use the current product branch and current `swblock`/testrunner binary.

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/mounted-failover-rf2-degraded-replica `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/mounted-failover-rf2-degraded-replica-chain.yaml
```

Adjust `product_root` only if the checked-out product path on m02 differs.

## Expected Result

The scenario should PASS.

Required evidence:

- Install log shows `logical_servers=2` and `expected_slots_per_volume=2`.
- RF=2 PVC `sw-block-rf2-degraded-pvc` binds.
- Two `app=sw-blockvolume` Deployments become available before degradation.
- The scenario scales the `r2` Deployment to zero and records its name in
  `rf2/degraded-deployment.txt`.
- Inventory exits 0 and writes the inventory bundle.
- Inventory summary keeps the topology visible:

```text
pvc=sw-block-rf2-degraded-pvc
rf=2 desired=2 observed=2
```

- Inventory summary reports `r2` as degraded/unhealthy:

```text
replica: ... replica=r2 ... status=unhealthy
replica_degraded=r2 status=unhealthy
ops_status=unhealthy reason=replication_role_not_ready
```

- Inventory summary does **not** claim a missing placement:

```text
generated_deployment_missing
observed_replicas=0 desired_replicas=2
replica_slot_missing=unknown
```

Those strings must be absent because the workload exists; it is degraded, not
missing.

## Fail Conditions

Fail the assignment if any of these occur:

- The RF=2 placement never creates two Deployments before the negative action.
- Scaling `r2` to zero is not recorded.
- Inventory reports `status=ok` for the degraded replica.
- Inventory collapses the topology to `observed=0` or `rf=1`.
- Inventory emits `generated_deployment_missing` for the scaled-down workload.
- The bundle lacks `replica_degraded=r2 status=unhealthy`.
- The bundle lacks
  `ops_status=unhealthy reason=replication_role_not_ready` or another explicit
  non-ready reason from the nested `sw-block ops status` bundle.
- Cleanup leaves an active session, sw-block process, or blockmaster
  port-forward.

## Report Template

```text
QA Report - Mounted Failover RF=2 Degraded Replica Gate

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
- degrade_one_replica:
- inventory_after_degraded_replica:
- inventory_asserts:
- collect_and_cleanup:

Degraded replica evidence:
- degraded deployment:
- inventory rf/desired/observed:
- r1 row:
- r2 row:
- issue lines:
- nested bundle count:

Residue audit:
- iSCSI sessions:
- sw-block processes:
- blockmaster port-forward:

Verdict:
Blocking findings:
Non-blocking findings:
```
