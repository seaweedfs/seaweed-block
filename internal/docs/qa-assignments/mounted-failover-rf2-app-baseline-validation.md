# QA Assignment: Mounted Failover RF=2 App-Path Baseline Gate

Status: requested for the active plan `Basic Mounted Failover And Reattach MVP`.

This is not the final mounted failover close gate. It validates the missing
baseline before primary-failure injection: an RF=2 PVC can run the normal
writer -> reader app path with two generated `blockvolume` replicas and an
inventory bundle after reader verification.

## Product Question

```text
Before injecting failover, can the documented Kubernetes app/PVC path write and
read data through an RF=2 Seaweed Block volume while inventory sees two
replicas?
```

## Command

Use the current product branch and current `swblock`/testrunner binary.

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/mounted-failover-rf2-app-baseline `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/mounted-failover-rf2-app-baseline-chain.yaml
```

Adjust `product_root` only if the checked-out product path on m02 differs.

## Expected Result

The scenario should PASS.

Required evidence:

- The generated demo manifest has `replicationFactor: "2"`.
- The demo logs `logical_servers=2` and `expected_slots_per_volume=2`.
- Writer verifies `/data/demo.bin`.
- Reader verifies the same `/data/demo.bin` after writer deletion and reader
  attach.
- Generated blockvolume manifest contains both `--replica-id=r1` and
  `--replica-id=r2`.
- Inventory after reader verification contains:

```text
pvc=sw-block-demo-pvc
rf=2 desired=2 observed=2
```

- Nested per-replica `sw-block ops status` bundles are collected.
- Cleanup leaves no active iSCSI sessions, sw-block processes, or blockmaster
  port-forwards.

## Fail Conditions

Fail the assignment if any of these occur:

- The demo falls back to RF=1.
- Only one blockvolume replica is generated.
- Writer or reader checksum evidence is missing.
- Inventory does not show `rf=2 desired=2 observed=2`.
- The bundle lacks nested per-replica ops-status evidence.
- Cleanup leaves an active session, sw-block process, or blockmaster
  port-forward.

## Report Template

```text
QA Report - Mounted Failover RF=2 App-Path Baseline Gate

Product commit:
Runner commit:
Run id:
Result:

Phase table:
- pre_clean:
- preflight:
- pin_build_alpha_images:
- render_rf2_demo_manifest:
- rf2_app_writer_reader:
- app_path_asserts:
- collect_and_cleanup:

App-path evidence:
- replicationFactor:
- logical_servers / expected_slots:
- writer checksum:
- reader checksum:
- generated replicas:
- inventory rf/desired/observed:
- nested bundle count:

Residue audit:
- iSCSI sessions:
- sw-block processes:
- blockmaster port-forward:

Verdict:
Blocking findings:
Non-blocking findings:
```
