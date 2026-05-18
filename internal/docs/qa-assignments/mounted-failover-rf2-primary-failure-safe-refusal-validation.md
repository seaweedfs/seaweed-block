# QA Assignment: Mounted Failover RF=2 Primary-Failure Safe-Refusal Gate

Status: requested for the active plan `Basic Mounted Failover And Reattach MVP`.

This is not the final recovery claim. It validates the next cold product
question after the RF=2 mounted app baseline: if the current primary is stopped
while the other replica is not ready, does the product refuse recovery
explicitly instead of pretending failover worked?

## Product Question

```text
After an RF=2 PVC app writes data successfully, can the product identify the
current primary, inject a scoped primary blockvolume stop, and emit a
self-explaining safe-refusal bundle without claiming post-failure data recovery?
```

## Command

Use the current product branch and current `swblock`/testrunner binary.

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/mounted-failover-rf2-primary-safe-refusal `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/mounted-failover-rf2-primary-failure-safe-refusal-chain.yaml
```

Adjust `product_root` only if the checked-out product path on m02 differs.

## Expected Result

The scenario should PASS.

Required evidence:

- Writer verifies `/data/demo.bin` before failure.
- The generated demo manifest uses `replicationFactor: "2"`.
- Before-failure inventory contains `rf=2 desired=2 observed=2` and one
  `role=primary` replica.
- `primary-failure-safe-refusal.txt` contains:

```text
failover_status: refused
ack_profile: best-effort
failure_class=primary-blockvolume-controlled-stop
failed_replica=<rN>
before_primary_replica=<same rN>
candidate_ready=false
candidate_evidence=<non-primary replica line showing not-ready/degraded state>
data_check_after_failover=not_claimed
reason=candidate_not_ready_for_primary
target_ready_replicas=0
after_issue_evidence=<actionable inventory issue>
```

- After-failure inventory is `inventory_status: unhealthy`.
- After-failure inventory has at least one actionable reason such as
  `replica_degraded=...`, `ops_status=unhealthy ...`, or
  `status_endpoint_...`.
- Reader recovery is not claimed; there must be no successful
  `/data/demo.bin: OK` reader line after the failure.
- `failed_replica` must equal `before_primary_replica`; a fallback to a
  hard-coded replica is a failure.
- Cleanup leaves no active iSCSI sessions, sw-block processes, or blockmaster
  port-forwards.

## Fail Conditions

Fail the assignment if any of these occur:

- The scenario falls back to RF=1.
- Writer checksum evidence is missing.
- The failure target is not a scoped `blockvolume` Deployment.
- The bundle claims recovered data after failure without running a reader and
  checksum check.
- The refusal reason is generic or missing.
- The candidate readiness line is a scripted constant without matching
  inventory evidence.
- The scaled primary does not reach `target_ready_replicas=0`.
- Inventory is missing before/after evidence.
- Cleanup leaves an active session, sw-block process, or blockmaster
  port-forward.

## Report Template

```text
QA Report - Mounted Failover RF=2 Primary-Failure Safe-Refusal Gate

Product commit:
Runner commit:
Run id:
Result:

Phase table:
- pre_clean:
- preflight:
- pin_build_alpha_images:
- render_rf2_demo_manifest:
- rf2_primary_failure_safe_refusal:
- safe_refusal_asserts:
- collect_and_cleanup:

Failure/refusal evidence:
- writer checksum:
- before-failure primary:
- failure_class / failed_replica:
- before_primary_replica matches failed_replica:
- candidate_ready:
- candidate_evidence:
- data_check_after_failover:
- refusal reason:
- target_ready_replicas:
- after-failure inventory status:
- actionable issue lines:
- reader recovery claim absent:

Residue audit:
- iSCSI sessions:
- sw-block processes:
- blockmaster port-forward:

Verdict:
Blocking findings:
Non-blocking findings:
QA needed next:
```
