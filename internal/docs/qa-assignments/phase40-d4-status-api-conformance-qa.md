# Phase 40 D4 QA: Operator-Status API Conformance Gate

## Goal

Validate that operator-status payloads fail fast against the real CRD schema and
the status/events-only RBAC boundary before live release QA.

This is a regression gate for the live-only defects found in Phases 35-39:

- snake_case payload drift,
- unsupported condition/action enum values,
- wrong CRD endpoint usage,
- RBAC boundary broadening,
- delete-safety status regressions.

## Scope

In:

- local scripted gate,
- TestOps runner gate,
- CRD schema and RBAC-equivalent validation,
- Helm render validation for operator-status write mode.

Out:

- live Helm install,
- CRD finalizer mutation,
- storage/workload/host mutation,
- automatic cleanup,
- upgrade execution.

## Minimal Local Gate

From the product repo:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-phase40-status-api-conformance.ps1
```

Expected artifact:

```text
results/phase40-status-api-conformance-*/phase40-status-api-conformance-summary.txt
```

Required summary lines:

```text
phase40_status_api_conformance_status=ok
casing_drift_gate=ok
enum_drift_gate=ok
wrong_endpoint_gate=ok
rbac_boundary_gate=ok
delete_safety_status_gate=ok
operator_status_mutation_scope=status_events_only
finalizer_mutation_allowed=false
```

## TestOps Gate

Run on the lab with the runner:

```text
swblock run testops/scenarios/operator-status-api-conformance-chain.yaml
```

If the product repo is not at `/tmp/seaweed_block` on `m02`, pass:

```text
-env product_root=/path/to/seaweed_block
```

Expected remote artifact:

```text
/mnt/smb/work/share/g15d-k8s/<run-id>-operator-status-api-conformance/phase40-status-api-conformance-summary.txt
```

## Failure Criteria

Fail D4 if any of the following happens:

- status payload casing drift is accepted,
- unsupported condition/action enum drift is accepted,
- main-object or `/finalizers` endpoint mutation is accepted,
- operator-status gains main resource, storage, workload, or host mutation
  authority,
- blocked/releasable delete-safety status tests fail,
- Helm cannot render operator-status with `operatorStatus.dryRun=false`.

## Close Report Template

```text
Phase 40 D4 Operator-Status API Conformance QA — PASS/FAIL

Source commit:
Runner:
Run ID:

Local/scripted gate:
- phase40_status_api_conformance_status:
- casing_drift_gate:
- enum_drift_gate:
- wrong_endpoint_gate:
- rbac_boundary_gate:
- delete_safety_status_gate:
- finalizer_mutation_allowed:

TestOps gate:
- scenario:
- actions:
- result:
- artifact path:

Blocking findings:
- ...

Non-blocking findings:
- ...
```
