# Phase 95 Finished Plan: Failback Live Deployed Suite Smoke

Status: complete. Live QA PASS on 2026-06-26.

## Problem

Phases 88 and 94 proved that failback target-owner/executor packaging renders
correctly and that the executor can call a real blockmaster `FailbackService` in
a local Go smoke. They did not prove that the same path works after Helm
deployment in Kubernetes.

The remaining gap was:

```text
chart + image + RBAC + CRD schema + deployed pods + in-cluster service DNS + gRPC runtime
```

must all agree before the returned-replica failback control path can be treated
as a deployable product path.

## Scope

Phase 95 adds a live k3s gate that installs the explicitly enabled failback
suite and drives one bounded authority failback through the deployed
target-owner and executor.

It does **not** claim:

- frontend publication after failback;
- workload-visible data-path switch;
- CSI republish;
- storage mutation;
- automatic failback on default installs.

## Deliverables

- `scripts/run-phase95-failback-live-deployed-suite-gate.sh`
- `testops/scenarios/failback-live-deployed-suite-chain.yaml`
- `internal/docs/current-plan.md`
- `internal/docs/qa-assignments/phase95-failback-live-deployed-suite-qa.md`

## Product Logic

The gate deliberately starts from a real first volume so blockmaster owns a live
authority line. It then patches only the status model needed to represent a
returned replica that is safe to fail back:

```text
live volume authority line
  -> SwBlockVolume.status current authority facts
  -> returned replica terminal evidence
  -> failback executor contract
  -> SwBlockReplicaFailback target
  -> executor gRPC call to blockmaster
  -> failed_back status with terminal evidence
```

The fake returned replica data/control addresses are acceptable for this phase
because the authority runtime is the component under test; it only republishes
authority metadata and returns `NoStorageMutation=true`. Frontend path
publication is explicitly held for a later phase.

## Verification

Static/local:

```text
bash -n scripts/run-phase95-failback-live-deployed-suite-gate.sh
swblock validate testops/scenarios/failback-live-deployed-suite-chain.yaml
helm lint charts/seaweed-block
```

Live:

```text
swblock run testops/scenarios/failback-live-deployed-suite-chain.yaml
```

Expected terminal evidence:

```text
phase95_failback_live_deployed_suite_status=ok
live_kubernetes_install_claimed=true
first_volume_writer_reader=pass
failback_target_created=true
failback_executor_completed=true
executor_status_failed_back=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
cleanup_status=ok
```

Observed live run:

```text
scenario=failback-live-deployed-suite-chain.yaml
run=20260626-152618-5993
result=PASS 22/22
phase95_failback_live_deployed_suite_status=ok
helm_install=pass
deployed_suite_pods_ready=true
blockmaster_grpc_ready=true
first_volume_writer_reader=pass
failback_target_created=true
failback_executor_completed=true
executor_status_failed_back=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
cleanup_status=ok
```

## Next

Use `failed_back` terminal target evidence as the input to a separate frontend
publication target-owner/executor path. That phase must keep default installs
off and should not reuse Phase 95's authority evidence as a data-path success
claim.
