# Phase 98 Finished Plan: Failback Frontend Workload Close Gate

Status: complete. Live QA PASS on 2026-06-26.

## Problem

Phases 95-97 proved the pieces of returned-replica failback and frontend
publication separately:

- a deployed failback suite can advance blockmaster authority and write
  terminal `failed_back` evidence;
- terminal failback evidence can create a frontend-publication target;
- the frontend-publication executor can call a runtime and write
  `frontend_published` from valid terminal evidence.

The remaining gap was user-visible:

```text
after failback and frontend publication, can a workload still write and read
through the PVC, with cleanup returning to zero residue?
```

## What Changed

Phase 98 adds the product-owned runtime and live close gate needed to answer
that question.

### Blockmaster Frontend Publication Runtime

`blockmaster` now has a disabled-by-default HTTP runtime:

```text
--frontend-publication-runtime-http
--frontend-publication-runtime-listen <addr>
/runtime/frontend-publication
```

The runtime verifies that the live Publisher authority line already matches
the terminal failback target:

```text
volumeID
replicaID
targetDataAddr
targetCtrlAddr
```

Only then does it return:

```text
frontendPublished=true
failbackStarted=false
noStorageMutation=true
noCrossVolumeIdentityChange=true
```

### Failback-source Publication Target

`SwBlockFrontendPublication.spec` now carries post-failback target data/control
addresses, copied from `SwBlockReplicaFailback.spec`. The target owner can
activate a failback-source frontend-publication target only with explicit
activation policy and runtime endpoint values.

### Deployed Close Gate

The new gate:

```text
scripts/run-phase98-failback-frontend-workload-close-gate.sh
testops/scenarios/failback-frontend-workload-close-chain.yaml
```

extends the Phase 95 deployed suite with frontend-publication runtime,
target-owner, and executor components. It then verifies post-publication
workload writer/reader I/O and zero-residue cleanup.

## Verification

Local checks:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockmaster ./core/host/master -count=1
helm lint charts/seaweed-block
swblock validate testops/scenarios/failback-frontend-workload-close-chain.yaml
git diff --check
```

Live:

```text
swblock run testops/scenarios/failback-frontend-workload-close-chain.yaml
run=20260626-171324-40b5
result=PASS 16/16
```

Key evidence:

```text
phase98_failback_frontend_workload_close_status=ok
first_volume_writer_reader=pass
failback_executor_completed=true
executor_status_failed_back=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
frontend_publication_target_created=true
frontend_publication_published=true
frontend_published=true
frontend_publication_failback_started=false
frontend_publication_storage_mutation_allowed=false
post_failback_publication_writer_verified=true
post_failback_publication_reader_verified=true
cleanup_status=ok
```

## Finding

The first live run found stale CRD schema in the lab:

```text
frontendPublicationDecision=enabled rejected by old CRD enum
```

Helm does not upgrade existing CRDs from `charts/*/crds`. The gate now applies
current CRDs before install. Release/upgrade docs should keep this explicit.

## Closed Boundary

Phase 98 closes the returned-replica operation path from evidence to workload
I/O:

```text
returned replica caught up
  -> ACK eligibility
  -> failback target
  -> deployed failback executor
  -> blockmaster authority failback
  -> frontend publication target
  -> deployed frontend publication executor
  -> product-owned frontend publication runtime
  -> workload writer/reader passes
```

Non-claims remain:

- no default automatic failback;
- no broad rebuild automation;
- no backup/snapshot/restore;
- no NVMe ANA parity;
- no production SLO.

## Next

This is a coherent stopping point for the operation/failback loop. The next
work should be either:

- release hardening with matching published images and pinned-image smoke; or
- a new large feature train such as NVMe ANA parity, backup/restore, or
  production soak.
