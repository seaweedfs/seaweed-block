# Current Plan: Phase 98 Failback Frontend Workload Close Gate

Status: complete.

## Goal

Phase 98 closes the returned-replica failback operation loop that Phases 46-97
built in small safety slices:

```text
install Helm suite
  -> first PVC writer/reader
  -> returned-replica failback
  -> product-owned frontend publication
  -> post-publication writer/reader I/O
  -> cleanup verifier reports zero residue
```

This is the first workload-visible post-failback frontend-publication claim.

## What Changed

### D1: Product-owned Frontend Publication Runtime

`blockmaster` now has an opt-in HTTP runtime endpoint:

```text
--frontend-publication-runtime-http
--frontend-publication-runtime-listen <addr>
/runtime/frontend-publication
```

The endpoint verifies that the current publisher authority line matches the
terminal failback target:

```text
replicaID
targetDataAddr
targetCtrlAddr
```

It returns published terminal evidence only when the authority line is already
the post-failback line. It does not start failback, mutate storage, or change a
different volume.

### D2: Failback-source Publication Targets

`SwBlockFrontendPublication.spec` now carries:

```text
targetDataAddr
targetCtrlAddr
```

The frontend publication target owner copies these fields from terminal
`SwBlockReplicaFailback` evidence and can activate a target only under explicit
policy and runtime endpoint configuration.

### D3: Deployed Suite Wiring

The Helm chart can explicitly enable:

```text
blockmaster.frontendPublicationRuntimeHTTP
frontendPublicationTargetOwner.activation
frontendPublicationExecutor.execution
```

Default installs remain off.

### D4: Live Close Gate

Added:

```text
scripts/run-phase98-failback-frontend-workload-close-gate.sh
testops/scenarios/failback-frontend-workload-close-chain.yaml
```

The gate reuses the deployed Phase 95 suite with
`SW_BLOCK_PHASE95_FRONTEND_PUBLICATION_CLOSE=true`, applies current CRDs before
install, and verifies:

- first PVC writer/reader passes;
- failback reaches `failed_back`;
- frontend publication target reaches `published`;
- publication evidence says `frontendPublished=true`;
- publication evidence says `failbackStarted=false`;
- publication evidence says `noStorageMutation=true`;
- post-publication writer and reader both pass;
- cleanup verifier reports zero residue.

## Verification

Local checks:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockmaster ./core/host/master -count=1
helm lint charts/seaweed-block
swblock validate testops/scenarios/failback-frontend-workload-close-chain.yaml
git diff --check
```

Live runner:

```text
swblock run testops/scenarios/failback-frontend-workload-close-chain.yaml
run=20260626-171324-40b5
result=PASS 16/16
```

Terminal evidence:

```text
phase98_failback_frontend_workload_close_status=ok
crds_applied=true
helm_install=pass
deployed_suite_pods_ready=true
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

Cleanup verifier:

```text
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Important Finding

The first Phase 98 live run exposed a lab/upgrade hazard: Helm does not update
pre-existing CRDs from `charts/*/crds`. The current CRD allowed
`frontendPublicationDecision=enabled`, but the lab still had an older schema
that allowed only `blocked` and `disabled`, causing a live 422.

The gate now applies current CRDs before install so it tests the working tree
instead of stale lab state. Release and upgrade docs should keep the same rule:
apply CRDs before installing or upgrading a chart that changes CRD schema.

## Closed Boundary

Phase 98 closes this operation-layer path:

```text
returned replica caught up
  -> ACK eligibility
  -> failback target
  -> deployed failback executor
  -> blockmaster authority failback
  -> frontend publication target
  -> deployed frontend publication executor
  -> product-owned publication runtime
  -> workload writer/reader still works
```

Non-claims:

- no default automatic failback;
- no broad returned-replica rebuild automation;
- no backup/snapshot/restore;
- no NVMe ANA parity;
- no production SLO or broad compatibility claim.

## Next

The operation loop has reached a useful close point. The next coherent choices
are:

1. release hardening for this milestone: publish matching images and run a
   pinned-image release smoke;
2. move to a new large feature train: NVMe ANA parity, backup/restore, or a
   broader production-soak track.

Avoid adding more small operation phases unless they close a concrete release
blocker.
