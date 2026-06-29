# Phase 98 QA Sign-off: Failback Frontend Workload Close Gate

Verdict: PASS.

Runner:

```text
swblock run testops/scenarios/failback-frontend-workload-close-chain.yaml
run=20260626-171324-40b5
result=PASS 16/16
```

## Gate Result

| Check | Result |
|---|---|
| Current CRDs applied before install | PASS |
| Helm install of opt-in deployed suite | PASS |
| First PVC writer/reader | PASS |
| Returned-replica failback target created | PASS |
| Failback executor completes `failed_back` | PASS |
| Master publisher epoch advances | PASS |
| Publish target swaps after failback | PASS |
| Frontend publication target created | PASS |
| Frontend publication target reaches `published` | PASS |
| Runtime evidence says no second failback | PASS |
| Runtime evidence says no storage mutation | PASS |
| Post-publication writer | PASS |
| Post-publication reader | PASS |
| Cleanup verifier | PASS |

## Terminal Evidence

```text
phase98_failback_frontend_workload_close_status=ok
phase98_scope=deployed_failback_frontend_publication_workload_io_cleanup
crds_applied=true
helm_lint=pass
helm_install=pass
deployed_suite_pods_ready=true
blockmaster_grpc_ready=true
first_volume_writer_reader=pass
live_volume_id=pvc-fc4905ba-d56b-4aa8-b90a-a86cffe3f839
live_current_primary=r1
live_current_epoch=1
swblockvolume_failback_contract_patched=true
failback_target_created=true
failback_executor_completed=true
executor_status_failed_back=true
master_publisher_epoch_advanced=true
publish_target_swapped_after_failback=true
failback_status_mutation_allowed=false
frontend_publication_target_created=true
frontend_publication_published=true
frontend_publication_target_published=true
frontend_published=true
frontend_publication_failback_started=false
frontend_publication_storage_mutation_allowed=false
post_failback_publication_writer_verified=true
post_failback_publication_reader_verified=true
cleanup_status=ok
```

The final `SwBlockFrontendPublication` status confirms:

```text
state=published
reasonCode=frontend_published
frontendPublished=true
failbackStarted=false
noStorageMutation=true
noCrossVolumeIdentityChange=true
publicationMutationAllowed=false
evidenceRefs=frontend_publication_authority_line_verified
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

## Finding Resolved During The Gate

The first live run failed because the lab still had an older
`SwBlockFrontendPublication` CRD schema:

```text
spec.frontendPublicationDecision: Unsupported value: "enabled"
supported values: "blocked", "disabled"
```

The working-tree CRD already allowed `enabled`; the failure was stale live
schema. Helm does not upgrade existing CRDs from `charts/*/crds`, so the gate
now applies current CRDs before Helm install.

This should be carried into release/upgrade docs: apply CRDs before a chart
install or upgrade that changes CRD schema.

## Boundary

This gate proves the user-visible post-failback workload path:

```text
first PVC writer/reader
  -> returned-replica failback
  -> frontend publication through product-owned runtime
  -> writer/reader after publication
  -> cleanup=0
```

It does not claim:

- default automatic failback;
- broad rebuild automation;
- backup/snapshot/restore;
- NVMe ANA parity;
- production SLOs.
