# Multi-Volume HA Support Evidence Contract

Date: 2026-05-23

Purpose: define the field names that support bundles, reports, dashboard views,
and future operator Conditions should reuse for multi-volume HA incidents.

This contract is based on Phase 27 D3/D4/D8 and Phase 28 D2 artifacts. It does
not add a new product claim.

## Current Evidence Sources

- D3 mounted failover matrix:
  `results/phase28-d2-flake-d3-mounted-n5/flake-summary.txt`
- D4 interleaved failover matrix:
  `results/phase28-d2-flake-d4-interleaved-n5-r3/flake-summary.txt`
- Representative mounted run:
  `20260523-194232-8ea4-helm-multi-volume-rf3-mounted-failover`
- Representative interleaved run:
  `20260523-192433-362e-helm-multi-volume-rf3-interleaved-failover`

## Run-Level Fields

These names should remain stable:

| Field | Meaning |
|---|---|
| `multi_volume_mounted_failover_status` | Overall mounted failover result. |
| `multi_volume_interleaved_failover_status` | Interleaved fault-window result, when applicable. |
| `requested_volume_count` | Number of PVC-backed volumes in the scenario. |
| `replication_factor` | RF requested through Helm/StorageClass path. |
| `failover_mode` | `sequential`, `interleaved`, or future explicit mode. |
| `target_volume_count` | Number of volumes intentionally faulted. |
| `app_node_selector` | Node targeting used by app pods. |
| `app_node_distribution_count` | Number of Kubernetes nodes hosting app writers. |
| `recovered_volume_count` | Number of target volumes that recovered. |
| `mounted_workload_checksum_passed_count` | Number of target workloads that verified data after failover. |
| `pod_recreate_used` | Whether recovery used pod recreate. |
| `cross_interference_observed` | Whether non-target volume behavior was disturbed. |
| `transparent_failover_claimed` | Whether mounted no-recreate failover is being claimed. |
| `cleanup_status` | External cleanup result. |

## Per-Volume Fields

These names should remain stable:

| Field | Meaning |
|---|---|
| `target_index` | Scenario-local volume index. |
| `volume_id` | Kubernetes PV / sw-block volume ID. |
| `pvc` | PVC name. |
| `before_primary` | Replica ID before injected failure. |
| `before_primary_node` | Kubernetes node for the old primary. |
| `before_publish_target` | Frontend address before failover. |
| `target_deployment` | Deployment stopped for the controlled fault. |
| `writer_pod` | Mounted workload pod name. |
| `writer_pod_uid_before` | Pod UID before fault. |
| `writer_node` | Node hosting the mounted workload. |
| `interleaved_fault` | Whether this volume was part of an interleaved fault window. |
| `failover_status` | `promoted`, `refused`, or future explicit terminal state. |
| `promoted_replica` | Replica promoted after failure. |
| `after_publish_target` | Frontend address after failover. |
| `post_failure_primary_count` | Number of primaries observed after promotion. |
| `target_ready_replicas` | Kubernetes ready replicas for the failed Deployment. |
| `stale_primary_fence_evidence` | Compact evidence that the old primary cannot serve writes. |
| `stale_primary_probe` | Probe type used for stale path validation. |
| `old_primary_stale_io_success_count` | Measured stale-path success count. Must be `0` for pass. |
| `rtpg_before_old_primary_aas` | ALUA AAS before failover for old primary path. |
| `rtpg_before_promoted_aas` | ALUA AAS before failover for promoted path. |
| `rtpg_after_old_primary_aas` | ALUA AAS after failover for old primary path. |
| `rtpg_after_promoted_aas` | ALUA AAS after failover for promoted path. |
| `rtpg_transition_verified` | Whether expected ALUA transition was observed. |
| `writer_pod_uid_after` | Pod UID after recovery. Must match before for transparent mounted claim. |
| `pod_recreate_used` | Whether this volume used pod recreate. |
| `data_check_after_failover` | Data verification result. |
| `target_recovered` | Whether this target volume recovered. |
| `cross_interference_observed` | Whether this volume's recovery disturbed other volumes. |
| `transparent_failover_claimed` | Whether this per-volume result claims mounted no-recreate failover. |

## Mapping To Product Surfaces

| Product Surface | Required Behavior |
|---|---|
| Support bundle | Preserve run-level and per-volume fields exactly. |
| `sw-block ops report` | When sourced from a bundle, carry the same names into JSON and HTML. |
| Dashboard | Display the same names or stable human labels derived from them. |
| `sw-block ops explain` | Explain failures using the same reason fields, not new synonyms. |
| Future operator Conditions | Map fields into Conditions without losing `volume_id`, `pvc`, `reason`, and `evidence_ref`. |

## Non-Claims

- This contract does not imply arbitrary scale.
- This contract does not imply production SLOs.
- This contract does not add mutating repair/promote/rebuild actions.
- This contract does not replace product-owned ManagedVolume state; it defines
  the compatibility vocabulary that product-owned state should expose.

