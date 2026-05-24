# Phase 29 Cleanup Ownership Matrix

Date: 2026-05-24

Purpose: define which component owns cleanup facts, which component executes
cleanup, and which evidence proves the lifecycle result. This is the D1
contract for Phase 29.

This document does not add mutating operator behavior. It classifies the
existing Helm/TestOps/helper cleanup path so later product code can take over
the right pieces without hiding residue behind scripts.

## Ownership Rule

Cleanup follows the same control model used by the ManagedVolume and protocol
docs:

```text
truth owner publishes facts
orchestration entity decides whether cleanup is complete or blocked
executor performs allowed cleanup action
evidence records what was observed and why the result passed or failed
TestOps remains the external auditor
```

Important boundary:

- Product-owned means the sw-block control plane or chart owns the desired
  lifecycle fact.
- Helper-owned means a script currently performs orchestration because the
  product does not yet expose a safe lifecycle primitive.
- TestOps-owned means the runner independently verifies no residue after the
  product/helper path claims success.

## Cleanup Matrix

| Resource class | Truth owner | Current executor | Evidence artifact | Failure reason code | Retry / idempotence rule | Migration target |
|---|---|---|---|---|---|---|
| Helm release | Helm release state | `helm uninstall` in scenario/helper | `helm-status.after-cleanup.txt`, `helm-list.after-cleanup.txt` | `helm_release_still_present` | `helm uninstall --ignore-not-found` style behavior; repeated uninstall must be safe | Stay Helm-owned; product reports status only |
| Chart-scoped Kubernetes resources | Kubernetes API + Helm labels | Helm uninstall, `uninstall-k8s-alpha.sh` fallback | `k8s-resources.after-cleanup.txt`, `k8s-residue.after-cleanup.txt` | `kubernetes_sw_block_resources_present` | bounded wait until no `sw-block`, `seaweed-block`, or CSI resources remain | Helm/operator lifecycle controller eventually owns desired absence |
| StorageClass / CSIDriver / RBAC | Kubernetes API + chart templates | Helm uninstall | same as chart-scoped resources | `kubernetes_sw_block_resources_present` | delete is idempotent; missing resource is success | Helm-owned until operator lifecycle exists |
| User demo pods | User workload / helper scenario | `run-basic-app-example.sh`, `run-multi-volume-example.sh`, TestOps | helper logs, namespace events, pod snapshots | `workload_pods_present` when a gate adds explicit count | delete with `--ignore-not-found`; wait bounded by helper | User-owned outside demos; TestOps-owned inside gates |
| PVC / PV demo resources | Kubernetes API + CSI provisioner | helper scenario | `pvc.txt`, `first-volume-summary.txt`, `multi-volume-summary.txt`, `k8s-resources.after-cleanup.txt` | `kubernetes_sw_block_resources_present` or future `pvc_residue_present` | delete PVCs, wait for generated deployments to drain, tolerate already-deleted PVCs | Product should expose volume lifecycle state; user still owns PVC deletion |
| Generated blockvolume Deployments | Launcher / blockmaster desired placement | launcher reconciler deletes after PVC removal; helper waits | `blockvolume-deployments.cleanup-timeout.txt` on failure; summary `cleanup_status` | `blockvolume_deployments_present` (future explicit), currently helper `cleanup_status=failed` | bounded wait with terminal `deployments_gone=true`; no second race-prone observation | Product-owned lifecycle controller should publish `desired_absent` / `observed_absent` |
| Blockmaster / CSI pods | Helm chart desired state | Helm uninstall | `k8s-resources.after-cleanup.txt`, `processes.after-cleanup.txt` | `kubernetes_sw_block_resources_present`, `sw_block_processes_present` | wait for pod deletion; process audit catches leaked host processes | Helm/operator lifecycle |
| iSCSI sessions | Host initiator state | CSI node unstaging and helper cleanup | `iscsi-sessions.after-cleanup.txt` | `iscsi_sessions_present` | repeated logout must be safe; missing session is success | CSI owns normal teardown; cleanup verifier audits |
| iSCSI node records | Host initiator database | CSI/helper cleanup | `iscsi-nodes.after-cleanup.txt` | `iscsi_node_records_present` | repeated node deletion must be safe; missing node is success | CSI/helper until product has host cleanup primitive |
| dm-multipath maps | Host multipath kernel/userspace | CSI/helper cleanup, optional verifier flush | `multipath.after-cleanup.txt`, `multipath-residue.after-cleanup.txt` | `multipath_maps_present` | verifier may flush only when explicitly enabled; otherwise fail closed with evidence | HostPathAuthority / CSI cleanup primitive later |
| dmsetup devices | Host device-mapper state | helper verifier observes, optional multipath flush removes | `dmsetup.after-cleanup.txt`, `multipath-residue.after-cleanup.txt` | `multipath_maps_present` | repeated `multipath -f` / `dmsetup remove -f` must be safe for stale maps only | HostPathAuthority evidence, CSI executor later |
| HostPath data dirs | Launcher/blockvolume hostPath config | helper verifier observes by prefix | `hostpath-residue.after-cleanup.txt` | `hostpath_residue_present` | only run-scoped prefixes may be removed by tests; durable user data must not be deleted implicitly | Product must distinguish test scratch from durable data before mutating |
| Product host processes | Kubernetes/kubelet/host runtime | Helm uninstall and process exit | `processes.after-cleanup.txt`, `process-residue.after-cleanup.txt` | `sw_block_processes_present` | process absence is observed, not killed by default, except explicit test cleanup paths | Kubernetes owns pods; TestOps audits host leakage |
| Support/report artifacts | Scenario/TestOps artifact store | helper/TestOps | `summary.txt`, `cluster-evidence.json`, `operator-snapshot.json`, `cleanup-summary.txt` | `support_artifact_missing` (future explicit) | artifacts are append-only per run; never cleaned before bundle capture | Product report owns schema; TestOps owns retention |

## Required Summary Fields

All cleanup-capable helpers and future product reports should converge on these
fields:

| Field | Meaning |
|---|---|
| `cleanup_status` | `ok` or `failed`; must be derived from terminal evidence, not best effort. |
| `k8s_residue_count` | Count of sw-block Kubernetes resources after cleanup. |
| `iscsi_residue_count` | Count of active sessions or node records matching Seaweed Block IQNs. |
| `multipath_residue_count` | Count of Seaweed Block or orphan `mpath*`/dmsetup maps. |
| `process_residue_count` | Count of product host processes after cleanup. |
| `hostpath_residue_count` | Count of run-scoped hostPath residue, when a prefix is configured. |
| `failure_count` | Number of failed residue classes. |
| `failed_phase` | First phase that failed, when the caller is a scenario helper. |

## Reason Code Registry

Current stable reason codes:

- `helm_release_still_present`
- `kubernetes_sw_block_resources_present`
- `iscsi_sessions_present`
- `iscsi_node_records_present`
- `multipath_maps_present`
- `sw_block_processes_present`
- `hostpath_residue_present`
- `missing-command-helm`
- `missing-command-kubectl`
- `missing-command-iscsiadm`

New cleanup logic should reuse these strings unless it adds a materially new
resource class.

## Phase 29 Migration Boundary

Phase 29 may do:

- make helper waits deterministic,
- make residue evidence fields consistent,
- add verifier checks for existing resource classes,
- add read-only report/dashboard fields that expose cleanup state.

Phase 29 must not do:

- add a mutating operator cleanup action,
- delete durable volume data by default,
- hide residue by force-removing host state before evidence capture,
- broaden the product claim beyond the documented alpha gates.

## Gate Mapping

| Gate | What it proves |
|---|---|
| D2 | Known helper TOCTOU races are removed from active multi-volume loops. |
| D3 | Summary/report/dashboard use one lifecycle evidence vocabulary. |
| D4 | Independent RF3 multi-volume reruns prove cleanup determinism. |
| D5 | QA confirms the matrix matches real artifacts and no open cleanup race remains. |

