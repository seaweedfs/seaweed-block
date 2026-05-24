# Phase 29 Lifecycle Evidence Contract

Date: 2026-05-24

Purpose: make lifecycle and cleanup results visible through one stable
vocabulary across helper summaries, support bundles, `sw-block ops report`,
dashboard HTML, and `operator-snapshot.json`.

This is a read-only evidence contract. It does not authorize cleanup, delete,
repair, rebuild, failback, or promote actions.

## Required Cleanup Fields

Every cleanup-capable gate should expose these fields when cleanup evidence is
available:

| Field | Required surface | Meaning |
|---|---|---|
| `cleanup_status` | helper summary, cleanup summary, report summary, dashboard, operator snapshot | `ok` or `failed`; derived from terminal evidence. |
| `k8s_residue_count` | cleanup summary, report summary, dashboard, operator snapshot | Count of sw-block Kubernetes resources after cleanup. |
| `iscsi_residue_count` | cleanup summary, report summary, dashboard, operator snapshot | Count of matching iSCSI sessions or node records. |
| `multipath_residue_count` | cleanup summary, report summary, dashboard, operator snapshot | Count of matching dm-multipath or dmsetup residue. |
| `process_residue_count` | cleanup summary, report summary, dashboard, operator snapshot | Count of sw-block host processes after cleanup. |
| `hostpath_residue_count` | cleanup summary, report summary, dashboard, operator snapshot | Count of run-scoped hostPath residue, when configured. |
| `failure_count` | cleanup summary, report summary, dashboard, operator snapshot | Number of residue classes that failed. |
| `failed_phase` | helper summary and report summary when known | First phase that failed. |
| `cleanup_evidence` / `evidence_ref` | report summary and operator snapshot | Artifact path that produced the cleanup result. |

Field names must not be renamed by product surfaces. Human labels may be
friendlier, but JSON and text keys should stay stable.

## Source Precedence

The product report uses this precedence:

1. Product-owned `cluster-evidence.json`, if present.
2. Inventory-derived observation if product evidence is absent.
3. `cleanup-summary.txt`, when present anywhere in the bundle tree.

`cleanup-summary.txt` is additive. It must not cause the report to invent a
volume when no volume evidence exists; it only adds lifecycle cleanup state to
the cluster/report.

## Surface Mapping

| Surface | Contract |
|---|---|
| Helper summaries | May include compact `cleanup_status`, `failed_phase`, and pointers to report artifacts. |
| `cleanup-summary.txt` | Source of residue counts and cleanup failure count. |
| `summary.txt` from `sw-block ops report` | Must echo cleanup fields when cleanup evidence exists. |
| Dashboard HTML | Must show a Lifecycle Cleanup section when cleanup evidence exists. |
| `operator-snapshot.json` | Must include cleanup evidence under the read-only cluster status. |
| `cluster-evidence.json` | May include `cleanup` when product observation owns the fact; bundle replay can also add it. |

## Reason Codes

Use the Phase 29 cleanup ownership matrix registry:

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

New reason codes require a new resource class or materially different failure
mode.

## Acceptance

D3 is complete when:

- `core/ops` tests prove `cleanup-summary.txt` is loaded from a bundle.
- `summary.txt` includes all required cleanup fields when evidence exists.
- Dashboard HTML includes lifecycle cleanup status and counts.
- `operator-snapshot.json` carries the same cleanup evidence under cluster
  status and remains read-only.
- No mutating action is added.

