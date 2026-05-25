# Phase 30 Cleanup Projection Ownership

Date: 2026-05-25

Purpose: make cleanup lifecycle status a single-owned projection chain instead
of separate report/dashboard/operator formatting logic.

## Ownership Chain

```text
cleanup-summary.txt
  -> CleanupEvidenceFromSummary
      -> CleanupEvidence
          -> ReportSummaryLines
          -> ReportRow
          -> operator-snapshot Cluster.Cleanup
```

`CleanupEvidence` is the projection owner for lifecycle cleanup state.

## Stable Fields

The stable cleanup vocabulary is:

- `cleanup_status`
- `k8s_residue_count`
- `iscsi_residue_count`
- `multipath_residue_count`
- `process_residue_count`
- `hostpath_residue_count`
- `failure_count`
- `failed_phase`
- `cleanup_evidence`

These fields are also represented in the ManagedVolume field contract under
`cleanup.*`.

## Surface Rules

- Bundle replay must parse cleanup through `CleanupEvidenceFromSummary`.
- Text report must render cleanup through `CleanupEvidence.ReportSummaryLines`.
- HTML report must render cleanup through `CleanupEvidence.ReportRow`.
- Operator snapshot must carry the same `CleanupEvidence` value under
  `cluster.cleanup`.

No report surface should independently invent cleanup field names or status
classification.

## Mutation Boundary

Cleanup evidence remains read-only in Phase 30. It can report residue and
recommend investigation, but it does not authorize Kubernetes deletes, iSCSI
logout, multipath flush, hostpath removal, repair, rebuild, or failback.

Future operator cleanup must add a separate action contract with executor,
policy gate, required facts, invariants, and evidence.

