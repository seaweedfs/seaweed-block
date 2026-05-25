# Phase 31 Restart Persistence Claim And QA Checklist

Date: 2026-05-25

Purpose: give PM, QA, and engineering one exact wording boundary for Kubernetes
restart persistence.

## Claim Wording

Recommended product claim:

```text
Seaweed Block supports restart persistence for promoted authority on the
documented durable Helm install mode: existing PVC data remains readable after
Kubernetes/product restart, and a previously promoted primary does not roll
back to an old primary unless a newer valid failover is evidenced.
```

Do not phrase this as:

```text
no data loss under any restart
```

That is too broad. The validated claim must stay tied to:

- same Kubernetes cluster,
- same block-capable nodes,
- same host disks,
- durable Helm mode,
- supported RF/ACK profile for the gate,
- evidence-backed authority and data checks.

## Required User-Facing Promise

The user should be able to trust these facts after restart:

- PVC/PV identity is still present.
- Data previously written through the PVC is still readable.
- The current primary/epoch/publish target are not rolled back.
- If a promotion happened before restart, the promoted replica remains the
  authority unless a later valid failover is recorded.
- A stale old primary cannot keep serving successful stale I/O.
- Report/dashboard/operator evidence explains what happened.

## QA Checklist

### A. Authority Monotonicity

Required fields:

- `before_restart_primary`
- `before_restart_epoch`
- `before_restart_publish_target`
- `after_restart_primary`
- `after_restart_epoch`
- `after_restart_publish_target`

Pass criteria:

- `after_restart_epoch >= before_restart_epoch`
- `after_restart_primary == before_restart_primary` unless a newer valid
  failover event exists
- `after_restart_publish_target` matches the authority line
- `old_primary_resurrected=false`

### B. Stale Primary Fencing

Do not accept role text alone.

Required probe:

```text
direct stale path read/write probe against old primary path
```

Pass criteria:

- `old_primary_stale_io_success_count=0`
- probe evidence path is present
- old primary is not counted as current frontend-primary-ready

### C. Data Continuity

Required fields:

- `writer_verified_before_restart=true`
- `reader_verified_after_restart=true`
- checksum path or log evidence

Pass criteria:

- same PVC/PV identity
- same data checksum after restart
- no empty-store recreation for an existing PVC

### D. Cross-Volume Isolation

Required for multi-volume restart smoke:

- `requested_volume_count=3`
- `reader_verified_count=3`
- `managed_volume_count=3`
- `cross_volume_authority_mixup=false`
- `duplicate_publish_target_for_distinct_volume=false` unless explicitly
  expected by design

Pass criteria:

- each volume keeps its own primary/epoch/publish target
- one volume's restart/reconcile path does not rewrite another volume's
  authority

### E. Evidence Surfaces

Required surfaces:

- `sw-block ops report` JSON
- report `summary.txt`
- report `index.html`
- `operator-snapshot.json`
- TestOps bundle artifacts

Pass criteria:

- all surfaces agree on restart status,
- all expose the same reason codes,
- all remain read-only,
- no mutating action is implied by the restart report.

## Explicit Non-Claims

- No fresh-cluster restore.
- No backup/snapshot/restore.
- No host disk loss survival.
- No all-replica loss survival.
- No automatic rebuild/failback of a returned old primary.
- No broad production availability SLO.

