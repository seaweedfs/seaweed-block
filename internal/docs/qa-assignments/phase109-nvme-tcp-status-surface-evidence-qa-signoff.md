# Phase 109 NVMe/TCP Status Surface Evidence QA Sign-off

Status: PASS.

Validated source branch:
`phase109-nvme-tcp-status-surface-evidence`.

Scenario:
`testops/scenarios/nvme-tcp-status-surface-evidence-chain.yaml`

QA run: `20260629-161917-a0ef`

Result: 22/22 PASS.

## Scope

This gate validates operational visibility for the supported-lab NVMe/TCP
multi-volume path. Phases 106-108 proved cross-node attach, two-PVC isolation,
and repeated lifecycle cleanup. Phase 109 proves that users can see the same
NVMe identity and readiness facts through every status surface.

The gate proves:

- two PVCs are provisioned with `protocol=nvme`;
- CSI creates `SwBlockVolume` CRs for both PVCs;
- lifecycle-owner adds the protection finalizer;
- operator-status writes `.status.nvme` for both volumes;
- CRD status, report summary, report `operator-snapshot.json`, dashboard
  `/operator-snapshot.json`, and `ops explain` agree on ready status, reason,
  protocol, NQN, namespace ID, NVMe address, and path count;
- `.status.publishTarget` matches `.status.nvme.nvmeAddr`;
- all status actions remain non-mutating;
- final cleanup leaves zero residue.

## Useful Failures Closed During The Gate

Earlier attempts failed for reasons that are now covered by the scenario:

- cached `sw-block:local` images can hide operator-status code changes, so the
  scenario now rebuilds and imports fresh images;
- stale cluster CRDs can prune new status fields because Helm does not upgrade
  CRDs under `crds/`, so the scenario now deletes block CRDs during pre-clean;
- `ops explain` uses `managed_volume <id>` text while report summary uses
  `managed_volume=<id>`, so the assertion now matches the actual explain
  surface instead of treating a formatting difference as a product failure.

## Terminal Evidence

```text
phase109_nvme_tcp_status_surface_status=ok
volume_count=2
crd_nvme_status_count=2
report_nvme_status_count=2
operator_snapshot_nvme_status_count=2
dashboard_nvme_status_count=2
explain_nvme_status_count=2
all_surfaces_status=ready
all_surfaces_reason=first_volume_verified
all_surfaces_protocol=nvme
publish_target_matches_nvme_addr=true
mutation_allowed=false
```

Strict cleanup audit:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Verdict

Phase 109 can close. The supported-lab NVMe/TCP path now has:

- Phase 106: single-PVC cross-node writer/reader attach;
- Phase 107: two-PVC cross-node identity and NQN isolation;
- Phase 108: repeated two-PVC lifecycle cleanup with zero per-cycle residue;
- Phase 109: CRD/report/operator-snapshot/dashboard/explain status agreement
  for NVMe protocol identity and readiness.

Non-claims remain: no RoCE/NVMe-RDMA, no performance/SLO, no broad
distro/kernel compatibility, no production HA, and no multi-path failover claim
across real hosts.
