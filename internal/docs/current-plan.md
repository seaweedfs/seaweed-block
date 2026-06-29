# Current Plan: Phase 109 NVMe/TCP Status Surface Evidence

Status: closed.

QA run `20260629-161917-a0ef` passed 22/22 actions. Sign-off:
`internal/docs/qa-assignments/phase109-nvme-tcp-status-surface-evidence-qa-signoff.md`.

## Why This Is Next

Phase 106 proved single-PVC cross-node NVMe/TCP attach. Phase 107 proved
two-PVC identity/NQN isolation. Phase 108 proved repeated two-PVC lifecycle
cleanup without per-cycle residue.

The next risk is operator visibility. A transport can work and clean up, but
still be operationally weak if the Kubernetes CRD status, report,
operator-snapshot, dashboard, and explain surfaces do not agree on the NVMe
protocol identity. For users, this is the difference between "it works in the
lab" and "I can diagnose which NQN/address a PVC is using."

## Product Goal

Prove the supported-lab NVMe/TCP multi-volume path publishes the same protocol,
NQN, namespace ID, address, path count, and readiness reason across all user
inspection surfaces.

Required behavior:

- install the NVMe/TCP chart path with `operatorStatus.create=true` and
  `lifecycleOwner.create=true`;
- CSI creates `SwBlockVolume` CRs for the two PVCs;
- lifecycle-owner adds the protection finalizer;
- operator-status writes `.status.nvme` for both volumes;
- report summary, `operator-snapshot.json`, dashboard
  `/operator-snapshot.json`, and `ops explain` agree with the CRD;
- `publishTarget` matches `nvme.nvmeAddr`;
- all surfaces show `ready/first_volume_verified`;
- cleanup leaves zero residue.

## D1: Surface Agreement Gate

Scenario:

```text
testops/scenarios/nvme-tcp-status-surface-evidence-chain.yaml
```

Expected terminal evidence:

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

Actual terminal evidence matched the expected keys exactly.

## D2: Cleanup Gate

After the surface agreement assertions, uninstall Helm and run the strict
cleanup verifier.

Expected evidence:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

Actual cleanup evidence matched the expected keys exactly.

## Non-Claims

Phase 109 still does not claim:

- RoCE/NVMe-RDMA;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA;
- multi-path failover across real hosts;
- more than the supported-lab two-PVC status surface agreement gate.
