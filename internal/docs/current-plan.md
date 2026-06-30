# Current Plan: Phase 110 NVMe/TCP Path-Loss Status Surface Honesty

Status: closed.

QA run `20260629-210857-5c3b` passed 23/23 actions. Sign-off:
`internal/docs/qa-assignments/phase110-nvme-tcp-path-loss-status-surface-qa-signoff.md`.

## Why This Is Next

Phase 109 proved healthy NVMe/TCP identity across CRD, report,
operator-snapshot, dashboard, and explain. Phase 101 already proved the live
standalone one-path-loss behavior: a mounted NVMe/TCP workload survives losing
one path, and the product must not claim `Ready=True` while only one of the
expected two paths remains.

The remaining risk is negative visibility. A transport can detect path loss but
still be operationally unsafe if support surfaces replay stale or over-optimistic
state. Phase 110 turns the real Phase 101 path-loss evidence into a support
bundle and proves every user-facing cold-reader surface preserves the same
non-ready reason.

## Product Goal

Prove that real NVMe/TCP one-path-loss evidence replays as
`blocked/nvme_multipath_path_missing` across report, operator-snapshot,
dashboard, and explain, with no false `Ready=True` and no mutating action.

Required behavior:

- run the existing live mounted NVMe/TCP failover/path-loss script;
- confirm live source evidence starts with two paths and ends with one path;
- confirm live source reason is `nvme_multipath_path_missing`;
- normalize `cluster-after-failover.json` into
  `product-observation/cluster-evidence.json`;
- replay with `sw-block ops report --from-bundle`;
- replay with `sw-block ops dashboard --from-bundle`;
- replay with `sw-block ops explain volume --from-bundle`;
- prove all surfaces agree on `blocked/nvme_multipath_path_missing`;
- prove `Ready=True` is absent everywhere;
- cleanup leaves zero NVMe residue.

## D1: Live Source Gate

Scenario:

```text
testops/scenarios/nvme-tcp-path-loss-status-surface-chain.yaml
```

Expected terminal evidence:

```text
phase101_nvme_path_failure_status=ok
before_path_count=2
after_path_count=1
after_nvme_reason=nvme_multipath_path_missing
after_ready_true=false
final_nvme_residue_count=0
```

Actual evidence:

```text
phase101_nvme_path_failure_status=ok
before_path_count=2
after_path_count=1
after_nvme_reason=nvme_multipath_path_missing
after_ready_true=false
final_nvme_residue_count=0
```

## D2: Support Surface Replay Gate

Expected terminal evidence:

```text
phase110_nvme_tcp_path_loss_status_surface_status=ok
live_path_loss_source=phase101
before_path_count=2
after_path_count=1
after_nvme_reason=nvme_multipath_path_missing
after_ready_true=false
report_reason=nvme_multipath_path_missing
operator_snapshot_reason=nvme_multipath_path_missing
dashboard_reason=nvme_multipath_path_missing
explain_reason=nvme_multipath_path_missing
surface_ready_true_count=0
mutation_allowed=false
```

Actual evidence matched the expected keys exactly.

## D3: Cleanup Gate

Expected terminal evidence:

```text
final_nvme_residue_count=0
```

Actual source and final cleanup evidence matched the expected key.

## Non-Claims

Phase 110 does not claim:

- RoCE/NVMe-RDMA;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA;
- live Kubernetes CRD negative projection for NVMe path loss;
- more than support-surface replay of real standalone one-path-loss evidence.
