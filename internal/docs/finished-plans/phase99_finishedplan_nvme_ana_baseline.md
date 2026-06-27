# Phase 99 Finished Plan: NVMe ANA Baseline

Status: complete. Local and remote runner PASS on 2026-06-26.

## Problem

Phase 98 closed the returned-replica operation loop. Before starting new NVMe
work, the repository needed a precise baseline because older NVMe audit text
still described implemented ANA and CSI features as missing.

The real gap is narrower:

```text
P4 proves target-level NVMe ANA multipath outside Kubernetes.
P5 proves Kubernetes CSI protocol selection for NVMe single-path.
Kubernetes CSI NVMe multipath attach is still missing.
```

## What Changed

Added:

```text
scripts/run-phase99-nvme-ana-baseline-gate.sh
testops/scenarios/nvme-ana-baseline-chain.yaml
```

Updated:

```text
internal/docs/current-plan.md
internal/docs/product-roadmap.md
docs/roadmap.md
internal/docs/ref/nvme-v2-coverage-gap-audit.md
internal/docs/qa-assignments/phase99-nvme-ana-baseline-qa-signoff.md
```

The gate proves the current baseline:

- ANA log page is served when an ANA provider is wired.
- Identify Controller and Identify Namespace advertise ANA fields only when a
  provider is wired.
- Identify and log ANA group IDs agree.
- Blockvolume projection maps current authority/replica state to ANA state.
- CSI NodeStage/NodeUnstage can use the NVMe utility path for single-path NVMe.
- Launcher renders NVMe blockvolume arguments.
- Existing P4/P5 scenario definitions remain syntactically valid.

## Verification

Local:

```text
bash scripts/run-phase99-nvme-ana-baseline-gate.sh .
phase99_nvme_ana_baseline_status=ok
go_test_nvme_blockvolume_csi_launcher=pass
nvme_scenarios_validate=pass
```

Remote runner:

```text
C:\work\swblock.exe run --env product_root=/tmp/seaweed_block --env ssh_key=C:/work/dev_server/testdev_key testops\scenarios\nvme-ana-baseline-chain.yaml
run=20260626-173602-7ea9
result=PASS 12/12
```

## Non-Claims

Phase 99 does not claim:

- Kubernetes CSI NVMe multipath attach;
- multi-node Kubernetes NVMe failover;
- RoCE or labelled performance;
- long soak;
- async ANA change notification behavior;
- DSM / Dataset Management;
- Write Zeroes;
- broad kernel or distro compatibility.

## Next

Phase 100 should implement Kubernetes CSI NVMe multipath attach:

```text
multiple NVMe frontends for one volume
  -> grouped publish context with one NQN/NSID and all addresses
  -> NodeStage connects all paths using native NVMe multipath
  -> app pod sees one mounted namespace
  -> cleanup proves no stale NVMe subsystem residue
```

Reuse the existing fact -> judgment -> action -> evidence model. Do not add a
separate NVMe-specific control plane unless repeated code proves it necessary.
