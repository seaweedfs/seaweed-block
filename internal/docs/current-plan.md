# Current Plan: Phase 99 NVMe ANA Baseline And Next Gap

Status: complete. Local and remote runner PASS.

## Goal

Phase 98 closed the returned-replica operation loop. Phase 99 starts the next
large feature train by pinning the current NVMe-oF/ANA state before adding new
NVMe work.

The immediate goal is not to claim full NVMe parity. It is to prevent stale
docs and vague roadmap wording from hiding the real gap:

```text
P4 proves target-level NVMe ANA multipath outside Kubernetes.
P5 proves Kubernetes CSI protocol selection for NVMe single-path.
The next product gap is Kubernetes CSI NVMe multipath attach.
```

## What Exists Now

Current code already has meaningful NVMe-oF support:

- NVMe/TCP target protocol core: IC handshake, Fabric Connect, admin/IO queue
  separation, Identify, Get Log Page, READ, WRITE, FLUSH, inline/R2T paths,
  KATO/KeepAlive, and disconnect handling.
- ANA provider seam and ANA log page support.
- Conditional ANA Identify advertisement when an `ANAProvider` is wired.
- Blockvolume projection-backed ANA provider mapping primary/standby/fault
  state into host-visible ANA state.
- Distinct controller IDs from replica IDs.
- CSI protocol selection for `protocol=nvme`.
- CSI NodeStage/NodeUnstage NVMe connect/disconnect component coverage.
- Launcher render coverage for NVMe blockvolume args.

## New Gate

Added:

```text
scripts/run-phase99-nvme-ana-baseline-gate.sh
testops/scenarios/nvme-ana-baseline-chain.yaml
```

The gate is intentionally a baseline/conformance gate:

- runs focused Go tests for NVMe protocol, blockvolume ANA projection, CSI
  NVMe stage/unstage, and launcher render;
- validates existing P4/P5 TestOps scenarios;
- records that live NVMe multipath and CSI gates remain release-required.

It accepts:

```text
GO_BIN=<path>       # optional Go override
SWBLOCK_BIN=<path>  # optional TestOps runner override
```

This keeps Windows/Git-Bash local validation from accidentally using stale WSL
Go 1.18 while preserving normal Linux execution on the lab.

## Verification

Local gate:

```text
bash scripts/run-phase99-nvme-ana-baseline-gate.sh .
phase99_nvme_ana_baseline_status=ok
go_binary=go.exe
go_version=go version go1.25.6 windows/amd64
go_test_nvme_blockvolume_csi_launcher=pass
ana_log_page_reports_provider_state=true
ana_identify_and_log_consistent=true
ana_identify_controller_advertised_with_provider=true
ana_identify_namespace_advertised_with_provider=true
projection_ana_state_mapping=true
projection_ana_group_dense=true
projection_ana_change_count_lineage=true
csi_nvme_node_stage=true
csi_nvme_unstage=true
launcher_nvme_manifest=true
swblock_binary=/mnt/c/work/swblock.exe
nvme_scenarios_validate=pass
nvme_p4_scenario_valid=true
nvme_p5_csi_scenario_valid=true
nvme_p5_component_scenario_valid=true
live_nvme_multipath_required_for_release=true
live_nvme_csi_required_for_release=true
```

Scenario syntax:

```text
C:\work\swblock.exe validate testops\scenarios\nvme-ana-baseline-chain.yaml
VALID: nvme-ana-baseline-chain
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

The next coherent NVMe deliverable is Kubernetes CSI NVMe multipath attach:

```text
master status exposes multiple NVMe frontend paths for one volume
  -> CSI publish context carries the grouped NQN/NSID plus all addresses
  -> NodeStage connects all paths under native NVMe multipath
  -> app pod sees one mounted namespace
  -> failover/cleanup artifacts prove no stale subsystem residue
```

That should be the first real Phase 100 implementation target. Do not add a
new control-plane model for it; reuse the existing fact -> judgment -> action
-> evidence model.
