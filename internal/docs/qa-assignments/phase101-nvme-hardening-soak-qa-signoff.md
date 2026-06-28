# Phase 101 NVMe Hardening And Soak QA Sign-off

Status: PASS.

Validated source: Phase 101 branch through `0e00a66`.

## Scope

This sign-off covers the supported-lab NVMe hardening slice after the Phase 100
Kubernetes CSI NVMe multipath attach gate:

```text
NVMe path status surfaces
  -> one-path loss does not claim false Ready
  -> repeated stage/unstage leaves zero NVMe residue
  -> bounded mounted writer/reader soak keeps path identity stable
```

## Local Verification

```text
bash -n scripts/run-nvme-mounted-failover-smoke.sh
C:\work\swblock.exe validate testops/scenarios/nvme-bounded-soak-chain.yaml
go test ./scripts ./internal/testops ./core/ops ./core/host/master ./cmd/sw-block ./core/frontend/nvme -count=1
```

Result: PASS.

## Runner Evidence

| Gate | Scenario | Run | Result |
|---|---|---|---|
| D2 path failure status | `nvme-path-failure-status-chain` | `20260628-013848-bdd2` | PASS 17/17 |
| D3 stage/unstage residue | `nvme-stage-unstage-residue-chain` | `20260628-014526-dcd3` | PASS 21/21 |
| D4 bounded soak | `nvme-bounded-soak-chain` | `20260628-015211-562f` | PASS 21/21 |

## D2 Path Failure Status Evidence

```text
phase101_nvme_path_failure_status=ok
before_path_count=2
after_path_count=1
after_reason=nvme_multipath_path_missing
after_nvme_reason=nvme_multipath_path_missing
after_ready_true=false
final_nvme_residue_count=0
```

The gate proves the status surface does not report a clean two-path Ready state
after one NVMe path disappears.

## D3 Stage/Unstage Evidence

```text
phase101_nvme_stage_unstage_status=ok
cycles=3
primary_replica=r2
cycle_1_connected_path_count=2
cycle_1_disconnected_path_count=0
cycle_2_connected_path_count=2
cycle_2_disconnected_path_count=0
cycle_3_connected_path_count=2
cycle_3_disconnected_path_count=0
final_nvme_residue_count=0
```

The gate proves repeated connect/disconnect cycles do not leave Seaweed Block
NVMe subsystem residue.

## D4 Bounded Soak Evidence

```text
phase101_nvme_soak_status=ok
soak_iterations=5
before_path_count=2
soak_1_path_count=2
soak_5_path_count=2
soak_completed_iterations=5
soak_false_ready_count=0
soak_identity_drift_count=0
final_nvme_residue_count=0
```

The soak is a bounded correctness gate. It does not claim throughput, latency,
or production SLO.

## Lab Cleanup

Final runner cleanup disconnected Seaweed Block NVMe test subsystems and killed
test blockmaster/blockvolume processes. The remaining `nvme list-subsys` output
on m02 is the host's physical Samsung device, not a Seaweed Block subsystem.

## Non-Claims

This PASS does not claim:

- RoCE or NVMe/RDMA;
- production HA;
- transparent Kubernetes node-loss failover;
- broad kernel/distro compatibility;
- performance or soak SLO;
- release-image validation for Phase 101.

## Verdict

PASS. Phase 101 closes the supported-lab NVMe hardening slice: path identity and
health are surfaced, one-path loss is negative-first, repeated stage/unstage is
clean, and a bounded mounted soak preserves identity without false Ready.
