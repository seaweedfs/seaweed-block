# Phase 101 Finished Plan: NVMe Hardening And Soak

Status: complete. Local tests and m02 runner gates PASS on 2026-06-28.

## Problem

Phase 100 proved the supported-lab Kubernetes CSI NVMe multipath attach path:

```text
dynamic PVC protocol=nvme, replicationFactor=2
  -> two NVMe frontend paths for one NQN/NSID
  -> CSI NodeStage connects both paths
  -> app writer/reader passes
  -> cleanup leaves zero Seaweed Block NVMe residue
```

That proved first attach. It did not yet prove that NVMe path identity, partial
path loss, repeated stage/unstage, and bounded mounted I/O were observable and
honest across the product status surfaces.

Phase 101 hardened that path without changing the public claim into production
HA, RoCE, performance, or broad kernel compatibility.

## What Changed

Added NVMe status projection:

- replica evidence now carries NVMe NQN and NSID;
- managed-volume projection derives NVMe path count, address list, multipath
  observation, ANA state, and path-health reason;
- CRD status, operator snapshot, report summary, dashboard JSON, and explain
  surfaces include the same NVMe status block;
- missing or mismatched NVMe path evidence blocks false `Ready=True`.

Added gates:

```text
testops/scenarios/nvme-path-failure-status-chain.yaml
testops/scenarios/nvme-stage-unstage-residue-chain.yaml
testops/scenarios/nvme-bounded-soak-chain.yaml
scripts/run-phase101-nvme-stage-unstage-gate.sh
```

Extended:

```text
scripts/run-nvme-mounted-failover-smoke.sh
```

The mounted failover smoke now also supports a bounded soak mode through
`SW_BLOCK_NVME_SOAK_ITERATIONS`.

## Verification

Focused local tests:

```text
go test ./scripts ./internal/testops ./core/ops ./core/host/master ./cmd/sw-block ./core/frontend/nvme -count=1
PASS
```

D2 path failure status gate:

```text
scenario=nvme-path-failure-status-chain
run=20260628-013848-bdd2
result=PASS 17/17
before_path_count=2
after_path_count=1
after_nvme_reason=nvme_multipath_path_missing
after_ready_true=false
final_nvme_residue_count=0
```

D3 stage/unstage residue gate:

```text
scenario=nvme-stage-unstage-residue-chain
run=20260628-014526-dcd3
result=PASS 21/21
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

D4 bounded soak gate:

```text
scenario=nvme-bounded-soak-chain
run=20260628-015211-562f
result=PASS 21/21
soak_completed_iterations=5
before_path_count=2
soak_1_path_count=2
soak_5_path_count=2
soak_false_ready_count=0
soak_identity_drift_count=0
final_nvme_residue_count=0
```

## Notes

- The D2 and D4 standalone status captures show `before_status=unknown` because
  the standalone gate has no PVC/CSI/workload facts. The NVMe path facts are
  still present and are the subject of those gates.
- The D2 failure gate intentionally proves that a one-path state does not
  surface as a clean two-path `Ready=True`.
- The D4 soak is bounded correctness evidence, not a performance or SLO test.

## Non-Claims

Phase 101 does not claim:

- RoCE or NVMe/RDMA;
- production HA;
- transparent Kubernetes node-loss failover;
- broad distro/kernel/initiator compatibility;
- throughput, latency, or soak SLO;
- backup/snapshot/restore;
- release-image validation for the Phase 101 gates.

## Next

If Phase 101 should become a release claim, publish matching `seaweed-block` and
`seaweed-block-csi` images and rerun the relevant NVMe gates against those
images. Otherwise the next protocol work should explicitly choose between RoCE
preflight, NVMe performance characterization, or deeper multi-host failure
semantics.
