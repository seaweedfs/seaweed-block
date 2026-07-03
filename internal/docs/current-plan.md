# Current Plan: Phase 120 NVMe/TCP Performance Baseline

Status: implementation and QA-gate packaging.

Phase 119 closed the RDMA evidence review: the mono RDMA/VFS/RustVolume/NIXL
work proves real object/VFS acceleration, but it is not a Linux
`nvme connect -t rdma` compatible NVMe-oF/RDMA target. Phase 120 therefore
takes the conservative next step: measure the supported block path
(`NVMe/TCP`) before spending more engineering time on RoCE/NVMe-RDMA.

This is a baseline phase, not a performance claim phase.

## Why This Phase Exists

The product already has a supported-lab NVMe/TCP path:

```text
Kubernetes PVC
  -> CSI dynamic provisioning
  -> blockmaster/blockvolume external NVMe/TCP publication
  -> Linux host nvme-tcp attach
  -> pod mount + write/read
```

The next question is not whether RDMA can move memory quickly in another
project. The next question is where the current Seaweed Block NVMe/TCP path
actually spends time under a real Kubernetes PVC workload.

Phase 120 records a repeatable baseline so later work can compare:

- NVMe/TCP tuning;
- storage-engine changes;
- NVMe/RDMA work;
- object/VFS/NIXL acceleration work that is not directly block PVC.

## Deliverables

1. Add an executable lab gate:

   ```text
   scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh
   testops/scenarios/nvme-tcp-performance-baseline-chain.yaml
   ```

2. The gate must use the supported Kubernetes NVMe/TCP path, not a standalone
   helper path.

3. The gate must collect terminal `key=value` evidence:

   ```text
   phase120_nvme_tcp_performance_baseline_status=ok
   protocol=nvme
   frontend_transport=tcp
   managed_volume_status=ready
   publish_target_loopback=false
   marker_verified=true
   final_data_verified=true
   seq_write_mibps=<number>
   seq_read_mibps=<number>
   small_write_iops=<number>
   cleanup_status=ok
   ```

4. The gate must explicitly record non-claims:

   ```text
   roce_claim_allowed=false
   nvme_rdma_claim_allowed=false
   performance_claim_allowed=false
   performance_slo_claim_allowed=false
   perf_gate_type=baseline_no_slo
   ```

5. Add a QA assignment:

   ```text
   internal/docs/qa-assignments/phase120-nvme-tcp-performance-baseline-qa.md
   ```

## Verification

Local/source checks:

```powershell
bash -n scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh
C:\work\swblock.exe validate testops/scenarios/nvme-tcp-performance-baseline-chain.yaml
go test ./cmd/blockvolume ./cmd/sw-block
```

Live gate:

```powershell
C:\work\swblock.exe run `
  testops/scenarios/nvme-tcp-performance-baseline-chain.yaml `
  -env product_root=/tmp/seaweed_block
```

The live gate may use local images while this remains a development phase. A
future release claim still needs matching published `seaweed-block` and
`seaweed-block-csi` images.

Any `publish_target=<ip>:4420` row in this phase is an NVMe/TCP target address
on the Kubernetes/LAN network. It is not a RoCE/RDMA address or evidence of
NVMe/RDMA attach.

## Exit Criteria

Phase 120 can close when:

- the source checks pass;
- the TestOps scenario validates;
- the live supported-lab gate passes or has a concrete lab/artifact blocker;
- the summary contains numeric baseline rows and the explicit non-claim rows;
- cleanup leaves `cleanup_status=ok`.

## Non-Claims

Phase 120 does not claim RoCE, NVMe/RDMA attach, NVMe/RDMA performance, a
performance SLO, production tuning, broad kernel/distro compatibility,
GPU/cuObject, NIXL production support, or published-image support.
