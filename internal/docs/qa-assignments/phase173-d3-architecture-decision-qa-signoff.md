# Phase 173 D3 Architecture Decision QA Sign-Off

Verdict: **PASS / NO BACKEND CHANGE**. Local diagnostic controls ran at
`c4bb1991d750`; the mounted NVMe/TCP close control ran at
`8ba98af72f360` after the teardown gate was hardened.

## Decision

Phase 173 selected no storage-backend candidate. This is an intended terminal
outcome, not an incomplete implementation:

```text
local_architecture_direction=no_backend_change_unstable_counterfactuals
shipped_control_stability_gate=pass
counterfactual_control_stability_gate=inconclusive
diagnostic_controls_candidate_eligible=false
architecture_candidate_selected=false
product_mutation_present=false
```

The authoritative D1 fixed-work baseline remained stable. The smaller D3
counterfactual controls were retained as attribution evidence, but two exceeded
their predeclared `1.25x` range. None of the three predeclared `1.30x` direction
signals fired:

```text
owner_queue_signal=false
writeback_interference_signal=false
media_separation_signal=false
deferred_four_vs_shipped_four_ratio=0.256
deferred_one_vs_deferred_four_ratio=0.557
split_vs_shared_scratch_ratio=0.993
```

Therefore an owner/queue rewrite, WAL/extent media split, or another local
WALStore implementation would not be evidence-selected. D4-D8 are not
applicable under the Phase 173 stop rules.

## Local Controls

The exact Linux run used `/dev/nvme0n1p1`, ext4, CPU set `0,2,4,6`, and
`GOMAXPROCS=4`. Each control had five isolated process runs and used
preconditioned persistent stores.

```text
control                         median MiB/s   max/min
shipped concurrent, 4 writers       255.743     1.203
deferred foreground, 1 writer        36.528     1.010
deferred foreground, 4 writers       65.538     1.539
prefilled flusher                    366.112     1.585
shared-file scratch                  424.485     1.252
split-file scratch                   421.580     1.196
```

All correctness and counter checks passed, the shipped diagnostic stayed
inside its range, no diagnostic was presented as product throughput, and
`store_residue_count=0`.

Earlier control runs that exposed allocation, CPU-placement, process-isolation,
preconditioning, and noisy-benchmark parsing defects remain in the evidence
share. They were not relabeled as passes:

```text
20260801T075555Z-phase173-d3-local
20260801T080104Z-phase173-d3-local
20260801T080352Z-phase173-d3-local
20260801T080623Z-phase173-d3-local
20260801T080904Z-phase173-d3-local
20260801T081147Z-phase173-d3-local
20260801T081426Z-phase173-d3-local
20260801T081634Z-phase173-d3-local
```

## Adapter And Replication Attribution

The fixed-iteration component controls completed without fallback:

```text
RF1 durable adapter:       61.28 MiB/s, 66,845 ns/op
RF3 TCP, 1 writer:         91.56 MiB/s, 28,683 ns/op ACK wait
RF3 TCP, 4 writers:        14.69 MiB/s, 1,068,505 ns/op ACK wait
rf3_queue_saturation_observed=true
rf1_rf3_component_attribution=complete
```

These are component diagnostics with different contracts from the D1 engine
baseline. They do not justify a direct throughput ratio or a backend change.
They do justify measuring the adapter/frontend/replication boundary next.

## Mounted NVMe/TCP Close Control

The final independent run used matching images built from `8ba98af72f360` on
all three schedulable nodes. The application ran on m02, the volume target ran
on m01, and the publish target used the 100 GbE data network rather than the
management LAN:

```text
phase122_nvme_tcp_100gbe_baseline_status=ok
publish_target=10.0.0.1:4420
publish_target_route_dev=enp1s0np0
internal_ip_not_reused_as_performance_target=true
managed_volume_status=ready
managed_volume_reason=first_volume_verified
marker_verified=true
seq_write_mibps=127.49
seq_read_mibps=262.30
small_write_iops=737.75
final_data_verified=true
pv_deleted_before_csi_uninstall=true
cleanup_status=ok
```

Cleanup evidence was zero across Kubernetes resources, NVMe targets, iSCSI,
processes, multipath, and host paths.

## Gate Findings

The first mounted attempt exposed stale image distribution when blockmaster
landed on tp01. The final run imported both exact images to m01, m02, and tp01.

An interrupted attempt also exposed an unsafe gate order: CSI was uninstalled
before the external provisioner had deleted the PV. Commit `8ba98af` now waits
for the exact PV to disappear while CSI is still available and records
`pv_deleted_before_csi_uninstall=true`. A stale VolumeAttachment from the
interrupted run was removed only after its PV was absent and both nodes proved
there was no mount, NVMe connection, target subsystem, or process for its NQN.
The final run began from and returned to a verifier-proven clean baseline.

The live run also produced repeated launcher Deployment/ReplicaSet churn while
the deleted PVC was converging out of master inventory. It did not prevent
safe detach, PV deletion, uninstall, or zero-residue cleanup. It is retained as
future lifecycle-noise evidence, not treated as a Phase 173 backend finding.

## Artifacts

```text
/mnt/smb/work/share/g15d-k8s/20260801T081908Z-phase173-d3-local.tar.gz
sha256=a025832f484a06958c90e7c0049363df0a7f7a0607e65c06fc4f9aeda8231e51

/mnt/smb/work/share/g15d-k8s/20260801T081908Z-phase173-d3-mounted-rerun4.tar.gz
sha256=bc716ea6590091cf730de8710d4a9de8a6abb34395e2424c46b2ac0807b7548b
```

Phase 173 closes with WALStore and all shipped defaults unchanged. README and
user capability claims do not change.
