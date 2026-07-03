# Phase 120 NVMe/TCP Performance Baseline QA Sign-off

Verdict: **FAIL**

Classification: **product** - the live gate failed in the synced current product
tree after successful local image build/import, Helm install, PVC binding, and
perf pod readiness. This was not a lab access, disk, image availability, or
cleanup failure.

## Scope

- Branch tested: `phase120-nvme-tcp-performance-baseline`
- Commit tested: `949a8e9`
- Scenario: `testops/scenarios/nvme-tcp-performance-baseline-chain.yaml`
- Run command: `C:\work\swblock.exe run -env product_root=/tmp/seaweed_block testops/scenarios/nvme-tcp-performance-baseline-chain.yaml`
- Run id: `20260702-163714-0048`
- Bundle path: `C:\work\seaweed_block\results\20260702-163714-0048`
- Remote artifact path: `/mnt/smb/work/share/g15d-k8s/20260702-163714-0048-phase120-nvme-perf`
- Image mode: local images (`sw-block:local`, `sw-block-csi:local`) built and imported by the gate

## Result

The scenario failed in `run_gate` before the `assert_summary` phase ran:

```text
volume status=ok, want ready
```

Runner state:

```text
pre_clean=pass
run_gate=fail
collect=pass
scenario_status=FAIL
```

The gate wrote partial summary rows only; no baseline performance rows were
collected.

## Full Summary Rows

```text
phase120_nvme_tcp_performance_baseline_status=running
protocol=nvme
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_claim_allowed=false
performance_slo_claim_allowed=false
perf_gate_type=baseline_no_slo
go_test_blockvolume_sw_block=pass
image_ready=true
generated_external_nvme=true
blockvolume_node=m01
app_node=m02
helm_template_external_nvme=true
helm_install=pass
pvc_bound=true
perf_pod_ready=true
```

Missing required PASS rows include:

```text
phase120_nvme_tcp_performance_baseline_status=ok
managed_volume_status=ready
managed_volume_reason=first_volume_verified
publish_target_loopback=false
marker_verified=true
final_data_verified=true
seq_write_mibps=<number>
seq_read_mibps=<number>
small_write_iops=<number>
cleanup_status=ok
```

## Failure Evidence

`status/cluster-evidence.json` contained a ready managed-volume view but the
gate script checked the older `volumes` status field:

```text
cluster_evidence.status=ok
cluster_evidence.volumes[0].status=ok
cluster_evidence.volumes[0].publish_target=192.168.1.181:4420
cluster_evidence.managed_volumes[0].status=ready
cluster_evidence.managed_volumes[0].reason_code=first_volume_verified
cluster_evidence.managed_volumes[0].publish_target=192.168.1.181:4420
```

The current gate script exits unless `cluster_evidence.volumes[0].status` is
`ready`, so it failed before marker verification and performance measurements.

## Cleanup Status

The gate exited before its in-script `verify-helm-cleanup.sh` step, so QA ran the
cleanup verifier separately after the failure:

```text
cleanup_status=ok
helm_release=sw-block
helm_namespace=kube-system
iqn_substr=io.seaweedfs
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
cleanup_observed_at=2026-07-02T23:41:10Z
```

Final residue checks found no `swblock*` CRDs, no `sw-block` Helm release, and no
`sw-block` pods/PVCs/PVs/namespaces. Only the physical local NVMe PCI subsystem
remained in `nvme list-subsys`.

## Lab Notes

- Synced the current Windows product tree to `m02:/tmp/seaweed_block` before the run.
- Sync verification hashes matched for the Phase 120 script, scenario, and `go.mod`.
- m01 and m02 were `Ready`; tp01 was `NotReady` but not used by this two-node gate.
- m02 root disk was 61% used.
- Pre-flight empty `swblock*` CRDs were removed before running.

## RE-VALIDATION (b9b3bac) - FAIL

Classification: **lab** - the fixed managed-volume readiness path passed and the
baseline reached marker verification, sequential/small-write measurements, and
final data verification. The scenario still failed because the in-gate cleanup
verifier observed one transient Kubernetes residue pod immediately after Helm
uninstall. A post-exit cleanup verifier run one minute later reported
`cleanup_status=ok` with all residue counts zero.

## Re-validation Scope

- Branch tested: `phase120-nvme-tcp-performance-baseline`
- Commit tested: `b9b3bac`
- Scenario: `testops/scenarios/nvme-tcp-performance-baseline-chain.yaml`
- Run command: `C:\work\swblock.exe run -env product_root=/tmp/seaweed_block testops/scenarios/nvme-tcp-performance-baseline-chain.yaml`
- Run id: `20260702-164616-ea17`
- Bundle path: `C:\work\seaweed_block\results\20260702-164616-ea17`
- Remote artifact path: `/mnt/smb/work/share/g15d-k8s/20260702-164616-ea17-phase120-nvme-perf`
- Image mode: local images (`sw-block:local`, `sw-block-csi:local`) built and imported by the gate
- Synced product root: `m02:/tmp/seaweed_block` from Git archive `b9b3bac`

## Re-validation Result

The scenario failed in `run_gate` before `assert_summary` ran because the gate
did not append `cleanup_status=ok` or the final
`phase120_nvme_tcp_performance_baseline_status=ok` row.

Runner state:

```text
pre_clean=pass
run_gate=fail
collect=pass
scenario_status=FAIL
```

Runner failure excerpt:

```text
ERROR: phase "run_gate" failed: action 0 (exec) failed: exec: code=1
```

The runner stderr only surfaced Docker legacy-builder warnings, but the artifact
summary and cleanup verifier show the actual terminal condition: all baseline
checks completed, then in-gate cleanup verification failed on Kubernetes residue.

## Re-validation Full Summary Rows

```text
phase120_nvme_tcp_performance_baseline_status=running
protocol=nvme
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_claim_allowed=false
performance_slo_claim_allowed=false
perf_gate_type=baseline_no_slo
go_test_blockvolume_sw_block=pass
image_ready=true
generated_external_nvme=true
blockvolume_node=m01
app_node=m02
helm_template_external_nvme=true
helm_install=pass
pvc_bound=true
perf_pod_ready=true
managed_volume_status=ready
managed_volume_reason=first_volume_verified
publish_target_loopback=false
publish_target=192.168.1.181:4420
marker_verify_ms=197
marker_verified=true
seq_size_mib=64
seq_write_duration_ms=750
seq_write_mibps=85.33
seq_read_duration_ms=219
seq_read_mibps=292.24
small_write_ops=256
small_write_block_bytes=4096
small_write_duration_ms=302
small_write_iops=847.68
small_write_mibps=3.31
final_data_verified=true
```

Missing required PASS rows:

```text
cleanup_status=ok
phase120_nvme_tcp_performance_baseline_status=ok
```

## Re-validation Cleanup Evidence

In-gate cleanup verifier rows:

```text
cleanup_status=failed
helm_release=sw-block
helm_namespace=kube-system
iqn_substr=io.seaweedfs
k8s_residue_count=1
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=1
cleanup_observed_at=2026-07-02T23:49:28Z
```

In-gate cleanup failure:

```text
kubernetes_sw_block_resources_present
pod/sw-block-csi-node-55jt2
```

Post-exit cleanup verifier rows:

```text
cleanup_status=ok
helm_release=sw-block
helm_namespace=kube-system
iqn_substr=io.seaweedfs
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
cleanup_observed_at=2026-07-02T23:50:34Z
```

Final residue checks found no `sw-block` Helm release, no `sw-block` pods/PVCs/PVs
or namespaces, no `swblock*` CRDs, and no `sw-block` admission policies.

## Re-validation Lab Notes

- m01 and m02 were `Ready`; tp01 was `NotReady` but not used by this gate.
- m02 root disk was 62% used.
- No pre-existing `sw-block` Helm, pod/PVC/PV/namespace, CRD, or admission-policy residue was present before the run.
- The remote product root was synced from `b9b3bac` before running; the synced gate script contained the fixed `managed_volume_status=ready` check.

## RE-VALIDATION (fb19f58) - PASS

Verdict: **PASS**

## Re-validation Scope

- Branch tested: `phase120-nvme-tcp-performance-baseline`
- Commit tested: `fb19f5894262cb598f0d321538242fac186e4358`
- Scenario: `testops/scenarios/nvme-tcp-performance-baseline-chain.yaml`
- Run command: `C:\work\swblock.exe run testops/scenarios/nvme-tcp-performance-baseline-chain.yaml -env product_root=/tmp/seaweed_block`
- Run id: `20260702-165514-8196`
- Bundle path: `C:\work\seaweed_block\results\20260702-165514-8196`
- Remote artifact path: `/mnt/smb/work/share/g15d-k8s/20260702-165514-8196-phase120-nvme-perf`
- Image mode: local images (`sw-block:local`, `sw-block-csi:local`) built and imported by the gate
- Synced product root: `m02:/tmp/seaweed_block` from Git archive `fb19f58`

## Re-validation Result

The scenario passed all phases and assertions:

```text
=== nvme-tcp-performance-baseline-chain === PASS (3m0.599s)
pre_clean=PASS
run_gate=PASS
assert_summary=PASS
collect=PASS
actions=24 passed, 0 failed
```

Asserted summary keys all matched:

```text
phase120_status_ok=1
protocol_nvme=1
frontend_transport_tcp=1
performance_claim_denied=1
roce_claim_denied=1
seq_write_metric=1
seq_read_metric=1
small_write_metric=1
final_data_verified=1
cleanup_ok=1
```

## Re-validation Full Summary Rows

```text
phase120_nvme_tcp_performance_baseline_status=running
protocol=nvme
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_claim_allowed=false
performance_slo_claim_allowed=false
perf_gate_type=baseline_no_slo
go_test_blockvolume_sw_block=pass
image_ready=true
generated_external_nvme=true
blockvolume_node=m01
app_node=m02
helm_template_external_nvme=true
helm_install=pass
pvc_bound=true
perf_pod_ready=true
managed_volume_status=ready
managed_volume_reason=first_volume_verified
publish_target_loopback=false
publish_target=192.168.1.181:4420
marker_verify_ms=163
marker_verified=true
seq_size_mib=64
seq_write_duration_ms=777
seq_write_mibps=82.37
seq_read_duration_ms=277
seq_read_mibps=231.05
small_write_ops=256
small_write_block_bytes=4096
small_write_duration_ms=332
small_write_iops=771.08
small_write_mibps=3.01
final_data_verified=true
cleanup_status=ok
phase120_nvme_tcp_performance_baseline_status=ok
```

## Re-validation Cleanup Evidence

In-gate cleanup verifier rows:

```text
cleanup_status=ok
helm_release=sw-block
helm_namespace=kube-system
iqn_substr=io.seaweedfs
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
cleanup_observed_at=2026-07-02T23:58:11Z
```

Final residue checks found no `sw-block` Helm release, no `sw-block` pods/PVCs/PVs
or namespaces, no `swblock*` CRDs, no `sw-block` admission policies, and no
Seaweed NVMe subsystem entries.

## Re-validation Lab Notes

- m01 and m02 were `Ready`; tp01 was `NotReady` but not used by this gate.
- m02 root disk was 63% used.
- No pre-existing `sw-block` Helm, pod/PVC/PV/namespace, CRD, or admission-policy residue was present before the run.
- The remote product root was synced from Git archive `fb19f58`; the synced gate script contained `SW_BLOCK_CLEANUP_WAIT_SECONDS="${SW_BLOCK_CLEANUP_WAIT_SECONDS:-180}"`.
