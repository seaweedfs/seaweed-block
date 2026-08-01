# Phase 174 D2 Mounted NVMe Phase Profile QA Sign-off

Verdict: **PASS.** The shipped Linux kernel mounted path uses inline NVMe Write
capsules for this workload, not the synthetic gate's R2T shape. No frontend
architecture candidate is selected.

## Source And Evidence

```text
source_commit=415942e2796590304c255dd8b8950f4aa3c327fc
host=m02
initiator=linux_kernel_nvme_tcp
workload=mounted_ext4_sequential_write
frontend_network_class=100gbe_tcp
artifact=/mnt/smb/work/share/g15d-k8s/20260801T113154Z-phase174-mounted-nvme.tar.gz
sha256=b4ff3807416a3405e055df702d96cc67f2364be2e280909a051075754a84f9d3
```

The gate installed the normal external NVMe/TCP path, mounted an ext4 PVC from
the Linux kernel initiator, measured a 256 MiB sequential write, verified the
read data, deleted the PV before CSI teardown, and returned the lab to zero
residue.

## Counter Reconciliation

The `/status/nvme` snapshots before and after the measured write reported:

```text
mounted_nvme_write_ops=8200
mounted_nvme_inline_write_ops=8200
mounted_nvme_r2t_write_ops=0
mounted_nvme_write_bytes=268509184
mounted_nvme_phase_counter_reconciliation=true
synthetic_fixed_work_write_shape=r2t
mounted_write_shape=inline
synthetic_fixed_work_write_shape_matches_mounted=false
```

Capsule receive/parse, dispatch, handler, completion queue, and completion-send
counts each equal all 8,200 writes. R2T collection equals the zero R2T writes,
and no H2C data was reported. This is a valid inline shape, not missing work.

## Mounted Attribution

```text
seq_write_mibps=156.00
seq_read_mibps=478.50
capsule_receive_parse=57.553 us/op
r2t_collection=0.000 us/op
dispatch_wait=43.502 us/op
handler=3037.612 us/op
completion_queue_wait=26.420 us/op
completion_send=40.316 us/op
server_phase_total=3205.404 us/op
mounted_nvme_dominant_phase=handler
```

These are accumulated concurrent durations and must not be compared directly
with wall-clock latency. The mounted workload also differs from the synthetic
fixed-work operation shape, so no cross-workload throughput ratio is allowed.

## Decision

- The synthetic loopback test client's R2T collection cost is not representative
  of this shipped mounted kernel path. An R2T optimization is rejected as a
  Phase 174 product candidate.
- The mounted handler bucket dominates, but it includes the durable backend
  call and does not isolate a new frontend ownership problem. Existing adapter
  and backend evidence does not provide a stable qualifying candidate.
- `mounted_shape_comparable=false`,
  `fixed_work_throughput_ratio_allowed=false`, and
  `architecture_candidate_selected=false` remain required.
- `final_data_verified=true`, `pv_deleted_before_csi_uninstall=true`, and
  `cleanup_status=ok` passed. Independent post-run checks found no Helm,
  pod/PVC/PV, SwBlock CRD, or NVMe subsystem residue.

The failed predecessor run at `d111454` is retained as harness evidence: the
product path and cleanup passed, but the wrapper incorrectly required a
positive R2T count. Commit `415942e` fixed only that gate assumption and the
exact-commit rerun passed.
