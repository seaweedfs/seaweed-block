# Phase 174 D2 Mounted NVMe Phase Profile QA

## Purpose

Cross-check the Phase 174 loopback test-client attribution with the shipped
path: Linux kernel NVMe/TCP initiator, ext4, mounted file write, durable target,
and normal PVC/PV teardown. This is a shape diagnostic, not a throughput ratio
against the synthetic fixed-work gate.

## Command

Run from an exact source commit on m02 with the Phase120 external-NVMe lab
settings used by the existing mounted baseline:

```bash
SW_BLOCK_ARTIFACT_DIR=/mnt/smb/work/share/g15d-k8s/<run-id>-phase174-mounted-nvme \
SW_BLOCK_PHASE174_SOURCE_COMMIT=<full-source-commit> \
SW_BLOCK_FRONTEND_IP_MAP=m01=10.0.0.1,m02=10.0.0.3 \
SW_BLOCK_EXPECTED_FRONTEND_ROUTE_DEV=enp1s0np0 \
SW_BLOCK_IMPORT_K3S_SSH_KEY=/opt/work/testdev_key \
  bash scripts/run-phase174-mounted-nvme-phase-profile-gate.sh "$PWD"
```

## Required Checks

1. The kernel initiator connects to the external NVMe/TCP target and the
   mounted writer/reader checks pass.
2. `/status/nvme` is read before and after only the measured sequential write.
3. Write operations equal capsule receive/parse, dispatch, handler, completion
   queue, and completion-send operations. R2T collection operations equal R2T
   Write commands; H2C bytes equal R2T bytes.
4. Each phase has an accumulated duration and the dominant mounted phase is
   recorded. Do not compare accumulated concurrent phase time to wall time.
5. Keep `mounted_shape_comparable=false`,
   `fixed_work_throughput_ratio_allowed=false`, and
   `architecture_candidate_selected=false`. A candidate requires compatible
   same-session controls, not a ratio across different workloads.
6. Writer/read data verification, exact PVC/PV teardown, NVMe disconnect, Helm
   uninstall, and `cleanup_status=ok` all pass.

## Verdict

- PASS: all counters reconcile, mounted data is correct, and cleanup is zero.
- FAIL: missing/contradictory counters, data failure, endpoint identity error,
  or residue.
- The result may nominate a same-session control for D3, but cannot select an
  architecture implementation by itself.
