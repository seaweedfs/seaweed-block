# Phase 150 QA Sign-off: WAL Multi-Block Runtime Opt-In

Status: **PASS**.

Branch: `phase150-wal-multiblock-runtime-opt-in`.

## Scope

Phase 150 wires the Phase 148/149 multi-block WAL record prototype into runtime
configuration behind an explicit default-off opt-in. It does not enable the
feature by default, does not change H2C defaults, and does not claim mounted
NVMe/TCP performance.

Runtime opt-in:

```text
--durable-wal-multiblock-records
```

Launcher/Helm opt-in:

```text
blockmaster.durableWALMultiBlockRecords=true
```

## Checks

```text
bash -n scripts/run-phase150-wal-multiblock-runtime-opt-in-gate.sh
bash scripts/run-phase150-wal-multiblock-runtime-opt-in-gate.sh
go test ./core/storage ./core/frontend/durable ./core/launcher ./cmd/blockvolume ./cmd/blockmaster \
  -run 'Phase150|MultiBlock|K8sRenderer_RendersBlockVolumeDeploymentArgs' -count=1
helm template sw-block charts/seaweed-block --namespace kube-system
helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.durableWALMultiBlockRecords=true
```

Result:

```text
ok  	github.com/seaweedfs/seaweed-block/core/storage
ok  	github.com/seaweedfs/seaweed-block/core/frontend/durable
ok  	github.com/seaweedfs/seaweed-block/core/launcher
ok  	github.com/seaweedfs/seaweed-block/cmd/blockvolume
ok  	github.com/seaweedfs/seaweed-block/cmd/blockmaster
```

## Summary

```text
phase150_wal_multiblock_runtime_opt_in_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_default=false
explicit_opt_in_reaches_walstore=true
single_block_compatibility=pass
current_recovery_compatibility=pass
helm_default_omits_opt_in=true
helm_explicit_renders_opt_in=true
phase150_decision=mounted_profile_next
next_recommendation=phase151_wal_multiblock_mounted_nvme_profile
cleanup_status=ok
```

## Verdict

Phase 150 passes. The runtime opt-in is wired and default-off. Phase 151 can run
a mounted NVMe/TCP profile with the opt-in enabled. Until that gate passes,
multi-block WAL records remain an experimental opt-in and not a release claim.
