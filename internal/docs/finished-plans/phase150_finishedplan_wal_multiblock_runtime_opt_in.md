# Phase 150 Finished Plan: WAL Multi-Block Runtime Opt-In

Status: **closed 2026-07-06, local gate PASS**.

## Problem

Phase 149 showed multi-block WAL records reduce encode record count locally.
The next step was to expose the prototype through runtime configuration without
changing defaults or claiming mounted NVMe/TCP performance.

## Work

Phase 150 added:

- `storage.WALStore.SetMultiBlockRecords`;
- `durable.ProviderConfig.WALMultiBlockRecords`;
- `blockvolume --durable-wal-multiblock-records`;
- blockmaster launcher flag
  `--launcher-durable-wal-multiblock-records`;
- Helm value `blockmaster.durableWALMultiBlockRecords`;
- default-off render/parse/provider tests;
- `scripts/run-phase150-wal-multiblock-runtime-opt-in-gate.sh`.

## Evidence

```text
phase150_wal_multiblock_runtime_opt_in_status=ok
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_default=false
explicit_opt_in_reaches_walstore=true
helm_default_omits_opt_in=true
helm_explicit_renders_opt_in=true
single_block_compatibility=pass
current_recovery_compatibility=pass
phase150_decision=mounted_profile_next
next_recommendation=phase151_wal_multiblock_mounted_nvme_profile
cleanup_status=ok
```

## Conclusion

The opt-in is wired but still default-off. The next phase should run a mounted
NVMe/TCP profile with `blockmaster.durableWALMultiBlockRecords=true`. No
performance or release claim should be made before that live gate.
