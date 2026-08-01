# Phase 171 D1 Checkpoint Correctness QA

Validate the exact assigned commit on Linux. Do not substitute an in-process
fd close for the SIGKILL crash windows.

## Environment

- Linux amd64
- Go 1.25 or the repository-supported newer version
- CGO enabled for `-race`
- no Kubernetes deployment is required

## Command

```bash
bash scripts/run-phase171-checkpoint-correctness-gate.sh
```

## Required Evidence

```text
focused_repetitions=20
focused_status=pass
sigkill_windows=after_extent_sync,after_checkpoint_pwrite,after_checkpoint_sync,after_tail_publish
sigkill_repetitions=20
sigkill_status=pass
race_repetitions=10
race_status=pass
storage_recovery_transport_replication_regression=pass
go_vet_storage=pass
checkpoint_metadata_durable_before_tail_reuse=true
stale_or_corrupt_dirty_record_fails_closed=true
close_lifecycle_fence=true
direct_base_ownership_restart_safe=true
phase171_checkpoint_correctness_status=ok
```

Report FAIL for any acknowledged-data loss, checkpoint/tail progress after a
failed metadata publication, dirty-state deletion after record corruption,
mutation escaping the Close fence, direct BASE bytes reverting after restart,
race finding, or regression failure.
