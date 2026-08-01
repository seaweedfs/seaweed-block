# Phase 175 D7 Restore Abort And Discard Live QA

Validate exact commit `31ac6a3` from an isolated clean worktree. Use fresh,
matching controller and CSI images on the m02 k3s lab. Follow
`QA-AGENT-RUNBOOK.md`. Component tests and helper summaries are prerequisites,
not proof of this gate.

## Required Live Path

1. Create a source PVC, mount it, and write identifiable data.
2. Create a real ready `VolumeSnapshot` and start restore to a distinct target.
3. Hold or fault the target in a real durable `restore_pending` window.
4. Request target deletion. The first delete must return a hold and durably
   record `restore_abort_requested` with one immutable operation ID.
5. Verify old target blockvolume Deployments and Pods are absent before any
   restore-discard Job begins.
6. Verify each cleanup Job is pinned to the recorded Kubernetes node, mounts
   only the exact replica hostPath leaf, has no service-account token, uses
   `restartPolicy: Never`, `backoffLimit: 0`, and
   `activeDeadlineSeconds: 120`.
7. Verify the target never gains authority or reports Ready during abort. The
   source PVC and source snapshot must remain readable and unchanged.
8. Verify the exact target data and restore-marker files are removed and the
   durable discard receipt/evidence matches operation, snapshot, volume,
   replica, backend identity, and geometry.
9. Verify every replica reaches terminal discard evidence, all owned Jobs and
   Pods disappear, the target reaches `restore_discarded`, and a retry of
   `DeleteVolume` succeeds.
10. Run supported uninstall and require `cleanup_status=ok` with zero catalog,
    hostPath, CRD, PVC/PV, session, multipath, Job, Pod, and process residue.

## Dirty Failure Requirement

Exercise at least one real failed cleanup attempt, malformed terminal
evidence, execution-fence loss, or Job deadline. It must persist a reason and
either retry after bounded backoff or enter `terminal_failure`. It must never
publish discard success from missing or malformed evidence.

## PASS Rules

- Foreground Job deletion must be observed against the real Kubernetes API;
  the volume cannot become discarded while a Job or its Pod remains.
- Assertions must cross-check Kubernetes objects, lifecycle/control status,
  and host files. A script echo followed by a grep of the same value is not
  evidence.
- If a real pending restore window cannot be produced, report **PARTIAL**. Do
  not replace the path with replay and call it live.

Write the sign-off to
`phase175-d7-restore-abort-discard-qa-signoff.md`, preserving exact commit,
commands, evidence path/hash, failure classification, and final cleanup.
