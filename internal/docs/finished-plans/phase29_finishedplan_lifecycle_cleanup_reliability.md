# Finished Plan: Phase 29 - Lifecycle/Cleanup Reliability Hardening

Closed: 2026-05-24

Verdict: PASS.

## Delivered Claim

For the documented alpha loops, cleanup outcome is deterministic and auditable:
the same run either proves clean residue with stable evidence fields or fails
closed with explicit reason/evidence.

Phase 29 does not add new HA or protocol semantics. It hardens the existing
Kubernetes product loop:

```text
install -> run multi-volume HA loops -> cleanup
-> residue check is deterministic
-> evidence vocabulary is stable
-> no helper TOCTOU race masks real state
```

## Delivered Work

- D1 cleanup ownership matrix:
  `internal/docs/ref/phase29-cleanup-ownership-matrix.md`
- D2 helper TOCTOU cleanup fix:
  `scripts/run-multi-volume-example.sh`
- D3 lifecycle evidence contract:
  `internal/docs/ref/phase29-lifecycle-evidence-contract.md`
- D4 deterministic cleanup QA replay:
  `internal/docs/qa-assignments/phase29-deterministic-cleanup-qa-validation.md`
- D5 close report:
  `internal/docs/qa-assignments/phase29-lifecycle-cleanup-reliability-close-report.md`

## Evidence

D2 dev regression:

- `20260524-140609-c204`, PASS, 29/29 actions.
- `20260524-141408-35e3`, PASS, 29/29 actions.
- `20260524-141615-7be6`, PASS, 29/29 actions.
- `20260524-141814-83f6`, PASS, 29/29 actions.

D3 targeted tests:

```text
go test ./core/ops ./cmd/sw-block
```

Result: PASS.

D4 independent QA replay:

- readiness: `20260524-152543-25d9`, PASS, 35/35.
- reattach recovery: `20260524-152815-3a6d`, PASS, 29/29.
- mounted failover: `20260524-153215-0d1e`, PASS, 48/48.
- interleaved failover: `20260524-153618-bf3c`, PASS, 56/56.
- cleanup residue: `20260524-153905-b0ca`, PASS, 13/13.

Total QA replay: 181/181 actions.

D5 hardening:

- `verify-helm-cleanup.sh` now emits `iscsi_residue_count`.
- `cleanup-residue-chain.yaml`: `20260524-215539-4285`, PASS, 13/13.

## Product Surfaces

Cleanup evidence is now aligned across:

- helper `cleanup-summary.txt`,
- `sw-block ops report` `summary.txt`,
- dashboard HTML `Lifecycle Cleanup`,
- `operator-snapshot.json` read-only cluster cleanup block.

Stable fields:

- `cleanup_status`
- `k8s_residue_count`
- `iscsi_residue_count`
- `multipath_residue_count`
- `process_residue_count`
- `hostpath_residue_count`
- `failure_count`
- `failed_phase`, when known
- `cleanup_evidence` / `evidence_ref`

## Important Non-Claims

- No production lifecycle SLO.
- No mutating operator cleanup action.
- No rebuild/failback implementation.
- No backup/snapshot/restore implementation.
- No new NVMe ANA claim.
- No broad upgrade safety claim.

## Followups

- Model/control-plane hardening should move helper-owned lifecycle decisions
  into product-owned read models before mutating operator workflows.
- Runner-native cleanup/wait actions can reduce shell-helper orchestration.
- Returned-replica rebuild/reintegration/failback remains the next major HA
  functional gap after model stability.

