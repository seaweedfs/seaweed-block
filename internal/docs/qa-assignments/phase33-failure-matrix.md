# Phase 33 Failure Matrix

Status: D1 complete; F1/F2/F4/F5 implementation evidence landed; D4
cleanup/replay gate passed as of 2026-05-29.

Purpose: turn Phase 33 from "test failures more" into executable release gates.
Every failure below must prove the same product rule:

```text
No false Ready=True.
Stable reason code.
Useful evidence.
Deterministic cleanup.
```

## Scope

In scope:

- TestOps scenarios and helper hardening.
- Read-only status/report/dashboard/explain agreement.
- Support-bundle replay from failed or partial evidence.
- Cleanup verification after blocked runs.

Out of scope:

- New HA feature claims.
- Rebuild/failback implementation.
- NVMe ANA expansion.
- Mutating operator/admin actions.
- Broad model or protocol refactor.

## Required Surfaces

Each release-blocking gate must check these surfaces unless the trigger happens
before the surface can exist:

- `summary.txt`
- `cluster-evidence.json`
- `operator-snapshot.json`
- dashboard `/operator-snapshot.json`
- `sw-block ops explain`
- cleanup summary / verifier output

## Matrix

| ID | Failure Class | Trigger | Expected Status / Reason | Required Evidence | Cleanup Gate | Source / Scenario Shape | Priority |
|---|---|---|---|---|---|---|---|
| F1 | CSI node image pull failure | Install with invalid CSI image or missing local image import | `Blocked`, `Ready=False`, `reason=csi_node_image_pull_failed` | K8s pods/events, support bundle, report, operator snapshot, explain dry-run action | zero k8s/iSCSI/multipath/process residue after cleanup | PASS: `helm-support-bundle-diagnostics-chain.yaml` run `20260528-190738-51a2`, 49/49 actions | P0 |
| F2 | Blockmaster/API unreachable / status endpoint unreachable | Stop or block access to status evidence before status/report capture | `Unknown`, no `Ready=True`, reason `status_endpoint_unreachable` | report, explain, dashboard, operator snapshot showing inability to prove readiness; live port-forward/log artifact for future master-specific gate | cleanup still succeeds without master API | PASS: replay scenario `status-endpoint-unreachable-replay-chain.yaml` run `20260529-155016-e9a5`, 17/17 actions; live master-specific scenario still optional | P0 |
| F3 | Evidence stale / reconverging snapshot | Replay bundle where newest evidence is post-restart but not fully reconverged | `Ready=Unknown`, reason `evidence_stale` or `unknown`; never false `Ready=True` | multiple cluster snapshots with timestamps, replay output, dashboard snapshot | replay-only; no live residue expected | Existing D7 replay behavior; convert to explicit scenario or component gate | P0 |
| F4 | Corrupt or partial bundle evidence | Put corrupt `cluster-evidence.json` beside a newer valid snapshot, or remove optional artifacts | replay skips corrupt candidate; status comes from newest valid evidence; if none valid, fail with clear error | corrupt file, valid file, replay stderr/stdout, report artifacts | replay-only; no live residue expected | Component gate around `BuildObservationFromBundle` plus CLI replay test | P0 |
| F5 | Cleanup residue after failed install/run | Intentionally leave iSCSI node DB, multipath map, or generated Deployment | cleanup status fails before scrub, then passes after cleanup; reason names residue class | `cleanup-summary.txt`, iSCSI nodes, multipath/dmsetup, k8s resources | all residue counters zero after final cleanup | PASS: `cleanup-residue-chain.yaml` run `20260529-155040-4519`, 13/13 actions; cleanup verifier contract test locks residue counters and reason vocabulary | P0 |
| F6 | Multi-volume cross-interference | Fail/restart one or two RF=3 volumes while another volume remains mounted/readable | target volumes recover or block independently; untouched volume remains stable; no mixed primary/publish target | per-volume summaries, operator snapshot with 3 identities, cross-interference flag | no leaked pods/deployments/iSCSI/multipath | Existing: RF3 interleaved and app-spread failover scenarios | P1 |
| F7 | Restart during or shortly after promotion | Restart k3s/product after promotion but before observation fully settles | authority does not roll back; report either shows correct primary or `Ready=Unknown`, never old primary as Ready | before/after primary, epoch, publish target, report replay, reader verification | hostPath and iSCSI cleanup zero | Existing restart persistence scenarios; add timing variant if needed | P1 |
| F8 | Loopback publish target on cross-node attach | Force multi-node pod attach while publish target is `127.0.0.1` on another node | safe refusal / blocked attach; no fake success; stable loopback/cross-node reason | CSI node logs, pod describe FailedMount, report/explain reason | cleanup removes PVC, sessions, node DB | Existing negative attach lineage; add Helm Day-1 variant if not covered | P1 |

## Hard Gate Rules

For P0 gates:

- The scenario must fail closed if the expected blocker is not observed.
- The scenario must assert absence of `Ready=True` in the blocked/stale path.
- Reason code must match across at least three surfaces:
  `summary.txt`, `operator-snapshot.json`, and `explain`.
- Suggested actions must be `read_only` or `dry_run`.
- Cleanup must be checked after the failure, not only before the next run.

For P1 gates:

- The scenario may be longer-running or lab-shape-dependent.
- It must still produce a cold-readable bundle and cleanup summary.
- It must not widen the public release claim unless promoted to P0 and rerun.

## D2 Implementation Queue

Minimum runner/helper work to make the P0 gates maintainable:

1. Standardize failure snapshot capture for Helm/K8s runs.
2. Add a helper or runner action for JSONPath waits where conditions do not
   exist, especially PVC phase and pod/job completion.
3. Ensure support-bundle collection marks required capture failures as failed.
4. Keep cleanup verifier as the final source of truth for residue counters.
5. Add replay tests for corrupt/partial evidence selection.

## Release Close Requirement

Phase 33 cannot close unless:

```text
F1-F5 PASS as release-blocking gates
minimal new-user validation still PASS
cleanup-residue gate PASS after negative gates
no public doc claims new HA/rebuild/operator features
```

F6-F8 may close as optional extended evidence or carry forward to the next
hardening cycle.
