# Finished Plan: Phase 30 - Control Model / ManagedVolume Hardening

Closed: 2026-05-25

Verdict: PASS.

## Delivered Claim

The Kubernetes block product has an explicit control-state read model before
adding mutating operator, rebuild/failback, NVMe ANA, or backup workflows.

Phase 30 delivered:

```text
PVC/PV + ManagedVolume + Launcher + CSI + Authority + HostPath + Cleanup
-> explicit fact authorities
-> explicit domain masters / projections
-> explicit read-only and dry-run action contracts
-> cleanup evidence with one projection owner
-> regression gates proving product loops still work
```

This is a core-stability and operational-hardening phase. It does not add a new
user-facing HA capability.

## Delivered Work

- D1 control-state dependency review:
  `internal/docs/ref/phase30-control-state-dependency-review.md`
- D2 ManagedVolume field/action contract tightening:
  `core/ops/managed_volume_contract.go`,
  `core/ops/managed_volume_contract_test.go`,
  `internal/docs/ref/managed-volume-operational-model-contract.md`
- D3 cleanup projection ownership cleanup:
  `core/ops/cleanup_evidence.go`,
  `core/ops/cleanup_evidence_test.go`,
  `internal/docs/ref/phase30-cleanup-projection-ownership.md`
- D4 regression gates:
  `helm-first-volume-via-sw-block-cli-chain.yaml`,
  `helm-multi-volume-day1-chain.yaml`,
  `helm-multi-volume-rf3-reattach-recovery-chain.yaml`,
  `cleanup-residue-chain.yaml`
- D5 close report:
  `internal/docs/qa-assignments/phase30-control-model-managed-volume-hardening-close-report.md`

## Evidence

Targeted tests:

```text
go test ./core/ops ./cmd/sw-block
```

Result: PASS.

Regression gates:

- `helm-first-volume-via-sw-block-cli-chain.yaml`:
  `20260525-004305-a872`, PASS, 34/34 actions.
- `helm-multi-volume-day1-chain.yaml`:
  `20260525-004405-c28a`, PASS, 29/29 actions.
- `helm-multi-volume-rf3-reattach-recovery-chain.yaml`:
  `20260525-004612-d656`, PASS, 29/29 actions.
- `cleanup-residue-chain.yaml`:
  `20260525-005026-fe98`, PASS, 13/13 actions.

Total TestOps regression actions: 105/105 PASS.

## Product Model Result

ManagedVolume field contract:

- identity,
- desired replication/ACK/profile/protocol,
- Kubernetes PVC state,
- placement,
- authority primary/epoch/endpoint/publish target,
- replica durable frontier,
- CSI stage target,
- host-path RTPG/stale-path evidence,
- workload verification,
- cleanup evidence,
- reason code.

ManagedVolume action contract:

- `observe.collect_bundle`
- `observe.wait_for_pvc_bound`
- `observe.inspect_mount_failure`
- `observe.inspect_host_path`
- `safe_k8s.reinstall_external_iscsi`
- `safe_k8s.import_csi_image`
- `authority.request_promotion` as disabled contract shape only

All actions are read-only, dry-run, or disabled in Phase 30.

## Important Non-Claims

- No mutating operator action.
- No repair/rebuild/failback implementation.
- No backup/snapshot/restore implementation.
- No NVMe ANA implementation.
- No production SLO or broad distro compatibility claim.
- No new multi-volume HA claim beyond already-gated Phase 27 behavior.

## Followups

- Use this model as the boundary for the next operator-grade operations phase.
- Extend projection ownership cleanup to CSI reattach vs host-path transparent
  recovery classification.
- Define explicit executor policy gates before any mutating cleanup, repair,
  rebuild, failback, or promotion action becomes executable.
- Keep helper scripts as executors/test drivers until operator policy and
  evidence gates exist.

