# Current Plan: Phase 31 - Kubernetes Restart Persistence

Status: active, 33% complete. Started on 2026-05-25 after Phase 30 control model
hardening closed.

## Product Goal

Make the Kubernetes block product survive normal cluster restart without
forgetting storage data or control-plane state.

The required product behavior is:

```text
PVC writes data
-> blockvolume / master / CSI / k3s restart
-> persisted authority/lifecycle/durable state is reloaded
-> launcher reconciles the existing desired state
-> CSI reattaches to the current publish target
-> reader verifies the same PVC data
-> report proves epoch, primary, promotion history, target, and cleanup state
```

If a volume was promoted before the restart, restart must preserve the promoted
primary/epoch/publish target. The product must not fall back to an old primary
or remint authority from stale topology.

## Scope Contract

| In | Out |
|---|---|
| durable Helm configuration for master authority/lifecycle and blockvolume walstore state | deleting/recreating a Kubernetes cluster from scratch |
| single-node k3s restart persistence gate | cloud disaster recovery |
| RF3 sync-quorum multi-node restart persistence gate | backup/snapshot/restore |
| promotion-before-restart persistence gate | returned-replica rebuild/failback |
| report/dashboard/operator evidence for restart recovery | new mutating operator action |
| cleanup/residue verification after restart gates | broad production SLO |

This phase is required before claiming a usable beta storage product. It is not
optional polish.

## D1: Restart Persistence Contract Review

Goal: define exactly which state must persist across restart and who owns it.

Acceptance:

- Define required persisted state:
  - blockvolume durable data,
  - master authority store,
  - lifecycle/PVC registration store,
  - launcher desired placement inputs,
  - current primary/epoch/endpoint_version/publish target,
  - CSI attach/stage recovery evidence.
- Identify current default gaps:
  - Helm default `blockmaster.stateHostPath=""` means master state uses
    `emptyDir`,
  - generated blockvolume state is not durable unless launcher state hostPath is
    configured,
  - complete k3s shutdown/start is not yet gated.
- Define non-claims and exact restart claim boundary.

PM/QA claim checklist:

- Use wording from
  `internal/docs/ref/phase31-restart-persistence-claim-and-qa-checklist.md`.
- Validate authority monotonicity: primary/epoch/publish target must not roll
  back.
- Validate stale-primary fencing with a direct stale path I/O probe, not only
  role text.
- Validate data continuity with writer/reader checksum before/after restart.
- Validate cross-volume isolation in the multi-volume restart smoke.

Status: PASS on 2026-05-25.

Artifacts:

- `internal/docs/ref/phase31-kubernetes-restart-persistence-contract.md`
- `internal/docs/ref/phase31-restart-persistence-claim-and-qa-checklist.md`

## D2: Durable Helm Values / Install Contract

Goal: make durable state an explicit Helm install mode.

Acceptance:

- Add or document values required for durable restart mode.
- `sw-block ops generate-helm-values` can generate a durable restart values file
  or clearly opt into one.
- Generated values set durable hostPath state for:
  - blockmaster authority/lifecycle store,
  - launcher-created blockvolume durable roots.
- Values/report surface `restart_persistence_mode=hostpath` or equivalent.
- Fast tests cover rendered Helm args and generated blockvolume hostPath layout.

Status: PASS on 2026-05-25.

Implementation:

- `sw-block ops generate-helm-values` now accepts
  `--restart-persistence ephemeral|hostpath`.
- `--restart-persistence hostpath --state-hostpath /var/lib/sw-block`
  generates:
  - `blockmaster.stateHostPath=/var/lib/sw-block`,
  - `restartPersistence.mode=hostpath`,
  - `restartPersistence.stateHostPath=/var/lib/sw-block`.
- The chart schema accepts the restart-persistence contract.

Validation:

- `go test ./cmd/sw-block`
- `go test ./core/launcher`
- `helm lint charts/seaweed-block`
- `helm template ... -f hostpath-values.yaml` renders
  `--launcher-state-hostpath=/var/lib/sw-block` and a master state hostPath
  mount.

## D3: Single-Node Restart Gate

Goal: prove the smallest user path survives a k3s restart.

Gate:

```text
Helm install with durable restart values
create one PVC
writer writes /data/demo.bin
restart k3s on the node
wait for master/CSI/blockvolume to return
reader mounts same PVC and verifies checksum
report captures authority/lifecycle/durable evidence
cleanup leaves no residue
```

Acceptance:

- Same PVC/PV survives.
- Data checksum survives.
- Authority epoch/publish target are consistent after restart.
- `sw-block ops report` contains restart evidence and no unsupported claim.

## D4: RF3 Restart After Promotion Gate

Goal: prove promotion state is not forgotten.

Gate:

```text
RF3 sync-quorum PVC
writer verifies data
controlled primary failure promotes r2
reader verifies through promoted target
restart k3s / product stack
reader verifies same PVC again
report proves primary remains r2 with epoch >= promoted epoch
old primary is not resurrected as primary
```

Acceptance:

- `before_restart_primary` equals promoted replica.
- `after_restart_primary` equals promoted replica unless a new valid failover
  happened and is evidenced.
- `after_restart_epoch >= before_restart_epoch`.
- publish target matches the post-promotion authority line.
- stale old-primary I/O success remains 0 and is measured by a direct stale
  path probe.
- reader checksum passes after restart.

## D5: Multi-Volume Restart Smoke

Goal: prove restart does not mix per-volume authority or placement.

Gate:

```text
3 PVCs, RF3 sync-quorum
each writer verifies
restart k3s/product stack
each reader verifies
report shows 3 ManagedVolumes Ready with independent primary/epoch/target
```

Acceptance:

- requested_volume_count=3
- reader_verified_count=3
- managed_volume_count=3
- duplicate_publish_target_for_distinct_volume=false unless expected by design
- cross_volume_authority_mixup=false

## D6: Close Gate

Goal: close only when restart persistence is proven and documented.

Acceptance:

- D1-D5 complete.
- QA independently reruns D3 and D4.
- README/quickstart/release note clearly distinguish:
  - default alpha quick path,
  - durable restart mode,
  - unsupported disaster recovery.
- Close report and finished plan are written.

## Progress

- D1: PASS
- D2: PASS
- D3: pending
- D4: pending
- D5: pending
- D6: pending
