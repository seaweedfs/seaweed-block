# Current Plan: Phase 31 - Kubernetes Restart Persistence

Status: PASS, 100% complete. Started and closed on 2026-05-25 after Phase 30
control model hardening closed.

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
- D3 dev run exposed and fixed the blockmaster hostPath permission gap:
  the chart now adds a root `state-permissions` initContainer for hostPath
  state, matching the generated blockvolume permission model.

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

Status: PASS on 2026-05-25; independent QA rerun passed.

Scenario:

- `testops/scenarios/helm-single-node-restart-persistence-chain.yaml`

Dev evidence:

- Run `20260525-103441-2e9c`: 39/39 actions PASS in 2m39s.
- Helm values generated `restart_persistence_mode=hostpath`.
- Helm template rendered `--launcher-state-hostpath=/var/lib/sw-block/testops-...`.
- Writer/reader verified before restart.
- `sudo systemctl restart k3s` completed and blockmaster/CSI/blockvolume
  rolled out again.
- Reader verified `/data/demo.bin: OK` after restart.
- Report summary contained `managed_volume=... status=ready`.
- Cleanup removed Helm release, app resources, iSCSI residue, processes, and
  test-scoped hostPath.

QA evidence:

- Run `20260525-104016-f3d4`: 40/40 actions PASS.
- Sign-off:
  `internal/docs/qa-assignments/phase31-restart-persistence-d3-qa-signoff.md`.

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

Status: PASS (strict) on 2026-05-25; independent QA rerun passed.

Scenario:

- `testops/scenarios/helm-rf3-promotion-restart-persistence-chain.yaml`

Dev evidence:

- Run `20260525-104247-d6da`: 34/34 actions PASS in 3m33s.
- Strict rerun after scenario hardening `20260525-122104-60c3`: 34/34
  actions PASS in 2m35s.
- Helm values generated `restart_persistence_mode=hostpath`.
- Helm template rendered both:
  - `--launcher-state-hostpath=/var/lib/sw-block/testops-...`,
  - `--launcher-replication-ack=sync-quorum`.
- RF3 recovery promoted `r1 -> r2` and verified the PVC by CSI/pod recreate.
- After `sudo systemctl restart k3s`, authority comparison reported:
  - `restart_promotion_status=ok`,
  - `before_restart_primary=r2`,
  - `after_restart_primary=r2`,
  - `before_restart_publish_target=192.168.1.184:3260`,
  - `after_restart_publish_target=192.168.1.184:3260`,
  - `before_restart_epoch=2`,
  - `after_restart_epoch=2`,
  - `post_restart_primary_count=1`,
  - `reason=authority_persisted`.
- Reader verified `/data/demo.bin: OK` after restart.
- Cleanup removed Helm release, app resources, iSCSI residue, processes, and
  test-scoped hostPath.
- Scenario hardening now:
  - pre-cleans generated `app=sw-blockvolume` Deployments and refuses to start
    if they remain,
  - port-forwards to a selected Running/Ready blockmaster pod,
  - retries the product `ops cluster` call instead of relying on a raw TCP
    port readiness probe.

Known limitation:

- The D4 dev gate validates authority persistence, data continuity, and
  single-primary after restart. The direct stale old-primary I/O probe remains
  required for QA/close hardening before D6; D4 currently preserves the
  Phase 27 measured stale-fencing requirement as a close criterion rather than
  reimplementing it inside the first dev gate.
- QA product-claim sign-off is captured at
  `internal/docs/qa-assignments/phase31-restart-persistence-d4-qa-signoff.md`.
  It verified `before_restart_primary=r2`, `after_restart_primary=r2`,
  `post_restart_primary_count=1`, and post-restart reader checksum. The final
  strict QA run `20260525-122723-f7ed` passed 34/34 actions and confirmed the
  port-forward/lab-serialization hardening.

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

Status: PASS (strict) on 2026-05-25; independent QA rerun passed.

Scenario:

- `testops/scenarios/helm-multi-volume-rf3-restart-smoke-chain.yaml`

Dev evidence:

- Run `20260525-110800-4f19`: 36/36 actions PASS in 4m11s.
- Helm values generated `restart_persistence_mode=hostpath`, RF=3, and
  `ackProfile: sync-quorum`.
- Three PVCs were created and verified before restart:
  - `writer_verified_count=3`,
  - `reader_verified_count=3`,
  - product-owned `cluster-evidence.json` exported before restart.
- After `sudo systemctl restart k3s`, authority comparison reported:
  - `multi_volume_restart_status=ok`,
  - `before_volume_count=3`,
  - `after_volume_count=3`,
  - `managed_volume_count=3`,
  - `duplicate_publish_target_for_distinct_volume=false`,
  - `cross_volume_authority_mixup=false`,
  - `reason=all_volumes_persisted`.
- Three reader pods verified `/data/demo.bin: OK` after restart.
- Cleanup removed Helm release, app resources, generated blockvolume
  Deployments, iSCSI residue, processes, multipath residue, and test-scoped
  hostPath.

QA evidence:

- Run `20260525-123233-541b`: 36/36 actions PASS.
- Sign-off:
  `internal/docs/qa-assignments/phase31-restart-persistence-d5-qa-signoff.md`.
- Hard evidence:
  - `multi_volume_restart_status=ok`,
  - `before_volume_count=3`,
  - `after_volume_count=3`,
  - `managed_volume_count=3`,
  - `reader_verified_count=3`,
  - `duplicate_publish_target_for_distinct_volume=false`,
  - `cross_volume_authority_mixup=false`,
  - `cleanup_status=ok`.

Note:

- The scenario intentionally treats `scripts/run-multi-volume-example.sh` as a
  setup helper for PVC/write/read/evidence only. D5's restart assertion uses
  product-owned `ops cluster` / `ops report` after restart so helper-level
  inventory/report status does not mask restart-persistence behavior.

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

Status: PASS on 2026-05-25.

Artifacts:

- Close report:
  `internal/docs/qa-assignments/phase31-kubernetes-restart-persistence-close-report.md`
- Finished plan:
  `internal/docs/finished-plans/phase31_finishedplan_kubernetes_restart_persistence.md`

## Progress

- D1: PASS
- D2: PASS
- D3: PASS
- D4: PASS (strict)
- D5: PASS (strict)
- D6: PASS
