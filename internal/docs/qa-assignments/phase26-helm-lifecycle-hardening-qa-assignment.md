# QA Assignment - Phase 26 Helm Lifecycle Hardening

Date: 2026-05-22

Owner: QA

Dev branch: `phase26-helm-lifecycle-hardening`

Dev status: 100% implementation complete. QA must independently validate before
this branch is treated as release-ready.

## Scope

Validate the Phase 26 product claim:

```text
Helm chart hygiene
-> Helm install
-> first PVC
-> narrow upgrade / rollback smoke
-> multi-PVC Day-1 smoke
-> read-only support-bundle replay
-> Helm uninstall and host cleanup
```

## Out Of Scope

- CRD/operator lifecycle.
- Mutating admin actions.
- Production-grade upgrade safety beyond the one gated smoke.
- Backup/snapshot/restore.
- Rebuild/failback.
- New HA or recovery semantics.
- Performance/RTO/SLO claims.

## Lab Prerequisites

- Use the 3-node k3s lab unless explicitly testing single-node fallback.
- Nodes should be Ready and schedulable.
- `helm` installed on the runner node.
- `KUBECONFIG=/etc/rancher/k3s/k3s.yaml` works on m02.
- No active `io.seaweedfs` iSCSI sessions before starting.
- No leftover `sw-block` Helm release before starting.

## Source Sync

QA should test the branch contents, not an older image or stale `/tmp` tree.

Recommended sync from Windows controller:

```powershell
cd C:\work\seaweed_block
git status --short --branch
tar --exclude='.git' --exclude='.gocache' --exclude='.gotmp' --exclude='results' --exclude='tmp' --exclude='work/test_server' --exclude='*.iso' -czf /tmp/sw-block-phase26-qa.tgz .
scp -i C:\work\dev_server\testdev_key -o StrictHostKeyChecking=no /tmp/sw-block-phase26-qa.tgz testdev@192.168.1.184:/tmp/sw-block-phase26-qa.tgz
ssh -i C:\work\dev_server\testdev_key -o StrictHostKeyChecking=no testdev@192.168.1.184 "rm -rf /tmp/seaweed_block && mkdir -p /tmp/seaweed_block && tar -xzf /tmp/sw-block-phase26-qa.tgz -C /tmp/seaweed_block"
```

## Required Gates

Run all four scenarios from the synced branch.

```powershell
cd C:\work\seaweed_block

C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results\phase26-qa \
  testops\scenarios\helm-release-hygiene-chain.yaml

C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results\phase26-qa \
  testops\scenarios\helm-lifecycle-upgrade-rollback-chain.yaml

C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results\phase26-qa \
  testops\scenarios\helm-multi-volume-day1-chain.yaml

C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results\phase26-qa \
  testops\scenarios\helm-support-bundle-diagnostics-chain.yaml
```

## Hard-Gate Acceptance

All are required for QA PASS:

- D1 chart release hygiene scenario passes all actions.
- D2 Helm lifecycle scenario passes all actions.
- D2 shows the same PV before upgrade, after upgrade, and after rollback.
- D3 multi-volume scenario passes all actions.
- D3 summary includes:
  - `multi_volume_status=ok`
  - `requested_volume_count=3`
  - `writer_verified_count=3`
  - `reader_verified_count=3`
  - `managed_volume_count=3`
  - `cleanup_status=ok`
- D4 support-bundle scenario passes all actions.
- D4 support bundle summary includes:
  - `support_bundle_status=ok`
  - `report_status=ok`
  - `explain_status=ok`
  - `timeline_status=ok`
  - `read_only=true`
- D4 blocked bundle explains `reason=csi_node_image_pull_failed`.
- Final residue audit is clean:
  - no active `io.seaweedfs` iSCSI sessions,
  - no `blockmaster`, `blockvolume`, `blockcsi`, or `iscsi-target` processes,
  - no leftover Helm release `sw-block`,
  - no generated `app=sw-blockvolume` Deployments.

## Blocking Findings

Treat any of these as blocking:

- Any scenario action fails.
- Any cleanup phase leaves sessions/processes/resources.
- D2 changes the existing PV identity across upgrade/rollback.
- D3 verifies fewer than three writer/reader PVCs.
- D3 report does not show three ManagedVolume rows.
- D4 cannot replay report/explain/timeline from a saved bundle.
- D4 support bundle lacks Helm/K8s/log/iSCSI evidence.
- D4 blocked bundle does not name a stable reason code.
- README, quickstart, release note, and close report make broader claims than
  the validated gates.

## Non-Blocking Findings

Treat as non-blocking unless they hide evidence:

- Cosmetic wording in reports.
- Repeated `placement_verified` timeline events.
- CRLF warnings from Windows checkout.
- Runtime variation if the hard evidence still passes.

## PM Review Targets

PM should review these files for product language and claim boundary:

- `README.md`
- `docs/quickstart-kubernetes.md`
- `docs/releases/v0.3.1-alpha.md`
- `internal/docs/qa-assignments/phase26-helm-lifecycle-hardening-close-report.md`

PM should verify the wording says:

- Helm is the supported alpha install path.
- One narrow upgrade/rollback smoke is gated.
- Broad production upgrade safety is not claimed.
- Support bundle/report/dashboard are read-only.
- Operator/CRD lifecycle is not included in this phase.

## Expected QA Output

QA should produce or update a close report with:

- run IDs for all four scenarios,
- per-gate action counts,
- key artifact paths,
- PV identity evidence from D2,
- three-volume evidence from D3,
- support-bundle replay evidence from D4,
- final residue audit,
- PASS/FAIL verdict.

