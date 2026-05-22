# Current Plan: Phase 26 - Helm Release Lifecycle Hardening

Status: active, 0% complete. Started on 2026-05-22 after PR #49 merged
v0.3 Helm observable first-volume alpha.

## Product Goal

Turn the v0.3 Helm-first alpha path into a more release-shaped Kubernetes
product loop:

```text
generate values
-> helm install
-> first PVC
-> multiple PVC smoke
-> read-only report/dashboard/support bundle
-> helm upgrade / rollback smoke
-> helm uninstall and host cleanup
```

This phase is about lifecycle confidence, not new HA semantics.

## Scope Contract

| In | Out |
|---|---|
| Helm install / upgrade / rollback / uninstall gates | CRD/operator implementation |
| chart version, appVersion, image tag/digest alignment | mutating dashboard/admin actions |
| single-volume and multi-volume smoke gates | new protocol or recovery capability |
| support bundle completeness for Helm installs | rebuild/failback implementation |
| chart packaging and release-note consistency | backup/snapshot/restore |
| cleanup and host residue verification | broad distro/performance/SLO claims |

Principle: Phase 26 may take small product fixes only when they block the Helm
release lifecycle. Do not refactor ManagedVolume, authority, CSI, or protocol
models unless a release gate proves the current behavior is wrong.

## Dependencies

- Phase 25 is closed: v0.3 Helm first-volume path works.
- Published immutable images are available for release validation.
- Read-only ops report/dashboard artifacts are available.
- ManagedVolume projection exists for first-volume and recovery explanations.

## D1: Chart Release Hygiene

Goal: make chart metadata and image identity release-grade enough for alpha
users and QA.

Required work:

- Align `Chart.yaml` version, `appVersion`, README, quickstart, and release
  note.
- Ensure generated values include image tags and digest evidence where
  available.
- Document immutable `sha-<commit>` as the release-validation path.
- Keep mutable `:alpha` as smoke/demo only.
- Add a chart/package validation command to the gate.

Acceptance:

```text
helm lint charts/seaweed-block PASS
helm template with generated values PASS
chart version/appVersion/image docs agree
release note names exact validated image path
```

## D2: Helm Lifecycle Gate

Goal: prove the chart handles the basic release lifecycle, not just first
install.

Required flow:

```text
helm install
-> first PVC writer/reader
-> sw-block ops report
-> helm upgrade with no data loss
-> writer/reader again
-> helm rollback or reinstall-safe fallback
-> helm uninstall
-> cleanup verification
```

Acceptance:

```text
install PASS
upgrade PASS or explicit safe-refusal with reason
rollback/reinstall-safe path PASS
PVC data check survives supported lifecycle step
cleanup_status=ok
no active iSCSI sessions
no sw-block processes
no test-scoped hostPath residue
```

## D3: Multi-Volume Day-1 Gate

Goal: move from "first PVC works" to "small user workload with multiple PVCs is
stable enough to evaluate".

Required flow:

- Create at least three PVCs through the Helm-installed StorageClass.
- Run writer/reader checksum on each PVC.
- Verify `sw-block ops report` and dashboard distinguish all volumes.
- Delete the PVCs and prove generated blockvolume workloads are cleaned up.

Acceptance:

```text
N>=3 PVCs Bound
N writer checksums PASS
N reader checksums PASS
ops report lists N ManagedVolumes with stable volume IDs
delete cleanup removes all generated blockvolume Deployments
cleanup residue clean
```

## D4: Support Bundle And Diagnostics Gate

Goal: make "user reports Kubernetes block is stuck" actionable without SSH log
spelunking.

Required artifact set:

- cluster evidence JSON
- timeline JSONL
- summary text
- Helm release metadata
- Kubernetes nodes / pods / PVC / PV / events
- CSI controller/node logs
- blockmaster logs
- blockvolume logs when volumes exist
- cleanup and iSCSI residue snapshots

Acceptance:

```text
one command or scenario step writes the bundle
bundle explains PASS and blocked first-volume cases
reason codes match report/dashboard
bundle is read-only evidence only
```

## D5: Phase Close And v0.3.x Release Note

Goal: close the phase only when the user-facing claim is exact.

Required:

- Update README / quickstart only for proven lifecycle behavior.
- Add a v0.3.x release note if lifecycle gates pass.
- Write close report with run IDs and evidence paths.
- Carry v0.4 operator items forward without starting implementation.

Acceptance:

```text
D1-D4 gates PASS
docs match gates
release note has non-claims
close report written
finished plan written
```

## Claim Matrix

| Area | Phase 26 Target Claim | Still Not Claimed |
|---|---|---|
| Install | Helm alpha install is repeatable and versioned | production installer |
| Upgrade | one narrow upgrade/rollback smoke is gated | general upgrade safety |
| Volumes | multiple small PVCs work in Day-1 smoke | scale/performance SLO |
| Ops | support bundle/report/dashboard are enough for first diagnosis | full observability platform |
| Cleanup | Helm uninstall plus host cleanup verification is gated | operator-owned lifecycle |
| HA | prior recovery claims remain documented | new recovery semantics |

## Risks

| Risk | Mitigation | Fallback |
|---|---|---|
| Upgrade mutates data path unexpectedly | run checksum before and after lifecycle step | document upgrade unsupported and block claim |
| Multi-volume exposes launcher/provisioner churn | D3 gate requires stable volume IDs and cleanup | keep v0.3 first-volume-only claim |
| Bundle grows without explaining root cause | require summary + reason codes + timeline | trim to required artifacts |
| Chart/image version drift | immutable tags and digest evidence | local/internal image for dev gates only |
| Operator scope creep | keep CRD/operator out of Phase 26 | start Phase 27 only after close |

## Progress

- D1: pending
- D2: pending
- D3: pending
- D4: pending
- D5: pending
