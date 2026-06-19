# Current Plan: v0.5 Operation Layer Release Close

Status: active.

Working branch: `phase45-engineering-wiki`

Scope note: the recent engineering wiki work is a documentation maintenance
slice, not a product phase. Do not count it as "Phase 45" in the product
roadmap. The next formal product phase should start after the v0.5 operation
layer release is closed.

Previous product phase: Phase 44 is closed in
`internal/docs/finished-plans/phase44_finishedplan_delete_lifecycle_close_gate.md`.

## Product Goal

Close the v0.5 beta-candidate release for the Operation Layer:

```text
install
-> PVC create
-> CSI-created SwBlockVolume CR
-> lifecycle-owner protection finalizer
-> operator-status status/Events
-> delete request
-> hold on unsafe cleanup evidence
-> release on clean cleanup evidence
-> uninstall zero residue
```

The release claim is narrow: bounded `SwBlockVolume` lifecycle ownership and
evidence-driven delete hold/release. It is not automatic cleanup, rebuild,
failback, backup/restore, upgrade execution, or a broad production operator.

## Why This Is The Current Plan

Phases 41-44 already delivered the product capability:

- lifecycle-owner role separation,
- real Kubernetes admission/RBAC boundary,
- finalizer add/release,
- delete-safety status,
- CSI-created CR identity,
- integrated hold/release close gate,
- multi-volume delete isolation,
- report/dashboard/explain surface agreement.

The remaining work is release closure, not a new feature phase.

## Scope Contract

| In | Out |
|---|---|
| release docs / README / quickstart boundary review | new storage features |
| publish or identify matching immutable images | automatic cleanup |
| pinned-image release smoke | returned-replica rebuild |
| verify status/events/finalizer/delete path on shipped artifacts | failback |
| admin merge / PR summary | NVMe ANA parity |
| keep wiki/docs as maintenance commits | GPUDirect/cuFile implementation |

## D1: Documentation Boundary Check

Goal: ensure public docs and release notes say exactly what v0.5 can do.

Acceptance:

```text
[x] README feature table matches v0.5 bounded lifecycle claim
[x] quickstart does not over-claim lifecycle-owner as production operator
[x] docs/releases/v0.5-beta-candidate.md is current
[x] docs/roadmap.md says next product work is rebuild/reintegration after v0.5
[x] wiki links remain internal/developer documentation, not release claims
```

## D2: Artifact / Image Check

Goal: confirm release images match the code that passed Phase 44.

Acceptance:

```text
[ ] matching seaweed-block and seaweed-block-csi images are published or named
[ ] release note records the immutable tags/digests
[ ] README/quickstart use the same public tags if this is a public release
[ ] chart defaults do not point at an incompatible older image
```

Current status: blocked on publish/identification of matching v0.5 images for
code commit `041b084` or a later release commit. The existing public quickstart
tag `sha-dc2972d0059b` is a v0.4/status-foundation image and must not be used
as v0.5 lifecycle-owner evidence.

## D3: Release Smoke

Goal: prove the shipped artifacts, not only local builds, run the user path.

Minimum gate:

```text
[ ] Helm install with pinned images
[ ] first PVC writer/reader passes
[ ] SwBlockVolume CR is created
[ ] lifecycle-owner adds protection finalizer
[ ] operator-status writes Ready/first_volume_verified and Events
[ ] delete request holds when cleanup evidence is missing/unsafe
[ ] clean cleanup evidence releases the protection finalizer
[ ] uninstall cleanup verifier reports zero residue
```

## D4: Merge / Release Close

Goal: merge the release docs and publish the release note.

Acceptance:

```text
[ ] PR/admin-merge summary uses product language, not phase-internal language
[ ] QA evidence is linked
[ ] non-claims are preserved
[ ] docs/wiki maintenance commits are described as documentation support
[ ] next product phase is explicitly named
```

## Next Formal Product Phase

Recommended next formal phase after v0.5 release close:

```text
Returned-replica rebuild / reintegration productization
```

That phase should reuse the v0.5 fact -> judgment -> action -> evidence model:

- observe returned replica,
- keep it frontend/ACK fenced,
- decide catch-up versus rebuild from frontier facts,
- show progress/status/Events,
- admit it back only after terminal evidence,
- prove multi-volume isolation and no false Ready.

NVMe ANA parity, failback, backup/restore, Docker adapter, and GPUDirect/cuFile
remain future trains until this lifecycle model is reused successfully for a
real storage recovery action.
