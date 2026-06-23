# Current Plan: Phase 55 Release And Documentation Hardening

Status: complete.

Branch target: `phase54-returned-replica-reintegration-executor`

## Goal

Close the documentation and release-claim gap after Phase 54.

Phase 54 proved the first bounded returned-replica authority executor mutation:

```text
SwBlockReplicaEligibility.status ACK eligibility
```

Phase 55 is intentionally not a new storage feature. It makes the public and
developer-facing documents precise enough that a release reviewer does not
mistake ACK eligibility recording for rebuild, frontend publication, or
failback.

## Scope

In scope:

- README capability and non-claim alignment.
- Release note for the v0.6 beta candidate.
- User-capability page update.
- Wiki/deep-dive update for returned-replica and read-write control-plane
  roadmap.
- Roadmap wording audit after Phase 54 close.
- Release smoke checklist for future published images.

Out of scope:

- No product code changes.
- No new executor mutation.
- No release-image publish.
- No QA re-run unless docs expose a missing gate.

## Deliverables

### D1: Public Docs Alignment

Status: implemented.

Update README and user capability docs so they say:

- returned-replica ACK eligibility executor is a beta-candidate gated
  capability,
- rebuild/failback/frontend publication remain non-claims,
- published quickstart image tags do not validate Phase 54 until matching
  release images are published.

### D2: Release Note

Status: implemented.

Add `docs/releases/v0.6-beta-candidate.md` and link it from release notes.

The note must include:

- narrow claim,
- Phase 54 live close evidence,
- pending image-publish warning,
- release checklist.

### D3: Developer Wiki Alignment

Status: implemented.

Update returned-replica and read-write control-plane wiki pages so future
developers see the current boundary:

```text
ACK eligibility target status is proven
rebuild/catch-up/failback still need their own executor and gates
```

### D4: Verification

Minimum checks:

```text
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
swblock validate testops/scenarios/authority-executor-live-close-chain.yaml
swblock validate testops/scenarios/authority-executor-multivolume-chain.yaml
```

Docs are complete only if all checks pass and the diff does not introduce a
broader product claim than Phase 54 proved.

## Exit

Phase 55 is closed when docs are committed and the next step is clear:

```text
either release smoke on published images,
or start the next executor milestone with the same evidence/action boundary.
```
