# Current Plan: Phase 45 - Engineering Wiki And Knowledge Base

Status: active.

Branch: `phase45-engineering-wiki`

Previous phase: Phase 44 is closed in
`internal/docs/finished-plans/phase44_finishedplan_delete_lifecycle_close_gate.md`.

## Product / Engineering Goal

Create an internal engineering wiki that explains Seaweed Block development at
developer depth:

```text
background
-> industry pattern
-> internal implementation structure
-> state machines and invariants
-> major code/function entry points
-> QA gates and failure evidence
-> current limits and next work
```

This is not a marketing site and not a replacement for finished plans. It is a
navigation and explanation layer over the existing docs, plans, protocol notes,
and TestOps scenarios.

## Why This Is Next

Seaweed Block has crossed more than forty phases. The hard problems are now
spread across:

```text
docs/
internal/docs/finished-plans/
internal/docs/ref/
internal/docs/protocol/
internal/docs/qa-assignments/
testops/
cmd/
core/
```

New developers need a structured way to answer:

```text
Why does this state exist?
Which code owns it?
Which invariant does it protect?
Which QA gate proves it?
What is explicitly out of scope?
```

Without this, future work like returned-replica rebuild, NVMe ANA parity, Docker
volume integration, and backup/restore will rediscover old decisions or repeat
old control-plane mistakes.

## Scope Contract

| In | Out |
|---|---|
| MkDocs Material static wiki scaffold | custom wiki server |
| developer-facing docs under `docs/wiki/` | moving source evidence |
| code map and state-machine map | exhaustive API reference |
| Phase 1-44 phase map | rewriting every finished plan |
| QA/TestOps map | replacing QA sign-offs |
| local and internal-server preview instructions | public product website |

## D1: Wiki Site Scaffold

Goal: make the wiki runnable as a static Markdown site.

Acceptance:

```text
[x] `mkdocs.yml` exists
[x] wiki pages live under `docs/wiki/`
[x] local preview command is documented
[x] strict MkDocs build passes
```

## D2: Developer Navigation Layer

Goal: give developers stable entry points.

Acceptance:

```text
[x] wiki index explains purpose and source-of-truth hierarchy
[x] developer guide defines what belongs in the wiki
[x] code map names commands, packages, and ownership split
[x] state-machine map names readiness, node, delete-safety, finalizer states
[x] phase map groups Phases 1-44 by product theme
[x] QA/TestOps map explains gate realism and scenario families
```

## D3: Deep-Dive Expansion Plan

Goal: identify the first detailed wiki deep dives to write next.

Acceptance:

```text
[x] domain coverage matrix exists
[x] major domains from the last several months are listed
[x] each domain maps to source evidence and current wiki coverage
[x] priority deep pages are ordered for review
[x] operation-layer v0.5 deep dive
[x] block engine / WAL dirty-failure deep dive
[x] CSI + SwBlockVolume lifecycle deep dive
[x] TestOps scenario authoring deep dive
[ ] returned-replica rebuild readiness deep dive
```

## D4: Publish / Serve Decision

Goal: choose how the team serves the wiki internally.

Recommended default:

```text
MkDocs Material static site
git-backed markdown
served by local `mkdocs serve`, Docker, or a simple internal static server
```

Acceptance:

```text
[ ] local preview works on Windows/WSL
[ ] Docker preview command works or is documented
[ ] internal host target is chosen
[ ] generated `site/` is not committed
```

## Current Progress

- MkDocs scaffold added in `mkdocs.yml`.
- Initial wiki pages added under `docs/wiki/`.
- Strict build validated in a temporary Python environment:
  `mkdocs build --strict --site-dir <temp>`.

## Next Step

Expand the wiki from navigation into detailed engineering chapters:

```text
1. Operation layer v0.5: facts -> judgment -> action -> admission -> evidence.
2. Block engine and WAL failure semantics: why false Ready was possible and how
   the gate forced a real fix.
3. CSI lifecycle: PVC -> CreateVolume -> SwBlockVolume CR -> status/finalizer.
4. TestOps authoring: avoid self-proof, prefer live/replay/adversarial gates.
```
