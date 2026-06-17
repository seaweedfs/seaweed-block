# Developer Guide

The engineering wiki exists to reduce rediscovery. It should answer:

```text
Why was this built?
What invariant does it protect?
Where is the code?
How is it tested?
What is still out of scope?
```

## Documentation Layers

| Layer | Purpose | Source |
|---|---|---|
| README / quickstart | first user path and narrow claims | `README.md`, `docs/quickstart-kubernetes.md` |
| Release notes | exact user-visible claims and image boundary | `docs/releases/` |
| Wiki | developer navigation and explanation | `docs/wiki/` |
| Finished plans | phase completion and historical decisions | `internal/docs/finished-plans/` |
| Ref docs | contracts, audits, design notes | `internal/docs/ref/` |
| Protocol docs | invariants and control-model reviews | `internal/docs/protocol/` |
| QA sign-offs | evidence that a gate passed or failed | `internal/docs/qa-assignments/` |
| TestOps | executable scenario definitions | `testops/` |

## Writing Rule

Do not copy a finished plan into the wiki. Instead, write the explanation a new
developer needs and link to the source evidence.

Good wiki content:

- problem context,
- ownership boundary,
- state transition,
- package/function entry point,
- gate name that proves the behavior,
- explicit non-claim.

Bad wiki content:

- phase diary copied line-by-line,
- PM wording without code references,
- claims that are not backed by QA,
- TODO lists without an owner or phase.

## Local Preview

Install MkDocs Material and serve the site:

```bash
pip install mkdocs-material
mkdocs serve
```

Open:

```text
http://127.0.0.1:8000
```

Docker alternative:

```bash
docker run --rm -it -p 8000:8000 -v "$PWD:/docs" squidfunk/mkdocs-material
```

The site is static. It can be served internally with any static file server
after:

```bash
mkdocs build
```

