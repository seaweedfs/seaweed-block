# Phase 87 Finished Plan: Failback Documentation Alignment

Status: complete.

Date: 2026-06-26.

## What Changed

Phase 87 aligns README, wiki, and product roadmap language with the current
failback implementation state.

The source tree now has an opt-in/source-gated returned-replica failback runtime
path, but not an automatic deployed failback release claim.

## Files Updated

```text
README.md
docs/wiki/deep-dives/returned-replica-failback.md
docs/wiki/index.md
docs/wiki/topic-inventory.md
internal/docs/product-roadmap.md
```

## Evidence

The gate proves the docs say:

```text
source-gated failback runtime exists
automatic deployed failback is not claimed
frontend publication after failback is not claimed
release smoke remains required
```

## Verification

```text
scripts/run-phase87-failback-docs-alignment-gate.sh .
swblock validate testops/scenarios/failback-docs-alignment-chain.yaml
```

Result:

```text
phase87_failback_docs_alignment_status=ok
automatic_failback_not_claimed=true
frontend_publication_after_failback_not_claimed=true
```
