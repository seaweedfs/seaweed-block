# Current Plan: Phase 87 Failback Documentation Alignment

Status: complete.

## Goal

Phase 87 aligns user-facing and engineering docs with the actual returned-replica
failback state after Phases 74-86.

The required wording is:

```text
opt-in/source-gated failback runtime exists
automatic deployed failback is not claimed
frontend publication after failback is not claimed
release image smoke is still required before public release claim
```

## Deliverables

### D1: README Claim Boundary

Updated `README.md`:

```text
Returned-replica failback runtime | Source-gated
Returned-replica rebuild traffic | Planned
Frontend publication after failback | Planned
```

The "What You Can Do Today" section now says the failback runtime can be run
from source through opt-in gates only, and the non-claims section still rejects
automatic deployed failback.

### D2: Engineering Wiki Deep Dive

Added:

```text
docs/wiki/deep-dives/returned-replica-failback.md
```

The page covers:

```text
domain background
product contract
state machine
ownership model
CRD shape
CLI/chart shape
code entry points
phase history
failure classes
implementation checklist
current limits
```

### D3: Wiki Inventory Links

Updated:

```text
docs/wiki/index.md
docs/wiki/topic-inventory.md
```

### D4: Product Roadmap Alignment

Updated:

```text
internal/docs/product-roadmap.md
```

The roadmap now records Phases 74-86 as the opt-in/source-gated failback runtime
path and still defers automatic deployed failback plus frontend publication
after failback.

### D5: Gate

Added:

```text
scripts/run-phase87-failback-docs-alignment-gate.sh
testops/scenarios/failback-docs-alignment-chain.yaml
```

## Verification

```text
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase87-failback-docs-alignment-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-docs-alignment-chain.yaml
```

Terminal evidence:

```text
phase87_failback_docs_alignment_status=ok
readme_names_source_gated_failback=true
readme_names_no_automatic_failback=true
readme_names_release_smoke_requirement=true
wiki_deep_dive_exists=true
wiki_names_terminal_evidence=true
wiki_names_code_entry_points=true
wiki_names_current_limits=true
wiki_index_links_failback=true
topic_inventory_classifies_failback=true
product_roadmap_names_phase86=true
product_roadmap_names_opt_in=true
product_roadmap_defers_automatic=true
failback_runtime_public_claim_aligned=true
automatic_failback_not_claimed=true
frontend_publication_after_failback_not_claimed=true
```

## Next

The next operation-layer step should be either:

```text
Kubernetes-deployed failback smoke with fresh local images
```

or:

```text
start NVMe planning if the team accepts failback as source-gated until release smoke
```
