# Product Spec Gate Template

Purpose: keep implementation from drifting into test-shaped features that pass
but no longer preserve the product semantics users need.

Use this template before starting any non-trivial plan slice. The spec is
deliberately colder than a roadmap: it defines what must not move during
implementation.

## 1. Product Question

One sentence:

```text
Can <target user/workload> do <specific action> under <specific topology/failure
mode> and know <specific outcome> without reading internal logs?
```

If this cannot be stated sharply, do not start code.

## 2. User-Visible Contract

Required fields:

- command or API the user runs,
- input resources the user creates,
- output resources/status the product owns,
- success behavior,
- failure behavior,
- support-bundle evidence.

Example shape:

```text
Given:
  topology=<...>
  protocol=<...>
  ack_profile=<...>
When:
  <user action or injected failure>
Then:
  <observable behavior>
And:
  <data/status/evidence invariant>
```

## 3. Non-Negotiable Semantics

These are not implementation details. Weakening any of them changes the product
claim and must force a plan edit before code continues.

For block-storage work, always consider:

- mounted I/O path,
- ACK profile,
- durable frontier,
- authority epoch / endpoint version,
- primary eligibility,
- stale-primary fencing,
- returned-replica eligibility,
- attach/reattach method,
- cleanup ownership,
- support-bundle issue wording.

## 4. Allowed Simplifications

List the simplifications that are allowed for the current slice.

Each simplification must say whether it is:

- `test_fixture_only`: allowed only inside a test fixture,
- `alpha_non_claim`: visible in docs and support bundles,
- `temporary_internal`: does not affect user-visible semantics,
- `safe_refusal`: product refuses the operation and explains why.

No silent simplifications.

## 5. Explicit Non-Claims

List what the slice does not prove. These must appear in user docs or bundle
non-claims when they affect operator expectations.

## 6. Evidence Contract

Define the minimum evidence before implementation:

- fast test names,
- runner scenario name,
- QA assignment path,
- required artifact files,
- required status lines,
- exit-code semantics,
- cleanup proof.

The evidence must prove the product contract, not just the helper abstraction.

## 7. Drift Checks

Before accepting a green test, answer:

1. Did the implementation weaken any non-negotiable semantic?
2. Did the test avoid the real user path?
3. Did a helper abstraction become the product claim?
4. Would a cold operator understand the bundle?
5. Is a safe refusal being reported as success?
6. Did we add a non-claim where the product still cannot do the thing?

Any "yes" to 1, 2, 3, or 5 blocks the slice. Any "no" to 4 or 6 blocks close.

## 8. Review Requirement

For every P0/P1 product slice:

- run fast tests before QA handoff,
- use internal review before broad runner validation,
- ask the reviewer specifically to find semantic weakening, not only code bugs,
- tell QA which product contract is being validated and which non-claims remain.

## 9. Close Rule

The slice closes only when:

- the user-visible contract is still true,
- all non-negotiable semantics are preserved or explicitly refused,
- allowed simplifications are documented,
- QA validates against the contract,
- no support bundle requires implementation knowledge to interpret.
