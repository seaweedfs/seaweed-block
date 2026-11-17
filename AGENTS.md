# Agent Guidelines

Behavioral guidelines to reduce common LLM coding mistakes. Merge with
project-specific instructions as needed.

Tradeoff: these guidelines bias toward caution over speed. For trivial tasks,
use judgment.

## 1. Think Before Coding

Do not assume. Do not hide confusion. Surface tradeoffs.

Before implementing:

- State assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them; do not pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop, name what is confusing, and ask.

## 2. Simplicity First

Minimum code that solves the problem. Nothing speculative.

- No features beyond what was asked.
- No abstractions for single-use code.
- No flexibility or configurability that was not requested.
- No error handling for impossible scenarios.
- If 200 lines could be 50, rewrite it.

Ask: would a senior engineer say this is overcomplicated? If yes, simplify.

## 3. Surgical Changes

Touch only what you must. Clean up only your own mess.

When editing existing code:

- Do not improve adjacent code, comments, or formatting.
- Do not refactor things that are not broken.
- Match existing style, even if you would do it differently.
- If you notice unrelated dead code, mention it; do not delete it.

When your changes create orphans:

- Remove imports, variables, and functions that your changes made unused.
- Do not remove pre-existing dead code unless asked.

Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

Define success criteria. Loop until verified.

Transform tasks into verifiable goals:

- "Add validation" -> "Write tests for invalid inputs, then make them pass."
- "Fix the bug" -> "Write a test that reproduces it, then make it pass."
- "Refactor X" -> "Ensure tests pass before and after."

For multi-step tasks, state a brief plan:

```text
1. [Step] -> verify: [check]
2. [Step] -> verify: [check]
3. [Step] -> verify: [check]
```

Strong success criteria let agents loop independently. Weak criteria such as
"make it work" require clarification.

These guidelines are working if there are fewer unnecessary changes in diffs,
fewer rewrites due to overcomplication, and clarifying questions come before
implementation rather than after mistakes.

## Protocol contract

Seaweed Block's state machines and invariants are in
[`docs/design/invariants.md`](docs/design/invariants.md). Read it before
changing a protocol path: the storage engines, durability and `Sync`,
authority and epochs, replication and replica roles, the chunk layer and the
cut, the catalog, GC and volume delete, or the workspace lease.

- Find the machine and transition (Part A) and the invariant IDs (Part B)
  your change touches, and name them in the PR.
- Follow the change rules (Part C): a new or changed state or transition
  updates Part A, the design-doc diagram, the ledger and the TLA+ spec that
  covers it; an ACTIVE row never loses its test; a bug found by tests, soak
  or TLA+ becomes a regression test plus a ledger row.
- Do not weaken a guard listed under "Enforced in" without updating its row.
