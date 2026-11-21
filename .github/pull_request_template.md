Feature: <roadmap item>
Machine/INV: <kind> / <INV rows>
Model: <spec + cfg + result, or NONE: reason>
Red-first: <commit + test that failed on the base, or NONE: reason>
Simplest: <the simplest version, and what this PR did not add>

<!--
The spec-trace check reads the lines above.

Machine/INV starts with exactly one kind:
  IMPL <machine>.<transition>  implements an existing transition; no new state, flag or reason.
  FIX <INV-ID>                 the red test is that invariant failing on the base.
  CHANGE <machine>             new or changed states, transitions or invariants; the automaton
                               text and rows land with or before the code.
A fix without an invariant, or a guard term without a transition, is a CHANGE.

NONE needs a reason of at least 10 characters. Simplest is required when the PR
changes non-test Go under core/ or cmd/.
-->
