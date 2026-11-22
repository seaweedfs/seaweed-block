----------------------------- MODULE SealedReopen -----------------------------
\* Same owner/epoch only. A legitimate new-owner takeover is a separate action.
EXTENDS Naturals, TLC
CONSTANT PreserveSeal
VARIABLES gen, state, reopened, prepared, serving
vars == <<gen, state, reopened, prepared, serving>>
Init == /\ gen = 7 /\ state = "detached" /\ reopened = FALSE
        /\ prepared = FALSE /\ serving = FALSE
Reopen == /\ ~reopened /\ reopened' = TRUE
          /\ gen' = IF PreserveSeal THEN gen ELSE gen + 1
          /\ state' = IF PreserveSeal THEN state ELSE "active"
          /\ prepared' = ~PreserveSeal /\ serving' = ~PreserveSeal
Spec == Init /\ [][Reopen]_vars
SealedReopenPreservesProof == gen = 7 /\ state = "detached"
SealedReopenCannotServe == ~prepared /\ ~serving
=============================================================================
