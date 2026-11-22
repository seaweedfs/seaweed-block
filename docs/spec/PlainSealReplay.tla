---------------------------- MODULE PlainSealReplay ----------------------------
\* One seal invocation, possibly after restart. Only a new filer observation
\* can justify success. No controller pending-record or boot decision is modeled.
EXTENDS Naturals, TLC
CONSTANTS PreferMemory, RequireExact, RequireLayer
Heads == {"valid", "active", "holder", "epoch", "bad-proof", "unreadable", "moved"}
VARIABLES pc, cached, kind, observed, success
vars == <<pc, cached, kind, observed, success>>
Init == /\ pc = 0 /\ cached = TRUE /\ kind \in Heads
        /\ observed = "none" /\ success = FALSE
Restart == /\ pc = 0 /\ pc' = 1 /\ cached' = FALSE
           /\ UNCHANGED <<kind, observed, success>>
Read == /\ pc \in {0, 1} /\ pc' = 2 /\ observed' = kind
        /\ UNCHANGED <<cached, kind, success>>
Reply == /\ pc \in {0, 1, 2}
         /\ (pc = 2 \/ (PreferMemory /\ cached)) /\ pc' = 3
         /\ success' = ((PreferMemory /\ cached) \/
                         (pc = 2 /\ (~RequireExact \/ observed = "valid") /\
                          (~RequireLayer \/ cached)))
         /\ UNCHANGED <<cached, kind, observed>>
Spec == Init /\ [][Restart \/ Read \/ Reply]_vars
SuccessNeedsFreshExactProof == success => observed = "valid"
ValidProofDoesNotNeedProcessMemory == pc = 3 /\ observed = "valid" => success
=============================================================================
