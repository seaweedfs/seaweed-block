------------------------ MODULE ScopedMaintenanceGC ------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS TraceForeign, LocalSweep, ImportGlobal
VARIABLES marks, localPasses, deleted, complete, early
vars == <<marks, localPasses, deleted, complete, early>>
Keys == {"rooted", "local", "foreign"}
Init == /\ marks = 0 /\ localPasses = 0 /\ deleted = {} /\ complete = FALSE /\ early = FALSE
\* A foreign-volume intent is the only root of the target's local key.
Roots == IF TraceForeign THEN {"rooted"} ELSE {}
Start == /\ ~complete /\ complete' = TRUE /\ UNCHANGED <<marks, localPasses, deleted, early>>
Finish == /\ complete /\ localPasses < 2
          /\ LET coverage == IF ImportGlobal THEN 1 ELSE marks
                 scope == IF LocalSweep THEN {"rooted", "local"} ELSE Keys
             IN /\ deleted' = deleted \cup (IF coverage = 1 THEN scope \ Roots ELSE {})
                /\ early' = (early \/ (coverage = 1 /\ localPasses = 0))
                /\ marks' = 1
          /\ localPasses' = localPasses + 1 /\ complete' = FALSE
CrashOrConfigChange == /\ marks' = 0 /\ localPasses' = 0 /\ complete' = FALSE
                       /\ UNCHANGED <<deleted, early>>
Next == Start \/ Finish \/ CrashOrConfigChange
Spec == Init /\ [][Next]_vars
ForeignRootSurvives == "rooted" \notin deleted
OwnCoverageOnly == ~early
ForeignCandidatesUntouched == "foreign" \notin deleted
=============================================================================
