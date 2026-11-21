------------------------ MODULE GCDeleteProgress ------------------------
EXTENDS Integers, FiniteSets
CONSTANT StopOnReject
VARIABLES next, deleted, rejected, complete
vars == <<next, deleted, rejected, complete>>
Bad == 1
Keys == 1..3
Init == /\ next = 1 /\ deleted = {} /\ rejected = {} /\ complete = FALSE
Step == /\ next \in Keys
        /\ IF next = Bad THEN
              /\ rejected' = rejected \cup {next}
              /\ deleted' = deleted
              /\ next' = IF StopOnReject THEN 4 ELSE next+1
              /\ complete' = StopOnReject
            ELSE
              /\ deleted' = deleted \cup {next}
              /\ rejected' = rejected
              /\ next' = next+1
              /\ complete' = next = 3
Spec == Init /\ [][Step]_vars
NoFalseSuccess == Bad \notin deleted
FailureVisible == complete => Bad \in rejected
LaterProgress == complete => deleted = Keys \ {Bad}
=============================================================================
