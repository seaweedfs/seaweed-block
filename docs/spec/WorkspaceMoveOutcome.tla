------------------------- MODULE WorkspaceMoveOutcome -------------------------
EXTENDS Naturals, FiniteSets
CONSTANT BypassUncertainty
VARIABLES uncertain, landed, cause, reply, uncertainAtReply
vars == <<uncertain, landed, cause, reply, uncertainAtReply>>
Causes == {"request", "forced_preflight", "head", "target", "endpoint", "canceled", "deadline"}
Init == /\ uncertain = FALSE /\ landed = FALSE /\ cause = "none"
        /\ reply = "none" /\ uncertainAtReply = FALSE
FailedWrite == /\ ~uncertain /\ reply = "none"
               /\ uncertain' = TRUE /\ landed' \in BOOLEAN
               /\ UNCHANGED <<cause, reply, uncertainAtReply>>
AdmissionError == /\ cause = "none" /\ reply = "none"
                  /\ cause' \in Causes
                  /\ UNCHANGED <<uncertain, landed, reply, uncertainAtReply>>
Reply == /\ cause # "none" /\ reply = "none"
         /\ reply' = IF cause \in {"canceled", "deadline"} THEN cause
                      ELSE IF uncertain /\ ~BypassUncertainty THEN "unknown"
                      ELSE "refused"
         /\ uncertainAtReply' = uncertain
         /\ UNCHANGED <<uncertain, landed, cause>>
Next == FailedWrite \/ AdmissionError \/ Reply
Spec == Init /\ [][Next]_vars
NoFalseRefusal == ~(reply = "refused" /\ uncertainAtReply)
CallerEndTyped == cause \in {"canceled", "deadline"} /\ reply # "none" => reply = cause
=============================================================================
