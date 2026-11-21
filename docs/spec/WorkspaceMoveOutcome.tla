------------------------- MODULE WorkspaceMoveOutcome -------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS BypassUncertainty, ConfuseDependencyEnd
VARIABLES uncertain, landed, cause, callerEnd, reply, uncertainAtReply
vars == <<uncertain, landed, cause, callerEnd, reply, uncertainAtReply>>
Causes == {"request", "forced_preflight", "head", "target", "endpoint",
           "dependency_canceled", "dependency_deadline"}
Init == /\ uncertain = FALSE /\ landed = FALSE /\ cause = "none"
        /\ callerEnd = "live" /\ reply = "none" /\ uncertainAtReply = FALSE
FailedWrite == /\ ~uncertain /\ reply = "none"
               /\ uncertain' = TRUE /\ landed' \in BOOLEAN
               /\ UNCHANGED <<cause, callerEnd, reply, uncertainAtReply>>
AdmissionError == /\ cause = "none" /\ reply = "none"
                  /\ cause' \in Causes
                  /\ UNCHANGED <<uncertain, landed, callerEnd, reply, uncertainAtReply>>
CallerEnds == /\ callerEnd = "live" /\ reply = "none"
              /\ callerEnd' \in {"canceled", "deadline"}
              /\ UNCHANGED <<uncertain, landed, cause, reply, uncertainAtReply>>
DependencyEnd == cause \in {"dependency_canceled", "dependency_deadline"}
Reply == /\ cause # "none" /\ reply = "none"
         /\ reply' = IF callerEnd # "live" THEN callerEnd
                      ELSE IF ConfuseDependencyEnd /\ DependencyEnd
                           THEN IF cause = "dependency_canceled" THEN "canceled" ELSE "deadline"
                      ELSE IF uncertain /\ ~BypassUncertainty THEN "unknown"
                      ELSE "refused"
         /\ uncertainAtReply' = uncertain
         /\ UNCHANGED <<uncertain, landed, cause, callerEnd>>
Next == FailedWrite \/ AdmissionError \/ CallerEnds \/ Reply
Spec == Init /\ [][Next]_vars
NoFalseRefusal == ~(reply = "refused" /\ uncertainAtReply)
CallerEndTyped == callerEnd # "live" /\ reply # "none" => reply = callerEnd
UncertainReplyClass == uncertainAtReply /\ callerEnd = "live" /\ reply # "none" => reply = "unknown"
=============================================================================
