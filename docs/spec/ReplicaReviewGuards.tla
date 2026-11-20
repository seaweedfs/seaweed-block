---------------------- MODULE ReplicaReviewGuards ----------------------
EXTENDS Integers, TLC
CONSTANTS RetireSettled, WaitForFrames, GuardActive, RestoreAck
VARIABLES current, refs, coverage, active, ready, missing, observedR
vars == <<current, refs, coverage, active, ready, missing, observedR>>
Init == /\ current = 1 /\ refs = [s \in 1..2 |-> 0] /\ coverage = {1}
        /\ active = FALSE /\ ready = FALSE /\ missing = TRUE /\ observedR = 100
Admit(s) == /\ s >= current /\ refs[s] = 0
            /\ current' = s /\ refs' = [refs EXCEPT ![s] = 1]
            /\ coverage' = (coverage \cup {s}) \ (
                IF RetireSettled /\ s # current /\ (refs[current] = 0 \/ ~WaitForFrames)
                THEN {current} ELSE {})
            /\ UNCHANGED <<active,ready,missing,observedR>>
Release(s) == /\ refs[s] = 1 /\ refs' = [refs EXCEPT ![s] = 0]
              /\ coverage' = IF RetireSettled /\ s # current THEN coverage \ {s} ELSE coverage
              /\ UNCHANGED <<current,active,ready,missing,observedR>>
Prepare == /\ ~active /\ active' = TRUE /\ ready' = FALSE
           /\ UNCHANGED <<current,refs,coverage,missing,observedR>>
DelayedProbe == /\ active /\ ready' = ~GuardActive
                /\ UNCHANGED <<current,refs,coverage,active,missing,observedR>>
Finish == /\ active /\ active' = FALSE /\ ready' = TRUE
          /\ UNCHANGED <<current,refs,coverage,missing,observedR>>
Ack == /\ missing /\ missing' = FALSE
       /\ observedR' = IF RestoreAck THEN 150 ELSE 100
       /\ UNCHANGED <<current,refs,coverage,active,ready>>
Next == (\E s \in 1..2 : Admit(s) \/ Release(s)) \/ Prepare \/ DelayedProbe \/ Finish \/ Ack
Spec == Init /\ [][Next]_vars
NoEarlyRetirement == \A s \in 1..2 : refs[s] = 1 => s \in coverage
NoSettledHistory == \A s \in coverage : s = current \/ refs[s] = 1
ReadyRequiresIdle == ready => ~active
AckRestoresProgress == ~missing => observedR = 150
=============================================================================